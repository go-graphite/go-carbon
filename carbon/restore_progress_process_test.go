//go:build unix

package carbon

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/go-graphite/go-carbon/points"
)

// Opt-in full executable reproduction: build the daemon, then set
// RESTORE_PROGRESS_BINARY=/absolute/path/to/go-carbon. The predecessor writes
// an actual legacy dump; the successor rejects an invalid checkpoint and must
// report progress on its diagnostic HTTP server while live input stays closed.
func TestRestoreProgressLegacyProcess(t *testing.T) {
	binary := os.Getenv("RESTORE_PROGRESS_BINARY")
	if binary == "" {
		t.Skip("set RESTORE_PROGRESS_BINARY to a built go-carbon executable")
	}
	if isolatedTestProcessSerial(t) {
		return
	}
	path, cfg := incrementalAppFixture(t)
	short, err := os.MkdirTemp("", "rg-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(short)
	cfg.Dump.Enabled, cfg.Dump.Path, cfg.Dump.Format = true, short, "legacy"
	cfg.Cache.MaxSize = 4_000_000
	cfg.Common.MetricInterval = &Duration{time.Hour}
	cfg.Common.User = ""
	cfg.Whisper.OutOfOrder = true
	if err := os.WriteFile(cfg.Whisper.SchemasFilename, []byte("[all]\npattern = .*\nretentions = 1s:3h\n"), 0600); err != nil {
		t.Fatal(err)
	}
	writeSharedConfig(t, path, cfg)
	old := startIncrementalApp(t, path)
	if !old.Carbonserver.HasMappedIndex() {
		old.Stop()
		old = startIncrementalApp(t, path)
	}
	old.Lock()
	old.Persister.Stop()
	old.Persister = nil
	old.Unlock()
	const metrics, perMetric = 256, 8192
	stamp := time.Now().Unix() - perMetric - 1
	for metric := range metrics {
		batch := &points.Points{Metric: fmt.Sprintf("restore.process.%d", metric), Data: make([]points.Point, perMetric)}
		for i := range batch.Data {
			batch.Data[i] = points.Point{Timestamp: stamp + int64(i), Value: float64(i) + .125}
		}
		old.Cache.Add(batch)
	}
	if err := old.DumpStop(); err != nil {
		t.Fatal(err)
	}
	old.Stop() // DumpStop leaves final process teardown to its caller.
	manifestFile := filepath.Join(short, recovery.ManifestName)
	raw, err := os.ReadFile(manifestFile)
	if err != nil {
		t.Fatal(err)
	}
	var manifest recovery.Manifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	manifest.Root += "-moved"
	raw, err = json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	raw = append([]byte(strings.Repeat(" ", (1<<20)+1)), raw...)
	if err := os.WriteFile(manifestFile, raw, 0600); err != nil {
		t.Fatal(err)
	}
	reserve := func() string {
		t.Helper()
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		return listener.Addr().String()
	}
	cfg.Tcp.Enabled, cfg.Tcp.Listen = true, reserve()
	cfg.Carbonserver.Listen = reserve()
	cfg.Prometheus.Enabled = true
	cfg.Pprof.Listen = "127.0.0.1:7007"
	cfg.Prometheus.Endpoint = "/metrics"
	cfg.Whisper.Workers = 1
	writeSharedConfig(t, path, cfg)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, "-config", path)
	logFile, err := os.Create(filepath.Join(short, "successor.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	waited := false
	defer func() {
		if !waited {
			cancel()
			<-exited
		}
	}()
	client := &http.Client{Timeout: time.Second}
	var sawLoad, sawDrain, sawRate bool
	var final string
	value := func(body, name string) float64 {
		for line := range strings.SplitSeq(body, "\n") {
			if rest, ok := strings.CutPrefix(line, name+" "); ok {
				v, _ := strconv.ParseFloat(rest, 64)
				return v
			}
		}
		return 0
	}
	for {
		select {
		case err := <-exited:
			waited = true
			logs, _ := os.ReadFile(logFile.Name())
			t.Fatalf("successor exited before readiness: %v\n%s", err, logs)
		case <-ctx.Done():
			logs, _ := os.ReadFile(logFile.Name())
			t.Fatal("startup timed out", string(logs))
		default:
		}
		response, err := client.Get("http://127.0.0.1:7007/metrics")
		if err != nil {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		text := string(body)
		loading := value(text, `restore_blocked{reason="load"}`) == 1
		draining := value(text, `restore_blocked{reason="drain"}`) == 1
		if loading || draining {
			for _, address := range []string{cfg.Tcp.Listen, cfg.Carbonserver.Listen} {
				conn, err := net.DialTimeout("tcp", address, 10*time.Millisecond)
				if err == nil {
					conn.Close()
					// A scrape can straddle completion. Only fail while a
					// second scrape still reports the receiver gate blocked.
					check, err := client.Get("http://127.0.0.1:7007/metrics")
					if err != nil {
						t.Fatal(err)
					}
					latest, _ := io.ReadAll(check.Body)
					check.Body.Close()
					if value(string(latest), `restore_blocked{reason="load"}`) == 1 || value(string(latest), `restore_blocked{reason="drain"}`) == 1 {
						t.Fatal("listener opened during restore", address)
					}
				}
			}
		}
		sawLoad = sawLoad || loading && value(text, "restore_loaded_points") > 0
		sawDrain = sawDrain || draining && value(text, "restore_backlog_points") > 0
		sawRate = sawRate || draining && value(text, "restore_persisted_points_per_second") > 0
		if !loading && !draining && value(text, "restore_loaded_points") == metrics*perMetric {
			final = text
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !sawLoad || !sawDrain || !sawRate {
		t.Fatalf("progress phases: load=%v drain=%v positive rate during drain=%v", sawLoad, sawDrain, sawRate)
	}
	if value(final, "restore_backlog_points") != 0 {
		t.Fatal("backlog survived the receiver gate")
	}
	for _, address := range []string{cfg.Tcp.Listen, cfg.Carbonserver.Listen} {
		deadline := time.Now().Add(5 * time.Second)
		for {
			conn, err := net.DialTimeout("tcp", address, time.Second)
			if err == nil {
				conn.Close()
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("listener did not open after drain", address, err)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	for line := range strings.SplitSeq(final, "\n") {
		if strings.HasPrefix(line, "restore_") {
			t.Log(line)
		}
	}
	t.Logf("legacy points=%d manifest_bytes=%d; observed loading, draining and positive successful-write rate with input closed", metrics*perMetric, len(raw))
	// Stop before opening the same store for an independent persisted-data check.
	_ = cmd.Process.Signal(syscall.SIGUSR2)
	err = <-exited
	waited = true
	if err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(cfg.Whisper.StoreDir, store.Options{ExactBlocks: true, Continuation: &whisperio.ContinuationCodec{}, IncrementalSource: cfg.Whisper.DataDir})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for metric := range metrics {
		snapshot, err := db.Snapshot(context.Background(), fmt.Sprintf("restore.process.%d", metric))
		if err != nil || len(snapshot.Archives) != 1 || len(snapshot.Archives[0].Points) != perMetric {
			t.Fatalf("persisted metric=%d err=%v", metric, err)
		}
		for i, point := range snapshot.Archives[0].Points {
			if point.Timestamp != stamp+int64(i) || point.Value != float64(i)+.125 {
				t.Fatalf("persisted point metric=%d index=%d point=%+v", metric, i, point)
			}
		}
	}
}
