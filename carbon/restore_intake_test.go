package carbon

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/tcp"
	"github.com/lomik/zapwriter"
)

// A real legacy dump, produced by DumpStop, with uncreated restored names.
// Disable the saved read index on restart to exercise ordered legacy fallback.
func legacyIntakeFixture(t *testing.T, metrics, perMetric int) (string, *Config, int64) {
	t.Helper()
	path, cfg := pendingCheckpointConfig(t)
	cfg.Common.MaxCPU = 4
	cfg.Whisper.StorageBackend, cfg.Whisper.StoreDir, cfg.Whisper.StoreEncoding = "pebble-chunk", t.TempDir(), "exact"
	cfg.Whisper.StoreCacheSize, cfg.Whisper.StoreMemTableSize = 1<<20, 1<<20
	cfg.Whisper.Workers = 4
	cfg.Dump.Format = "legacy"
	cfg.InputLog.Enabled, cfg.InputLog.Path = true, t.TempDir()
	cfg.InputLog.SegmentSize = 16 << 20
	cfg.Cache.MaxSize = 0
	cfg.Buckyd.Enabled = false
	first := New(path)
	first.Config = cfg
	if err := first.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(first.Stop)
	stamp := time.Now().Unix() - 1500
	if raw := os.Getenv("RESTORE_REPRO_STAMP"); raw != "" {
		if _, err := fmt.Sscan(raw, &stamp); err != nil {
			t.Fatal(err)
		}
	}
	first.Cache.Add(points.OnePoint("anchor", 1, stamp))
	deadline := time.Now().Add(10 * time.Second)
	for !first.Cache.IsEmpty() || !first.Carbonserver.HasMappedIndex() {
		if time.Now().After(deadline) {
			t.Fatal("anchor did not persist")
		}
		time.Sleep(time.Millisecond)
	}
	first.Persister.Stop()
	first.Persister = nil
	for i := range metrics {
		data := make([]points.Point, perMetric)
		for j := range data {
			data[j] = points.Point{Timestamp: stamp + int64(j), Value: float64(i*perMetric+j) + .125}
		}
		first.Cache.Add(&points.Points{Metric: fmt.Sprintf("saved.metric%05d", i), Data: data})
	}
	// The later input dump corrects one point in the cache dump.
	first.FlushTraces = func() { first.Cache.Add(points.OnePoint("saved.metric00000", 777.125, stamp+1)) }
	if err := first.DumpStop(); err != nil {
		t.Fatal(err)
	}
	first.FlushTraces = nil
	first.ReleaseForHandoff()
	first.Stop()
	manifestPath := filepath.Join(cfg.Dump.Path, recovery.ManifestName)
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal("missing checkpoint", err)
	}
	// Exercise the actual large-manifest decoder and full validation without
	// allocating a >100 GiB dump merely to produce 1 MiB of chunk descriptors.
	raw = append([]byte(strings.Repeat(" ", (1<<20)+1)), raw...)
	if err := os.WriteFile(manifestPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	cfg.Carbonserver.FileListCache = ""
	cfg.Tcp.Enabled, cfg.Tcp.Listen = true, "127.0.0.1:0"
	return path, cfg, stamp
}

func TestLegacyRestoreIntakeLifecycle(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	path, cfg, stamp := legacyIntakeFixture(t, 64, 10)
	cfg.Whisper.MaxUpdatesPerSecond = 1
	app := New(path)
	app.Config = cfg
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(app.Stop)
	if app.Cache.RestoreOutstanding() == 0 || app.Cache.IsEmpty() || app.Collector == nil {
		t.Fatal("startup still waited for global drain")
	}
	if !app.Carbonserver.MetricExists("saved.metric00063") {
		t.Fatal("uncreated restored name not discoverable at intake")
	}
	client := &http.Client{Timeout: time.Second}
	read := func(name string) int {
		t.Helper()
		url := fmt.Sprintf("http://%s/render/?target=%s&from=%d&until=%d&format=json", cfg.Carbonserver.Listen, name, stamp, stamp+10)
		resp, err := client.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}
	if status := read("anchor"); status != http.StatusOK {
		t.Fatal("ready metric unavailable during drain", status)
	}
	for i := range 64 {
		name := fmt.Sprintf("saved.metric%05d", i)
		if app.Cache.RestorePending(name) {
			if status := read(name); status != http.StatusServiceUnavailable {
				t.Fatal("incomplete restored metric escaped read gate", status)
			}
			break
		}
	}
	conn, err := net.Dial("tcp", app.Receivers[0].Receiver.(*tcp.TCP).Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := fmt.Fprintf(conn, "saved.metric00063 999.125 %d\nlive.new 5 %d\n", stamp+1, stamp+2); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for !app.Cache.Has("live.new") {
		if time.Now().After(deadline) {
			t.Fatal("live input not admitted during drain")
		}
		time.Sleep(time.Millisecond)
	}
	started := time.Now()
	app.Stop()
	if time.Since(started) > 3*time.Second {
		t.Fatal("stop waited for a finite prefix with the persister stopping")
	}
}

// Run this unchanged on the base and candidate to measure a real restart.
func TestMeasureLegacyRestoreInputDowntime(t *testing.T) {
	if os.Getenv("RESTORE_INTAKE_REPRO") != "1" {
		t.Skip("set RESTORE_INTAKE_REPRO=1")
	}
	defer zapwriter.Test()()
	const metrics, perMetric = 3000, 1000
	path, cfg, stamp := legacyIntakeFixture(t, metrics, perMetric)
	cfg.Whisper.MaxUpdatesPerSecond = 200
	app := New(path)
	app.Config = cfg
	started := time.Now()
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	defer app.Stop()
	conn, err := net.Dial("tcp", app.Receivers[0].Receiver.(*tcp.TCP).Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	out := bufio.NewWriter(conn)
	fmt.Fprintf(out, "saved.metric00000 999.125 %d\nlive.new 5 %d\n", stamp+1, stamp+2)
	if err := out.Flush(); err != nil {
		t.Fatal(err)
	}
	conn.Close()
	inputDowntime := time.Since(started)
	queuedAtOpen := app.Cache.Size()
	deadline := time.Now().Add(90 * time.Second)
	for !app.Cache.IsEmpty() {
		if time.Now().After(deadline) {
			t.Fatal("restore did not converge", app.Cache.Size())
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Observe the TCP rows too: the first empty observation could precede the
	// receiver processing the final write.
	for {
		if _, err := app.MetricStore.Metadata(context.Background(), "live.new"); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("live input never persisted")
		}
		time.Sleep(10 * time.Millisecond)
	}
	app.Persister.Stop()
	live, err := app.MetricStore.Snapshot(context.Background(), "live.new")
	if err != nil || len(live.Archives) != 1 || len(live.Archives[0].Points) != 1 || live.Archives[0].Points[0].Value != 5 || live.Archives[0].Points[0].Timestamp != stamp+2 {
		t.Fatalf("live input changed: %+v %v", live, err)
	}
	hash := sha256.New()
	var raw [16]byte
	var count int
	for i := range metrics {
		snapshot, err := app.MetricStore.Snapshot(context.Background(), fmt.Sprintf("saved.metric%05d", i))
		if err != nil {
			t.Fatal(err)
		}
		for _, archive := range snapshot.Archives {
			for _, p := range archive.Points {
				if i == 0 && p.Timestamp == stamp+1 && p.Value != 999.125 {
					t.Fatal("live correction lost its source precedence", p)
				}
				binary.LittleEndian.PutUint64(raw[:8], uint64(p.Timestamp))
				binary.LittleEndian.PutUint64(raw[8:], math.Float64bits(p.Value))
				hash.Write(raw[:])
				count++
			}
		}
	}
	if count != metrics*perMetric {
		t.Fatal("restored point count changed", count)
	}
	t.Logf("points=%d manifest_bytes>1MiB input_downtime=%v queued_at_input=%d total_restart=%v stored_points=%d sha256=%x", metrics*perMetric, inputDowntime, queuedAtOpen, time.Since(started), count, hash.Sum(nil))
}
