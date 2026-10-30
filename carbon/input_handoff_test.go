//go:build unix

package carbon

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/tcp"
	"github.com/lomik/zapwriter"
)

func TestLegacySharedOwnerProcess(t *testing.T) {
	dir := os.Getenv("GRAPHITE_TEST_LEGACY_SHARED_OWNER_DIR")
	if dir == "" {
		return
	}
	db, err := store.Open(dir, store.Options{ExactBlocks: true})
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("READY")
	_, _ = io.Copy(io.Discard, os.Stdin)
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
}

func startLegacySharedOwner(t *testing.T, dir string) (func() error, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestLegacySharedOwnerProcess$")
	cmd.Env = append(os.Environ(), "GRAPHITE_TEST_LEGACY_SHARED_OWNER_DIR="+dir)
	cmd.Stderr = os.Stderr
	input, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	output, err := cmd.StdoutPipe()
	if err != nil {
		_ = input.Close()
		return nil, err
	}
	if err = cmd.Start(); err != nil {
		_ = input.Close()
		return nil, err
	}
	line, err := bufio.NewReader(output).ReadString('\n')
	if err != nil || line != "READY\n" {
		_ = input.Close()
		_ = cmd.Wait()
		return nil, fmt.Errorf("legacy shared owner did not open: %q: %w", line, err)
	}
	return func() error { _ = input.Close(); return cmd.Wait() }, nil
}

func TestEarlyHandoffInputFits(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		limit  uint64
		saved  uint64
		tail   []byte
		expect bool
	}{
		{"room for input", 100, 20, make([]byte, 40), true},
		{"tail can fill cache", 100, 80, make([]byte, 40), false},
		{"odd tail rounds up", 100, 80, make([]byte, 39), false},
		{"checkpoint fills cache", 100, 100, nil, false},
		{"checkpoint exceeds cache", 100, 101, nil, false},
		{"unbounded cache uses normal startup", 0, 20, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := New("")
			app.Config = NewConfig()
			app.Config.Cache.MaxSize = tc.limit
			app.Config.Dump.Path = t.TempDir()
			if tc.tail != nil {
				if err := os.WriteFile(filepath.Join(app.Config.Dump.Path, "input.123.tail.bin"), tc.tail, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if got := app.earlyHandoffInputFits(tc.saved); got != tc.expect {
				t.Fatalf("fits = %v, want %v", got, tc.expect)
			}
		})
	}
}

func waitHandoffLatest(url string, target float64) bool {
	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			var reply struct {
				Metrics []struct {
					Values   []float64
					IsAbsent []bool
				}
			}
			err = json.NewDecoder(resp.Body).Decode(&reply)
			_ = resp.Body.Close()
			if err == nil && resp.StatusCode == http.StatusOK {
				for _, metric := range reply.Metrics {
					for i, value := range metric.Values {
						if !metric.IsAbsent[i] && value >= target {
							return true
						}
					}
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

const handoffRecoveryCacheSize = 16 << 20

// handoffTestConfig is a compressed, checkpointing store with plaintext input.
func handoffTestConfig(t *testing.T, root string) *Config {
	t.Helper()
	config := testConfig(t, root)
	if err := os.WriteFile(filepath.Join(root, "schemas.conf"), []byte("[all]\npattern = .*\nretentions = 1s:1h\n"), 0600); err != nil {
		t.Fatal(err)
	}
	app := New(config)
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	cfg := app.Config
	cfg.Whisper.Compressed = true
	// Repeated corrections to an already persisted compressed point need
	// the existing out-of-order policy, independently of restart handoff.
	cfg.Whisper.OutOfOrder = true
	cfg.Tags.Enabled = false
	cfg.Dump.Enabled = true
	// Unix socket paths must fit sockaddr_un, including on macOS where
	// the default testing temporary directory is too long.
	dumpDir, err := os.MkdirTemp("/tmp", "carbon-handoff-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dumpDir) })
	cfg.Dump.Path = dumpDir
	cfg.Dump.RestorePerSecond = 0
	cfg.Udp.Enabled = false
	cfg.Tcp.Enabled = true
	cfg.Tcp.Listen = "127.0.0.1:0"
	cfg.Pickle.Enabled = false
	cfg.Grpc.Enabled = false
	cfg.Carbonlink.Enabled = false
	cfg.Common.MetricInterval = &Duration{time.Hour}
	cfg.Carbonserver.Enabled = true
	cfg.Carbonserver.Listen = "127.0.0.1:0"
	cfg.Carbonserver.TrieIndex = true
	cfg.Carbonserver.ConcurrentIndex = true
	cfg.Carbonserver.RealtimeIndex = 100
	// Repeated full index snapshots are incidental to input handoff. Keep them
	// active without letting snapshot I/O starve the paced TCP writers.
	cfg.Carbonserver.ScanFrequency = &Duration{200 * time.Millisecond}
	cfg.Carbonserver.FileListCache = filepath.Join(t.TempDir(), "flc")
	cfg.Carbonserver.FileListCacheVersion = 2
	cfg.Carbonserver.QueryCacheEnabled = false
	cfg.Carbonserver.FindCacheEnabled = false
	cfg.Carbonserver.GlobCacheEnabled = false
	return cfg
}

func startHandoffTestApp(t *testing.T, app *App) *Config {
	t.Helper()
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	// Keep the first instance's config immutable while giving clients and the
	// successor the ports that its live listeners own.
	cfg := *app.Config
	input := *cfg.Tcp
	input.Listen = app.Receivers[0].Receiver.(*tcp.TCP).Addr().String()
	cfg.Tcp = &input
	cfg.Carbonserver.Listen = app.Carbonserver.HTTPListener().Addr().String()
	return &cfg
}

// handoffSuccessor selects capabilities for the shared successor stub.
func handoffSuccessor(t *testing.T, input bool) string {
	t.Helper()
	capabilities := "read,shared"
	if input {
		capabilities += ",input"
	}
	return filepath.Join(os.Getenv(testHandoffSuccessorDir), capabilities)
}

// TestRestartHandsInputOver restarts a store while clients keep writing on the
// same connections. No write may fail, every point must survive, and points
// that arrived after the checkpoint must not overtake older ones.
func testRestartHandsInputOver(t *testing.T) {
	for _, tc := range []struct {
		name            string
		input           bool
		mirror          bool
		sharedSuccessor bool
		legacyOwner     bool
	}{
		{"input handoff", true, false, false, false},
		{"reads only", false, false, false, false},
		{"mirrored rollback input", true, true, false, false},
		{"mirrored rollback reads only", false, true, false, false},
		{"mirrored shared successor input", true, true, true, false},
		{"mirrored shared successor reads only", false, true, true, false},
		{"legacy shared owner input", true, true, true, true},
		{"legacy shared owner reads only", false, true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if isolatedTestProcess(t) {
				return
			}
			defer zapwriter.Test()()
			root := t.TempDir()
			cfg := handoffTestConfig(t, root)
			if tc.mirror {
				cfg.Whisper.StorageBackend = "pebble-chunk"
				cfg.Whisper.StoreEncoding = "exact"
				cfg.Whisper.StoreMirrorFiles = true
				cfg.Whisper.StoreDir = t.TempDir()
			}
			first := New(filepath.Join(root, "go-carbon.conf"))
			first.Config = cfg
			cfg = startHandoffTestApp(t, first)
			defer first.Stop()
			deadline := time.Now().Add(5 * time.Second)
			for !first.Carbonserver.HasMappedIndex() {
				if time.Now().After(deadline) {
					t.Fatal("first snapshot did not complete", zapwriter.TestString())
				}
				time.Sleep(10 * time.Millisecond)
			}

			next := New(filepath.Join(root, "go-carbon.conf"))
			nextConfig := *cfg
			// This bounded stream never approaches the cache limit. Leave room
			// for the conservative slack bound used by tail replay.
			nextConfig.Cache.MaxSize = handoffRecoveryCacheSize
			if tc.mirror && !tc.sharedSuccessor {
				// Stock rollback opens the continuously updated original files.
				nextConfig.Whisper.StorageBackend = "files"
				nextConfig.Whisper.StoreEncoding = ""
				nextConfig.Whisper.StoreMirrorFiles = false
			}
			if tc.sharedSuccessor {
				// Reopening a mirrored store must retain the normal full
				// reconciliation gate before publishing shared reads.
				nextConfig.Whisper.StoreLiveMigration = true
				// The checkpoint may be served before the store opens; the
				// metadata prewarm must still start once it has.
				nextConfig.Whisper.StoreMetadataCacheSize = 16 << 20
				nextConfig.Whisper.StoreMetadataCachePrewarm = true
			}
			next.Config = &nextConfig
			nextStarted := make(chan error, 1)
			var closeLegacyOwner func() error
			first.SetReadHandoff(handoffSuccessor(t, tc.input), func() {
				if tc.legacyOwner {
					// Model the prior binary: the database lock stays held until
					// socket handoff returns. Use a real second Pebble owner,
					// rather than faking an open failure in the successor.
					if err := first.closeMetricStore(); err != nil {
						nextStarted <- err
						return
					}
					var err error
					closeLegacyOwner, err = startLegacySharedOwner(t, cfg.Whisper.StoreDir)
					if err != nil {
						nextStarted <- err
						return
					}
				}
				// The service manager starts the next instance now.
				go func() {
					next.RegisterReadHandoff()
					nextStarted <- next.Start()
				}()
			})
			defer next.Stop()

			// Each client owns one metric with a point per second, and they all
			// overwrite one shared timestamp of a "latest" metric with their
			// sequence number: the last write must win.
			const clients, perClient = 6, 1500
			base := time.Now().Unix() - perClient - 10
			var wg sync.WaitGroup
			var failed sync.Map
			var lastMu sync.Mutex
			last := map[int]int{}
			finishWriting := make(chan struct{})
			finishClients := sync.OnceFunc(func() { close(finishWriting) })
			t.Cleanup(finishClients)
			for c := range clients {
				conn, err := net.Dial("tcp", cfg.Tcp.Listen)
				if err != nil {
					t.Fatal(err)
				}
				wg.Go(func() {
					defer conn.Close()
					for seq := range perClient {
						// A slow stop must leave enough future writes for the
						// freshness check while the database is still locked.
						if seq == perClient-32 {
							<-finishWriting
						}
						line := fmt.Sprintf("client%d.metric %d %d\nlatest.c%d %d %d\n", c, seq, base+int64(seq), c, seq, base)
						if _, err := conn.Write([]byte(line)); err != nil {
							failed.Store(c, err)
							return
						}
						lastMu.Lock()
						last[c] = seq
						lastMu.Unlock()
						time.Sleep(2 * time.Millisecond)
					}
				})
			}
			deadline = time.Now().Add(5 * time.Second)
			created := 0.0
			for {
				// Realtime names can precede their Whisper headers. A restart
				// must begin after initial creation, while updates still flow.
				first.Persister.Stat(func(name string, value float64) {
					if name == "created" {
						created += value
					}
				})
				indexed := true
				for c := range clients {
					indexed = indexed && first.Carbonserver.MetricExists(fmt.Sprintf("client%d.metric", c)) &&
						first.Carbonserver.MetricExists(fmt.Sprintf("latest.c%d", c))
				}
				if indexed && created >= 2*clients {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("initial input metrics were not created and indexed", zapwriter.TestString())
				}
				time.Sleep(10 * time.Millisecond)
			}
			if tc.legacyOwner && tc.input {
				// Pin one checkpoint-only metric: it must be discoverable and
				// readable before the successor can start its persister.
				first.Persister.Stop()
				first.Cache.Add(points.OnePoint("pending_cache_only.value", 42, base))
			}
			stopped := make(chan error, 1)
			go func() {
				err := first.DumpStop()
				lastMu.Lock()
				target := float64(last[0] + 10)
				lastMu.Unlock()
				finishClients()
				if closeLegacyOwner != nil {
					if tc.input {
						// Open is still blocked by the real external database lock.
						// New input must already keep the successor's reads fresh.
						url := fmt.Sprintf("http://%s/render/?target=latest.c0&from=%d&until=%d&format=json", cfg.Carbonserver.Listen, base-1, base+1)
						if !waitHandoffLatest(url, target) && err == nil {
							err = fmt.Errorf("inherited input did not keep reads fresh while database remained locked")
						}
						url = fmt.Sprintf("http://%s/render/?target=pending_cache_only.value&from=%d&until=%d&format=json", cfg.Carbonserver.Listen, base-1, base+1)
						if !waitHandoffLatest(url, 42) && err == nil {
							err = fmt.Errorf("cache-only metric could not read policy before storage opened")
						}
					}
					if closeErr := closeLegacyOwner(); err == nil {
						err = closeErr
					}
				}
				stopped <- err
			}()
			select {
			case err := <-nextStarted:
				if err != nil {
					t.Fatal("next start", err, zapwriter.TestString())
				}
			case <-time.After(20 * time.Second):
				t.Fatal("next did not start", zapwriter.TestString())
			}
			if err := <-stopped; err != nil {
				t.Fatal("dump stop", err)
			}
			if tc.sharedSuccessor {
				resp, err := http.Get("http://" + cfg.Carbonserver.Listen + "/admin/info?scopes=storage")
				if err != nil {
					t.Fatal(err)
				}
				var info struct {
					Storage struct{ Engine map[string]interface{} }
				}
				err = json.NewDecoder(resp.Body).Decode(&info)
				_ = resp.Body.Close()
				if err != nil || len(info.Storage.Engine) == 0 {
					t.Fatalf("handoff lost live storage statistics: %+v, %v", info, err)
				}
				done := next.metadataPrewarmDone.Load()
				if done == nil {
					t.Fatal("no metadata prewarm started", zapwriter.TestString())
				}
				select {
				case <-*done:
				case <-time.After(10 * time.Second):
					t.Fatal("metadata prewarm did not finish")
				}
				if logs := zapwriter.TestString(); !logHas(logs, "source", `"pending checkpoint"`) {
					t.Fatalf("prewarm log:\n%s", logs)
				}
			}
			if tc.mirror && first.MetricStore != nil {
				t.Fatal("mirrored predecessor retained its shared handle after reads stopped")
			}
			wg.Wait()

			logs := zapwriter.TestString()
			if tc.legacyOwner && !strings.Contains(logs, "taking mirrored sockets before reopening predecessor storage") {
				t.Fatal("legacy owner did not exercise socket takeover before opening storage")
			}
			if tc.input {
				failed.Range(func(c, err any) bool { t.Fatalf("client %v write failed: %v", c, err); return false })
				for _, want := range []string{"input handed over", "took over input from stopping instance", "input tail loaded"} {
					if !strings.Contains(logs, want) {
						t.Fatalf("missing %q: %s", want, logs)
					}
				}
			} else if strings.Contains(logs, "input handed over") {
				t.Fatal("input handed to a successor without support")
			}

			if !tc.input {
				// Senders see input closed and lose what was in flight.
				for c := range clients {
					if _, ok := failed.Load(c); !ok {
						t.Fatalf("client %d never saw input close", c)
					}
				}
				return
			}
			read := func(target string, from, until int64) map[int64]float64 {
				url := fmt.Sprintf("http://%s/render/?target=%s&from=%d&until=%d&format=json", cfg.Carbonserver.Listen, target, from, until)
				resp, err := http.Get(url)
				if err != nil {
					t.Fatal(err)
				}
				defer resp.Body.Close()
				raw, err := io.ReadAll(resp.Body)
				if err != nil {
					t.Fatal(err)
				}
				if resp.StatusCode != http.StatusOK {
					t.Fatalf("read status %d: %s\n%s", resp.StatusCode, raw, zapwriter.TestString())
				}
				var body struct {
					Metrics []struct {
						Name      string
						StartTime int64
						StepTime  int64
						Values    []float64
						IsAbsent  []bool
					}
				}
				if err := json.Unmarshal(raw, &body); err != nil {
					t.Fatal(err, string(raw))
				}
				out := map[int64]float64{}
				for _, m := range body.Metrics {
					for i, v := range m.Values {
						if !m.IsAbsent[i] {
							out[m.StartTime+int64(i)*m.StepTime] = v
						}
					}
				}
				return out
			}
			deadline = time.Now().Add(20 * time.Second)
			for c := range clients {
				sent := last[c]
				for {
					got := read(fmt.Sprintf("client%d.metric", c), base-1, base+perClient)
					missing := 0
					for seq := 0; seq <= sent; seq++ {
						if v, ok := got[base+int64(seq)]; !ok || v != float64(seq) {
							missing++
						}
					}
					latest := read(fmt.Sprintf("latest.c%d", c), base-1, base+1)[base]
					if missing == 0 && latest == float64(sent) {
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("client %d: %d of %d points missing, latest %v want %d", c, missing, sent+1, latest, sent)
					}
					time.Sleep(50 * time.Millisecond)
				}
			}
		})
	}
}
