package carbon

import (
	"context"
	"fmt"
	"math"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
	whisper "github.com/go-graphite/go-whisper"
)

func waitInputReplay(t *testing.T, test func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !test() {
		if time.Now().After(deadline) {
			t.Fatal("input replay condition timed out")
		}
		time.Sleep(time.Millisecond)
	}
}

func crashInputFixture(t *testing.T, root string, backend string, ooo bool) (string, int64) {
	t.Helper()
	config := inputLogTestConfig(t, root, nil)
	app := startApp(t, config)
	stamp := time.Now().Unix() - 100
	for cycle := range 4 {
		for i := range 8 {
			app.Cache.Add(points.OnePoint("ordered", float64(cycle*8+i)+0.25, stamp+int64(cycle*8+i)))
		}
		if err := app.inputLog.Load().Sync(); err != nil {
			t.Fatal(err)
		}
	}
	app.inputLog.Swap(nil).Kill()
	app.Stop()
	config = inputLogTestConfig(t, root, func(c *Config) {
		c.InputLog.BackgroundReplay = true
		c.Dump.Enabled = true // empty dump directory is the normal crash path
		c.Whisper.Enabled, c.Whisper.Compressed, c.Whisper.OutOfOrder = true, true, ooo
		c.Whisper.StorageBackend = backend
		c.Whisper.StoreDir = filepath.Join(root, "store")
		c.Whisper.StoreCacheSize, c.Whisper.StoreMemTableSize = 1<<20, 1<<20
		c.Cache.MaxSize = 128 // history window is smaller than the journal
		c.Cache.WriteoutMinPoints, c.Cache.WriteoutMaxDelay = 1000, Duration{30 * time.Minute}
		c.Tcp.Enabled, c.Tcp.Listen = true, "127.0.0.1:0"
	})
	if err := os.WriteFile(filepath.Join(root, "schemas.conf"), []byte("[all]\npattern = .*\nretentions = 1s:2h\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return config, stamp
}

func sendDuringReplay(t *testing.T, app *App, stamp int64) {
	t.Helper()
	conn, err := net.Dial("tcp", app.Receivers[0].Receiver.(interface{ Addr() net.Addr }).Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// A correction to the newest historical timestamp, followed by new data.
	// Non-OOO file Whisper cannot overwrite an already persisted timestamp;
	// that case checks strictly newer live input. Chunkstore checks corrections
	// with OOO both disabled and enabled.
	if app.Config.Whisper.OutOfOrder || app.Config.Whisper.StorageBackend == "pebble-chunk" {
		if _, err := fmt.Fprintf(conn, "ordered 900.25 %d\n", stamp+31); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := fmt.Fprintf(conn, "ordered 901.25 %d\n", stamp+32); err != nil {
		t.Fatal(err)
	}
	waitInputReplay(t, func() bool {
		got := app.Cache.Get("ordered")
		return len(got) > 0 && got[len(got)-1].Timestamp == stamp+32
	})
	if err := app.inputLog.Load().Sync(); err != nil {
		t.Fatal(err)
	}
}

func checkCrashInputDisk(t *testing.T, app *App, root string, stamp int64) {
	t.Helper()
	if err := app.Cache.SetWriteoutBatching(0, 0); err != nil {
		t.Fatal(err)
	}
	waitInputReplay(t, app.Cache.IsEmpty)
	var from, step int
	var values []float64
	if app.MetricStore != nil {
		series, err := app.MetricStore.Fetch(context.Background(), "ordered", int(stamp-1), int(stamp+33))
		if err != nil || series == nil {
			t.Fatalf("fetch: %v / %v", series, err)
		}
		from, step, values = series.FromTime, series.Step, series.Values
	} else {
		w, err := whisper.OpenWithOptions(filepath.Join(root, "ordered.wsp"), &whisper.Options{OutOfOrder: app.Config.Whisper.OutOfOrder})
		if err != nil {
			t.Fatal(err)
		}
		defer w.Close()
		series, err := w.Fetch(int(stamp-1), int(stamp+33))
		if err != nil || series == nil {
			t.Fatalf("fetch: %v / %v", series, err)
		}
		from, step, values = series.FromTime(), series.Step(), series.Values()
	}
	for i := range 33 {
		want := float64(i) + 0.25
		if i == 32 || (i == 31 && (app.Config.Whisper.OutOfOrder || app.Config.Whisper.StorageBackend == "pebble-chunk")) {
			want = float64(900+i-31) + 0.25
		}
		offset := (int(stamp) + i - from) / step
		if offset < 0 || offset >= len(values) || math.Float64bits(values[offset]) != math.Float64bits(want) {
			t.Fatalf("point %d: want %v, series from=%d step=%d values=%v", i, want, from, step, values)
		}
	}
}

func TestInputLogBackgroundReceivesAndOrders(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	for _, backend := range []string{"files", "pebble-chunk"} {
		for _, ooo := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/ooo=%v", backend, ooo), func(t *testing.T) {
				root := t.TempDir()
				config, stamp := crashInputFixture(t, root, backend, ooo)
				app := New(config)
				if err := app.ParseConfig(); err != nil {
					t.Fatal(err)
				}
				gate := make(chan struct{})
				app.inputReplayBeforeRead = func() {
					select {
					case <-gate:
					case <-app.inputReplay.stop:
					}
				}
				if err := app.Start(); err != nil {
					t.Fatal(err)
				}
				defer app.Stop()
				if app.inputReplay == nil || !app.inputReplayActive.Load() {
					t.Fatal("background replay not active")
				}
				sendDuringReplay(t, app, stamp)
				if app.inputLog.Load().Metrics().ReplayedPoints != 0 {
					t.Fatal("live points were gated behind replay")
				}
				close(gate)
				waitInputReplay(t, func() bool { return !app.inputReplayActive.Load() })
				checkCrashInputDisk(t, app, root, stamp)
			})
		}
	}
}

func TestInputLogBackgroundDoubleCrash(t *testing.T) {
	if config := os.Getenv("CARBON_REPLAY_CRASH_CONFIG"); config != "" {
		app := New(config)
		if err := app.ParseConfig(); err != nil {
			t.Fatal(err)
		}
		entered := make(chan struct{})
		app.inputReplayBeforeRead = func() {
			r := app.inputReplay
			read := r.read
			var once sync.Once
			r.read = func(apply func(*points.Points), stop <-chan struct{}) (inputlog.ReplayStats, error) {
				return read(func(p *points.Points) {
					apply(p)
					once.Do(func() { close(entered); <-stop })
				}, stop)
			}
		}
		if err := app.Start(); err != nil {
			t.Fatal(err)
		}
		select {
		case <-entered:
		case <-time.After(30 * time.Second):
			t.Fatal("background reader did not start")
		}
		var stamp int64
		if _, err := fmt.Sscan(os.Getenv("CARBON_REPLAY_CRASH_STAMP"), &stamp); err != nil {
			t.Fatal(err)
		}
		sendDuringReplay(t, app, stamp)
		if err := os.WriteFile(os.Getenv("CARBON_REPLAY_CRASH_READY"), []byte("live input synced while replay is active"), 0600); err != nil {
			t.Fatal(err)
		}
		select {} // parent sends SIGKILL; no cleanup or graceful stop
	}
	if isolatedTestProcess(t) {
		return
	}
	root := t.TempDir()
	config, stamp := crashInputFixture(t, root, "pebble-chunk", false)
	ready := filepath.Join(root, "child-ready")
	cmd := exec.Command(os.Args[0], "-test.run=^TestInputLogBackgroundDoubleCrash$", "-test.parallel=1")
	cmd.Env = append(os.Environ(), "CARBON_REPLAY_CRASH_CONFIG="+config, "CARBON_REPLAY_CRASH_READY="+ready, fmt.Sprintf("CARBON_REPLAY_CRASH_STAMP=%d", stamp))
	log, err := os.Create(filepath.Join(root, "child.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	waitInputReplay(t, func() bool { _, err := os.Stat(ready); return err == nil })
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("child did not crash")
	}
	app := startApp(t, config)
	defer app.Stop()
	waitInputReplay(t, func() bool { return !app.inputReplayActive.Load() })
	checkCrashInputDisk(t, app, root, stamp)
}

// Failure and cancellation keep both journal generations recoverable. Neither
// may release live writeout past history that has not reached disk.
func TestInputLogBackgroundFailureAndCancelRetainHistory(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint("failure=", fail), func(t *testing.T) {
			root := t.TempDir()
			config, stamp := crashInputFixture(t, root, "files", false)
			app := New(config)
			if err := app.ParseConfig(); err != nil {
				t.Fatal(err)
			}
			gate := make(chan struct{})
			app.inputReplayBeforeRead = func() {
				select {
				case <-gate:
				case <-app.inputReplay.stop:
				}
				if fail {
					app.inputReplay.read = func(func(*points.Points), <-chan struct{}) (inputlog.ReplayStats, error) {
						return inputlog.ReplayStats{Corrupt: 1}, nil
					}
				}
			}
			if err := app.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(app.Stop)
			sendDuringReplay(t, app, stamp)
			if fail {
				close(gate)
				waitInputReplay(t, app.inputReplayFailed.Load)
				if !app.inputReplayActive.Load() || app.Cache.WriteoutReady("ordered") {
					t.Fatal("failed replay released live writeout")
				}
			}
			app.Stop()
			// Reopen the unchanged journal through the default blocking path with no
			// persister, so exact ordering is observable without the disk cache.
			config = inputLogTestConfig(t, root, nil)
			recovered := startApp(t, config)
			defer recovered.Stop()
			got := recovered.Cache.Get("ordered")
			if len(got) != 33 {
				t.Fatalf("retained %d points, want 33", len(got))
			}
			for i, p := range got {
				want := float64(i) + 0.25
				if i == 32 {
					want = 901.25
				}
				if p.Timestamp != stamp+int64(i) || math.Float64bits(p.Value) != math.Float64bits(want) {
					t.Fatalf("point %d = %v", i, p)
				}
			}
		})
	}
}

func TestInputLogBackgroundFullLiveCacheMakesProgress(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	root := t.TempDir()
	config, stamp := crashInputFixture(t, root, "pebble-chunk", false)
	app := New(config)
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	gate := make(chan struct{})
	app.inputReplayBeforeRead = func() {
		select {
		case <-gate:
		case <-app.inputReplay.stop:
		}
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	defer app.Stop()
	// Fill live admission before allowing any history to decode. Recovery must
	// retain its independent bounded window rather than wait for live writeout.
	for i := range 128 {
		app.Cache.Add(points.OnePoint("ordered", float64(i), stamp+32+int64(i)))
	}
	if got := app.Cache.Size(); got != 128 {
		t.Fatalf("live cache size %d", got)
	}
	close(gate)
	waitInputReplay(t, func() bool { return !app.inputReplayActive.Load() })
	if app.inputReplayFailed.Load() {
		t.Fatal("full live cache blocked recovery")
	}
}
