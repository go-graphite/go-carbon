package carbon

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/ingestoracle"
	"github.com/go-graphite/go-carbon/persister"
	"github.com/go-graphite/go-carbon/points"
)

func TestStopDrainConfig(t *testing.T) {
	if NewConfig().Dump.StopDrainTimeout.Value() != 0 {
		t.Fatal("stop drain must be opt-in")
	}
	for _, value := range []string{"stop-drain-timeout = \"-1s\"", "stop-drain-target-points = -1"} {
		filename := filepath.Join(t.TempDir(), "config")
		if err := os.WriteFile(filename, []byte("[dump]\n"+value+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadConfig(filename); err == nil {
			t.Fatal("negative drain bound accepted", value)
		}
	}
}

func TestStopDrainSkipsDisabledAndUnderTarget(t *testing.T) {
	app := New("")
	app.Config = NewConfig()
	app.Cache = cache.New()
	app.Cache.Add(points.OnePoint("held", 42, 1))
	app.Persister = &persister.Whisper{}
	app.drainBeforeDump() // Disabled and no persister.
	app.Config.Dump.StopDrainTimeout = Duration{time.Second}
	app.Config.Dump.StopDrainTargetPoints = 1
	app.drainBeforeDump()
	if app.Cache.Size() != 1 {
		t.Fatal("skipped drain changed cache")
	}
}

func TestStopDrainTargetTimeoutAndExactValues(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	for _, timeout := range []bool{false, true} {
		t.Run(fmt.Sprint(timeout), func(t *testing.T) {
			filename, cfg := sharedAppConfig(t)
			cfg.Buckyd.Enabled = false
			cfg.Common.MetricEndpoint = ""
			cfg.Whisper.Compressed, cfg.Whisper.OutOfOrder = true, true
			cfg.Whisper.StoreEncoding = "exact"
			cfg.Whisper.MaxUpdatesPerSecond = 1
			cfg.Cache.WriteStrategy = "lexical"
			cfg.Cache.WriteoutMinPoints = 1000
			cfg.Cache.WriteoutMaxDelay = Duration{time.Hour}
			cfg.Dump.Enabled, cfg.Dump.Path = true, t.TempDir()
			cfg.Dump.StopDrainTimeout = Duration{3 * time.Second}
			cfg.Dump.StopDrainTargetPoints = 4
			if timeout {
				cfg.Dump.StopDrainTimeout = Duration{150 * time.Millisecond}
				cfg.Dump.StopDrainTargetPoints = 0
			}
			app := New(filename)
			writeSharedConfig(t, filename, cfg)
			if err := app.ParseConfig(); err != nil {
				t.Fatal(err)
			}
			cfg = app.Config
			if err := app.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(app.Stop)
			stamp := time.Now().Unix() - 30
			want := make(map[string][]points.Point)
			bits := []uint64{0x8000000000000000, 0x3ff0000000000001, 1, 0x7ff0000000000000}
			for i := range 3 {
				name := fmt.Sprintf("drain.metric%d", i)
				for j := range (i + 1) * 4 {
					want[name] = append(want[name], points.Point{Timestamp: stamp + int64(j), Value: math.Float64frombits(bits[j%len(bits)])})
				}
				app.Cache.Add(&points.Points{Metric: name, Data: want[name]})
			}
			var input sync.WaitGroup
			if timeout {
				input.Go(func() {
					for i := range 12 {
						p := points.Point{Timestamp: stamp + int64(i), Value: float64(i) + .5}
						want["live"] = append(want["live"], p)
						app.Cache.Add(&points.Points{Metric: "live", Data: []points.Point{p}})
						_ = app.Cache.Get("live") // Reads continue during drain.
						time.Sleep(5 * time.Millisecond)
					}
				})
			}
			started := time.Now()
			app.Lock()
			app.drainBeforeDump()
			app.Unlock()
			elapsed := time.Since(started)
			input.Wait()
			if timeout {
				if elapsed < 150*time.Millisecond || elapsed > 2*time.Second || app.Cache.Size() == 0 || len(want["live"]) != 12 {
					t.Fatal("timeout/input bound failed", elapsed, app.Cache.Size())
				}
			} else if app.Cache.Size() > cfg.Dump.StopDrainTargetPoints {
				t.Fatal("drain did not reach target", app.Cache.Size(), elapsed)
			}
			cfg.Dump.StopDrainTimeout = Duration{}
			if err := app.DumpStop(); err != nil {
				t.Fatal(err)
			}
			app.Stop()
			cfg.Whisper.MaxUpdatesPerSecond = 0
			cfg.Cache.WriteoutMinPoints, cfg.Cache.WriteoutMaxDelay = 0, Duration{}
			next := New(filename)
			next.Config = cfg
			if err := next.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(next.Stop)
			waitExpiration(t, next.Cache.IsEmpty)
			for name, expected := range want {
				snapshot, err := next.MetricStore.Snapshot(context.Background(), name)
				if err != nil {
					t.Fatal(name, err)
				}
				actual := snapshot.Archives[0].Points
				if len(actual) != len(expected) {
					t.Fatal(name, "lost points", len(actual), len(expected))
				}
				for i, p := range expected {
					if actual[i].Timestamp != p.Timestamp || math.Float64bits(actual[i].Value) != math.Float64bits(p.Value) {
						t.Fatal(name, "changed point bits", i)
					}
				}
				if _, _, err := ingestoracle.VerifyFetch(context.Background(), next.MetricStore, snapshot, time.Now().Unix()); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
