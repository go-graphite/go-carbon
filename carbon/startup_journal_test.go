package carbon

import (
	"context"
	"errors"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/points"
)

func journalStartupFixture(t *testing.T, root string) *App {
	t.Helper()
	cfg := NewConfig()
	cfg.Dump.Enabled, cfg.Dump.StoreInputHandoff = true, true
	cfg.Dump.Path = filepath.Join(root, "dump")
	cfg.InputLog.Enabled, cfg.InputLog.StartupAppend = true, true
	cfg.InputLog.Path = filepath.Join(root, "log")
	cfg.InputLog.SegmentSize = 16 << 20
	cfg.InputLog.SyncInterval = Duration{time.Hour}
	cfg.InputLog.MaxAge = Duration{24 * time.Hour}
	cfg.InputLog.TruncateLag = Duration{}
	cfg.Whisper.StorageBackend = "pebble-chunk"
	cfg.Tcp.Enabled, cfg.Tcp.Listen = true, "127.0.0.1:0"
	cfg.Udp.Enabled, cfg.Pickle.Enabled = false, false
	cfg.Tags.Enabled = true
	if err := os.MkdirAll(cfg.Dump.Path, 0700); err != nil {
		t.Fatal(err)
	}
	app := New("")
	app.Config = cfg
	if err := app.startStartupInput(); err != nil {
		t.Fatal(err)
	}
	if app.startupInput == nil || app.startupInput.journal == nil {
		t.Fatal("startup did not select input log")
	}
	return app
}
func closeJournalFixture(app *App) {
	app.startupInput.receiver.Stop()
	_ = app.startupInput.close()
	app.closeInputLog()
}
func attachJournalFixture(t *testing.T, app *App) {
	t.Helper()
	core := cache.New()
	core.SetMaxSize(0)
	core.SetTagsEnabled(true)
	app.Cache = core
	if err := app.openInputLog(core); err != nil {
		t.Fatal(err)
	}
	app.finishStartupInput(core)
}
func TestStartupJournalBeforeCacheBitExact(t *testing.T) {
	app := journalStartupFixture(t, t.TempDir())
	defer closeJournalFixture(app)
	p := releaseCrashPoints()
	for i, point := range p.Data {
		name := p.Metric + ";z=last;a=first"
		if i%2 == 0 {
			name = p.Metric + ";a=first;z=last"
		}
		one := &points.Points{Metric: name, Data: []points.Point{point}}
		if err := app.startupInput.Write(one.AppendBinary(nil), 1); err != nil {
			t.Fatal(err)
		}
	}
	if app.Cache != nil || app.inputLog.Load().Metrics().PointsWritten != int64(len(p.Data)) {
		t.Fatal("not durable before cache")
	}
	paths, _ := filepath.Glob(filepath.Join(app.Config.Dump.Path, "*.capture-*"))
	if len(paths) != 0 {
		t.Fatal("capture spool created", paths)
	}
	attachJournalFixture(t, app)
	name := p.Metric + ";a=first;z=last"
	awaitCapture(t, func() bool { return len(app.Cache.Get(name)) == len(p.Data) })
	assertReleaseReplayBits(t, app.Cache.Get(name), p.Data)
	before := app.inputLog.Load().Metrics().PointsWritten
	if err := app.inputLog.Load().Sync(); err != nil {
		t.Fatal(err)
	}
	if app.inputLog.Load().Metrics().PointsWritten != before {
		t.Fatal("startup input relogged")
	}
	awaitCapture(t, func() bool { return app.startupInput.journal.fence() == nil })
	if err := app.startupInput.receiver.SwitchCapture(app.Cache.Add, app.startupInput.journal.fence); err != nil {
		t.Fatal(err)
	}
	app.startupInput.direct.Store(true)
	awaitCapture(t, func() bool { return app.startupInput.journal.joined.Load() })
	// Source ownership must outlive application and the reader join.
	if app.startupInput.journal.parts[(hashJournalName(name))].source.Pending() == 0 {
		t.Fatal("source released before confirmation")
	}
	app.startupInput.journal.releasePersisted()
	if app.startupInput.journal.released {
		t.Fatal("retired unpersisted startup source")
	}
}
func hashJournalName(name string) uint16 { return uint16(journalPartition(name)) }

func TestStartupJournalCrashProcess(t *testing.T) {
	stage := os.Getenv("STARTUP_JOURNAL_CRASH_STAGE")
	if stage == "" {
		return
	}
	root := os.Getenv("STARTUP_JOURNAL_CRASH_ROOT")
	app := journalStartupFixture(t, root)
	p := releaseCrashPoints()
	crash := func(at string) {
		if at == stage {
			os.Exit(99)
		}
	}
	crash("before_append")
	for i := 0; i < 3; i++ {
		batch := &points.Points{Metric: p.Metric, Data: p.Data[i*len(p.Data)/3 : (i+1)*len(p.Data)/3]}
		if err := app.startupInput.Write(batch.AppendBinary(nil), len(batch.Data)); err != nil {
			t.Fatal(err)
		}
	}
	crash("after_append")
	if err := app.inputLog.Load().Sync(); err != nil {
		t.Fatal(err)
	}
	crash("after_sync")
	attachJournalFixture(t, app)
	crash("after_attach")
	awaitCapture(t, func() bool { return len(app.Cache.Get(p.Metric)) == len(p.Data) })
	crash("after_replay")
	if strings.HasPrefix(stage, "cursor_") || stage == "snapshot_synced" {
		app.startupInput.pauseReplay()
		if err := app.saveStartupResumeAt(app.startupInput, crash); err != nil {
			t.Fatal(err)
		}
		t.Fatal("snapshot crash stage not reached", stage)
	}
	awaitCapture(t, func() bool { return app.startupInput.journal.fence() == nil })
	crash("before_switch")
	if err := app.startupInput.receiver.SwitchCapture(app.Cache.Add, app.startupInput.journal.fence); err != nil {
		t.Fatal(err)
	}
	crash("after_fence")
	app.startupInput.direct.Store(true)
	awaitCapture(t, func() bool { return app.startupInput.journal.joined.Load() })
	crash("after_switch")
	db := releaseCrashStore(t, filepath.Join(root, "store"))
	batch, ok := app.Cache.PopNotConfirmed(p.Metric)
	if !ok {
		t.Fatal("no journal batch")
	}
	if err := db.UpdateMany(context.Background(), p.Metric, batch.Data); err != nil {
		t.Fatal(err)
	}
	app.Cache.Confirm(batch)
	crash("after_confirm")
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	crash("after_store_sync")
	app.startupInput.sink.Load().syncStore = db.Sync
	app.startupInput.journal.releasePersisted()
	if !app.startupInput.journal.released {
		t.Fatal("persistence did not release pin")
	}
	crash("after_retention_release")
	t.Fatal("unknown crash stage", stage)
}
func TestStartupJournalCrashArchivesMatchUninterrupted(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	for _, stage := range []string{"before_append", "after_append", "after_sync", "after_attach", "after_replay", "snapshot_synced", "cursor_synced", "cursor_renamed", "cursor_committed", "before_switch", "after_fence", "after_switch", "after_confirm", "after_store_sync", "after_retention_release"} {
		t.Run(stage, func(t *testing.T) {
			root := t.TempDir()
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStartupJournalCrashProcess$")
			cmd.Env = append(os.Environ(), "STARTUP_JOURNAL_CRASH_STAGE="+stage, "STARTUP_JOURNAL_CRASH_ROOT="+root)
			out, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 99 {
				t.Fatalf("child %v: %s", err, out)
			}
			app := journalStartupFixture(t, root)
			defer closeJournalFixture(app)
			attachJournalFixture(t, app)
			p := releaseCrashPoints()
			awaitCapture(t, func() bool { return app.startupInput.journal.fence() == nil })
			app.startupInput.direct.Store(true)
			awaitCapture(t, func() bool { return app.startupInput.journal.joined.Load() })
			db := releaseCrashStore(t, filepath.Join(root, "store"))
			defer db.Close()
			if batch, ok := app.Cache.PopNotConfirmed(p.Metric); ok {
				if err := db.UpdateMany(ctx, p.Metric, batch.Data); err != nil {
					t.Fatal(err)
				}
				app.Cache.Confirm(batch)
			}
			if stage == "before_append" {
				if got := app.inputLog.Load().Metrics().ReplayedPoints; got != 0 {
					t.Fatal("unexpected input", got)
				}
				return
			}
			if err := db.Sync(); err != nil {
				t.Fatal(err)
			}
			ref := releaseCrashStore(t, filepath.Join(t.TempDir(), "reference"))
			defer ref.Close()
			if err := ref.UpdateMany(ctx, p.Metric, p.Data); err != nil {
				t.Fatal(err)
			}
			actual, err := db.Snapshot(ctx, p.Metric)
			if err != nil {
				t.Fatal(err)
			}
			expected, err := ref.Snapshot(ctx, p.Metric)
			if err != nil {
				t.Fatal(err)
			}
			if len(actual.Archives) != len(expected.Archives) {
				t.Fatal("archive count")
			}
			for i, a := range actual.Archives {
				b := expected.Archives[i]
				if len(a.Points) != len(b.Points) {
					t.Fatal("archive size")
				}
				for n, p := range a.Points {
					q := b.Points[n]
					if p.Timestamp != q.Timestamp || math.Float64bits(p.Value) != math.Float64bits(q.Value) {
						t.Fatal("stored archive bits changed", i, n)
					}
				}
			}
		})
	}
}

func TestStartupJournalAppRestart(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	path, cfg := incrementalAppFixture(t)
	cfg.Dump.Enabled, cfg.Dump.StoreInputHandoff = true, true
	cfg.Dump.Path = t.TempDir()
	cfg.InputLog.Enabled, cfg.InputLog.StartupAppend = true, true
	cfg.InputLog.Path = filepath.Join(t.TempDir(), "log")
	cfg.InputLog.SegmentSize = 16 << 20
	cfg.Tcp.Enabled, cfg.Tcp.Listen = true, "127.0.0.1:0"
	writeSharedConfig(t, path, cfg)
	app := startIncrementalApp(t, path)
	s := app.startupInput
	if s == nil || s.journal == nil {
		t.Fatal("app did not select startup journal")
	}
	stamp := time.Now().Unix() - 5
	p := points.OnePoint("journal.app", 1, stamp)
	if err := s.Write(p.AppendBinary(nil), 1); err != nil {
		t.Fatal(err)
	}
	awaitCapture(t, func() bool { return s.direct.Load() && app.Cache.IsEmpty() })
	if app.inputLog.Load().Metrics().PointsWritten < 1 {
		t.Fatal("no startup journal writes")
	}
	if paths, _ := filepath.Glob(filepath.Join(cfg.Dump.Path, "*.capture-*")); len(paths) > 0 {
		t.Fatal("spool created", paths)
	}
	if err := app.DumpStop(); err != nil {
		t.Fatal(err)
	}
	app.Stop()
	next := startIncrementalApp(t, path)
	defer next.Stop()
	awaitCapture(t, func() bool { return next.startupInput.direct.Load() })
	snapshot, err := next.MetricStore.Snapshot(context.Background(), p.Metric)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, a := range snapshot.Archives {
		for _, point := range a.Points {
			if point.Timestamp == stamp && math.Float64bits(point.Value) == math.Float64bits(1) {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("restart lost stored point")
	}
}
