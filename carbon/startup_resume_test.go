//go:build unix

package carbon

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/tcp"
	"github.com/klauspost/compress/zstd"
	"github.com/lomik/zapwriter"
)

func TestStartupHistoryReleaseWhileReplayIncomplete(t *testing.T) {
	defer zapwriter.Test()()
	s, core, sink := spoolReleaseFixture(t)
	s.pauseReplay()
	s.sink.Store(sink)
	s.historyReady.Store(false)
	root := s.legacy.first.Load()
	root.offset.Store(root.size.Load())
	s.reclaim()
	if !root.retired.Load() || s.legacy.first.Load() != nil {
		t.Fatal("published history prefix held until whole-history completion")
	}
	// Replay can discover a new source after the published prefix becomes empty.
	path := filepath.Join(filepath.Dir(s.path), "inherited.tail.bin")
	raw := points.OnePoint("release.inherited", 1, 1).AppendBinary(nil)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	next := &startupSegment{file: f, path: path, unlock: func() error { return nil }, source: s.legacy.stream.Next()}
	next.size.Store(int64(len(raw)))
	s.size.Add(int64(len(raw)))
	s.appendHistory(root, next)
	if s.legacy.first.Load() != next {
		t.Fatal("new history source lost after empty prefix")
	}
	core.AddRecoveredFrom(points.OnePoint("release.inherited", 1, 1), next.source)
	next.sealed.Store(true)
	next.replayed.Store(true)
	s.reclaim()
	if next.retired.Load() {
		t.Fatal("unpersisted inherited segment released")
	}
	batch, _ := core.PopNotConfirmed("release.inherited")
	core.Confirm(batch)
	sink.syncStore = func() error { return errors.New("fence held") }
	s.reclaim()
	if s.historyReleasableBytes.Load() != int64(len(raw)) || s.liveReleasableBytes.Load() != 0 {
		t.Fatal("history/live releasable metrics")
	}
	sink.syncStore = func() error { return nil }
	s.reclaim()
	if !next.retired.Load() || s.historyReady.Load() {
		t.Fatal("confirmed inherited segment not released during history replay")
	}
}

func TestStartupResumeGracefulPartialHistoryBitExact(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	s, _ := captureFixture(t)
	want := releaseCrashPoints()
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()
	path := fmt.Sprintf("%s.capture-epoch-%020d.%06d.%012d.tail.bin", s.path, s.epoch-1, 0, 0)
	first := (&points.Points{Metric: want.Metric, Data: want.Data[:32]}).AppendBinary(nil)
	second := (&points.Points{Metric: want.Metric, Data: want.Data[32:]}).AppendBinary(nil)
	raw := recovery.EncodeCaptureFrame(enc, first, nil, 32)
	raw = append(raw, recovery.EncodeCaptureFrame(enc, second, nil, 32)...)
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	core := cache.New()
	core.SetMaxSize(2)
	app := New("")
	app.Config = NewConfig()
	app.Config.Dump.Path = filepath.Dir(s.path)
	app.Cache = core
	app.startupInput = s
	app.finishStartupInput(core)
	awaitCapture(t, func() bool { return s.historyPoints.Load() == 32 })
	if s.historyReady.Load() {
		t.Fatal("fixture did not stop mid-segment")
	}
	if err = app.DumpStop(); err != nil {
		t.Fatal(err)
	}
	state, err := readStartupResume(filepath.Dir(s.path))
	if err != nil || state == nil {
		t.Fatal("no durable cursor", err)
	}
	if state.Offsets[filepath.Base(path)] <= 0 || state.Offsets[filepath.Base(path)] >= int64(len(raw)) {
		t.Fatal("not a frame boundary cursor", state)
	}
	next, err := openStartupInputWithPolicy(s.path, s.policy)
	if err != nil {
		t.Fatal(err)
	}
	defer next.close()
	resumeTestReceiver(t, next)
	next.resume = state
	core2 := cache.New()
	core2.SetMaxSize(0)
	app2 := New("")
	app2.Config = NewConfig()
	app2.Cache = core2
	app2.startupInput = next
	app2.finishStartupInput(core2)
	awaitCapture(t, func() bool { return next.historyReady.Load() })
	if next.historyPoints.Load() != 32 {
		t.Fatal("graceful restart re-replayed completed frames", next.historyPoints.Load())
	}
	assertReleaseReplayBits(t, core2.Get(want.Metric), want.Data)
	// A second graceful restart proves snapshot and segment cursors compose.
	if err = app2.DumpStop(); err != nil {
		t.Fatal(err)
	}
	state2, err := readStartupResume(filepath.Dir(s.path))
	if err != nil {
		t.Fatal(err)
	}
	last, err := openStartupInputWithPolicy(s.path, s.policy)
	if err != nil {
		t.Fatal(err)
	}
	defer last.close()
	resumeTestReceiver(t, last)
	last.resume = state2
	final := cache.New()
	final.SetMaxSize(0)
	app3 := New("")
	app3.Config = NewConfig()
	app3.Cache = final
	app3.startupInput = last
	app3.finishStartupInput(final)
	awaitCapture(t, func() bool { return last.historyReady.Load() })
	if last.historyPoints.Load() != 0 {
		t.Fatal("second graceful restart re-replayed history", last.historyPoints.Load())
	}
	assertReleaseReplayBits(t, final.Get(want.Metric), want.Data)
	db := releaseCrashStore(t, filepath.Join(t.TempDir(), "resumed"))
	defer db.Close()
	ref := releaseCrashStore(t, filepath.Join(t.TempDir(), "reference"))
	defer ref.Close()
	batch, ok := final.PopNotConfirmed(want.Metric)
	if !ok {
		t.Fatal("missing restored batch")
	}
	ctx := context.Background()
	if err = db.UpdateMany(ctx, want.Metric, batch.Data); err != nil {
		t.Fatal(err)
	}
	if err = ref.UpdateMany(ctx, want.Metric, want.Data); err != nil {
		t.Fatal(err)
	}
	a, err := db.Snapshot(ctx, want.Metric)
	if err != nil {
		t.Fatal(err)
	}
	b, err := ref.Snapshot(ctx, want.Metric)
	if err != nil {
		t.Fatal(err)
	}
	for i, archive := range a.Archives {
		for j, p := range archive.Points {
			q := b.Archives[i].Points[j]
			if p.Timestamp != q.Timestamp || math.Float64bits(p.Value) != math.Float64bits(q.Value) {
				t.Fatal("stored values differ after resume", i, j)
			}
		}
	}
}

func resumeTestReceiver(t *testing.T, s *startupInput) {
	t.Helper()
	opts := tcp.NewOptions()
	opts.Listen = "127.0.0.1:0"
	h, bound, err := listenStartupCapture(opts)
	if err != nil {
		t.Fatal(err)
	}
	s.receiver, err = tcp.NewInheritedCapture("tcp", bound, h, s)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.receiver.Stop)
}

// Real process exits exercise both sides of the snapshot/cursor commit and
// every inherited-source durability/release step with more history outstanding.
func TestStartupHistoryDurabilityCrashProcess(t *testing.T) {
	stage := os.Getenv("HISTORY_RELEASE_CRASH_STAGE")
	if stage == "" {
		return
	}
	root := os.Getenv("HISTORY_RELEASE_CRASH_ROOT")
	s, err := openStartupInputWithPolicy(filepath.Join(root, "input.1.2.tail.bin"), chooseStartupPolicy(1, 1<<30, 1<<30))
	if err != nil {
		t.Fatal(err)
	}
	want := releaseCrashPoints()
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	bounds := []int{0, 16, 32, 64}
	var firstPath string
	for i := 0; i < 3; i++ {
		path := fmt.Sprintf("%s.capture-epoch-%020d.%06d.%012d.tail.bin", s.path, s.epoch-1, 0, i)
		p := &points.Points{Metric: want.Metric, Data: want.Data[bounds[i]:bounds[i+1]]}
		if err = os.WriteFile(path, recovery.EncodeCaptureFrame(enc, p.AppendBinary(nil), nil, len(p.Data)), 0600); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			firstPath = path
		}
	}
	enc.Close()
	if stage == "before_replay" {
		os.Exit(99)
	}
	core := cache.New()
	core.SetMaxSize(2)
	app := New("")
	app.Config = NewConfig()
	app.Cache = core
	app.startupInput = s
	app.finishStartupInput(core)
	awaitCapture(t, func() bool { return s.historyPoints.Load() == 16 })
	if strings.HasPrefix(stage, "cursor_") || stage == "snapshot_synced" {
		s.pauseReplay()
		if err = app.saveStartupResumeAt(s, func(at string) {
			if at == stage {
				os.Exit(99)
			}
		}); err != nil {
			t.Fatal(err)
		}
		t.Fatal("cursor crash point not reached", stage)
	}
	if stage == "after_replay" {
		os.Exit(99)
	}
	db := releaseCrashStore(t, filepath.Join(root, "store"))
	batch, ok := core.PopNotConfirmed(want.Metric)
	if !ok {
		t.Fatal("no batch")
	}
	if err = db.UpdateMany(context.Background(), want.Metric, batch.Data); err != nil {
		t.Fatal(err)
	}
	core.Confirm(batch)
	awaitCapture(t, func() bool { return s.historyPoints.Load() == 32 })
	if s.historyReady.Load() {
		t.Fatal("history finished before crash")
	}
	if stage == "after_confirm" {
		os.Exit(99)
	}
	s.sink.Load().syncStore = func() error {
		if stage == "before_store_sync" {
			os.Exit(99)
		}
		if err := db.Sync(); err != nil {
			return err
		}
		if stage == "after_store_sync" {
			os.Exit(99)
		}
		return nil
	}
	var target *startupSegment
	for seg := s.legacy.first.Load(); seg != nil; seg = seg.next.Load() {
		if seg.path == firstPath {
			target = seg
			break
		}
	}
	if target == nil {
		t.Fatal("missing inherited source")
	}
	unlock := target.unlock
	target.unlock = func() error {
		err := unlock()
		if err == nil && stage == "after_unlock" {
			os.Exit(99)
		}
		return err
	}
	unlinked := false
	s.reclaimWith(func(path string) error {
		err := os.Remove(path)
		if err == nil && path == firstPath {
			unlinked = true
			if stage == "after_unlink" {
				os.Exit(99)
			}
		}
		return err
	}, func(path string) error {
		err := syncDir(path)
		if err == nil && unlinked && stage == "after_directory_sync" {
			os.Exit(99)
		}
		return err
	})
	if stage == "after_prefix_advance" && target.retired.Load() {
		os.Exit(99)
	}
	t.Fatal("crash point not reached", stage)
}

func TestStartupHistoryDurabilityCrashBitDifferential(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	for _, stage := range []string{"before_replay", "after_replay", "after_confirm", "before_store_sync", "after_store_sync", "after_unlink", "after_directory_sync", "after_unlock", "after_prefix_advance", "snapshot_synced", "cursor_synced", "cursor_renamed", "cursor_committed"} {
		t.Run(stage, func(t *testing.T) {
			root := t.TempDir()
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStartupHistoryDurabilityCrashProcess$")
			cmd.Env = append(os.Environ(), "HISTORY_RELEASE_CRASH_STAGE="+stage, "HISTORY_RELEASE_CRASH_ROOT="+root)
			out, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 99 {
				t.Fatalf("child: %v\n%s", err, out)
			}
			s, err := openStartupInputWithPolicy(filepath.Join(root, "input.1.2.tail.bin"), chooseStartupPolicy(1, 1<<30, 1<<30))
			if err != nil {
				t.Fatal(err)
			}
			defer s.close()
			s.resume, err = readStartupResume(root)
			if err != nil {
				t.Fatal(err)
			}
			core := cache.New()
			core.SetMaxSize(0)
			app := New("")
			app.Config = NewConfig()
			app.Cache = core
			app.startupInput = s
			app.finishStartupInput(core)
			awaitCapture(t, func() bool { return s.historyReady.Load() })
			want := releaseCrashPoints()
			expected := want.Data
			if stage == "after_unlink" || stage == "after_directory_sync" || stage == "after_unlock" || stage == "after_prefix_advance" {
				expected = expected[16:]
			}
			assertReleaseReplayBits(t, core.Get(want.Metric), expected)
			if s.resume != nil && s.historyPoints.Load() != 48 {
				t.Fatal("durable cursor did not resume", s.historyPoints.Load())
			}
			db := releaseCrashStore(t, filepath.Join(root, "store"))
			defer db.Close()
			if batch, ok := core.PopNotConfirmed(want.Metric); ok {
				if err = db.UpdateMany(ctx, want.Metric, batch.Data); err != nil {
					t.Fatal(err)
				}
			}
			if err = db.Sync(); err != nil {
				t.Fatal(err)
			}
			ref := releaseCrashStore(t, filepath.Join(t.TempDir(), "reference"))
			defer ref.Close()
			if err = ref.UpdateMany(ctx, want.Metric, want.Data); err != nil {
				t.Fatal(err)
			}
			a, err := db.Snapshot(ctx, want.Metric)
			if err != nil {
				t.Fatal(err)
			}
			b, err := ref.Snapshot(ctx, want.Metric)
			if err != nil {
				t.Fatal(err)
			}
			for i, archive := range a.Archives {
				for j, p := range archive.Points {
					q := b.Archives[i].Points[j]
					if p.Timestamp != q.Timestamp || math.Float64bits(p.Value) != math.Float64bits(q.Value) {
						t.Fatal("crash changed stored bits", i, j)
					}
				}
			}
		})
	}
}

func TestStartupResumeGracefulLiveShard(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	s, _ := captureFixture(t)
	core := cache.New()
	core.SetMaxSize(2)
	app := New("")
	app.Config = NewConfig()
	app.Cache = core
	app.startupInput = s
	app.finishStartupInput(core)
	awaitCapture(t, func() bool { return s.historyReady.Load() })
	want := releaseCrashPoints()
	for _, data := range [][]points.Point{want.Data[:32], want.Data[32:]} {
		if err := s.shards[0].Write((&points.Points{Metric: want.Metric, Data: data}).AppendBinary(nil), len(data)); err != nil {
			t.Fatal(err)
		}
	}
	awaitCapture(t, func() bool { return s.replayedPoints.Load() == 32 })
	if err := app.DumpStop(); err != nil {
		t.Fatal(err)
	}
	next, err := openStartupInputWithPolicy(s.path, s.policy)
	if err != nil {
		t.Fatal(err)
	}
	defer next.close()
	next.resume, err = readStartupResume(filepath.Dir(s.path))
	if err != nil {
		t.Fatal(err)
	}
	core2 := cache.New()
	core2.SetMaxSize(0)
	app2 := New("")
	app2.Config = NewConfig()
	app2.Cache = core2
	app2.startupInput = next
	app2.finishStartupInput(core2)
	awaitCapture(t, func() bool { return next.historyReady.Load() })
	if next.historyPoints.Load() != 32 {
		t.Fatal("live shard prefix re-replayed", next.historyPoints.Load())
	}
	assertReleaseReplayBits(t, core2.Get(want.Metric), want.Data)
}

func TestStartupResumeSelectsCursorWithoutOriginalManifest(t *testing.T) {
	defer zapwriter.Test()()
	s, _ := captureFixture(t)
	core := cache.New()
	core.AddRecovered(points.OnePoint("resume.saved", 7, 1700000000))
	app := New("")
	app.Config = NewConfig()
	app.Config.Dump.Path = filepath.Dir(s.path)
	app.Cache = core
	if err := app.saveStartupResume(s); err != nil {
		t.Fatal(err)
	}
	s.receiver.Stop()
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	next := New("")
	next.Config = NewConfig()
	cfg := next.Config
	cfg.Dump.Enabled, cfg.Dump.StoreInputHandoff, cfg.Dump.Path = true, true, filepath.Dir(s.path)
	cfg.InputLog.Enabled = true
	cfg.Whisper.StorageBackend = "pebble-chunk"
	cfg.Tcp.Enabled, cfg.Tcp.Listen = true, "127.0.0.1:0"
	cfg.Udp.Enabled, cfg.Pickle.Enabled = false, false
	if err := next.startStartupInput(); err != nil {
		t.Fatal(err)
	}
	if !next.hasStartupResume() || next.startupInput.path != s.path {
		t.Fatal("durable cursor did not select its source namespace")
	}
	defer next.startupInput.close()
	defer next.startupInput.receiver.Stop()
	// The cursor's snapshot supersedes ordinary checkpoint files.
	loaded := false
	next.RestoreFromDir(cfg.Dump.Path, func(*points.Points) { loaded = true })
	if loaded {
		t.Fatal("superseded checkpoint restored ahead of snapshot")
	}
}

func TestStartupResumeGracefulLegacyRecords(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	s, _ := captureFixture(t)
	want := releaseCrashPoints()
	var raw []byte
	for _, data := range [][]points.Point{want.Data[:32], want.Data[32:]} {
		raw = (&points.Points{Metric: want.Metric, Data: data}).AppendBinary(raw)
	}
	if err := os.WriteFile(s.path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	s.legacy.current.Load().size.Store(int64(len(raw)))
	s.size.Store(int64(len(raw)))
	core := cache.New()
	core.SetMaxSize(2)
	app := New("")
	app.Config = NewConfig()
	app.Cache, app.startupInput = core, s
	app.finishStartupInput(core)
	awaitCapture(t, func() bool { return core.Size() == 32 })
	if err := app.DumpStop(); err != nil {
		t.Fatal(err)
	}
	next, err := openStartupInputWithPolicy(s.path, s.policy)
	if err != nil {
		t.Fatal(err)
	}
	defer next.close()
	next.resume, err = readStartupResume(filepath.Dir(s.path))
	if err != nil {
		t.Fatal(err)
	}
	first := (&points.Points{Metric: want.Metric, Data: want.Data[:32]}).AppendBinary(nil)
	if next.resume.Offsets[filepath.Base(s.path)] != int64(len(first)) {
		t.Fatal("legacy cursor includes reader lookahead", next.resume.Offsets)
	}
	core2 := cache.New()
	core2.SetMaxSize(0)
	app2 := New("")
	app2.Config = NewConfig()
	app2.Cache, app2.startupInput = core2, next
	app2.finishStartupInput(core2)
	awaitCapture(t, func() bool { return next.historyReady.Load() })
	assertReleaseReplayBits(t, core2.Get(want.Metric), want.Data)
	// A later crash can leave the old cursor after reclaim removed the stable
	// raw-tail name. Recreating that name empty must not apply its old offset.
	db := releaseCrashStore(t, filepath.Join(t.TempDir(), "persisted"))
	defer db.Close()
	batch, ok := core2.PopNotConfirmed(want.Metric)
	if !ok {
		t.Fatal("missing legacy batch")
	}
	if err := db.UpdateMany(context.Background(), want.Metric, batch.Data); err != nil {
		t.Fatal(err)
	}
	core2.Confirm(batch)
	next.sink.Load().syncStore = db.Sync
	next.reclaim()
	if _, err := os.Stat(s.path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("stable tail not released", err)
	}
	if err := next.close(); err != nil {
		t.Fatal(err)
	}
	last, err := openStartupInputWithPolicy(s.path, s.policy)
	if err != nil {
		t.Fatal(err)
	}
	defer last.close()
	last.resume, err = readStartupResume(filepath.Dir(s.path))
	if err != nil {
		t.Fatal(err)
	}
	app3 := New("")
	app3.Config, app3.Cache, app3.startupInput = NewConfig(), cache.New(), last
	app3.finishStartupInput(app3.Cache)
	awaitCapture(t, func() bool { return last.historyReady.Load() })

}

func TestStartupResumePartialJournalSnapshot(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	cfg := NewConfig()
	cfg.InputLog.Enabled = true
	cfg.InputLog.Path = filepath.Join(t.TempDir(), "journal")
	cfg.InputLog.SyncInterval = Duration{time.Hour}
	cfg.InputLog.SegmentSize = 16 << 20
	writer := New("")
	writer.Config, writer.Cache = cfg, cache.New()
	if err := writer.openInputLog(writer.Cache); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 40; i++ {
		writer.Cache.Add(points.OnePoint(fmt.Sprintf("journal.resume.%03d", i), float64(i), 1700000000))
	}
	if err := writer.syncInputLog(); err != nil {
		t.Fatal(err)
	}
	writer.closeInputLog()
	s, _ := captureFixture(t)
	app := New("")
	app.Config, app.Cache, app.startupInput = cfg, cache.New(), s
	app.Cache.SetMaxSize(1)
	if err := app.openInputLog(app.Cache); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- app.replayInputLogWith(app.Cache, s.replayStop, cfg.InputLog.Path, true, app.Cache.AddRecovered)
	}()
	awaitCapture(t, func() bool { return app.Cache.Size() > 0 })
	close(s.replayStop)
	if err := <-done; err == nil {
		t.Fatal("journal did not stop partially")
	}
	saved := app.Cache.Size()
	if saved >= 40 {
		t.Fatal("journal fixture already complete", saved)
	}
	if err := app.saveStartupResume(s); err != nil {
		t.Fatal(err)
	}
	app.closeInputLog()
	s.receiver.Stop()
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	next, err := openStartupInputWithPolicy(s.path, s.policy)
	if err != nil {
		t.Fatal(err)
	}
	defer next.close()
	next.resume, err = readStartupResume(filepath.Dir(s.path))
	if err != nil || len(next.resume.JournalOffsets) == 0 {
		t.Fatal("journal cursor missing", err)
	}
	app2 := New("")
	app2.Config, app2.Cache, app2.startupInput = cfg, cache.New(), next
	app2.Cache.SetMaxSize(0)
	if err := app2.openInputLog(app2.Cache); err != nil {
		t.Fatal(err)
	}
	defer app2.closeInputLog()
	app2.finishStartupInput(app2.Cache)
	awaitCapture(t, func() bool { return next.historyReady.Load() })
	if got := app2.inputLog.Load().Metrics().ReplayedPoints; got != int64(40-saved) {
		t.Fatal("journal replay repeated snapshot prefix", got, saved)
	}
	for i := 0; i < 40; i++ {
		assertReleaseReplayBits(t, app2.Cache.Get(fmt.Sprintf("journal.resume.%03d", i)), []points.Point{{Value: float64(i), Timestamp: 1700000000}})
	}
	for i := 0; i < 40; i++ {
		batch, ok := app2.Cache.PopNotConfirmed(fmt.Sprintf("journal.resume.%03d", i))
		if !ok {
			t.Fatal("missing journal persistence batch", i)
		}
		app2.Cache.Confirm(batch)
	}
	next.reclaim()
	release, err := next.sink.Load().startLog()
	if err != nil {
		t.Fatal("resumed journal could not enter a fresh direct epoch", err)
	}
	if !next.journalReset {
		t.Fatal("resumed journal boundary not reset")
	}
	if err := release(); err != nil {
		t.Fatal(err)
	}

}
