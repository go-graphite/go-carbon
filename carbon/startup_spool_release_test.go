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
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
	"github.com/lomik/zapwriter"
	"github.com/prometheus/client_golang/prometheus"
)

func spoolReleaseFixture(t *testing.T) (*startupInput, *cache.Cache, *startupSink) {
	t.Helper()
	p := chooseStartupPolicy(1, 1<<30, 1<<30)
	p.segmentBytes = 1 // Every acknowledged write starts its own segment.
	s, err := openStartupInputWithPolicy(filepath.Join(t.TempDir(), "input.1.2.tail.bin"), p)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.close(); err != nil {
			t.Error(err)
		}
	})
	core := cache.New()
	core.SetMaxSize(0)
	sink := &startupSink{admission: core, add: core.AddRecoveredFrom, liveAdd: core.AddCapturedFrom, canRetire: func() bool { return true }, drain: core.SetRecoveryDrain}
	s.startReplay(sink, func() error { return nil })
	awaitCapture(t, func() bool { return allCaptureLive(s) })
	return s, core, sink
}

func writeReleasePoints(t *testing.T, s *startupInput, n int) {
	t.Helper()
	for i := range n {
		if err := s.shards[0].Write(points.OnePoint(fmt.Sprintf("release.%d", i), float64(i), 1700000000).AppendBinary(nil), 1); err != nil {
			t.Fatal(err)
		}
	}
	awaitCapture(t, func() bool { return s.replayedPoints.Load() == int64(n) })
}

func confirmReleasePoint(t *testing.T, core *cache.Cache, i int) {
	t.Helper()
	p, ok := core.PopNotConfirmed(fmt.Sprintf("release.%d", i))
	if !ok {
		t.Fatal("missing confirmation batch", i)
	}
	core.Confirm(p)
}

func TestStartupSpoolReleaseDrainsBeforeDirect(t *testing.T) {
	s, core, _ := spoolReleaseFixture(t)
	if err := core.SetWriteoutBatching(960, 1800*time.Second); err != nil {
		t.Fatal(err)
	}
	writeReleasePoints(t, s, 3)
	if core.WriteoutReady("release.0") {
		t.Fatal("fixture did not retain a live partial batch")
	}
	before := s.size.Load()
	s.reclaim()
	if !core.WriteoutReady("release.0") {
		t.Fatal("startup waited for direct switch before requesting persistence")
	}
	if s.retiredBytes.Load() != 0 {
		t.Fatal("unconfirmed source released")
	}
	confirmReleasePoint(t, core, 0)
	s.reclaim()
	if s.retiredBytes.Load() == 0 || s.retiredBytes.Load() >= before {
		t.Fatal("confirmed prefix did not release incrementally")
	}
	if s.direct.Load() || s.shards[0].current.Load().retired.Load() {
		t.Fatal("release required capture exit or passed the append frontier")
	}
	// Capture and replay continue after disk space has been returned.
	if err := s.shards[0].Write(points.OnePoint("release.next", 4, 1700000001).AppendBinary(nil), 1); err != nil {
		t.Fatal(err)
	}
	awaitCapture(t, func() bool { return s.replayedPoints.Load() == 4 })
}

func TestStartupSpoolReleaseCheckpointAndFence(t *testing.T) {
	s, core, sink := spoolReleaseFixture(t)
	var checkpointRetired atomic.Bool
	sink.canRetire = checkpointRetired.Load
	writeReleasePoints(t, s, 3)
	first := s.shards[0].first.Load()
	second := first.next.Load()
	confirmReleasePoint(t, core, 0)
	s.reclaim()
	if s.retiredBytes.Load() != 0 {
		t.Fatal("released ahead of checkpoint retirement")
	}
	checkpointRetired.Store(true)
	syncs := 0
	sink.syncStore = func() error {
		syncs++
		if syncs == 1 {
			confirmReleasePoint(t, core, 1)
		}
		return nil
	}
	s.reclaim()
	if !first.retired.Load() || second.retired.Load() {
		t.Fatal("confirmation arriving during Sync crossed the durability fence")
	}
	if _, err := os.Stat(second.path); err != nil {
		t.Fatal("newly confirmed source disappeared before its next sync", err)
	}
	s.reclaim()
	if !second.retired.Load() || syncs != 2 {
		t.Fatal("confirmed prefix not released at next durability fence", syncs)
	}
}

func TestStartupSpoolReleaseSyncFailureIsLoud(t *testing.T) {
	defer zapwriter.Test()()
	s, core, sink := spoolReleaseFixture(t)
	writeReleasePoints(t, s, 3)
	for i := range 3 {
		confirmReleasePoint(t, core, i)
	}
	var fail atomic.Bool
	fail.Store(true)
	syncs := 0
	sink.syncStore = func() error {
		syncs++
		if fail.Load() {
			return errors.New("injected store sync failure")
		}
		return nil
	}
	s.reclaim()
	if s.retiredBytes.Load() != 0 || s.releasableBytes.Load() == 0 || s.releaseFailures.Load() != 1 {
		t.Fatal("sync failure lost release state")
	}
	if logs := zapwriter.TestString(); !strings.Contains(logs, "ERROR") || !strings.Contains(logs, "startup spool release failed") {
		t.Fatal("release failure not logged at ERROR", logs)
	}
	fail.Store(false)
	s.reclaim()
	if s.retiredBytes.Load() == 0 || s.releasableBytes.Load() != 0 || syncs != 2 {
		t.Fatal("release retry did not complete with one store fence per batch")
	}
}

func TestStartupSpoolReleaseRetryAfterUnlink(t *testing.T) {
	for _, stage := range []string{"unlink", "directory sync", "unlock"} {
		t.Run(stage, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "segment")
			f, err := os.Create(path)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			calls := 0
			seg := &startupSegment{file: f, path: path, unlock: func() error {
				calls++
				if stage == "unlock" {
					return errors.New("injected unlock failure")
				}
				return nil
			}}
			remove, syncDirectory := os.Remove, syncDir
			if stage == "unlink" {
				remove = func(string) error { return errors.New("injected unlink failure") }
			}
			if stage == "directory sync" {
				syncDirectory = func(string) error { return errors.New("injected directory sync failure") }
			}
			if err := seg.release(nil, remove, syncDirectory); err == nil {
				t.Fatal("injection did not fail")
			}
			if err := seg.release(nil, os.Remove, syncDir); err != nil {
				t.Fatal("interrupted release is not retryable", err)
			}
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("retry retained source", err)
			}
			if calls != 1 {
				t.Fatal("unlock repeated", calls)
			}
		})
	}
}

func TestStartupSpoolReleaseMetrics(t *testing.T) {
	s := &startupInput{sealed: make(chan struct{})}
	s.size.Store(1000)
	s.retiredBytes.Store(300)
	s.releasableBytes.Store(200)
	s.releaseFailures.Store(2)
	app := &App{}
	app.startupCaptureState.Store(s)
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(&migrationCollector{app: app})
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]float64{"startup_capture_retained_bytes": 700, "startup_capture_releasable_bytes": 200, "startup_capture_released_bytes_total": 300, "startup_capture_release_failures_total": 2}
	for _, f := range families {
		if value, ok := want[f.GetName()]; ok {
			m := f.Metric[0]
			got := m.GetGauge().GetValue()
			if strings.HasSuffix(f.GetName(), "_total") {
				if m.Counter == nil {
					t.Fatal("counter exported as gauge", f)
				}
				got = m.GetCounter().GetValue()
			}
			if got != value {
				t.Fatal("release metric", f.GetName(), got, value)
			}
			delete(want, f.GetName())
		}
	}
	if len(want) != 0 {
		t.Fatal("missing release metrics", want)
	}
}

func releaseCrashPoints() *points.Points {
	p := &points.Points{Metric: "release.crash"}
	bits := []uint64{0, 1 << 63, 0x7ff8000000000042, 0x7ff0000000000000, 0xfff0000000000000, 1, 0x3ff0000000000001}
	for i := range 64 {
		p.Add(math.Float64frombits(bits[i%len(bits)]), 1700000000+int64(i/3-i%2))
	}
	return p
}

func releaseCrashStore(t *testing.T, dir string) *store.Store {
	t.Helper()
	db, err := store.Open(dir, store.Options{ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20, Now: func() time.Time { return time.Unix(1700001000, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Create(context.Background(), store.MetricConfig{Name: "release.crash", Retentions: []store.Retention{{Step: 1, Count: 4096}}, AggregationMethod: store.Average}); err != nil && !errors.Is(err, store.ErrExists) {
		t.Fatal(err)
	}
	return db
}

func TestStartupSpoolReleaseCrashProcess(t *testing.T) {
	stage := os.Getenv("SPOOL_RELEASE_CRASH_STAGE")
	if stage == "" {
		return
	}
	root := os.Getenv("SPOOL_RELEASE_CRASH_ROOT")
	db := releaseCrashStore(t, filepath.Join(root, "store"))
	p := chooseStartupPolicy(1, 1<<30, 1<<30)
	p.segmentBytes = 1
	s, err := openStartupInputWithPolicy(filepath.Join(root, "input.1.2.tail.bin"), p)
	if err != nil {
		t.Fatal(err)
	}
	want := releaseCrashPoints()
	for _, point := range want.Data {
		if err := s.shards[0].Write(points.OnePoint(want.Metric, point.Value, point.Timestamp).AppendBinary(nil), 1); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.shards[0].sync(); err != nil {
		t.Fatal(err)
	}
	if stage == "before_replay" {
		os.Exit(99)
	}
	core := cache.New()
	core.SetMaxSize(0)
	s.startReplay(&startupSink{admission: core, add: core.AddRecoveredFrom, canRetire: func() bool { return true }, syncStore: db.Sync}, func() error { return nil })
	awaitCapture(t, func() bool { return s.replayedPoints.Load() == int64(len(want.Data)) })
	assertReleaseReplayBits(t, core.Get(want.Metric), want.Data)
	if stage == "after_replay_before_persist" {
		os.Exit(99)
	}
	batch, ok := core.PopNotConfirmed(want.Metric)
	if !ok {
		t.Fatal("no replay batch")
	}
	if err := db.UpdateMany(context.Background(), want.Metric, batch.Data); err != nil {
		t.Fatal(err)
	}
	core.Confirm(batch)
	if stage == "after_confirm_before_sync" {
		os.Exit(99)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	if stage == "after_persist" {
		os.Exit(99)
	}
	seg := s.shards[0].first.Load()
	remove := func(path string) error {
		err := os.Remove(path)
		if err == nil && stage == "after_unlink" {
			os.Exit(99)
		}
		return err
	}
	syncDirectory := func(path string) error {
		err := syncDir(path)
		if err == nil && stage == "after_directory_sync" {
			os.Exit(99)
		}
		return err
	}
	if err := seg.release(nil, remove, syncDirectory); err != nil {
		t.Fatal(err)
	}
	if stage == "after_release" {
		os.Exit(99)
	}
	t.Fatal("unknown crash stage", stage)
}

func TestStartupSpoolReleaseCrashBitDifferential(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	for _, stage := range []string{"before_replay", "after_replay_before_persist", "after_confirm_before_sync", "after_persist", "after_unlink", "after_directory_sync", "after_release"} {
		t.Run(stage, func(t *testing.T) {
			root := t.TempDir()
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStartupSpoolReleaseCrashProcess$")
			cmd.Env = append(os.Environ(), "SPOOL_RELEASE_CRASH_STAGE="+stage, "SPOOL_RELEASE_CRASH_ROOT="+root)
			out, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 99 {
				t.Fatalf("crash child: %v\n%s", err, out)
			}
			db := releaseCrashStore(t, filepath.Join(root, "store"))
			defer db.Close()
			files, err := filepath.Glob(filepath.Join(root, "*.capture-*.tail.bin"))
			if err != nil {
				t.Fatal(err)
			}
			core := cache.New()
			core.SetMaxSize(0)
			for _, path := range files {
				if _, err := recovery.ReadCaptureFile(path, core.Add); err != nil {
					t.Fatal(err)
				}
			}
			expected := releaseCrashPoints().Data
			if stage == "after_unlink" || stage == "after_directory_sync" || stage == "after_release" {
				expected = expected[1:]
			}
			assertReleaseReplayBits(t, core.Get("release.crash"), expected)
			if batch, ok := core.PopNotConfirmed("release.crash"); ok {
				if err := db.UpdateMany(ctx, batch.Metric, batch.Data); err != nil {
					t.Fatal(err)
				}
				core.Confirm(batch)
			}
			if err := db.Sync(); err != nil {
				t.Fatal(err)
			}
			got, err := db.Snapshot(ctx, "release.crash")
			if err != nil {
				t.Fatal(err)
			}
			ref := releaseCrashStore(t, filepath.Join(t.TempDir(), "reference"))
			defer ref.Close()
			want := releaseCrashPoints()
			if err := ref.UpdateMany(ctx, want.Metric, want.Data); err != nil {
				t.Fatal(err)
			}
			if err := ref.Sync(); err != nil {
				t.Fatal(err)
			}
			oracle, err := ref.Snapshot(ctx, want.Metric)
			if err != nil {
				t.Fatal(err)
			}
			if len(got.Archives) != len(oracle.Archives) {
				t.Fatal("archive shape")
			}
			for i, a := range got.Archives {
				b := oracle.Archives[i]
				if a.Retention != b.Retention || len(a.Points) != len(b.Points) {
					t.Fatal("archive shape", stage)
				}
				for j, p := range a.Points {
					if p.Timestamp != b.Points[j].Timestamp || math.Float64bits(p.Value) != math.Float64bits(b.Points[j].Value) {
						t.Fatal("crash replay differs at value bits/timestamp", stage, j, p, b.Points[j])
					}
				}
			}
		})
	}
}

func TestStartupSpoolReleaseCurrentConfirmedFile(t *testing.T) {
	s, core, _ := spoolReleaseFixture(t)
	s.policy.segmentBytes = 1 << 30
	writeReleasePoints(t, s, 1)
	sh := s.shards[0]
	old := sh.current.Load()
	s.rotateConfirmed()
	// FIFO appender fence joins the opportunistic rotation request.
	reply := make(chan bool, 1)
	sh.requests <- startupAppend{promoted: reply}
	<-reply
	if old.sealed.Load() {
		t.Fatal("unconfirmed append file rotated")
	}
	confirmReleasePoint(t, core, 0)
	s.rotateConfirmed()
	awaitCapture(t, func() bool { return old.sealed.Load() && sh.applied.Load().segment != old })
	s.reclaim()
	if !old.retired.Load() || s.retiredBytes.Load() != s.size.Load() {
		t.Fatal("confirmed current file retained until capture exit")
	}
	if s.direct.Load() {
		t.Fatal("fixture switched to direct input")
	}
	if err := sh.Write(points.OnePoint("release.continuing", 7, 1700000001).AppendBinary(nil), 1); err != nil {
		t.Fatal(err)
	}
	awaitCapture(t, func() bool { return s.replayedPoints.Load() == 2 })
}

func TestStartupSpoolReleaseKeepsEarlierJournalPin(t *testing.T) {
	s, core, sink := spoolReleaseFixture(t)
	s.pauseReplay()
	s.sink.Store(sink)
	root := s.legacy.first.Load()
	next := &startupSegment{source: s.legacy.stream.Next()}
	next.sealed.Store(true)
	next.replayed.Store(true)
	root.next.Store(next)
	core.AddRecoveredFrom(points.OnePoint("earlier.journal", 1, 1700000000), next.source)
	released := false
	s.releaseLog = func() error { released = true; return nil }
	s.reclaim()
	if !root.retired.Load() || released || s.releaseLog == nil {
		t.Fatal("partial history release unpinned an unconfirmed journal")
	}
	batch, ok := core.PopNotConfirmed("earlier.journal")
	if !ok {
		t.Fatal("missing journal batch")
	}
	core.Confirm(batch)
	s.reclaim()
	if !released || s.releaseLog != nil {
		t.Fatal("confirmed history retained journal pin")
	}
}

func assertReleaseReplayBits(t *testing.T, got, want []points.Point) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatal("replay lost or duplicated points", len(got), len(want))
	}
	for i, p := range got {
		if p.Timestamp != want[i].Timestamp || math.Float64bits(p.Value) != math.Float64bits(want[i].Value) {
			t.Fatal("replay changed value bits, timestamp or order", i)
		}
	}
}
