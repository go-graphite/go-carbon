//go:build unix

package carbon

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/tcp"
	"github.com/lomik/zapwriter"
	"github.com/zeebo/xxh3"
)

func directCaptureFixture(t testing.TB) (*startupInput, *App, *cache.Cache, string) {
	t.Helper()
	s, address := captureFixture(t)
	core := cache.New()
	core.SetMaxSize(0)
	core.SetRecoveryMemory(func() (uint64, bool) { return 24 << 30, true })
	app := New("")
	app.Config = NewConfig()
	app.Config.InputLog.Enabled = true
	app.Config.InputLog.Path = filepath.Join(t.TempDir(), "journal")
	app.Config.InputLog.SegmentSize = 16 << 20
	app.startupInput = s
	if err := app.openInputLog(core); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { app.closeInputLog() })
	return s, app, core, address
}

func allCaptureLive(s *startupInput) bool {
	if !s.historyReady.Load() {
		return false
	}
	for _, sh := range s.shards {
		if !sh.live.Load() {
			return false
		}
	}
	return true
}

func goDirect(t testing.TB, s *startupInput) {
	t.Helper()
	awaitCapture(t, func() bool { return allCaptureLive(s) })
	s.reclaim()
	now := time.Now()
	s.maybeDirect(now)
	if s.direct.Load() {
		t.Fatal("transition ignored hysteresis")
	}
	s.maybeDirect(now.Add(startupDirectDelay - time.Millisecond))
	if s.direct.Load() {
		t.Fatal("transition ignored stable-live delay")
	}
	s.maybeDirect(now.Add(startupDirectDelay))
	if !s.direct.Load() {
		t.Fatal("capture did not switch", zapwriter.TestString())
	}
}

func TestStartupDirectBitDifferential(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	s, app, core, address := directCaptureFixture(t)
	ordinary := cache.New()
	ordinary.SetMaxSize(0)
	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// The ordinary plaintext parser rejects NaN. Arbitrary NaN payloads are
	// covered at the binary capture/cache boundary below.
	bits := []uint64{0, 1 << 63, 0x7ff0000000000000, 0xfff0000000000000, 1, 0x7fefffffffffffff}
	const metric = "capture.straddles.switch"
	write := func(first, count int) {
		for i := first; i < first+count; i++ {
			p := points.OnePoint(metric, math.Float64frombits(bits[i%len(bits)]), int64(1700000000+i/3-i%2))
			if _, err := fmt.Fprintf(conn, "%s %g %d\n", p.Metric, p.Data[0].Value, p.Data[0].Timestamp); err != nil {
				t.Fatal(err)
			}
			ordinary.Add(p)
		}
		if first == 0 {
			awaitCapture(t, func() bool { return s.points.Load() == int64(count) })
		} else {
			awaitCapture(t, func() bool { return core.Size() == int64(first+count) })
		}
	}
	write(0, 128)
	app.finishStartupInput(core)
	awaitCapture(t, func() bool { return core.Size() == 128 })
	write(128, 128)
	goDirect(t, s)
	control := New("")
	control.Config = NewConfig()
	control.Config.InputLog.Enabled = true
	control.Config.InputLog.Path = filepath.Join(t.TempDir(), "ordinary-journal")
	control.Config.InputLog.SegmentSize = 16 << 20
	ordinary.EnableInputLogFromNow()
	if err := control.openInputLog(ordinary); err != nil {
		t.Fatal(err)
	}
	defer control.closeInputLog()
	physical, captured, replayed := s.size.Load(), s.points.Load(), s.replayedPoints.Load()
	write(256, 256)
	if s.size.Load() != physical || s.points.Load() != captured || s.replayedPoints.Load() != replayed || captured != replayed {
		t.Fatal("capture continued after switch")
	}
	if err := app.syncInputLog(); err != nil {
		t.Fatal(err)
	}
	if got := app.inputLog.Load().Metrics().PointsWritten; got != 256 {
		t.Fatal("direct points not journalled exactly once", got)
	}
	if err := control.syncInputLog(); err != nil {
		t.Fatal(err)
	}
	if got, want := app.inputLog.Load().Metrics().BytesWritten, control.inputLog.Load().Metrics().BytesWritten; got != want {
		t.Fatal("journal bytes differ from a fresh ordinary writer", got, want)
	}
	got, want := core.Get(metric), ordinary.Get(metric)
	if len(got) != len(want) || len(got) != 512 {
		t.Fatal("missing or duplicate input", len(got), len(want))
	}
	h := sha256.New()
	var raw [16]byte
	for i, p := range got {
		if p.Timestamp != want[i].Timestamp || math.Float64bits(p.Value) != math.Float64bits(want[i].Value) {
			t.Fatal("bit/order differential", i, p, want[i])
		}
		binary.LittleEndian.PutUint64(raw[:8], math.Float64bits(p.Value))
		binary.LittleEndian.PutUint64(raw[8:], uint64(p.Timestamp))
		h.Write(raw[:])
	}
	// A final segment with pending captured points must survive direct input.
	s.reclaim()
	paths, _ := filepath.Glob(s.path + ".capture-*.tail.bin")
	if len(paths) < 2 || s.directFence.retired.Load() {
		t.Fatal("unpersisted source/fence retired")
	}
	p, ok := core.PopNotConfirmed(metric)
	if !ok {
		t.Fatal("missing batch")
	}
	core.Confirm(p)
	s.reclaim()
	paths, _ = filepath.Glob(s.path + ".capture-*.tail.bin")
	if len(paths) != 0 || s.releaseLog != nil {
		t.Fatal("persisted epoch did not fully retire", paths)
	}
	t.Logf("points=512 exact_bits_order_sha256=%x captured=%d direct_logged=256 spool_bytes_after_switch=%d", h.Sum(nil), captured, physical)
}

func TestStartupDirectDisabledAndAdmission(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	s, app, core, address := directCaptureFixture(t)
	app.Config.InputLog.Enabled = false
	app.finishStartupInput(core)
	awaitCapture(t, func() bool { return allCaptureLive(s) })
	s.reclaim()
	for i := range 120 {
		s.maybeDirect(time.Now().Add(time.Duration(i) * time.Second))
	}
	if s.direct.Load() {
		t.Fatal("switched without configured journal")
	}
	// Reattach only in the test, before any transition, to test the normal
	// live overflow boundary against accepted recovery records.
	sink := s.sink.Load()
	sinkCopy := *sink
	log := app.inputLog.Load()
	sinkCopy.startLog = func() (func() error, error) { _, err := log.PinCopy(); return log.ReleaseCopy, err }
	s.sink.Store(&sinkCopy)
	s.liveSince = time.Time{}
	throttled := make(chan struct{}, 1)
	core.SetThrottle(func(*points.Points, bool) bool {
		throttled <- struct{}{}
		return true
	})
	name := "admission.straddles"
	sh := s.shards[xxh3.HashString(name)%uint64(len(s.shards))]
	for i := range 8 {
		if err := sh.Write(points.OnePoint(name, float64(i), 1).AppendBinary(nil), 1); err != nil {
			t.Fatal(err)
		}
	}
	awaitCapture(t, func() bool { return core.Size() == 8 && allCaptureLive(s) })
	core.SetMaxSize(4)
	goDirect(t, s)
	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintf(conn, "%s 99 1\n", name); err != nil {
		t.Fatal(err)
	}
	conn.Close()
	awaitCapture(t, func() bool {
		var received float64
		s.receiver.Stat(func(n string, v float64) {
			if n == "metricsReceived" {
				received = v
			}
		})
		return received > 0
	})
	if core.Size() != 8 {
		t.Fatal("direct input bypassed ordinary throttle", core.Size())
	}
	select {
	case <-throttled:
	default:
		t.Fatal("ordinary throttle was not consulted")
	}
}

func TestStartupDirectRefusesUnappliedFrontier(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	s, app, core, _ := directCaptureFixture(t)
	app.finishStartupInput(core)
	awaitCapture(t, func() bool { return allCaptureLive(s) })
	s.reclaim()
	sh := s.shards[0]
	// Stop replay but retain live=true to model a stale monitor observation.
	close(s.replayStop)
	<-s.replayDone
	if err := sh.Write(points.OnePoint("frontier", 1, 1).AppendBinary(nil), 1); err != nil {
		t.Fatal(err)
	}
	// On the original append-applies-live path it is already applied. Make
	// the observed applied frontier stale; the fence must inspect the owner.
	sh.applied.Store(&startupPosition{segment: sh.current.Load()})
	s.liveSince = time.Now().Add(-startupDirectDelay)
	s.maybeDirect(time.Now())
	if s.direct.Load() {
		t.Fatal("accepted an unapplied frontier")
	}
	for _, shard := range s.shards {
		if shard.current.Load().sealed.Load() {
			t.Fatal("refusal partially sealed the capture")
		}
	}
	if _, err := os.Stat(s.path + fmt.Sprintf(".capture-epoch-%020d%s", s.epoch, startupDirectSuffix)); !os.IsNotExist(err) {
		t.Fatal("refusal published a fence", err)
	}
}

func directCrashHasInput(stage string) bool {
	return stage == "direct_after_switch" || stage == "direct_partial_retirement" || stage == "direct_full_retirement" || stage == "direct_next_epoch"
}

func directNextEpochPoints(stamp int64) []*points.Points {
	return []*points.Points{points.OnePoint("handoff.metric", 8, stamp-2), points.OnePoint("tail.only", 13, stamp)}
}

// This child exits without defers at each boundary. The existing seven-stage
// archive/readback oracle drives it through the real cold App.Start path.
func startupDirectCrashChild(t *testing.T, stage string, stamp int64) {
	defer zapwriter.Test()()
	app := startIncrementalApp(t, os.Getenv("HANDOFF_CRASH_CONFIG"))
	if app.pendingDone != nil {
		select {
		case <-app.pendingDone:
		case <-time.After(5 * time.Second):
			t.Fatal("initial checkpoint did not finish")
		}
	}
	awaitCapture(t, func() bool { return app.Cache.IsEmpty() && app.Cache.PendingOutstanding() == 0 })
	app.stopPendingRecovery()
	app.Persister.Stop()
	app.Persister = nil
	if err := app.syncInputLog(); err != nil {
		t.Fatal(err)
	}
	before, err := app.MetricStore.Snapshot(context.Background(), "handoff.metric")
	if err != nil || len(before.Archives) == 0 || len(before.Archives[0].Points) != 2 {
		t.Fatalf("initial checkpoint missing before capture: %+v %v\n%s", before.Archives, err, zapwriter.TestString())
	}
	original := app.Receivers[0].Receiver.(*tcp.TCP)
	address := original.Addr().String()
	h, err := original.Detach()
	if err != nil {
		t.Fatal(err)
	}
	p := chooseStartupPolicy(2, 1<<30, 1<<30)
	p.shards = 2
	s, err := openStartupInputWithPolicy(os.Getenv("HANDOFF_CRASH_TAIL"), p)
	if err != nil {
		t.Fatal(err)
	}
	opts := *app.Config.Tcp
	opts.Listen = address
	s.receiver, err = tcp.NewInheritedCapture("tcp", &opts, h, s)
	if err != nil {
		t.Fatal(err)
	}
	app.Receivers[0].Receiver = s.receiver
	app.startupInput = s
	app.finishStartupInput(app.Cache)
	awaitCapture(t, func() bool { return allCaptureLive(s) })
	s.reclaim()
	for i, ps := range startupPoints(stamp, 1) {
		if err := s.shards[i].Write(ps.AppendBinary(nil), len(ps.Data)); err != nil {
			t.Fatal(err)
		}
	}
	awaitCapture(t, func() bool { return app.Cache.Size() == 3 && allCaptureLive(s) })
	for _, sh := range s.shards {
		if err := sh.sync(); err != nil {
			t.Fatal(err)
		}
	}
	if stage == "direct_before_switch" {
		os.Exit(99)
	}
	if stage == "direct_during_seal" {
		_ = s.receiver.SwitchCapture(app.Cache.Add, func() error {
			for _, sh := range s.shards {
				if err := sh.fence(false); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.shards[0].fence(true); err != nil {
				t.Fatal(err)
			}
			os.Exit(99)
			return nil
		})
	}
	if stage == "direct_after_fence" {
		_ = s.receiver.SwitchCapture(app.Cache.Add, func() error {
			if err := s.sealForDirect(); err != nil {
				t.Fatal(err)
			}
			os.Exit(99)
			return nil
		})
	}
	goDirect(t, s)
	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	for _, ps := range startupPoints(stamp, 2) {
		for _, d := range ps.Data {
			if _, err := fmt.Fprintf(conn, "%s %g %d\n", ps.Metric, d.Value, d.Timestamp); err != nil {
				t.Fatal(err)
			}
		}
	}
	conn.Close()
	awaitCapture(t, func() bool { return app.Cache.Size() == 7 })
	if err := app.syncInputLog(); err != nil {
		t.Fatal(err)
	}
	if stage == "direct_after_switch" {
		os.Exit(99)
	}
	if stage == "direct_next_epoch" {
		// Stop without a dump, then accept newer captured corrections before
		// starting storage/history. A second crash must keep journal between
		// these two capture epochs, rather than at either end of the sequence.
		app.Stop()
		next := New(os.Getenv("HANDOFF_CRASH_CONFIG"))
		if err := next.ParseConfig(); err != nil {
			t.Fatal(err)
		}
		if err := next.startStartupInput(); err != nil {
			t.Fatal(err)
		}
		if next.startupInput == nil {
			t.Fatal("did not reopen capture")
		}
		for _, ps := range directNextEpochPoints(stamp) {
			sh := next.startupInput.shards[xxh3.HashString(ps.Metric)%uint64(len(next.startupInput.shards))]
			if err := sh.Write(ps.AppendBinary(nil), len(ps.Data)); err != nil {
				t.Fatal(err)
			}
			if err := sh.sync(); err != nil {
				t.Fatal(err)
			}
		}
		os.Exit(99)
	}
	for i, name := range []string{"handoff.metric", "tail.only"} {
		batch, ok := app.Cache.PopNotConfirmed(name)
		if !ok {
			t.Fatal("missing store batch", name)
		}
		if _, err := app.MetricStore.Create(context.Background(), store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 120}, {Step: 5, Count: 120}, {Step: 30, Count: 120}}, AggregationMethod: store.Average, XFilesFactor: .5}); err != nil && !errors.Is(err, store.ErrExists) {
			t.Fatal(err)
		}
		if err := app.MetricStore.UpdateMany(context.Background(), name, batch.Data); err != nil {
			t.Fatal(err)
		}
		if err := app.MetricStore.Flush(); err != nil {
			t.Fatal(err)
		}
		app.Cache.Confirm(batch)
		s.reclaim()
		if i == 0 && stage == "direct_partial_retirement" {
			if s.directFence.retired.Load() {
				t.Fatal("terminal fence retired ahead of source")
			}
			// Advance several zero-lag cycles after confirming the correction.
			// Retention must still preserve it for the mixed older epoch.
			for range 3 {
				if err := app.syncInputLog(); err != nil {
					t.Fatal(err)
				}
			}
			os.Exit(99)
		}
	}
	if !s.directFence.retired.Load() {
		t.Fatal("full retirement left its epoch fence")
	}
	os.Exit(99)
}

// Install capture on a running app to exercise its complete ordinary stop and
// actual socket handoff after the switch, including still-unpersisted sources.
func installDirectCaptureForTest(t *testing.T, app *App) *startupInput {
	t.Helper()
	original := app.Receivers[0].Receiver.(*tcp.TCP)
	address := original.Addr().String()
	h, err := original.Detach()
	if err != nil {
		t.Fatal(err)
	}
	s, err := openStartupInput(filepath.Join(app.Config.Dump.Path, "input.1.2.tail.bin"))
	if err != nil {
		t.Fatal(err)
	}
	opts := *app.Config.Tcp
	opts.Listen = address
	s.receiver, err = tcp.NewInheritedCapture("tcp", &opts, h, s)
	if err != nil {
		t.Fatal(err)
	}
	app.Receivers[0].Receiver = s.receiver
	app.startupInput = s
	app.startupCaptureState.Store(s)
	app.finishStartupInput(app.Cache)
	awaitCapture(t, func() bool { return allCaptureLive(s) })
	if err := app.Cache.SetWriteoutBatching(1000000, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := s.shards[0].Write(points.OnePoint("capture.stop.seed", 1, time.Now().Unix()-1).AppendBinary(nil), 1); err != nil {
		t.Fatal(err)
	}
	awaitCapture(t, func() bool { return allCaptureLive(s) && s.points.Load() == s.replayedPoints.Load() })
	goDirect(t, s)
	if s.shards[0].current.Load().source.Pending() == 0 {
		t.Fatal("stop fixture must retain an unpersisted capture source")
	}
	return s
}

func TestStartupDirectBinaryBitDifferential(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	s, app, core, _ := directCaptureFixture(t)
	app.finishStartupInput(core)
	awaitCapture(t, func() bool { return allCaptureLive(s) })
	ordinary := cache.New()
	ordinary.SetMaxSize(0)
	bits := []uint64{0, 1 << 63, 0x7ff8000000000042, 0x7ff8000000000001, 0x7ff0000000000000, 0xfff0000000000000, 1, 0x7fefffffffffffff}
	const name = "binary.bits.straddle"
	sh := s.shards[xxh3.HashString(name)%uint64(len(s.shards))]
	for i := range 512 {
		if i == 256 {
			goDirect(t, s)
		}
		p := points.OnePoint(name, math.Float64frombits(bits[i%len(bits)]), int64(1700000000+i/3-i%5))
		ordinary.Add(p)
		if s.direct.Load() {
			s.sink.Load().directAdd(p)
		} else if err := sh.Write(p.AppendBinary(nil), 1); err != nil {
			t.Fatal(err)
		}
	}
	awaitCapture(t, func() bool { return core.Size() == 512 })
	got, want := core.Get(name), ordinary.Get(name)
	if len(got) != 512 {
		t.Fatal("lost or duplicated binary points", len(got))
	}
	for i, p := range got {
		if p.Timestamp != want[i].Timestamp || math.Float64bits(p.Value) != math.Float64bits(want[i].Value) {
			t.Fatal("NaN/float bit or FIFO mismatch", i)
		}
	}
	if err := app.syncInputLog(); err != nil {
		t.Fatal(err)
	}
	if app.inputLog.Load().Metrics().PointsWritten != 256 {
		t.Fatal("binary direct points were not journalled")
	}
}
