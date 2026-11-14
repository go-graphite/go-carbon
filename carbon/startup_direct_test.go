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
	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/stream/codec"
	"github.com/go-graphite/go-carbon/receiver/tcp"
	whisper "github.com/go-graphite/go-whisper"
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

// Count the actual writeout receipts before comparing closed/reopened archives:
// archive snapshots alone cannot reveal duplicate corrections at one timestamp.
func assertDirectStoreDifferential(t *testing.T, captured, ordinary *cache.Cache, name string, want []points.Point) {
	t.Helper()
	ctx := context.Background()
	options := store.Options{ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20, Now: func() time.Time { return time.Unix(1700001000, 0) }}
	write := func(core *cache.Cache) store.Snapshot {
		path := t.TempDir()
		db, err := store.Open(path, options)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if db != nil {
				_ = db.Close()
			}
		}()
		if _, err := db.Create(ctx, store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 4096}}, AggregationMethod: store.Average}); err != nil {
			t.Fatal(err)
		}
		batch, ok := core.PopNotConfirmed(name)
		if !ok || len(batch.Data) != len(want) {
			t.Fatal("missing/duplicate store receipt", ok)
		}
		for i, p := range batch.Data {
			if p.Timestamp != want[i].Timestamp || math.Float64bits(p.Value) != math.Float64bits(want[i].Value) {
				t.Fatal("store receipt bit/order mismatch", i)
			}
		}
		if err := db.UpdateMany(ctx, name, batch.Data); err != nil {
			t.Fatal(err)
		}
		if err := db.Sync(); err != nil {
			t.Fatal(err)
		}
		core.Confirm(batch)
		if _, ok := core.PopNotConfirmed(name); ok {
			t.Fatal("duplicate store batch")
		}
		err = db.Close()
		db = nil
		if err != nil {
			t.Fatal(err)
		}
		reopened, err := store.Open(path, options)
		if err != nil {
			t.Fatal(err)
		}
		defer reopened.Close()
		snapshot, err := reopened.Snapshot(ctx, name)
		if err != nil || len(snapshot.Archives) != 1 || len(snapshot.Archives[0].Points) == 0 {
			t.Fatal("missing stored archive", err)
		}
		return snapshot
	}
	got, reference := write(captured), write(ordinary)
	for i, archive := range got.Archives {
		other := reference.Archives[i]
		if archive.Retention != other.Retention || len(archive.Points) != len(other.Points) {
			t.Fatal("stored archive shape differs")
		}
		for j, p := range archive.Points {
			if p.Timestamp != other.Points[j].Timestamp || math.Float64bits(p.Value) != math.Float64bits(other.Points[j].Value) {
				t.Fatal("reopened store bits differ", i, j)
			}
		}
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
	assertDirectStoreDifferential(t, core, ordinary, metric, want)
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
	s.maybeDirect(time.Now())
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
	zapwriter.Test()
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
	s.maybeDirect(time.Now()) // Establish the stable-promotion observation.
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
	// This stop fixture deliberately retains a partial live batch so the dump
	// must cover it. Suppress automatic source draining only in this fixture;
	// the reclamation tests exercise the production drain policy separately.
	sink := *s.sink.Load()
	sink.drain = nil
	s.sink.Store(&sink)
	app.Cache.SetRecoveryDrain(false)
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
	assertDirectStoreDifferential(t, core, ordinary, name, want)
}

func TestStartupDirectStreamBitDifferential(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	s, app, core, address := directCaptureFixture(t)
	app.finishStartupInput(core)
	awaitCapture(t, func() bool { return allCaptureLive(s) })
	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	encoder, err := codec.NewEncoder(codec.Options{Flags: codec.FlagZstd})
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()
	wire, err := codec.AppendPreamble(nil, codec.FlagZstd)
	if err != nil {
		t.Fatal(err)
	}
	ordinary := cache.New()
	ordinary.SetMaxSize(0)
	bits := []uint64{0, 1 << 63, 0x7ff8000000000042, 0x7ff8000000000001, 0x7ff0000000000000, 0xfff0000000000000, 1, 0x7fefffffffffffff}
	const name = "stream.bits.straddle"
	for phase := range 2 {
		if phase == 1 {
			goDirect(t, s)
		}
		for i := phase * 256; i < (phase+1)*256; i++ {
			value, ts := math.Float64frombits(bits[i%len(bits)]), int64(1700000000+i/3-i%5)
			ordinary.Add(points.OnePoint(name, value, ts))
			if err := encoder.Add([]byte(name), value, ts); err != nil {
				t.Fatal(err)
			}
		}
		wire, err = encoder.Flush(wire)
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
		if _, err := conn.Write(wire); err != nil {
			t.Fatal(err)
		}
		wire = wire[:0]
		awaitCapture(t, func() bool { return core.Size() == int64((phase+1)*256) })
	}
	if err := app.syncInputLog(); err != nil {
		t.Fatal(err)
	}
	if app.inputLog.Load().Metrics().PointsWritten != 256 {
		t.Fatal("direct stream points were not journalled exactly once")
	}
	assertDirectStoreDifferential(t, core, ordinary, name, ordinary.Get(name))
}

func TestStartupDirectKeepsPendingHistory(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	s, app, core, _ := directCaptureFixture(t)
	const name = "pending.history.straddles"
	if err := s.Write(points.OnePoint(name, 0, 1).AppendBinary(nil), 1); err != nil {
		t.Fatal(err)
	}
	app.finishStartupInput(core)
	awaitCapture(t, func() bool { return allCaptureLive(s) })
	sh := s.shards[xxh3.HashString(name)%uint64(len(s.shards))]
	if err := sh.Write(points.OnePoint(name, 1, 1).AppendBinary(nil), 1); err != nil {
		t.Fatal(err)
	}
	awaitCapture(t, func() bool { return core.Size() == 2 && allCaptureLive(s) })
	if s.legacy.first.Load() == nil {
		t.Fatal("history fixture retired too early")
	}
	goDirect(t, s)
	s.sink.Load().directAdd(points.OnePoint(name, 2, 1))
	if err := app.syncInputLog(); err != nil {
		t.Fatal(err)
	}
	if s.legacy.first.Load() == nil || s.directFence.retired.Load() {
		t.Fatal("direct transition lost unpersisted history authority")
	}
	got := core.Get(name)
	if len(got) != 3 || got[0].Value != 0 || got[1].Value != 1 || got[2].Value != 2 {
		t.Fatal("history/capture/direct order", got)
	}
	p, ok := core.PopNotConfirmed(name)
	if !ok {
		t.Fatal("missing pending batch")
	}
	core.Confirm(p)
	s.reclaim()
	if s.legacy.first.Load() != nil || !s.directFence.retired.Load() || s.releaseLog != nil {
		t.Fatal("confirmed history/capture did not retire")
	}
}

func TestStartupDirectMonitorSwitchesWithoutManualControl(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	s, app, core, _ := directCaptureFixture(t)
	app.finishStartupInput(core)
	s.startMonitor(1 << 30)
	t.Cleanup(func() { s.receiver.Stop(); _ = s.close() })
	awaitCapture(t, func() bool { return s.direct.Load() })
	if time.Since(s.started) < startupDirectDelay {
		t.Fatal("monitor skipped hysteresis")
	}
}

func TestStartupDirectFenceRefusesUnsupportedRecovery(t *testing.T) {
	app := New("")
	app.Config = NewConfig()
	app.Config.Dump.Path = t.TempDir()
	path := filepath.Join(app.Config.Dump.Path, "input.1.2.tail.bin.capture-epoch-00000000000000000001"+startupDirectSuffix)
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := app.startStartupInput(); err == nil {
		t.Fatal("disabled journal silently ignored a sealed direct epoch")
	}
	app.Config.InputLog.Enabled = true
	app.Config.Tcp.Enabled = false
	if err := app.startStartupInput(); err == nil {
		t.Fatal("unsupported recovery silently reordered journal and capture")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal("refused recovery changed the retained boundary", err)
	}
}

func TestStartupDirectEmptyFenceAdoptsLegacyTail(t *testing.T) {
	app := New("")
	app.Config = NewConfig()
	cfg := app.Config
	cfg.Dump.Enabled, cfg.Dump.StoreInputHandoff, cfg.Dump.Path = true, true, t.TempDir()
	cfg.InputLog.Enabled = true
	cfg.Whisper.StorageBackend = "pebble-chunk"
	cfg.Tcp.Enabled, cfg.Tcp.Listen = true, "127.0.0.1:0"
	cfg.Udp.Enabled, cfg.Pickle.Enabled = false, false
	manifest := []byte(`{"cache":{"name":"cache.1.2.bin"},"wal":{"name":"input.1.2.bin"}}`)
	if err := os.WriteFile(filepath.Join(cfg.Dump.Path, recovery.ManifestName), manifest, 0600); err != nil {
		t.Fatal(err)
	}
	tail := filepath.Join(cfg.Dump.Path, "input.1.2.tail.bin")
	if err := os.WriteFile(tail, points.OnePoint("legacy", 1, 1).AppendBinary(nil), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tail+".capture-epoch-00000000000000000001"+startupDirectSuffix, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := app.startStartupInput(); err != nil {
		t.Fatal(err)
	}
	if app.startupInput == nil {
		t.Fatal("empty direct fence ignored despite a retained legacy tail")
	}
	app.startupInput.receiver.Stop()
	if err := app.startupInput.close(); err != nil {
		t.Fatal(err)
	}
}

func TestStartupDirectResetsEarlierJournalBeforeNextBoundary(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	first, app, core, _ := directCaptureFixture(t)
	app.Config.InputLog.TruncateLag = Duration{time.Hour}
	app.finishStartupInput(core)
	goDirect(t, first)
	for _, name := range []string{"reset.a", "reset.b"} {
		first.sink.Load().directAdd(points.OnePoint(name, 2, 1))
	}
	if err := app.syncInputLog(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"reset.a", "reset.b"} {
		p, _ := core.PopNotConfirmed(name)
		core.Confirm(p)
	}
	first.reclaim()
	first.receiver.Stop()
	if err := first.close(); err != nil {
		t.Fatal(err)
	}
	app.closeInputLog()

	next, app2, core2, _ := directCaptureFixture(t)
	app2.closeInputLog()
	app2.Config.InputLog.Path = app.Config.InputLog.Path
	app2.Config.InputLog.TruncateLag = Duration{time.Hour}
	if err := app2.openInputLog(core2); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"reset.a", "reset.b"} {
		sh := next.shards[xxh3.HashString(name)%uint64(len(next.shards))]
		if err := sh.Write(points.OnePoint(name, 3, 1).AppendBinary(nil), 1); err != nil {
			t.Fatal(err)
		}
	}
	app2.finishStartupInput(core2)
	awaitCapture(t, func() bool { return allCaptureLive(next) && core2.Size() == 4 })
	for _, name := range []string{"reset.a", "reset.b"} {
		p, ok := core2.PopNotConfirmed(name)
		if !ok || len(p.Data) != 2 || p.Data[0].Value != 2 || p.Data[1].Value != 3 {
			t.Fatal("journal/new capture order", name, p)
		}
		core2.Confirm(p)
	}
	next.reclaim()
	goDirect(t, next)
	// No new direct update to b: replaying the earlier journal after the
	// new capture would otherwise roll its stored value back from 3 to 2.
	next.sink.Load().directAdd(points.OnePoint("reset.a", 4, 1))
	if err := app2.syncInputLog(); err != nil {
		t.Fatal(err)
	}
	next.receiver.Stop()
	if err := next.close(); err != nil {
		t.Fatal(err)
	}
	app2.closeInputLog()
	log, err := inputlog.Open(app2.Config.InputLog.Path, inputlog.Options{SyncInterval: time.Second, SegmentSize: 16 << 20, MaxAge: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	var got []*points.Points
	if _, err := log.ReplayStopped(func(p *points.Points) { got = append(got, p.Copy()) }, nil); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Metric != "reset.a" || len(got[0].Data) != 1 || got[0].Data[0].Value != 4 {
		t.Fatal("new boundary retained stale earlier corrections", got)
	}
}

func TestStartupDirectDoesNotReuseEarlierEpochPin(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	first, app, core, _ := directCaptureFixture(t)
	app.finishStartupInput(core)
	awaitCapture(t, func() bool { return allCaptureLive(first) })
	if err := first.shards[0].Write(points.OnePoint("pin.metric", 1, 1).AppendBinary(nil), 1); err != nil {
		t.Fatal(err)
	}
	awaitCapture(t, func() bool { return core.Size() == 1 && allCaptureLive(first) })
	goDirect(t, first)
	first.sink.Load().directAdd(points.OnePoint("pin.metric", 2, 1))
	if err := app.syncInputLog(); err != nil {
		t.Fatal(err)
	}
	first.receiver.Stop()
	if err := first.close(); err != nil {
		t.Fatal(err)
	}
	app.closeInputLog()

	next, err := openStartupInputWithPolicy(first.path, first.policy)
	if err != nil {
		t.Fatal(err)
	}
	opts := tcp.NewOptions()
	opts.Listen = "127.0.0.1:0"
	h, bound, err := listenStartupCapture(opts)
	if err != nil {
		t.Fatal(err)
	}
	next.receiver, err = tcp.NewInheritedCapture("tcp", bound, h, next)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { next.receiver.Stop(); _ = next.close() })
	core2 := cache.New()
	core2.SetMaxSize(0)
	app2 := New("")
	app2.Config = NewConfig()
	app2.Config.InputLog = app.Config.InputLog
	app2.startupInput = next
	if err := app2.openInputLog(core2); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(app2.closeInputLog)
	if err := next.shards[0].Write(points.OnePoint("pin.metric", 3, 1).AppendBinary(nil), 1); err != nil {
		t.Fatal(err)
	}
	app2.finishStartupInput(core2)
	awaitCapture(t, func() bool { return allCaptureLive(next) && core2.Size() == 3 })
	if next.releaseLog == nil {
		t.Fatal("reopen did not retain the earlier journal")
	}
	now := time.Now()
	next.maybeDirect(now)
	next.maybeDirect(now.Add(startupDirectDelay))
	if next.direct.Load() || next.directFence != nil {
		t.Fatal("reused a previous epoch's journal pin for a new boundary")
	}
	if !core2.WriteoutState().Draining {
		t.Fatal("earlier pinned epoch did not request a persistence drain")
	}
	p, ok := core2.PopNotConfirmed("pin.metric")
	if !ok || len(p.Data) != 3 || p.Data[0].Value != 1 || p.Data[1].Value != 2 || p.Data[2].Value != 3 {
		t.Fatal("capture/journal/capture FIFO", p)
	}
	core2.Confirm(p)
	next.reclaim()
	if next.releaseLog != nil || next.legacy.first.Load() != nil {
		t.Fatal("confirmed earlier epoch did not release its pin")
	}
	next.liveSince = time.Time{}
	goDirect(t, next)
	if core2.WriteoutState().Draining {
		t.Fatal("direct input retained the temporary recovery drain")
	}
}

func TestStartupDirectHysteresisDetectsPromotionBetweenSamples(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	s, app, core, _ := directCaptureFixture(t)
	app.finishStartupInput(core)
	awaitCapture(t, func() bool { return allCaptureLive(s) })
	s.reclaim()
	now := time.Now()
	s.maybeDirect(now)
	sh := s.shards[0]
	before := sh.promoted.Load()
	sh.live.Store(false)
	awaitCapture(t, func() bool { return sh.live.Load() && sh.promoted.Load() > before })
	s.maybeDirect(now.Add(startupDirectDelay))
	if s.direct.Load() {
		t.Fatal("a new promotion between monitor samples bypassed hysteresis")
	}
	s.maybeDirect(now.Add(2 * startupDirectDelay))
	if !s.direct.Load() {
		t.Fatal("stable promotion did not permit transition")
	}
}

// Recovery-only writes need names in the saved file index when Buckyd/catalog
// discovery is disabled. Keep the crash oracle about durability and ordering.
func primeStartupDirectIndex(t *testing.T, cfg *Config) {
	t.Helper()
	for _, name := range []string{"handoff/metric.wsp", "tail/only.wsp"} {
		path := filepath.Join(cfg.Whisper.DataDir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
			t.Fatal(err)
		}
		w, err := whisper.CreateWithOptions(path, whisper.NewRetentionsNoPointer([]whisper.Retention{whisper.NewRetention(1, 120), whisper.NewRetention(5, 120), whisper.NewRetention(30, 120)}), whisper.Average, .5, &whisper.Options{Compressed: true, OutOfOrder: true, FLock: true})
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func convertStartupDirectIndex(t *testing.T, app *App) {
	t.Helper()
	for _, name := range []string{"handoff.metric", "tail.only"} {
		if _, err := app.Persister.ConvertFile(t.Context(), name); err != nil {
			t.Fatal(err)
		}
	}
}
