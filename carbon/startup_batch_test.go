//go:build unix

package carbon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
	"github.com/klauspost/compress/zstd"
)

func batchReplayOwner() *startupInput {
	return &startupInput{policy: startupCapturePolicy{readBuffer: 4 << 20}, stop: make(chan struct{}), replayStop: make(chan struct{})}
}

func TestCaptureBatchStoredDifferential(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	defer enc.Close()
	bits := []uint64{0, 1 << 63, 1, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000042, 0x3ff0000000000001}
	var epochs [3][]byte
	wants := map[string][]points.Point{}
	for i := range 192 {
		var raw []byte
		for j := range 4 {
			name := fmt.Sprintf("batch.metric.%d", j)
			p := points.OnePoint(name, math.Float64frombits(bits[(i+j)%len(bits)]), 1700000000+int64(i%23))
			// Include multi-point records and same-record corrections too.
			if i%5 == 0 {
				p.Add(math.Float64frombits(bits[(i+j+1)%len(bits)]), p.Data[0].Timestamp)
			}
			wants[name] = append(wants[name], p.Data...)
			raw = p.AppendBinary(raw)
		}
		n := 0
		_ = points.ReadBinaryBytesBorrowed(raw, func(_ []byte, p []points.Point) { n += len(p) })
		epochs[i/64] = append(epochs[i/64], recovery.EncodeCaptureFrame(enc, raw, nil, n)...)
	}
	fast, ref := cache.New(), cache.New()
	fast.SetMaxSize(0)
	fast.SetRecoveryMemory(func() (uint64, bool) { return 1 << 30, true })
	ref.SetMaxSize(0)
	var fastStream, refStream cache.RecoveryStream
	for _, epoch := range epochs {
		sink := &startupSink{admission: fast, add: fast.AddRecoveredFrom, groupedAdd: fast.AddRecoveryMetric}
		source := fastStream.Next()
		cursor := int64(0)
		_, err := batchReplayOwner().replayCaptureBatches(bytes.NewReader(epoch), source, sink, func(end int64, _ int) {
			if end <= cursor {
				t.Fatal("cursor did not advance")
			}
			cursor = end
		})
		if err != nil || cursor != int64(len(epoch)) {
			t.Fatal("replay/source frontier", cursor, err, source.Pending())
		}
		rs := refStream.Next()
		sh := &startupShard{}
		_, err = recovery.ReadCaptureFrames(bytes.NewReader(epoch), func(raw []byte, _ int) error {
			return sh.apply(raw, rs, &startupSink{admission: ref, add: ref.AddRecoveredFrom})
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for name, want := range wants {
		assertDirectStoreDifferential(t, fast, ref, name, want)
	}
}

func TestCaptureBatchTagAliasOrder(t *testing.T) {
	core := cache.New()
	core.SetMaxSize(0)
	core.SetTagsEnabled(true)
	names := []string{"metric;b=2;a=1", "other", "metric;a=1;b=2", "other", "metric;b=2;a=1"}
	var raw []byte
	for i, name := range names {
		raw = points.OnePoint(name, float64(i), 7).AppendBinary(raw)
	}
	sink := &startupSink{admission: core, add: core.AddRecoveredFrom, groupedAdd: core.AddRecoveryMetric}
	var b captureBatch
	if err := b.prepare(raw, sink); err != nil {
		t.Fatal(err)
	}
	var stream cache.RecoveryStream
	b.apply(stream.Next(), sink, false)
	got := core.Get("metric;a=1;b=2")
	if len(got) != 3 || got[0].Value != 0 || got[1].Value != 2 || got[2].Value != 4 {
		t.Fatal(got)
	}
}

func TestCaptureBatchCrashProcess(t *testing.T) {
	root := os.Getenv("CAPTURE_BATCH_CRASH_ROOT")
	if root == "" {
		return
	}
	db := releaseCrashStore(t, filepath.Join(root, "store"))
	if _, err := db.Create(context.Background(), store.MetricConfig{Name: "crash.other", Retentions: []store.Retention{{Step: 1, Count: 4096}}, AggregationMethod: store.Average}); err != nil {
		t.Fatal(err)
	}
	core := cache.New()
	core.SetMaxSize(0)
	var stream cache.RecoveryStream
	source := stream.Next()
	wire, err := os.ReadFile(filepath.Join(root, "capture"))
	if err != nil {
		t.Fatal(err)
	}
	sink := &startupSink{admission: core, add: core.AddRecoveredFrom}
	sink.groupedAdd = func(m cache.RecoveryMetric, p *points.Points, source *cache.RecoverySource, live bool) {
		core.AddRecoveryMetric(m, p, source, live)
		if source.Pending() == 0 {
			t.Fatal("source released before apply completed")
		}
		batch, ok := core.PopNotConfirmed(m.Name())
		if !ok {
			t.Fatal("missing partial group")
		}
		if err := db.UpdateMany(context.Background(), m.Name(), batch.Data); err != nil {
			t.Fatal(err)
		}
		core.Confirm(batch)
		if err := db.Sync(); err != nil {
			t.Fatal(err)
		}
		os.Exit(99) // Unacknowledged window/source remains; no deferred cleanup.
	}
	_, _ = batchReplayOwner().replayCaptureBatches(bytes.NewReader(wire), source, sink, func(int64, int) { _ = os.WriteFile(filepath.Join(root, "advanced"), []byte("unsafe"), 0600) })
	t.Fatal("crash hook not reached")
}

func TestCaptureBatchCrashMidFrameStoredDifferential(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	root := t.TempDir()
	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	defer enc.Close()
	var raw []byte
	bits := []uint64{0, 1 << 63, 1, 0x7ff8000000000042, 0x7ff0000000000000, 0xfff0000000000000}
	for i := range 90 {
		for _, name := range []string{"release.crash", "crash.other"} {
			raw = points.OnePoint(name, math.Float64frombits(bits[i%len(bits)]), 1700000000+int64(i%17)).AppendBinary(raw)
		}
	}
	wire := recovery.EncodeCaptureFrame(enc, raw, nil, 180)
	if err := os.WriteFile(filepath.Join(root, "capture"), wire, 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestCaptureBatchCrashProcess$")
	cmd.Env = append(os.Environ(), "CAPTURE_BATCH_CRASH_ROOT="+root)
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 99 {
		t.Fatalf("child did not crash as expected: %v %s", err, out)
	}
	if _, err := os.Stat(filepath.Join(root, "advanced")); !os.IsNotExist(err) {
		t.Fatal("source cursor advanced past unapplied group", err)
	}
	actual := releaseCrashStore(t, filepath.Join(root, "store"))
	reference := releaseCrashStore(t, filepath.Join(root, "reference"))
	for _, db := range []*store.Store{reference} {
		if _, err := db.Create(context.Background(), store.MetricConfig{Name: "crash.other", Retentions: []store.Retention{{Step: 1, Count: 4096}}, AggregationMethod: store.Average}); err != nil {
			t.Fatal(err)
		}
	}
	core, ref := cache.New(), cache.New()
	core.SetMaxSize(0)
	ref.SetMaxSize(0)
	var stream cache.RecoveryStream
	sink := &startupSink{admission: core, add: core.AddRecoveredFrom, groupedAdd: core.AddRecoveryMetric}
	_, err = batchReplayOwner().replayCaptureBatches(bytes.NewReader(wire), stream.Next(), sink, func(int64, int) {})
	if err != nil {
		t.Fatal(err)
	}
	if err := points.ReadBinaryBytesBorrowed(raw, func(name []byte, data []points.Point) {
		ref.AddRecovered(&points.Points{Metric: string(name), Data: data})
	}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"release.crash", "crash.other"} {
		for i, c := range []*cache.Cache{core, ref} {
			p, ok := c.PopNotConfirmed(name)
			if !ok {
				t.Fatal("missing", name)
			}
			db := actual
			if i == 1 {
				db = reference
			}
			if err := db.UpdateMany(context.Background(), name, p.Data); err != nil {
				t.Fatal(err)
			}
			c.Confirm(p)
		}
	}
	if err := actual.Close(); err != nil {
		t.Fatal(err)
	}
	if err := reference.Close(); err != nil {
		t.Fatal(err)
	}
	actual = releaseCrashStore(t, filepath.Join(root, "store"))
	defer actual.Close()
	reference = releaseCrashStore(t, filepath.Join(root, "reference"))
	defer reference.Close()
	for _, name := range []string{"release.crash", "crash.other"} {
		got, err := actual.Snapshot(context.Background(), name)
		if err != nil {
			t.Fatal(err)
		}
		want, err := reference.Snapshot(context.Background(), name)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Archives) != len(want.Archives) {
			t.Fatal("archive shape")
		}
		for i, a := range got.Archives {
			other := want.Archives[i]
			if a.Retention != other.Retention || len(a.Points) != len(other.Points) {
				t.Fatal("archive shape")
			}
			for j, p := range a.Points {
				if p.Timestamp != other.Points[j].Timestamp || math.Float64bits(p.Value) != math.Float64bits(other.Points[j].Value) {
					t.Fatal("crash changed stored bits", name, i, j)
				}
			}
		}
	}
}

func TestCaptureBatchSplitsAtHardAdmissionLimit(t *testing.T) {
	core := cache.New()
	core.SetMaxSize(4)
	core.SetRecoveryMemory(func() (uint64, bool) { return 1 << 30, true })
	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	defer enc.Close()
	var wire []byte
	for i := range 32 {
		p := points.OnePoint("bounded.group", float64(i), int64(i))
		wire = append(wire, recovery.EncodeCaptureFrame(enc, p.AppendBinary(nil), nil, 1)...)
	}
	var stream cache.RecoveryStream
	sink := &startupSink{admission: core, add: core.AddRecoveredFrom, groupedAdd: core.AddRecoveryMetric}
	owner := batchReplayOwner()
	done := make(chan error, 1)
	go func() {
		_, err := owner.replayCaptureBatches(bytes.NewReader(wire), stream.Next(), sink, func(int64, int) {})
		done <- err
	}()
	var got []points.Point
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		if p, ok := core.PopNotConfirmed("bounded.group"); ok {
			got = append(got, p.Data...)
			core.Confirm(p)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			if p, ok := core.PopNotConfirmed("bounded.group"); ok {
				got = append(got, p.Data...)
				core.Confirm(p)
			}
			if len(got) != 32 {
				t.Fatal("lost split points", len(got))
			}
			for i, p := range got {
				if p.Timestamp != int64(i) || p.Value != float64(i) {
					t.Fatal("split changed FIFO", i, p)
				}
			}
			return
		case <-deadline.C:
			close(owner.replayStop)
			<-done
			t.Fatal("group larger than hard limit deadlocked")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestCaptureAppendWritesMultiPointRecords(t *testing.T) {
	policy := chooseStartupPolicy(1, 1<<30, 1<<30)
	s, err := openStartupInputWithPolicy(filepath.Join(t.TempDir(), "input.1.2.tail.bin"), policy)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	done := make(chan error, 64)
	for i := range 64 {
		s.shards[0].SubmitCapture(points.OnePoint("coalesced.metric", float64(i), int64(i%7)).AppendBinary(nil), 1, done)
	}
	for range 64 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	total, largest := 0, 0
	_, err = recovery.ReadCaptureFile(s.shards[0].path+".000000000000.tail.bin", func(p *points.Points) {
		largest = max(largest, len(p.Data))
		for _, pt := range p.Data {
			if pt.Value != float64(total) || pt.Timestamp != int64(total%7) {
				t.Error("capture grouping changed point bits/order")
			}
			total++
		}
	})
	if err != nil || total != 64 || largest <= 1 {
		t.Fatal("capture did not coalesce", total, largest, err)
	}
}
