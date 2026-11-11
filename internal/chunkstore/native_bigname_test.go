package chunkstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

func TestNativeSkipCannotBecomeEmptyOrdinaryTransfer(t *testing.T) {
	r := TransferMetric{Config: MetricConfig{Name: "skipped", AggregationMethod: Sum, Retentions: []Retention{{Step: 1, Count: 10}}}}
	r.Version.Name = r.Config.Name
	r.NativeSkip = &NativeSkip{Name: r.Config.Name, PhysicalPoints: nativeMetricPoints + 1, Reason: "name limit"}
	if _, err := TransferWireSize([]TransferMetric{r}); !errors.Is(err, ErrFormat) {
		t.Fatalf("v1 size: %v", err)
	}
	var wire bytes.Buffer
	if err := WriteTransfer(&wire, []TransferMetric{r}); !errors.Is(err, ErrFormat) || wire.Len() != 0 {
		t.Fatalf("v1 write: %v (%d bytes)", err, wire.Len())
	}
	s, _ := openTestStore(t, t.TempDir(), Options{})
	defer closeTestStore(t, s)
	if _, err := s.ImportTransfer(context.Background(), []TransferMetric{r}); !errors.Is(err, ErrFormat) {
		t.Fatalf("ordinary import: %v", err)
	}
	if _, err := s.Metadata(context.Background(), r.Config.Name); !errors.Is(err, ErrNotFound) {
		t.Fatalf("skip published a name: %v", err)
	}
}

// The fixture and wire live on disk/in a pipe: the test cannot hide an archive
// allocation in its input buffer. Every physical point is checked after copy.
func TestNativeLargeHistoryBoundedHeap(t *testing.T) {
	// Twice the former 1,048,576-point per-name cap, with the same oracle.
	testNativeLargeHistoryBoundedHeap(t, 2<<20)
}

func TestNativeTwentyMillionPointsBoundedHeap(t *testing.T) {
	if testing.Short() {
		t.Skip("exhaustive scale variant; the large-history proof still runs")
	}
	testNativeLargeHistoryBoundedHeap(t, 20_000_000)
}

func testNativeLargeHistoryBoundedHeap(t *testing.T, count int) {
	t.Helper()
	if count <= 1<<20 || count%128 != 0 {
		t.Fatal("fixture must exceed the former name cap and fill its point blocks")
	}
	ctx := context.Background()
	opts := Options{fs: nativeTestDiskFS{vfs.Default}, ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20}
	source, _ := openTestStore(t, t.TempDir(), opts)
	defer closeTestStore(t, source)
	target, _ := openTestStore(t, t.TempDir(), opts)
	defer closeTestStore(t, target)
	cfg := MetricConfig{Name: "large.history", AggregationMethod: Sum, Retentions: []Retention{{Step: 1, Count: count}}}
	m, err := source.Create(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.Create(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	// A present survivor forces a real combine, including target-wins ties.
	if err := target.UpdateMany(ctx, cfg.Name, []Point{{Timestamp: int64(count), Value: 42}}); err != nil {
		t.Fatal(err)
	}
	batch := source.db.NewBatch()
	for index := 0; index < count/128; index++ {
		var c chunk
		for slot := range 128 {
			c.set(slot, Point{Timestamp: int64(count + index*128 + slot), Value: math.Copysign(0, -1)})
		}
		key := binary.BigEndian.AppendUint16(chunkPrefix(m.ID, m.Generation), 0)
		key = binary.BigEndian.AppendUint32(key, uint32(index))
		if err := batch.Set(key, source.encodeChunk(&c), nil); err != nil {
			t.Fatal(err)
		}
		if batch.Len() >= 1<<20 {
			if err := batch.Commit(pebble.NoSync); err != nil {
				t.Fatal(err)
			}
			batch.Reset()
		}
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	batch.Close()
	if err := source.db.Flush(); err != nil {
		t.Fatal(err)
	}
	pin, err := source.PinNativeSource()
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Close()
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	var peak atomic.Uint64
	stop, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				// Include uncollected garbage: this conservatively bounds retained
				// heap without forcing a global collection every 20ms.
				var ms runtime.MemStats
				runtime.ReadMemStats(&ms)
				for old := peak.Load(); ms.HeapAlloc > old; old = peak.Load() {
					if peak.CompareAndSwap(old, ms.HeapAlloc) {
						break
					}
				}
			}
		}
	}()
	input, output := io.Pipe()
	exported := make(chan error, 1)
	go func() { err := pin.Write(ctx, []string{cfg.Name}, output); output.CloseWithError(err); exported <- err }()
	receipt, err := target.NativeImport(ctx, combineTestRange([]string{cfg.Name}), input)
	input.CloseWithError(err)
	exportErr := <-exported
	close(stop)
	<-stopped
	if err != nil || exportErr != nil {
		t.Fatal(err, exportErr)
	}
	if receipt.CopiedCount != 1 || len(receipt.Skipped) != 0 {
		t.Fatal(receipt)
	}
	retained := int64(peak.Load()) - int64(before.HeapAlloc)
	if retained > nativeCombineWorkerMemory {
		t.Fatalf("retained heap grew %d bytes (worker budget %d)", retained, nativeCombineWorkerMemory)
	}
	t.Logf("%d points: peak retained heap delta %d bytes; worker budget %d", count, retained, nativeCombineWorkerMemory)
	// Keep process-wide heap sampling isolated, then let the exhaustive
	// readback run alongside the other parallel correctness tests.
	t.Parallel()
	copied, err := target.Metadata(ctx, cfg.Name)
	if err != nil {
		t.Fatal(err)
	}
	prefix := chunkPrefix(copied.ID, copied.Generation)
	it, err := target.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	points := 0
	for it.First(); it.Valid(); it.Next() {
		var c chunk
		if err := decodeChunk(it.Value(), &c); err != nil {
			t.Fatal(err)
		}
		index := int(binary.BigEndian.Uint32(it.Key()[len(prefix)+2:]))
		for slot, p := range c.Points {
			if !c.has(slot) {
				continue
			}
			want := math.Float64bits(math.Copysign(0, -1))
			if index == 0 && slot == 0 {
				want = math.Float64bits(42)
			}
			if p.Timestamp != int64(count+index*128+slot) || math.Float64bits(p.Value) != want {
				t.Fatalf("point changed: %d/%d %+v", index, slot, p)
			}
			points++
		}
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	if points != count {
		t.Fatal("lost points", points)
	}
}

func TestNativeOversizedMixedRangeAndResume(t *testing.T) {
	for _, combine := range []bool{false, true} {
		t.Run(fmt.Sprint(combine), func(t *testing.T) {
			ctx := context.Background()
			opts := Options{fs: nativeTestDiskFS{vfs.Default}, ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20}
			source, _ := openTestStore(t, t.TempDir(), opts)
			defer closeTestStore(t, source)
			targetDir := t.TempDir()
			target, _ := openTestStore(t, targetDir, opts)
			defer func() { closeTestStore(t, target) }()
			names := []string{"mixed.a", "mixed.huge", "mixed.z"}
			for _, name := range names {
				cfg := MetricConfig{Name: name, AggregationMethod: Sum, Retentions: []Retention{{Step: 1, Count: 1000}}}
				if name == names[1] {
					cfg.Retentions[0].Count = nativeMetricPoints + 1
				}
				if _, err := source.Create(ctx, cfg); err != nil {
					t.Fatal(err)
				}
				if err := source.UpdateMany(ctx, name, []Point{{Timestamp: 99900, Value: math.Copysign(0, -1)}}); err != nil {
					t.Fatal(err)
				}
			}
			pin, err := source.PinNativeSource()
			if err != nil {
				t.Fatal(err)
			}
			var wire bytes.Buffer
			if err := pin.Write(ctx, names, &wire); err != nil {
				t.Fatal(err)
			}
			pin.Close()
			req := combineTestRange(names)
			req.Combine = combine
			if combine {
				target.nativeTestHook = func(stage string) error {
					if stage == "combine-name-published" {
						return errors.New("interrupt after a published name")
					}
					return nil
				}
				if _, err := target.NativeImport(ctx, req, bytes.NewReader(wire.Bytes())); err == nil {
					t.Fatal("missing interruption")
				}
				if err := target.syncWAL(); err != nil {
					t.Fatal(err)
				}
				closeTestStore(t, target)
				target, _ = openTestStore(t, targetDir, opts)
			}
			var input io.Reader = bytes.NewReader(wire.Bytes())
			if combine {
				input = nativeUnreadable{}
			}
			receipt, err := target.NativeImport(ctx, req, input)
			if err != nil {
				t.Fatal(err)
			}
			if err := receipt.Validate(); err != nil {
				t.Fatal(err)
			}
			if receipt.CopiedCount != 2 || receipt.DeferredCount != 1 || len(receipt.Skipped) != 1 {
				t.Fatal(receipt)
			}
			skip := receipt.Skipped[0]
			if skip.Ordinal != 1 || skip.Name != names[1] || skip.ArchiveSlots != nativeMetricPoints+1 || skip.Bytes <= 0 || skip.PhysicalPoints <= 0 {
				t.Fatal(skip)
			}
			if _, err := target.Metadata(ctx, names[1]); !errors.Is(err, ErrNotFound) {
				t.Fatal("skipped name published", err)
			}
			for _, name := range []string{names[0], names[2]} {
				want, err := source.Snapshot(ctx, name)
				if err != nil {
					t.Fatal(err)
				}
				got, err := target.Snapshot(ctx, name)
				if err != nil {
					t.Fatal(err)
				}
				if err := EqualSnapshots(want, got); err != nil {
					t.Fatal(err)
				}
			}
			closeTestStore(t, target)
			target, _ = openTestStore(t, targetDir, opts)
			again, err := target.NativeReceipt(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			if len(again.Skipped) != 1 || again.Skipped[0] != skip {
				t.Fatal("lost durable skip", again)
			}
		})
	}
}

func TestNativeWorkerBudgetSkipsBeforeMaterialization(t *testing.T) {
	s := &Store{}
	r := TransferMetric{Config: MetricConfig{Name: "large.irregular", Retentions: []Retention{{Step: 1, Count: 20_000_000}}}}
	var skip *NativeSkip
	if err := s.admitNativeCombineRecords(r, r, true); !errors.As(err, &skip) || skip.MemoryBytes <= nativeCombineWorkerMemory {
		t.Fatal("materialization bypassed admission", err)
	}
}

func TestNativeSkipWireRoundTrip(t *testing.T) {
	skip := NativeSkip{Name: "wire.large", Bytes: nativeMetricBytes + 1, PhysicalPoints: 40_000_000, ArchiveSlots: 50_000_000, Reason: "name limit"}
	record := TransferMetric{Config: MetricConfig{Name: skip.Name}, Version: TransferVersion{Name: skip.Name, ID: 1, Generation: 1, Revision: 1}, Continuation: true, NativeSkip: &skip}
	var wire bytes.Buffer
	if err := WriteTransfer2(&wire, []TransferMetric{record}); err != nil {
		t.Fatal(err)
	}
	if size := TransferWireSize2([]TransferMetric{record}); size != int64(wire.Len()) {
		t.Fatal("skip wire size", size, wire.Len())
	}
	records, err := ReadTransfer(bytes.NewReader(wire.Bytes()), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := NewTransferStream(bytes.NewReader(wire.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	streamed, err := stream.Next()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := stream.Finish(); err != nil {
		t.Fatal(err)
	}
	for _, got := range []TransferMetric{records[0], streamed} {
		if got.NativeSkip == nil || *got.NativeSkip != skip || !got.Continuation || len(got.Keys) != 0 {
			t.Fatal(got)
		}
	}
}

func TestNativeWorkerSkipPieceResumesWithoutChangingTarget(t *testing.T) {
	ctx := context.Background()
	opts := Options{fs: nativeTestDiskFS{vfs.Default}, ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20}
	source, _ := openTestStore(t, t.TempDir(), opts)
	defer closeTestStore(t, source)
	dir := t.TempDir()
	target, _ := openTestStore(t, dir, opts)
	defer func() { closeTestStore(t, target) }()
	names := []string{"budget.a", "budget.large", "budget.z"}
	for _, name := range names {
		cfg := MetricConfig{Name: name, AggregationMethod: Sum, Retentions: []Retention{{Step: 1, Count: 1000}}}
		if name == names[1] {
			cfg.Retentions[0].Count = 20_000_000
		}
		for _, s := range []*Store{source, target} {
			if _, err := s.Create(ctx, cfg); err != nil {
				t.Fatal(err)
			}
			if err := s.UpdateMany(ctx, name, []Point{{Timestamp: 99900, Value: 42}}); err != nil {
				t.Fatal(err)
			}
		}
	}
	// A noncanonical timestamp requires the reference path, whose whole-archive
	// scratch does not fit. The other names must still publish and resume.
	old, err := target.Metadata(ctx, names[1])
	if err != nil {
		t.Fatal(err)
	}
	raw, err := source.Metadata(ctx, names[1])
	if err != nil {
		t.Fatal(err)
	}
	var c chunk
	c.set(0, Point{Timestamp: 123, Value: math.NaN()})
	key := binary.BigEndian.AppendUint16(chunkPrefix(raw.ID, raw.Generation), 0)
	key = binary.BigEndian.AppendUint32(key, 0)
	if err := source.db.Set(key, source.encodeChunk(&c), pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	pin, err := source.PinNativeSource()
	if err != nil {
		t.Fatal(err)
	}
	var wire bytes.Buffer
	if err := pin.Write(ctx, names, &wire); err != nil {
		t.Fatal(err)
	}
	pin.Close()
	pieces := 0
	target.nativeTestHook = func(stage string) error {
		if stage == "combine-name-published" {
			pieces++
			if pieces == 2 {
				return errors.New("interrupt after skip piece")
			}
		}
		return nil
	}
	req := combineTestRange(names)
	if _, err := target.NativeImport(ctx, req, &wire); err == nil {
		t.Fatal("missing interruption")
	}
	if err := target.syncWAL(); err != nil {
		t.Fatal(err)
	}
	closeTestStore(t, target)
	target, _ = openTestStore(t, dir, opts)
	receipt, err := target.NativeImport(ctx, req, nativeUnreadable{})
	if err != nil {
		t.Fatal(err)
	}
	if err := receipt.Validate(); err != nil {
		t.Fatal(err)
	}
	if receipt.CopiedCount != 2 || len(receipt.Skipped) != 1 || receipt.Skipped[0].Ordinal != 1 || receipt.Skipped[0].MemoryBytes <= nativeCombineWorkerMemory {
		t.Fatal(receipt)
	}
	got, err := target.Metadata(ctx, names[1])
	if err != nil || transferVersion(got) != transferVersion(old) {
		t.Fatal("size skip changed target", got, old, err)
	}
}
