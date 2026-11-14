package carbon

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/points"
)

// A real dump/restart leaves a large saved set unconfirmed, with most names
// absent from Pebble. Native copy must start after loading and defer that set.
func TestNativeCopyAfterRestoreBeforeSavedWriteout(t *testing.T) {
	testNativeCopyAfterRestoreBeforeSavedWriteout(t, false)
}

func TestNativeCopyAfterLiveClaimsAboveRecoveryLimit(t *testing.T) {
	testNativeCopyAfterRestoreBeforeSavedWriteout(t, true)
}

// The recovery fixture uses a few MiB on real disk. Its correctness must not
// depend on the host filesystem's production free-space reserve. Each case
// runs in its own process, so replacing vfs.Default cannot affect other cases.
type nativeRestoreDiskFS struct{ vfs.FS }

func (nativeRestoreDiskFS) GetDiskUsage(string) (vfs.DiskUsage, error) {
	return vfs.DiskUsage{}, vfs.ErrUnsupported
}

func testNativeCopyAfterRestoreBeforeSavedWriteout(t *testing.T, liveClaims bool) {
	if isolatedTestProcess(t) {
		return
	}
	previousFS := vfs.Default
	vfs.Default = nativeRestoreDiskFS{previousFS}
	t.Cleanup(func() { vfs.Default = previousFS })
	path, cfg := pendingCheckpointConfig(t)
	cfg.Whisper.StorageBackend, cfg.Whisper.StoreDir = "pebble-chunk", t.TempDir()
	cfg.Whisper.StoreEncoding = "exact"
	cfg.Whisper.StoreCacheSize, cfg.Whisper.StoreMemTableSize = 1<<20, 1<<20
	cfg.Buckyd.Enabled, cfg.Buckyd.Bind = true, "127.0.0.1:0"
	cfg.Cache.MaxSize = 2_000_000
	first := New(path)
	first.Config = cfg
	if err := first.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(first.Stop)
	stamp := time.Now().Unix() - 1100
	first.Cache.Add(points.OnePoint("anchor", 1, stamp))
	waitExpiration(t, func() bool { return first.Cache.IsEmpty() && first.Carbonserver.HasMappedIndex() })
	first.Persister.Stop()
	first.Persister = nil
	const metrics, perMetric = 128, 1024
	names := make([]string, metrics+1)
	valueBits := []uint64{0x8000000000000000, 0x3ff0000000000001, 0x7ff8000000000012, 1, 0x7ff0000000000000}
	saved := make([]points.Point, perMetric)
	for i := range saved {
		saved[i] = points.Point{Timestamp: stamp + int64(i), Value: math.Float64frombits(valueBits[i%len(valueBits)])}
	}
	for i := range metrics {
		names[i] = fmt.Sprintf("saved.metric%04d", i)
		first.Cache.Add(&points.Points{Metric: names[i], Data: saved})
	}
	names[metrics] = "native.absent"
	if err := first.DumpStop(); err != nil {
		t.Fatal(err)
	}
	first.ReleaseForHandoff()
	first.Stop()
	cfg.Whisper.MaxUpdatesPerSecond = 1
	if liveClaims {
		// Accepted input can claim saved history above either admission
		// ceiling. The loader must notice those claims without allocating
		// them again or waiting for writeout to drain below the limit.
		cfg.Cache.MaxSize = 1
	}
	next := New(path)
	next.Config = cfg
	if err := next.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(next.Stop)
	// Freeze writeout, not restore loading. This makes the saved-set assertion
	// deterministic without a multi-minute test or a scheduling race.
	next.Persister.Stop()
	if liveClaims {
		correction := points.Point{Timestamp: stamp, Value: 123}
		for _, name := range names[:metrics] {
			next.Cache.AddRecovered(&points.Points{Metric: name, Data: []points.Point{correction}})
		}
		saved = append(saved, correction)
		if next.Cache.Size() <= int64(cfg.Cache.MaxSize) {
			t.Fatal("fixture did not exceed recovery admission")
		}
	}
	if next.storeRestoreLoaded == nil {
		t.Fatal("restart did not use pending checkpoint recovery")
	}
	select {
	case <-next.storeRestoreLoaded:
	case <-time.After(10 * time.Second):
		t.Fatal("saved checkpoint did not finish loading")
	}
	if next.Cache.PendingOutstanding() < metrics-2 {
		t.Fatal("fixture lost its large unconfirmed saved set", next.Cache.PendingOutstanding())
	}
	select {
	case <-next.storeRestoreDone:
		t.Fatal("native readiness test ran after writeout")
	default:
	}
	ctx := context.Background()
	donor, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer donor.Close()
	reference, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer reference.Close()
	config := func(name string) store.MetricConfig {
		return store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 3600}}, AggregationMethod: store.Average}
	}
	for _, name := range names {
		if _, err := donor.Create(ctx, config(name)); err != nil {
			t.Fatal(err)
		}
		if err := donor.UpdateMany(ctx, name, []store.Point{{Timestamp: stamp - 1, Value: 42}, {Timestamp: stamp, Value: 99}}); err != nil {
			t.Fatal(err)
		}
	}
	// Source sessions, candidates and imports all share the readiness fence.
	pin, err := next.MetricStore.PinNativeSource()
	if err != nil {
		t.Fatal("native session still waits for saved writeout", err)
	}
	pin.Close()
	candidates, err := next.MetricStore.NativeCandidates(ctx, names)
	if err != nil {
		t.Fatal(err)
	}
	for i := range names {
		if got := candidates[i/8]&(1<<uint(i%8)) != 0; got != (i == metrics) {
			t.Fatalf("candidate %s = %v", names[i], got)
		}
	}
	source, err := donor.PinNativeSource()
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	var wire bytes.Buffer
	if err := source.Write(ctx, names, &wire); err != nil {
		t.Fatal(err)
	}
	request := store.NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: strings.Repeat("3", 64), Source: "donor", End: int64(len(names)), Names: names}
	// Deliberately ignore the candidate optimization to exercise the final
	// publication fence too, as a stale precheck or older client can do.
	receipt, err := next.MetricStore.NativeImport(ctx, request, &wire)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.CopiedCount != 1 || !receipt.IsCopied(metrics) || receipt.DeferredCount != metrics {
		t.Fatal("native publication intersected saved names", receipt)
	}
	// Persist every saved value, then run the existing target-wins importer
	// for the deferred names. A separate reference receives the same saved
	// points before any native operation.
	for i, name := range names[:metrics] {
		if receipt.IsCopied(i) {
			t.Fatal("saved and native-published name", name)
		}
		if _, err := reference.Create(ctx, config(name)); err != nil {
			t.Fatal(err)
		}
		if err := reference.UpdateMany(ctx, name, saved); err != nil {
			t.Fatal(err)
		}
		if batch, ok := next.Cache.PopNotConfirmed(name); ok {
			if _, err := next.MetricStore.Create(ctx, config(name)); err != nil {
				t.Fatal(err)
			}
			if err := next.MetricStore.UpdateMany(ctx, name, batch.Data); err != nil {
				t.Fatal(err)
			}
			next.Cache.Confirm(batch)
		}
	}
	for start := 0; start < len(names); start += 8 {
		group := names[start:min(start+8, len(names))]
		records, err := donor.ExportTransfer(ctx, group)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := reference.ImportTransfer(ctx, records); err != nil {
			t.Fatal(err)
		}
		if start < metrics {
			if _, err := next.MetricStore.ImportTransfer(ctx, records); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, name := range names {
		want, err := reference.Snapshot(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		got, err := next.MetricStore.Snapshot(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.EqualSnapshots(want, got); err != nil {
			t.Fatalf("lost or changed points for %s: %v", name, err)
		}
	}
	select {
	case <-next.storeRestoreDone:
	case <-time.After(5 * time.Second):
		t.Fatal("saved history did not retire after confirmation")
	}
	t.Logf("saved points=%d, native copied=%d deferred=%d; all archives bit-exact", metrics*perMetric, receipt.CopiedCount, receipt.DeferredCount)
}
