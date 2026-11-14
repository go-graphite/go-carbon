package whisperio

import (
	"context"
	"math"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

type exportCountingIO struct {
	mapContinuationIO
	reads int
}

func (kv *exportCountingIO) Get(key []byte) ([]byte, error) {
	kv.reads++
	return kv.mapContinuationIO.Get(key)
}

func (kv *exportCountingIO) Visit(prefix []byte, fn func([]byte, []byte) error) error {
	return kv.mapContinuationIO.Visit(prefix, func(k, v []byte) error { kv.reads++; return fn(k, v) })
}

func TestNativeExportCoalescedDifferential(t *testing.T) {
	ctx := context.Background()
	clock := func() time.Time { return time.Unix(30000, 0) }
	codec := &ContinuationCodec{}

	path := filepath.Join(t.TempDir(), "buffers.wsp")
	w, err := whisper.CreateWithOptions(path, whisper.MustParseRetentionDefs("1s:20000s,60s:60000s,600s:120000s"), whisper.Sum, 0.5, &whisper.Options{Compressed: true, Now: clock, PointsPerBlock: 256})
	if err != nil {
		t.Fatal(err)
	}
	points := make([]*whisper.TimeSeriesPoint, 19001)
	for i := range points {
		value := math.Sin(float64(i))
		if i%17 == 0 {
			value = math.Copysign(0, -1)
		}
		if i%31 == 0 {
			value = math.SmallestNonzeroFloat64
		}
		points[i] = &whisper.TimeSeriesPoint{Time: 10999 + i, Value: value}
	}
	if err := w.UpdateMany(points); err != nil {
		t.Fatal(err)
	}
	windows := w.BufferedRollupWindows()
	beforeWindows := len(windows)
	afterWindows := len(coalesceNativeRollupWindows(windows, []store.Retention{{Step: 1, Count: 20000}, {Step: 60, Count: 1000}, {Step: 600, Count: 200}}))
	if afterWindows >= beforeWindows {
		t.Fatalf("fixture has no repeated windows: %d -> %d", beforeWindows, afterWindows)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	snapshot, err := ReadSnapshot(ctx, "export.buffers", path)
	if err != nil {
		t.Fatal(err)
	}
	data := mapContinuationIO{}
	if err := codec.Import(ctx, data, path, snapshot.Metadata, clock); err != nil {
		t.Fatal(err)
	}

	var before, after int
	for _, window := range []store.NativeWindow{{From: 11000, Until: 29999}, {From: 29501, Until: 29999}, {From: 19001, Until: 19111}, {From: 1, Until: 1000}, {From: 29950, Until: 30001}} {
		oldKV, newKV := &exportCountingIO{mapContinuationIO: data}, &exportCountingIO{mapContinuationIO: data}
		want, ws, we := codec.legacyExportWindowSnapshot(ctx, oldKV, snapshot.Metadata, clock, window)
		got, gs, ge := codec.NativeWindowSnapshot(ctx, newKV, snapshot.Metadata, clock, window)
		if (ge == nil) != (we == nil) || (ge != nil && ge.Error() != we.Error()) || !reflect.DeepEqual(ws, gs) {
			t.Fatalf("%+v: errors %v/%v, skips %v/%v", window, we, ge, ws, gs)
		}
		if we == nil {
			if err := store.EqualSnapshots(want, got); err != nil {
				t.Fatal(window, err)
			}
		}
		before += oldKV.reads
		after += newKV.reads
	}

	t.Logf("same float bits/timestamps/skips/errors; buffered fetch windows %d -> %d; KV visits %d -> %d", beforeWindows, afterWindows, before, after)
}

func TestCoalesceNativeRollupWindows(t *testing.T) {
	input := []whisper.ArchiveWindow{{Archive: 1, From: 60, Until: 120}, {Archive: 1, From: 0, Until: 60}, {Archive: 1, From: 60, Until: 120}, {Archive: 0, From: 4, Until: 5}, {Archive: 1, From: 240, Until: 300}}
	got := coalesceNativeRollupWindows(input, []store.Retention{{Step: 1, Count: 10}, {Step: 60, Count: 2}})
	want := []whisper.ArchiveWindow{{Archive: 0, From: 4, Until: 5}, {Archive: 1, From: 0, Until: 120}, {Archive: 1, From: 240, Until: 300}}
	if !reflect.DeepEqual(got, want) {
		t.Fatal(got)
	}
}
