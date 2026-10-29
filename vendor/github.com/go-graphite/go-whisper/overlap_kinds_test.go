package whisper

import "testing"

// TestOverlapKinds reproduces each batch shape that sends a compressed update
// down the classic replay path, and the common shapes that only divert.
func TestOverlapKinds(t *testing.T) {
	pt := func(ts int, v float64) *TimeSeriesPoint { return &TimeSeriesPoint{Time: ts, Value: v} }
	const now = 1_700_000_040 // a minute boundary

	t.Run("no buffered_rollup cycle from mid-slot batches", func(t *testing.T) {
		// A replay encodes the unfinished hour. Completing it puts its
		// rollup behind the hourly watermark. Batches from a sub-resolution
		// emitter start in the last written minute; the tail correction
		// still patches the rollup in place, so no hour replays again.
		f := newShapeFile(t, "1m:1d,1h:7d,1d:30d", Average, 0)
		f.prefill("1m:1d,1h:7d,1d:30d", now) // wrapped rings, as in production
		if r := f.write(now+5, []*TimeSeriesPoint{pt(now, 1), pt(now+30, 2)}); r.kind != OverlapFuture {
			t.Fatalf("seed replay: %v", r.kind)
		}
		ts := now + 25
		for i := 0; i < 32; i++ {
			end := ts + 960
			if r := f.write(end, seriesPoints(ts, end, 10)); r.kind != OverlapNone {
				t.Fatalf("batch %d replayed (%v): the buffered_rollup cycle is back", i, r.kind)
			}
			ts = end
		}
	})

	t.Run("minute-aligned batches end the cycle", func(t *testing.T) {
		f := newShapeFile(t, "1m:1d,1h:7d,1d:30d", Average, 0)
		f.prefill("1m:1d,1h:7d,1d:30d", now)
		if r := f.write(now+5, []*TimeSeriesPoint{pt(now, 1), pt(now+30, 2)}); r.kind != OverlapFuture {
			t.Fatalf("seed replay: %v", r.kind)
		}
		replays := 0
		for ts := now + 60; ts < now+4*3600; ts += 960 {
			if r := f.write(ts+960, seriesPoints(ts, ts+960, 60)); r.kind != OverlapNone {
				replays++
			}
		}
		if replays != 0 {
			t.Fatalf("%d replays, want the tail correction to end the cycle", replays)
		}
	})

	t.Run("sidecar_slot", func(t *testing.T) {
		f := newShapeFile(t, "1s:10m,1m:1h", Average, 0)
		f.write(now, seriesPoints(now-300, now, 1))
		// Older than both base buffer windows: diverted to the sidecar.
		if r := f.write(now+1, []*TimeSeriesPoint{pt(now-200, 7)}); r.kind != OverlapNone || r.diverted == 0 {
			t.Fatalf("late point: %+v, want a divert", r)
		}
		// One base retention later the same slot receives a new timestamp.
		if r := f.write(now+600, seriesPoints(now+1, now+401, 1)); r.kind != OverlapSidecarSlot {
			t.Fatalf("got %v, want sidecar_slot", r.kind)
		}
	})

	t.Run("future", func(t *testing.T) {
		f := newShapeFile(t, "1m:1h,10m:6h", Average, 0)
		f.write(now, seriesPoints(now-7200, now, 60)) // wrapped base ring
		if r := f.write(now+60, append(seriesPoints(now, now+60, 10), pt(now+120, 1))); r.kind != OverlapFuture {
			t.Fatalf("got %v, want future", r.kind)
		}
	})

	t.Run("stored_slot", func(t *testing.T) {
		f := newShapeFile(t, "1m:1h", Average, 0)
		f.write(now, seriesPoints(now-1800, now+1800, 60)) // stored future points
		if r := f.write(now, []*TimeSeriesPoint{pt(now-1860, 1)}); r.kind != OverlapStoredSlot {
			t.Fatalf("got %v, want stored_slot", r.kind)
		}
	})

	t.Run("batch_slot", func(t *testing.T) {
		f := newShapeFile(t, "1m:1h,10m:6h", Average, 0)
		if r := f.write(now, []*TimeSeriesPoint{pt(now+600, 1), pt(now+600-21600, 2)}); r.kind != OverlapBatchSlot {
			t.Fatalf("got %v, want batch_slot", r.kind)
		}
	})

	t.Run("divert-only shapes", func(t *testing.T) {
		// A sub-resolution emitter cut mid-slot into a single archive
		// rewrites the last slot: diverted, never replayed.
		f := newShapeFile(t, "5m:1d", Average, 0)
		f.write(now, seriesPoints(now-3600, now+130, 10))
		if r := f.write(now+1000, seriesPoints(now+130, now+1000, 10)); r.kind != OverlapNone || r.diverted == 0 {
			t.Fatalf("single archive mid-slot: %+v, want a divert only", r)
		}
		// With a coarser archive the base buffer absorbs the same cut, and
		// duplicates inside it.
		f = newShapeFile(t, "1m:1d,1h:7d", Average, 0)
		f.write(now, seriesPoints(now-3600, now+30, 10))
		if r := f.write(now+990, append(seriesPoints(now+30, now+990, 10), pt(now, 3))); r.kind != OverlapNone || r.diverted != 0 {
			t.Fatalf("buffered mid-slot and duplicate: %+v, want neither", r)
		}
	})
}
