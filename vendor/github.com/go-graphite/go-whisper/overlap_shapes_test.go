package whisper

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// shapeFile drives a compressed, out-of-order-enabled file the way go-carbon's
// persister does: one open, UpdateMany and close per batch, optionally followed
// by the persister's size-gated, rate-limited sidecar compaction.
type shapeFile struct {
	t    testing.TB
	path string
	now  int
	// compact, when set, runs after every update like compactContinuation.
	compact func(w *Whisper)
}

func newShapeFile(t testing.TB, retentions string, method AggregationMethod, xff float32) *shapeFile {
	t.Helper()
	rets, err := ParseRetentionDefs(retentions)
	if err != nil {
		t.Fatal(err)
	}
	f := &shapeFile{t: t, path: filepath.Join(t.TempDir(), "m.wsp"), now: 1_700_000_000}
	w, err := CreateWithOptions(f.path, rets, method, xff, f.options())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *shapeFile) options() *Options {
	return &Options{Compressed: true, OutOfOrder: true, Now: func() time.Time { return time.Unix(int64(f.now), 0) }}
}

type shapeResult struct {
	kind     OverlapKind
	diverted uint32
	sidecar  bool
}

func (f *shapeFile) write(now int, points []*TimeSeriesPoint) shapeResult {
	f.t.Helper()
	f.now = now
	w, err := OpenWithOptions(f.path, f.options())
	if err != nil {
		f.t.Fatal(err)
	}
	var before [overlapKinds]uint64
	for k := range before {
		before[k] = overlapUpdates[k].Load()
	}
	input := make([]*TimeSeriesPoint, len(points))
	for i, p := range points {
		input[i] = &TimeSeriesPoint{Time: p.Time, Value: p.Value}
	}
	if err := w.UpdateMany(input); err != nil {
		f.t.Fatal(err)
	}
	var r shapeResult
	for k := range before {
		if overlapUpdates[k].Load() != before[k] {
			r.kind = OverlapKind(k)
		}
	}
	r.diverted = w.OutOfOrderPoints
	if f.compact != nil {
		f.compact(w)
	}
	r.sidecar = w.oooPath != ""
	if err := w.Close(); err != nil {
		f.t.Fatal(err)
	}
	return r
}

// persisterCompaction mirrors compactContinuation with go-carbon's defaults:
// merge only once the sidecar occupies 64 KiB, at most when allow says so.
func persisterCompaction(t testing.TB, allow func() bool) func(w *Whisper) {
	return func(w *Whisper) {
		path := w.OutOfOrderPath()
		if path == "" {
			return
		}
		// go-carbon's continuation store reports a classic sidecar's size as
		// its written 4 KiB pages; count non-zero pages the same way.
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		size := 0
		for off := 0; off < len(data); off += 4096 {
			end := off + 4096
			if end > len(data) {
				end = len(data)
			}
			page := data[off:end]
			for _, b := range page {
				if b != 0 {
					size += 4096
					break
				}
			}
		}
		if size < 65536 || !allow() {
			return
		}
		if _, err := w.MergeOutOfOrderWithOptions(OutOfOrderMergeOptions{MaxPointAge: 5 * time.Minute, RetentionMargin: time.Minute}); err != nil {
			t.Fatal(err)
		}
	}
}

// prefill writes history at the base resolution so every archive has wrapped
// at least once, as converted production files have. It returns the end time.
func (f *shapeFile) prefill(retentions string, start int) {
	f.t.Helper()
	rets, err := ParseRetentionDefs(retentions)
	if err != nil {
		f.t.Fatal(err)
	}
	step := rets[0].SecondsPerPoint()
	from := start - rets[0].MaxRetention() - 86400
	chunk := rets[0].MaxRetention() / 20
	if chunk < 86400 {
		chunk = 86400
	}
	chunk -= chunk % step
	for t := from - from%step; t < start; t += chunk {
		until := t + chunk
		if until > start {
			until = start
		}
		f.write(until, seriesPoints(t, until, step))
	}
}
