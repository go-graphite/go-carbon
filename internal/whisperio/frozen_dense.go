package whisperio

import (
	"context"
	"errors"
	"math"
	"sort"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// Only immutable header/index metadata is cached. Payload reads still use the
// current admitted read view. Noncanonical layouts keep the reference engine.
type frozenDensePlan struct {
	blocks        []whisper.ContinuationBlock
	bounds        []whisper.ArchiveWindow
	windows       []whisper.ArchiveWindow
	supported     bool
	overlays      [][]whisper.ArchiveWindow
	overlayValues [][][]float64
}

func (d *frozenReadDecoder) densePlan() (*frozenDensePlan, error) {
	if d.dense != nil {
		return d.dense, nil
	}
	p := &frozenDensePlan{}
	if !d.w.IsCompressed() || d.w.OutOfOrderPath() != "" || d.w.AggregationMethod() == whisper.Mix {
		d.dense = p
		return p, nil
	}
	layout, err := d.w.ContinuationLayout()
	if err != nil {
		return nil, err
	}
	p.blocks, p.bounds, p.windows = layout.Blocks, d.w.StoredArchiveBounds(), d.w.BufferedRollupWindows()
	sort.Slice(p.blocks, func(i, j int) bool {
		a, b := p.blocks[i], p.blocks[j]
		if a.Archive != b.Archive {
			return a.Archive < b.Archive
		}
		return a.First < b.First
	})
	p.supported = true
	for i, b := range p.blocks {
		if b.First <= 0 || i > 0 && p.blocks[i-1].Archive == b.Archive && p.blocks[i-1].Last >= b.First {
			p.supported = false
		}
	}
	p.overlays = make([][]whisper.ArchiveWindow, len(d.m.Retentions))
	p.overlayValues = make([][][]float64, len(d.m.Retentions))
	for ai := range p.overlays {
		var windows []whisper.ArchiveWindow
		for _, w := range p.windows {
			if w.Archive == ai || w.Archive == ai+1 {
				w.Archive = ai
				windows = append(windows, w)
			}
		}
		sort.Slice(windows, func(i, j int) bool { return windows[i].From < windows[j].From })
		merged := windows[:0]
		for _, w := range windows {
			n := len(merged)
			if n > 0 && w.From <= merged[n-1].Until {
				merged[n-1].Until = max(merged[n-1].Until, w.Until)
			} else {
				merged = append(merged, w)
			}
		}
		p.overlays[ai] = merged
		p.overlayValues[ai] = make([][]float64, len(merged))
	}
	d.dense = p
	return p, nil
}

// FetchDense follows the already-used compressedArchiveSums proof: ordered
// physical blocks, no sidecar/Mix, no newer owner beyond the aligned query end,
// and canonical timestamps. Small buffer/rollup windows use Whisper itself.
// This removes storedPoints and full dense-output allocation without copying
// Whisper's aggregation, sidecar, or circular-owner implementation.
func (d *frozenReadDecoder) FetchDense(ctx context.Context, from, until int, dst *store.Series) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	fallback := func() (bool, error) {
		series, err := d.Fetch(ctx, from, until)
		if err != nil || series == nil {
			return false, err
		}
		*dst = *series
		return true, nil
	}
	if from > until {
		return fallback()
	}
	now := int(d.clock().Unix())
	oldest := now - d.w.MaxRetention()
	if from > now || until < oldest {
		return false, nil
	}
	loBound, hiBound := max(from, oldest), min(until, now)
	ai := len(d.m.Retentions) - 1
	for i, r := range d.m.Retentions {
		if r.MaxRetention() >= now-loBound {
			ai = i
			break
		}
	}
	r := d.m.Retentions[ai]
	align := func(t int) int { return t - (t%r.Step+r.Step)%r.Step + r.Step }
	lo, hi := align(loBound), align(hiBound)
	if lo < 0 || lo == hi {
		return fallback()
	}
	p, err := d.densePlan()
	if err != nil {
		return false, err
	}
	if !p.supported {
		return fallback()
	}
	for _, b := range p.bounds {
		if b.Archive == ai && b.Until > hi {
			return fallback()
		}
	}
	for _, w := range p.windows {
		if w.Archive == ai && w.From >= hi {
			return fallback()
		}
	}
	d.prepareFetch()
	defer func() {
		if cap(d.fs.snapshotBlock) > maxFetchBlockPoints {
			d.fs.snapshotBlock = nil
			d.fs.snapshotBlockOK = false
		}
	}()
	n := (hi - lo) / r.Step
	if cap(dst.Values) < n {
		dst.Values = make([]float64, n)
	}
	values := dst.Values[:n]
	for i := range values {
		values[i] = math.NaN()
	}
	covered := func(ts int) bool {
		for _, w := range p.overlays[ai] {
			if ts >= w.From && ts < w.Until {
				return true
			}
		}
		return false
	}
	f := d.w.StorageFile().(*continuationFile)
	for _, b := range p.blocks {
		if b.Archive != ai || b.Last < lo || b.First >= hi {
			continue
		}
		visit := func(point store.Point) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			ts := int(point.Timestamp)
			if ts%r.Step != 0 {
				return errDigestOffGrid
			}
			if ts >= lo && ts < hi && !covered(ts) {
				values[(ts-lo)/r.Step] = point.Value
			}
			return nil
		}
		var err error
		if d.fs.snapshotReads && !d.fs.rangeReads {
			var points []whisper.TimeSeriesPoint
			points, err = f.ReadStorageBlockRange(b, lo, hi-1)
			if err == nil {
				for _, point := range points {
					if err = visit(store.Point{Timestamp: int64(point.Time), Value: point.Value}); err != nil {
						break
					}
				}
			}
		} else {
			_, err = f.walkStorageBlock(b, nil, lo, hi-1, true, visit)
		}
		if errors.Is(err, errDigestOffGrid) {
			return fallback()
		}
		if err != nil {
			return false, err
		}
	}
	// Small immutable buffer/rollup results are decoder scratch, never a full
	// rendered archive cache. Consumers receive copies in their own dense output.
	for wi, w := range p.overlays[ai] {
		first, end := max(lo, w.From), min(hi, w.Until)
		if first >= end {
			continue
		}
		cached := p.overlayValues[ai][wi]
		readWindow := w
		if (w.Until-w.From)/r.Step > maxFetchBlockPoints {
			readWindow.From, readWindow.Until = first, end
		}
		if cached == nil {
			series, err := d.w.FetchArchiveWindow(readWindow)
			if err != nil {
				return false, err
			}
			if series == nil || series.FromTime() != readWindow.From || series.UntilTime() != readWindow.Until || series.Step() != r.Step {
				return false, store.ErrFormat
			}
			cached = series.Values()
			if readWindow == w {
				p.overlayValues[ai][wi] = cached
			}
		}
		copy(values[(first-lo)/r.Step:(end-lo)/r.Step], cached[(first-readWindow.From)/r.Step:(end-readWindow.From)/r.Step])
	}
	*dst = store.Series{Metadata: d.m, FromTime: lo, UntilTime: hi, Step: r.Step, Values: values}
	return true, nil
}
