package v2read

import (
	"context"
	"errors"
	"math"
	"math/bits"
)

// Series borrows read-only Values and Occupied from the render workspace until
// the caller releases it.
type Series struct {
	Metric            Metric
	From, Until, Step int64
	Values            []float64
	// Occupied marks a physical-slot replacement, including an occupied NaN or
	// a newer timestamp outside this range that invalidates an older ring slot.
	// It has the same lifetime as Values.
	Occupied []bool
}

func alignNext(t, step int64) int64 {
	r := t % step
	if r < 0 {
		r += step
	}
	return t - r + step
}

// SlotRange is a half-open interval of physical archive slots. A source keyed
// by (series, archive, physical slot, version) needs at most two bounded seeks.
type SlotRange struct{ From, Until int64 }

// SlotRanges translates an aligned render interval to physical ring slots.
// Sources must return all versions in these ranges, even timestamps outside
// the render interval, so wrapped replacements still invalidate old values.
// Archive dimensions and alignment are validated by render before Scan.
func SlotRanges(a Archive, from, until int64) ([2]SlotRange, int) {
	var ranges [2]SlotRange
	n := (until - from) / a.Step
	if n <= 0 {
		return ranges, 0
	}
	if n >= a.Points {
		ranges[0] = SlotRange{0, a.Points}
		return ranges, 1
	}
	first := (from / a.Step) % a.Points
	if first < 0 {
		first += a.Points
	}
	end := first + n
	ranges[0] = SlotRange{first, min(end, a.Points)}
	if end > a.Points {
		ranges[1] = SlotRange{0, end - a.Points}
		return ranges, 2
	}
	return ranges, 1
}

// render follows Whisper archive selection, clamping and next-slot alignment.
// Each archive is already resolved by the write/rollup engine; reads only choose
// the latest authoritative version of each slot, preserving IEEE value bits.
// Values, winner sequence, timestamp, presence and touched index take at most
// 32B per slot, so a render holds at most work/32 slots. The returned workspace
// backs the series; it is non-nil whenever buffers were taken, even with an
// error, and the caller releases it once the series is consumed.
func render(ctx context.Context, m Metric, source Source, from, until, now, work int64) (*Series, *renderWorkspace, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if now < 0 || now > math.MaxUint32 {
		return nil, nil, errors.New("clock outside storage timestamp range")
	}
	if from > until {
		return nil, nil, errors.New("invalid time interval")
	}
	if len(m.Archives) == 0 {
		return nil, nil, errors.New("metric has no archives")
	}
	var retention int64
	for _, a := range m.Archives {
		if a.Step <= 0 || a.Points <= 0 || a.Points > math.MaxUint32/a.Step {
			return nil, nil, errors.New("invalid archive")
		}
		retention = max(retention, a.Step*a.Points)
	}
	oldest := now - retention
	if from > now || until < oldest {
		return nil, nil, nil
	}
	from = max(from, oldest)
	until = min(until, now)
	a := m.Archives[len(m.Archives)-1]
	for _, candidate := range m.Archives {
		if candidate.Step*candidate.Points >= now-from {
			a = candidate
			break
		}
	}
	if from > math.MaxInt64-a.Step || until > math.MaxInt64-a.Step {
		return nil, nil, errors.New("interval overflows")
	}
	start, end := alignNext(from, a.Step), alignNext(until, a.Step)
	if start == end {
		populated, err := source.HasArchive(ctx, m.ID, a.Step)
		if err != nil {
			return nil, nil, err
		}
		if populated {
			if end > math.MaxInt64-a.Step {
				return nil, nil, errors.New("interval overflows")
			}
			end += a.Step
		}
	}
	n := (end - start) / a.Step
	// No allocation grows with duplicate/input count.
	if n < 0 || n > math.MaxUint32 || n > work/32 {
		return nil, nil, ErrAdmission
	}
	workspace := takeRenderWorkspace(int(n))
	out := &Series{Metric: m, From: start, Until: end, Step: a.Step, Values: workspace.values, Occupied: workspace.seen}
	seq, times, seen := workspace.sequences, workspace.times, workspace.seen
	workspace.touched = workspace.touched[:0]
	firstSlot := (start / a.Step) % a.Points
	if firstSlot < 0 {
		firstSlot += a.Points
	}
	// A resolved source already owns one authoritative winner per physical
	// slot. Use that slot directly instead of arbitrating every sample again.
	if source, ok := source.(*resolvedSource); ok && source.id == m.ID {
		archive := source.archive(a.Step)
		ranges, count := SlotRanges(a, start, end)
		for _, interval := range ranges[:count] {
			if err := ctx.Err(); err != nil {
				return nil, workspace, err
			}
			for i := source.lower(archive, interval.From); i < len(source.records) && source.records[i].Archive == archive && source.records[i].Slot < interval.Until; i++ {
				record := source.records[i]
				j := record.Slot - firstSlot
				if j < 0 {
					j += a.Points
				}
				seen[j] = true
				workspace.touched = append(workspace.touched, uint32(j))
				value := math.NaN()
				if record.Sample.Time == start+j*a.Step {
					value = math.Float64frombits(record.Sample.Bits)
				}
				out.Values[j] = value
			}
		}
		return out, workspace, ctx.Err()
	}
	// Sources check cancellation at block boundaries. Per-sample polling takes
	// a cancel-context mutex for every version, even for fully cached blocks.
	emit := func(p Sample) error {
		tick := p.Time / a.Step
		if p.Time-tick*a.Step != 0 {
			return errors.New("source returned unaligned archive slot")
		}
		slot := tick % a.Points
		if slot < 0 {
			slot += a.Points
		}
		i := slot - firstSlot
		if i < 0 {
			i += a.Points
		}
		if i >= n {
			return nil
		}
		if seen[i] && p.Sequence == seq[i] && (math.Float64bits(out.Values[i]) != p.Bits || times[i] != p.Time) {
			return ErrConflict
		}
		if !seen[i] || p.Sequence > seq[i] {
			if !seen[i] {
				workspace.touched = append(workspace.touched, uint32(i))
			}
			seen[i] = true
			seq[i] = p.Sequence
			times[i] = p.Time
			out.Values[i] = math.Float64frombits(p.Bits)
		}
		return nil
	}
	if err := source.Scan(ctx, m.ID, a, start, end, emit); err != nil {
		return nil, workspace, err
	}
	if err := ctx.Err(); err != nil {
		return nil, workspace, err
	}
	for _, i := range workspace.touched {
		if times[i] != start+int64(i)*a.Step {
			out.Values[i] = math.NaN()
		}
	}
	return out, workspace, nil
}

// Idle caches are bounded by size class, independently of active admission.
// Each workspace allocates exactly its query length; a size class reuses only
// exact matches, preserving the public 32B/slot active admission bound.
// Total idle storage is below 112MiB. Queries above 131072 slots are uncached.
type renderWorkspace struct {
	values    []float64
	sequences []uint64
	times     []int64
	seen      []bool
	touched   []uint32
}

var renderWorkspaces = func() [12]chan *renderWorkspace {
	var pools [12]chan *renderWorkspace
	for i := range pools {
		// 64 .. 131072 slots, at most 1MiB per class, with a minimum
		// of two entries at the largest sizes.
		slots := 64 << i
		depth := max(2, min(64, (1<<20)/(slots*29)))
		pools[i] = make(chan *renderWorkspace, depth)
	}
	return pools
}()

func renderWorkspaceClass(n int) int {
	if n < 0 || n > 131072 {
		return -1
	}
	return max(0, bits.Len(uint(max(n, 1)-1))-6)
}

// Retain the original hour/day concurrency capacity while extending reuse to
// minute/coarse query lengths. The generic classes have separate fixed bounds.
var hourRenderWorkspaces = make(chan *renderWorkspace, 64)
var dayRenderWorkspaces = make(chan *renderWorkspace, 32)

func renderWorkspacePool(n int) chan *renderWorkspace {
	if n == 3600 {
		return hourRenderWorkspaces
	}
	if n == 86400 {
		return dayRenderWorkspaces
	}
	if class := renderWorkspaceClass(n); class >= 0 {
		return renderWorkspaces[class]
	}
	return nil
}
func takeRenderWorkspace(n int) *renderWorkspace {
	if pool := renderWorkspacePool(n); pool != nil {
		select {
		case w := <-pool:
			if len(w.values) == n {
				return w
			}
		default:
		}
	}
	w := &renderWorkspace{values: make([]float64, n), sequences: make([]uint64, n), times: make([]int64, n), seen: make([]bool, n), touched: make([]uint32, 0, n)}
	for i := range w.values {
		w.values[i] = math.NaN()
	}
	return w
}
func releaseRenderWorkspace(w *renderWorkspace) {
	// Values are valid until release. Reset only the slots this render wrote;
	// all untouched slots are already canonical NaNs from allocation/last release.
	for _, i := range w.touched {
		w.values[i] = math.NaN()
		w.seen[i] = false
	}
	w.touched = w.touched[:0]
	if pool := renderWorkspacePool(len(w.values)); pool != nil {
		select {
		case pool <- w:
		default:
		}
	}
}
