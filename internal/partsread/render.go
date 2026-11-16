package partsread

import (
	"context"
	"errors"
	"math"
	"math/bits"
)

// Series returned by Render borrows read-only Values and Occupied until Handle.Close.
// RenderInto uses caller-owned writable Values; its Occupied is still read-only.
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

// Render follows Whisper archive selection, clamping and next-slot alignment.
// Each archive is already resolved by the write/rollup engine; reads only choose
// the latest authoritative version of each slot, preserving IEEE value bits.
func (h *Handle) Render(ctx context.Context, name string, from, until, now int64) (*Series, error) {
	return h.render(ctx, name, from, until, now, nil)
}

// RenderInto overlays occupied v2 slots onto an already aligned base result.
// Untouched slots retain the caller's values. Occupied NaNs and wrapped winners
// invalidate the base just as they do in Render. The caller owns base.Values;
// it must not alias any live handle's workspace. Occupied remains handle-owned.
func (h *Handle) RenderInto(ctx context.Context, name string, from, until, now int64, base *Series) (*Series, error) {
	if base == nil {
		return nil, errors.New("nil render base")
	}
	return h.render(ctx, name, from, until, now, base)
}
func (h *Handle) render(ctx context.Context, name string, from, until, now int64, base *Series) (*Series, error) {
	if h.closed.Load() {
		return nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if now < 0 || now > math.MaxUint32 {
		return nil, errors.New("clock outside storage timestamp range")
	}
	if from > until {
		return nil, errors.New("invalid time interval")
	}
	m, ok := h.v.view.Catalog.Lookup(name)
	if !ok {
		return nil, ErrNotFound
	}
	if len(m.Archives) == 0 {
		return nil, errors.New("metric has no archives")
	}
	var retention int64
	for _, a := range m.Archives {
		if a.Step <= 0 || a.Points <= 0 || a.Points > math.MaxUint32/a.Step {
			return nil, errors.New("invalid archive")
		}
		retention = max(retention, a.Step*a.Points)
	}
	oldest := now - retention
	if from > now || until < oldest {
		return nil, nil
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
		return nil, errors.New("interval overflows")
	}
	start, end := alignNext(from, a.Step), alignNext(until, a.Step)
	groups := [][]Source{h.v.view.Blocks, h.v.view.Parts, h.v.view.Head}
	if start == end {
		populated := false
		for _, sources := range groups {
			for _, s := range sources {
				exists, err := s.HasArchive(ctx, m.ID, a.Step)
				if err != nil {
					return nil, err
				}
				populated = populated || exists
			}
		}
		if populated {
			if end > math.MaxInt64-a.Step {
				return nil, errors.New("interval overflows")
			}
			end += a.Step
		}
	}
	n := (end - start) / a.Step
	// Values, winner sequence, timestamp, presence and touched indices use at most 32B/slot;
	// no allocation grows with duplicate/input count.
	if n < 0 || n > math.MaxUint32 || n > h.work/32 {
		return nil, ErrAdmission
	}
	if base != nil && (base.From != start || base.Until != end || base.Step != a.Step || int64(len(base.Values)) != n) {
		return nil, ErrShape
	}
	// Budget stays charged until the handle is closed, including prior results.
	h.work -= n * 32
	out := &Series{Metric: m, From: start, Until: end, Step: a.Step, Values: nil}
	workspace := takeRenderWorkspace(int(n))
	workspace.next = h.workspaces
	h.workspaces = workspace
	out.Values = workspace.values
	seq, times, seen := workspace.sequences, workspace.times, workspace.seen
	out.Occupied = seen
	workspace.touched = workspace.touched[:0]
	if base != nil {
		out.Values = base.Values
	}
	firstSlot := (start / a.Step) % a.Points
	if firstSlot < 0 {
		firstSlot += a.Points
	}
	// Sources check cancellation at block boundaries. Per-sample polling takes
	// a cancel-context mutex for every version, even for fully cached blocks.
	emit := func(p Sample) error {
		if p.Time%a.Step != 0 {
			return errors.New("source returned unaligned archive slot")
		}
		slot := (p.Time / a.Step) % a.Points
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
	readCtx := &blockReadContext{Context: ctx, workspace: workspace}
	for _, sources := range groups {
		for _, s := range sources {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if err := s.Scan(readCtx, m.ID, a, start, end, emit); err != nil {
				return nil, err
			}
		}
	}
	for _, i := range workspace.touched {
		if times[i] != start+int64(i)*a.Step {
			out.Values[i] = math.NaN()
		}
	}
	return out, nil
}

// Idle caches are bounded by size class, independently of active admission.
// Each workspace allocates exactly its query length; a size class reuses only
// exact matches, preserving the public 32B/slot active admission bound.
// Total idle storage is below 112MiB. Queries above 131072 slots are uncached.
type renderWorkspace struct {
	payload   []byte
	values    []float64
	sequences []uint64
	times     []int64
	seen      []bool
	touched   []uint32
	next      *renderWorkspace
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
	// Values are valid until Close. Reset only the slots that this handle wrote;
	// all untouched slots are already canonical NaNs from allocation/last close.
	// Overlay values belong to the caller and are never retained in w.values.
	for _, i := range w.touched {
		w.values[i] = math.NaN()
		w.seen[i] = false
	}
	w.touched = w.touched[:0]
	w.next = nil
	// Payload scratch belongs to the active worker only, outside idle pools.
	w.payload = nil
	if pool := renderWorkspacePool(len(w.values)); pool != nil {
		select {
		case pool <- w:
		default:
		}
	}
}
