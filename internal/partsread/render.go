package partsread

import (
	"context"
	"errors"
	"math"
)

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
	// Values, winner sequence, timestamp and presence use at most 32B/slot;
	// no allocation grows with duplicate/input count.
	if n < 0 || n > h.work/32 {
		return nil, ErrAdmission
	}
	// Budget stays charged until the handle is closed, including prior results.
	h.work -= n * 32
	out := &Series{Metric: m, From: start, Until: end, Step: a.Step, Values: nil}
	workspace := takeRenderWorkspace(int(n))
	workspace.next = h.workspaces
	h.workspaces = workspace
	out.Values = workspace.values
	seq, times, seen := workspace.sequences, workspace.times, workspace.seen
	clear(seen)
	out.Occupied = seen
	for i := range out.Values {
		out.Values[i] = math.NaN()
	}
	emit := func(p Sample) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		if p.Time%a.Step != 0 {
			return errors.New("source returned unaligned archive slot")
		}
		i := ((p.Time/a.Step)%a.Points - (start/a.Step)%a.Points + a.Points) % a.Points
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
			seen[i] = true
			seq[i] = p.Sequence
			times[i] = p.Time
			out.Values[i] = math.Float64frombits(p.Bits)
		}
		return nil
	}
	for _, sources := range groups {
		for _, s := range sources {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if err := s.Scan(ctx, m.ID, a, start, end, emit); err != nil {
				return nil, err
			}
		}
	}
	for i := range out.Values {
		if !seen[i] || times[i] != start+int64(i)*a.Step {
			out.Values[i] = math.NaN()
		}
	}
	return out, nil
}

// Fixed-size idle caches retain at most 76MiB. Active arrays remain charged to
// their handle until Close; an arbitrary query size is allocated exactly and
// never cached. Idle memory is separate from the active-reader byte budget.
type renderWorkspace struct {
	values    []float64
	sequences []uint64
	times     []int64
	seen      []bool
	next      *renderWorkspace
}

var hourRenderWorkspaces = make(chan *renderWorkspace, 64)
var dayRenderWorkspaces = make(chan *renderWorkspace, 32)

func renderWorkspacePool(n int) chan *renderWorkspace {
	switch n {
	case 3600:
		return hourRenderWorkspaces
	case 86400:
		return dayRenderWorkspaces
	}
	return nil
}
func takeRenderWorkspace(n int) *renderWorkspace {
	if pool := renderWorkspacePool(n); pool != nil {
		select {
		case w := <-pool:
			return w
		default:
		}
	}
	return &renderWorkspace{values: make([]float64, n), sequences: make([]uint64, n), times: make([]int64, n), seen: make([]bool, n)}
}
func releaseRenderWorkspace(w *renderWorkspace) {
	w.next = nil
	if pool := renderWorkspacePool(len(w.values)); pool != nil {
		select {
		case pool <- w:
		default:
		}
	}
}
