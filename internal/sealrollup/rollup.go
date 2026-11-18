// Package sealrollup reduces ordered sealed batches to immutable archive-slot
// replacements. It performs no I/O and mutates neither its input nor its view.
// The caller owns publication, retention of the view, and durable batch order.
package sealrollup

import (
	"fmt"
	"math"
	"slices"
)

type Method uint32

const (
	Average Method = iota + 1
	Sum
	Last
	Max
	Min
	First
)

type Archive struct{ Step, Count int64 }
type Config struct {
	Archives     []Archive
	Method       Method
	XFilesFactor float32
}

// Sample retains float bits, including stored NaNs and signed zero. Sequence
// breaks ties only between identical source timestamps within one batch.
type Sample struct {
	Time           int64
	Bits, Sequence uint64
}

// Batch is one original UpdateMany boundary, not an arbitrary sorted partition.
// Now is its original admission clock; Order is a strictly increasing source
// batch sequence. Replaying with a different clock can select another archive.
type Batch struct {
	Order  uint64
	Now    int64
	Points []Sample
}

// Delta replaces one physical ring slot. Resolve by (Archive,Slot) and Order
// BEFORE filtering Time. NaN is a stored value, not an absent slot/tombstone.
type Delta struct {
	Archive int
	Slot    int64
	Order   uint64
	Sample  Sample
}

// View returns the latest physical slot, including its actual epoch timestamp.
// It must be immutable for the entire Reduce call. nil means an empty history.
type View func(archive int, slot int64) (Sample, bool)

// RangeView appends all occupied samples in [start,end), in ascending Time.
// It must resolve physical-slot versions before filtering timestamps, just as
// View does. The returned storage belongs to dst; no snapshot slices may escape.
// A sparse index avoids probing every absent slot in a coarse source bucket.
type RangeView func(archive int, start, end int64, dst []Sample) []Sample
type key struct {
	archive int
	slot    int64
}

func (c Config) Validate() error {
	if len(c.Archives) == 0 {
		return fmt.Errorf("no archives")
	}
	if c.Method < Average || c.Method > First {
		return fmt.Errorf("unsupported aggregation method %d", c.Method)
	}
	if math.IsNaN(float64(c.XFilesFactor)) || c.XFilesFactor < 0 || c.XFilesFactor > 1 {
		return fmt.Errorf("invalid xFilesFactor")
	}
	for i, a := range c.Archives {
		if a.Step <= 0 || a.Count <= 0 || a.Step > math.MaxInt32/a.Count {
			return fmt.Errorf("invalid archive %d", i)
		}
		if i > 0 {
			p := c.Archives[i-1]
			if a.Step <= p.Step || a.Step%p.Step != 0 || a.Step*a.Count <= p.Step*p.Count || a.Step/p.Step > p.Count {
				return fmt.Errorf("invalid archive ladder at %d", i)
			}
		}
	}
	return nil
}
func align(t, step int64) int64     { return t - t%step }
func slot(t int64, a Archive) int64 { return (t / a.Step) % a.Count }

// Reduce applies original batches in order, returning only final replacements
// for touched slots. Fine neighbors come from the supplied sealed snapshot;
// lower values are never combined as partial sums or averages. Failed XFF
// leaves the previous coarse slot untouched, matching Whisper propagation.
func Reduce(c Config, view View, batches []Batch) ([]Delta, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return reduce(c, view, nil, batches, nil)
}

// Workspace is reusable scratch for one serial owner. No returned SealPlan
// aliases it. Keep independent workspaces for concurrent planning calls.
type Workspace struct {
	positions                  []uint32
	offsets                    []int
	dense                      bool
	sparseOnly                 bool
	updates                    []Delta
	points, current, neighbors []Sample
	changed                    []int64
}

// preparePositions allocates once per owner/high-water mark, before point work.
// Dense updates use direct physical-slot indices. Sparse updates use an
// open-addressed array bounded by input size, never the retention length.
func (w *Workspace) preparePositions(c Config, points int) {
	w.offsets = slices.Grow(w.offsets[:0], len(c.Archives)+1)
	total := int64(0)
	for _, a := range c.Archives {
		w.offsets = append(w.offsets, int(total))
		total += a.Count
	}
	w.offsets = append(w.offsets, int(total))
	bound := min(total, int64(points)*int64(len(c.Archives)))
	w.dense = !w.sparseOnly && total <= 1<<18 && total <= 4*bound
	size := int(total)
	if !w.dense {
		size = 16
		for int64(size) < 2*bound {
			size *= 2
		}
	}
	if cap(w.positions) < size {
		w.positions = make([]uint32, size)
	} else {
		w.positions = w.positions[:size]
	}
	// A direct entry is validated against the current updates, so it needs no
	// retention-sized clear. Sparse probe chains must start empty each call.
	if !w.dense {
		clear(w.positions)
	}
}
func (w *Workspace) position(k key) int {
	if w.dense {
		return w.offsets[k.archive] + int(k.slot)
	}
	h := uint64(k.slot)*0x9e3779b97f4a7c15 ^ uint64(k.archive)*0xc2b2ae3d27d4eb4f
	h ^= h >> 32
	i := int(h) & (len(w.positions) - 1)
	for {
		n := w.positions[i]
		if n == 0 {
			return i
		}
		d := w.updates[int(n)-1]
		if d.Archive == k.archive && d.Slot == k.slot {
			return i
		}
		i = (i + 1) & (len(w.positions) - 1)
	}
}
func (w *Workspace) lookup(k key) (Delta, bool) {
	i := w.positions[w.position(k)]
	if i == 0 || int(i) > len(w.updates) {
		return Delta{}, false
	}
	d := w.updates[int(i)-1]
	return d, d.Archive == k.archive && d.Slot == k.slot
}
func (w *Workspace) replace(k key, d Delta) {
	pos := w.position(k)
	i := w.positions[pos]
	if i != 0 && int(i) <= len(w.updates) {
		old := w.updates[int(i)-1]
		if old.Archive == k.archive && old.Slot == k.slot {
			w.updates[int(i)-1] = d
			return
		}
	}
	w.updates = append(w.updates, d)
	w.positions[pos] = uint32(len(w.updates))
}

type windowClosure struct{ through, advance int64 }

func reduce(c Config, view View, ranges RangeView, batches []Batch, w *Workspace) ([]Delta, error) {
	return reduceMode(c, view, ranges, batches, w, nil)
}
func reduceMode(c Config, view View, ranges RangeView, batches []Batch, w *Workspace, closing *windowClosure) ([]Delta, error) {
	if w == nil {
		// One-shot callers cannot amortize a direct retention-sized array.
		w = &Workspace{sparseOnly: true}
	}
	pointCount := 0
	for i, b := range batches {
		pointCount += len(b.Points)
		if b.Now <= 0 || b.Now > math.MaxUint32 {
			return nil, fmt.Errorf("invalid batch clock")
		}
		if i > 0 && b.Order <= batches[i-1].Order {
			return nil, fmt.Errorf("batch order must increase")
		}
		for _, p := range b.Points {
			if p.Time <= 0 || p.Time > math.MaxUint32 {
				return nil, fmt.Errorf("timestamp outside storage range")
			}
		}
	}
	w.updates = w.updates[:0]
	if closing != nil {
		pointCount++
	}
	w.preparePositions(c, pointCount)
	get := func(a int, t int64) (Sample, bool) {
		k := key{a, slot(t, c.Archives[a])}
		if d, ok := w.lookup(k); ok {
			return d.Sample, d.Sample.Time == t
		}
		if view == nil {
			return Sample{}, false
		}
		p, ok := view(a, k.slot)
		return p, ok && p.Time == t
	}
	neighbors := w.neighbors
	defer func() { w.neighbors = neighbors }()
	for _, b := range batches {
		w.points = append(w.points[:0], b.Points...)
		points := w.points
		// Equivalent to Whisper's reverse, stable newest-first, reverse. For
		// equal timestamps the final source sequence wins; within an aligned slot
		// the latest timestamp wins, even when it arrived earlier in the batch.
		slices.SortStableFunc(points, func(a, z Sample) int {
			if a.Time < z.Time {
				return -1
			}
			if a.Time > z.Time {
				return 1
			}
			if a.Sequence < z.Sequence {
				return -1
			}
			if a.Sequence > z.Sequence {
				return 1
			}
			return 0
		})
		set := func(a int, t int64, bits uint64) {
			p := Sample{Time: t, Bits: bits, Sequence: b.Order}
			k := key{a, slot(t, c.Archives[a])}
			w.replace(k, Delta{a, k.slot, b.Order, p})
		}

		aggregateAt := func(lower int, start int64, closingWindow bool) bool {
			higher, coarse := c.Archives[lower-1], c.Archives[lower]
			neighbors = neighbors[:0]
			if ranges == nil {
				for ts := start; ts < start+coarse.Step; ts += higher.Step {
					if p, ok := get(lower-1, ts); ok {
						neighbors = append(neighbors, p)
					}
				}
			} else {
				neighbors = ranges(lower-1, start, start+coarse.Step, neighbors)
				kept := neighbors[:0]
				for _, p := range neighbors {
					// Even a replacement with a different epoch invalidates
					// the older occupied physical slot in the snapshot.
					if _, replaced := w.lookup(key{lower - 1, slot(p.Time, higher)}); !replaced {
						kept = append(kept, p)
					}
				}
				neighbors = kept
				for _, d := range w.updates {
					if d.Archive == lower-1 && d.Sample.Time >= start && d.Sample.Time < start+coarse.Step {
						neighbors = append(neighbors, d.Sample)
					}
				}
				slices.SortFunc(neighbors, func(a, b Sample) int {
					if a.Time < b.Time {
						return -1
					}
					if a.Time > b.Time {
						return 1
					}
					return 0
				})
			}
			if len(neighbors) == 0 || float32(len(neighbors))/float32(coarse.Step/higher.Step) < c.XFilesFactor {
				return false
			}
			set(lower, start, math.Float64bits(aggregateSamples(c.Method, neighbors)))
			if closingWindow {
				var order uint64
				for _, p := range neighbors {
					order = max(order, p.Sequence)
				}
				k := key{lower, slot(start, coarse)}
				d, _ := w.lookup(k)
				d.Order, d.Sample.Sequence = order, order
				w.replace(k, d)
			}
			return true
		}
		if closing != nil {
			through := max(closing.through, closing.advance)
			for _, point := range b.Points {
				through = max(through, point.Time)
			}
			// Flush source windows before any new epoch can replace their ring
			// slots. Ascending archives make the last child available to its parent.
			for lower := 1; lower < len(c.Archives); lower++ {
				a := c.Archives[lower]
				start := align(closing.through, a.Step)
				if closing.through > 0 && start+a.Step <= through {
					aggregateAt(lower, start, true)
				}
			}
			closing.through = through
		}
		for archive, a := range c.Archives {
			current := w.current[:0]
			for _, p := range points {
				target := -1
				for j, r := range c.Archives {
					if p.Time >= b.Now-r.Step*r.Count {
						target = j
						break
					}
				}
				if target == archive {
					current = append(current, p)
				}
			}
			w.current = current
			if len(current) == 0 {
				continue
			}
			changed := w.changed[:0]
			for _, p := range current {
				t := align(p.Time, a.Step)
				set(archive, t, p.Bits)
				changed = append(changed, t)
			}
			w.changed = changed
			// Every original interval remains eligible at each level. Do not
			// propagate only the successful child buckets: Whisper does not do that.
			for lower := archive + 1; lower < len(c.Archives); lower++ {
				coarse := c.Archives[lower]
				wrote := false
				previous := int64(-1)
				for _, t := range changed {
					start := align(t, coarse.Step)
					if start == previous {
						continue
					}
					previous = start
					if closing != nil && start+coarse.Step > closing.through {
						continue
					}
					if !aggregateAt(lower, start, false) {
						continue
					}
					wrote = true
				}
				if !wrote {
					break
				}
			}
		}
	}
	out := w.updates
	slices.SortFunc(out, func(a, b Delta) int {
		if a.Archive < b.Archive {
			return -1
		}
		if a.Archive > b.Archive {
			return 1
		}
		if a.Slot < b.Slot {
			return -1
		}
		if a.Slot > b.Slot {
			return 1
		}
		return 0
	})
	return out, nil
}
func aggregate(method Method, values []float64) float64 {
	switch method {
	case Average, Sum:
		result := 0.0
		for _, v := range values {
			result += v
		}
		if method == Average {
			result /= float64(len(values))
		}
		return result
	case First:
		return values[0]
	case Last:
		return values[len(values)-1]
	case Min:
		result := values[0]
		for _, v := range values {
			if v < result {
				result = v
			}
		}
		return result
	case Max:
		result := values[0]
		for _, v := range values {
			if v > result {
				result = v
			}
		}
		return result
	}
	panic("validated method")
}

func aggregateSamples(method Method, values []Sample) float64 {
	switch method {
	case Average, Sum:
		result := 0.0
		for _, v := range values {
			result += math.Float64frombits(v.Bits)
		}
		if method == Average {
			result /= float64(len(values))
		}
		return result
	case First:
		return math.Float64frombits(values[0].Bits)
	case Last:
		return math.Float64frombits(values[len(values)-1].Bits)
	case Min:
		result := math.Float64frombits(values[0].Bits)
		for _, v := range values {
			if value := math.Float64frombits(v.Bits); value < result {
				result = value
			}
		}
		return result
	case Max:
		result := math.Float64frombits(values[0].Bits)
		for _, v := range values {
			if value := math.Float64frombits(v.Bits); value > result {
				result = value
			}
		}
		return result
	}
	panic("validated method")
}
