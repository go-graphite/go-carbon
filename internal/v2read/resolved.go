package v2read

import (
	"context"
	"slices"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
)

// resolvedSource owns one immutable winner per physical slot. Canonical plans
// already sort by archive/slot; merge their replacements without a timestamp
// Freeze/copy/sort. A wrapped timestamp replaces the old slot before filtering.
type resolvedSource struct {
	id       uint64
	archives []sealrollup.Archive
	records  []sealrollup.Record
}

func recordCompare(a, b sealrollup.Record) int {
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
}
func newResolvedSource(id uint64, archives []sealrollup.Archive, records []sealrollup.Record) *resolvedSource {
	records = slices.Clone(records)
	slices.SortFunc(records, recordCompare)
	return &resolvedSource{id, archives, records}
}
func (s *resolvedSource) replace(updates []sealrollup.Record) *resolvedSource {
	out := make([]sealrollup.Record, 0, len(s.records)+len(updates))
	i, j := 0, 0
	for i < len(s.records) && j < len(updates) {
		cmp := recordCompare(s.records[i], updates[j])
		if cmp < 0 {
			out = append(out, s.records[i])
			i++
		} else {
			out = append(out, updates[j])
			j++
			if cmp == 0 {
				i++
			}
		}
	}
	out = append(out, s.records[i:]...)
	out = append(out, updates[j:]...)
	return &resolvedSource{s.id, s.archives, out}
}
func (s *resolvedSource) lower(a int, slot int64) int {
	i, _ := slices.BinarySearchFunc(s.records, sealrollup.Record{Delta: sealrollup.Delta{Archive: a, Slot: slot}}, recordCompare)
	return i
}
func (s *resolvedSource) view(a int, slot int64) (sealrollup.Sample, bool) {
	i := s.lower(a, slot)
	if i < len(s.records) && s.records[i].Archive == a && s.records[i].Slot == slot {
		return s.records[i].Sample, true
	}
	return sealrollup.Sample{}, false
}
func (s *resolvedSource) rangeSamples(a int, start, end int64, dst []sealrollup.Sample) []sealrollup.Sample {
	ar := s.archives[a]
	rs, n := SlotRanges(Archive{ar.Step, ar.Count}, start, end)
	for _, r := range rs[:n] {
		for i := s.lower(a, r.From); i < len(s.records) && s.records[i].Archive == a && s.records[i].Slot < r.Until; i++ {
			p := s.records[i].Sample
			if p.Time >= start && p.Time < end {
				dst = append(dst, p)
			}
		}
	}
	return dst
}
func (s *resolvedSource) archive(step int64) int {
	for i, a := range s.archives {
		if a.Step == step {
			return i
		}
	}
	return -1
}
func (s *resolvedSource) HasArchive(ctx context.Context, id uint64, step int64) (bool, error) {
	a := s.archive(step)
	if a < 0 || id != s.id {
		return false, ctx.Err()
	}
	i := s.lower(a, 0)
	return i < len(s.records) && s.records[i].Archive == a, ctx.Err()
}
func (s *resolvedSource) Scan(ctx context.Context, id uint64, a Archive, from, until int64, yield func(Sample) error) error {
	archive := s.archive(a.Step)
	if archive < 0 || id != s.id {
		return ctx.Err()
	}
	rs, n := SlotRanges(a, from, until)
	for _, r := range rs[:n] {
		if err := ctx.Err(); err != nil {
			return err
		}
		for i := s.lower(archive, r.From); i < len(s.records) && s.records[i].Archive == archive && s.records[i].Slot < r.Until; i++ {
			p := s.records[i]
			if err := yield(Sample{Time: p.Sample.Time, Bits: p.Sample.Bits, Sequence: p.Order}); err != nil {
				return err
			}
		}
	}
	return ctx.Err()
}
func (s *resolvedSource) RetainedBytes() int64 {
	return 64 + int64(cap(s.records))*64 + int64(len(s.archives))*16
}

// A bounded history survives overlapping snapshot pins without retaining any
// snapshot. The sealed generation is the authority, never a pending frontier.
type resolvedEntry struct {
	version    any
	generation uint64
	name       string
	source     *resolvedSource
	bytes      int64
}

// Missing names keep the sealed generation fence; existing names share their
// exact immutable part-set identity across unrelated owner seals.
func (s *sealedData) metricVersion(name string) any {
	if s.index != nil {
		if v := s.index.MetricVersion(name); v != nil {
			return v
		}
	}
	return s.number
}

func (s *sealedData) resolved(name string, policy v2core.RollupPolicy, meta v2core.RollupMetadata) (*resolvedSource, error) {
	return s.resolvedContext(context.Background(), name, policy, meta)
}
func (s *sealedData) resolvedContext(ctx context.Context, name string, policy v2core.RollupPolicy, meta v2core.RollupMetadata) (*resolvedSource, error) {
	c := s.cache
	version := s.metricVersion(name)
	c.fullMu.Lock()
	for _, e := range c.full {
		if e.version == version && e.name == name {
			c.fullMu.Unlock()
			return e.source, nil
		}
	}
	c.fullMu.Unlock()
	records, err := s.index.Records(ctx, name)
	if err != nil {
		return nil, err
	}
	source := newResolvedSource(meta.ID, policy.Config.Archives, records)
	size := source.RetainedBytes() + int64(len(name)) + 64
	if size > c.limit {
		return source, nil
	}
	c.fullMu.Lock()
	defer c.fullMu.Unlock()
	for _, e := range c.full {
		if e.version == version && e.name == name {
			return e.source, nil
		}
	}
	for len(c.full) > 0 && (len(c.full) >= 64 || c.fullBytes+size > c.limit) {
		c.fullBytes -= c.full[0].bytes
		copy(c.full, c.full[1:])
		c.full[len(c.full)-1] = resolvedEntry{}
		c.full = c.full[:len(c.full)-1]
	}
	c.full = append(c.full, resolvedEntry{version, s.number, name, source, size})
	c.fullBytes += size
	return source, nil
}

// A pending prefix is reused only after exact comparison of every original
// boundary, clock and scalar sample. No digest or order-only shortcut can turn
// a different publication into the same history. Entries retain no generation.
type pendingReduction struct {
	version    any
	generation uint64
	name       string
	id         uint64
	policy     []byte
	batches    []sealrollup.Batch
	frontier   sealrollup.Frontier
	source     *resolvedSource
	bytes      int64
}

func samePrefix(prefix, batches []sealrollup.Batch) bool {
	if len(prefix) > len(batches) {
		return false
	}
	for i, p := range prefix {
		b := batches[i]
		if p.Order != b.Order || p.Now != b.Now || !slices.Equal(p.Points, b.Points) {
			return false
		}
	}
	return true
}
func (s *sealedData) cachedReduction(m *metricState) *pendingReduction {
	c := s.cache
	c.pendingMu.Lock()
	candidates := append([]pendingReduction(nil), c.reductions...)
	c.pendingMu.Unlock()
	var best *pendingReduction
	for i := range candidates {
		e := &candidates[i]
		if e.version != s.metricVersion(m.name) || e.name != m.name || e.id != m.meta.ID || !slices.Equal(e.policy, m.meta.Policy) || (best != nil && len(e.batches) <= len(best.batches)) {
			continue
		}
		if samePrefix(e.batches, m.pending) {
			best = e
		}
	}
	return best
}
func (s *sealedData) rememberReduction(m *metricState, next sealrollup.Frontier) {
	c := s.cache
	e := pendingReduction{version: s.metricVersion(m.name), generation: s.number, name: m.name, id: m.meta.ID, policy: m.meta.Policy, batches: m.pending, frontier: next, source: m.frozen}
	e.bytes = m.frozen.RetainedBytes() + int64(len(e.name)+len(e.policy)) + int64(cap(e.batches))*64 + 128
	for _, b := range e.batches {
		e.bytes += int64(cap(b.Points)) * 24
	}
	if e.bytes > c.limit {
		return
	}
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	// Keep the newest prefix per metric and sealed generation; old readers retain
	// their own immutable source even after this bounded history evicts it.
	for i, old := range c.reductions {
		if old.version == e.version && old.name == e.name {
			if len(old.batches) > len(e.batches) {
				return
			}
			c.reductionBytes -= old.bytes
			copy(c.reductions[i:], c.reductions[i+1:])
			c.reductions[len(c.reductions)-1] = pendingReduction{}
			c.reductions = c.reductions[:len(c.reductions)-1]
			break
		}
	}
	for len(c.reductions) > 0 && (len(c.reductions) >= 64 || c.reductionBytes+e.bytes > c.limit) {
		c.reductionBytes -= c.reductions[0].bytes
		copy(c.reductions, c.reductions[1:])
		c.reductions[len(c.reductions)-1] = pendingReduction{}
		c.reductions = c.reductions[:len(c.reductions)-1]
	}
	c.reductions = append(c.reductions, e)
	c.reductionBytes += e.bytes
}

// Stable ordering is reusable independently of batch order/clock. The canonical
// reducer still owns admission and aggregation, and rechecks its input order.
// Exact scalar comparison makes this useful for overlapping pending arenas and
// repeated corrections without hash collision or pointer-lifetime assumptions.
type orderedSamples struct {
	original, sorted []sealrollup.Sample
	bytes            int64
}

func (c *sealedCache) orderSamples(input []sealrollup.Sample) []sealrollup.Sample {
	c.orderMu.Lock()
	for _, e := range c.ordered {
		if slices.Equal(e.original, input) {
			c.orderMu.Unlock()
			return e.sorted
		}
	}
	c.orderMu.Unlock()
	sorted := slices.Clone(input)
	slices.SortStableFunc(sorted, func(a, b sealrollup.Sample) int {
		if a.Time < b.Time {
			return -1
		}
		if a.Time > b.Time {
			return 1
		}
		if a.Sequence < b.Sequence {
			return -1
		}
		if a.Sequence > b.Sequence {
			return 1
		}
		return 0
	})
	size := int64(cap(input)+cap(sorted))*24 + 64
	if size > c.limit {
		return sorted
	}
	c.orderMu.Lock()
	defer c.orderMu.Unlock()
	for _, e := range c.ordered {
		if slices.Equal(e.original, input) {
			return e.sorted
		}
	}
	for len(c.ordered) > 0 && (len(c.ordered) >= 64 || c.orderedBytes+size > c.limit) {
		c.orderedBytes -= c.ordered[0].bytes
		copy(c.ordered, c.ordered[1:])
		c.ordered[len(c.ordered)-1] = orderedSamples{}
		c.ordered = c.ordered[:len(c.ordered)-1]
	}
	c.ordered = append(c.ordered, orderedSamples{input, sorted, size})
	c.orderedBytes += size
	return sorted
}
