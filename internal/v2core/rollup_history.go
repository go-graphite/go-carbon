package v2core

import (
	"bytes"
	"container/list"
	"context"
	"errors"
	"slices"
	"unsafe"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2dict"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// A lane owns this cache and its planner serially. Cached windows own scalar
// samples only; snapshots, iterators, metric pointers and part directories do
// not survive a plan. Commit patches cached source windows after durable ingest.
const rollupWindowCacheBytes = 1 << 20
const rollupWindowCacheEntries = 256

type rollupWindowKey struct {
	id         uint64
	archive    int
	start, end int64
}
type rollupWindow struct {
	key     rollupWindowKey
	samples []sealrollup.Sample
	bytes   int
}
type rollupWindowCache struct {
	entries     map[rollupWindowKey]*list.Element
	byMetric    map[uint64]map[rollupWindowKey]*list.Element
	lru         list.List
	bytes       int
	loads, hits uint64
}

func (c *rollupWindowCache) remove(e *list.Element) {
	w := e.Value.(*rollupWindow)
	c.bytes -= w.bytes
	delete(c.entries, w.key)
	delete(c.byMetric[w.key.id], w.key)
	if len(c.byMetric[w.key.id]) == 0 {
		delete(c.byMetric, w.key.id)
	}
	c.lru.Remove(e)
}
func (c *rollupWindowCache) rangeSamples(r *rollupHistoryReader, m *rollupMetric, meta metricMetadata, a int, start, end int64) ([]sealrollup.Sample, error) {
	key := rollupWindowKey{m.id, a, start, end}
	if e := c.entries[key]; e != nil {
		c.hits++
		c.lru.MoveToFront(e)
		return e.Value.(*rollupWindow).samples, nil
	}
	c.loads++
	samples, err := r.load(m, meta, a, start, end)
	if err != nil {
		return nil, err
	}
	size := 256 + cap(samples)*int(unsafe.Sizeof(sealrollup.Sample{}))
	if size <= rollupWindowCacheBytes {
		if c.entries == nil {
			c.entries = map[rollupWindowKey]*list.Element{}
			c.byMetric = map[uint64]map[rollupWindowKey]*list.Element{}
		}
		for c.bytes+size > rollupWindowCacheBytes || len(c.entries) >= rollupWindowCacheEntries {
			c.remove(c.lru.Back())
		}
		e := c.lru.PushFront(&rollupWindow{key, samples, size})
		c.entries[key] = e
		if c.byMetric[m.id] == nil {
			c.byMetric[m.id] = map[rollupWindowKey]*list.Element{}
		}
		c.byMetric[m.id][key] = e
		c.bytes += size
	}
	return samples, nil
}
func (c *rollupWindowCache) commit(m *rollupMetric, records []sealrollup.Record, expired bool) {
	for _, e := range c.byMetric[m.id] {
		if expired {
			c.remove(e)
			continue
		}
		w := e.Value.(*rollupWindow)
		ar := m.schema().Archives[w.key.archive]
		for _, record := range records {
			if record.Archive != w.key.archive {
				continue
			}
			p := record.Sample
			// Resolve physical replacement before filtering the cached time window.
			kept := w.samples[:0]
			for _, old := range w.samples {
				if (old.Time/ar.Step)%ar.Count != record.Slot {
					kept = append(kept, old)
				}
			}
			w.samples = kept
			if p.Time >= w.key.start && p.Time < w.key.end {
				w.samples = append(w.samples, p)
			}
		}
		slices.SortFunc(w.samples, compareHistorySamples)
		size := 256 + cap(w.samples)*int(unsafe.Sizeof(sealrollup.Sample{}))
		c.bytes += size - w.bytes
		w.bytes = size
	}
	for c.bytes > rollupWindowCacheBytes {
		c.remove(c.lru.Back())
	}
}
func compareHistorySamples(a, b sealrollup.Sample) int {
	if a.Time < b.Time {
		return -1
	}
	if a.Time > b.Time {
		return 1
	}
	return 0
}

// The ordinary live path needs only existing part scopes, not a second
// per-name read directory. Each cold window performs bounded seeks in those
// parts. Copied stores reuse the existing move-aware resolver for precedence
// and coarse repair authority, rather than duplicating those rules here.
type rollupHistoryReader struct {
	db       *pebble.DB
	dict     *v2dict.Dict // the store dictionary of ID-keyed parts, or nil
	snapshot *pebble.Snapshot
	// parts lists each (group, vshard)'s parts of both key generations,
	// keyed by the scope prefix without its magic and partition.
	parts  map[string][]*rollupHistoryPart
	opened list.List
	copied *RollupSnapshot
}

type rollupHistoryPart struct {
	descriptor v2part.Descriptor
	iterator   *archiveIter
	age        *list.Element
}

func (r *rollupHistoryReader) close() {
	for e := r.opened.Front(); e != nil; e = e.Next() {
		_ = e.Value.(*rollupHistoryPart).iterator.Close()
	}
	if r.snapshot != nil {
		_ = r.snapshot.Close()
	}
}
func (r *rollupHistoryReader) open() error {
	if r.snapshot != nil {
		return nil
	}
	r.snapshot = r.db.NewSnapshot()
	ctx := context.Background()
	found, err := snapshotHasCopiedParts(r.snapshot)
	if err != nil {
		return err
	}
	if found {
		identities := &PolicyCatalog{}
		identities.dict.Store(r.dict)
		r.copied, err = NewRollupSnapshot(ctx, r.snapshot, nil, identities, nil, 0)
		return err
	}
	selected := map[string]v2part.Descriptor{}
	inventory, scopes, err := rollupInventory(ctx, r.snapshot, nil, nil, selected)
	if err != nil {
		return err
	}
	if inventory == nil {
		r.copied, err = NewRollupSnapshot(ctx, r.snapshot, nil, nil, nil, 0)
		return err
	}
	r.parts = map[string][]*rollupHistoryPart{}
	known := map[v2part.Digest]*rollupHistoryPart{}
	for _, scope := range scopes {
		n := len(scope) - len(v2part.PartID{})
		if n < 0 {
			return errors.New("invalid rollup history scope")
		}
		d := selected[scope]
		part := known[d.SHA256]
		if part == nil {
			part = &rollupHistoryPart{descriptor: d}
			known[d.SHA256] = part
		}
		route := scope[len(partKeyMagic) : n-8]
		r.parts[route] = append(r.parts[route], part)
	}
	return nil
}
func (r *rollupHistoryReader) load(m *rollupMetric, meta metricMetadata, a int, start, end int64) ([]sealrollup.Sample, error) {
	if meta.Expired {
		return nil, nil
	}
	if err := r.open(); err != nil {
		return nil, err
	}
	ar := m.schema().Archives[a]
	lo := (start / ar.Step) % ar.Count
	hi := lo + (end-start)/ar.Step
	winners := map[int64]sealrollup.Record{}
	emit := func(record sealrollup.Record) error {
		if r.copied == nil && record.Order > meta.Frontier.Order {
			return errors.New("history exceeds committed frontier")
		}
		if r.copied == nil && record.Order <= meta.ResetOrder {
			return nil
		}
		old, ok := winners[record.Slot]
		newer := !ok
		if ok {
			var err error
			newer, err = newerOrigin(partOrigin{Class: LivePart}, record, partOrigin{Class: LivePart}, old)
			if err != nil {
				return err
			}
		}
		if newer {
			winners[record.Slot] = record
		}
		return nil
	}
	scan := func(first, last int64) error {
		if r.copied != nil {
			return r.copied.ScanArchive(context.Background(), string(m.policy.Name), a, first, last, emit)
		}
		scope, err := v2part.PartitionPrefixForGroup(m.policy.Group, m.vshard, 0)
		if err != nil {
			return err
		}
		ids := metricIDs{dict: r.dict, known: m.PolicyIdentity}
		for _, part := range r.parts[string(scope[len(partKeyMagic):len(scope)-8])] {
			// Older logical timestamps cannot mask a winner in this window. Newer
			// parts remain eligible even outside end: resolve ring epochs first.
			if part.descriptor.Coverage.MaxTime < start {
				continue
			}
			if part.iterator == nil {
				if r.opened.Len() == 64 {
					old := r.opened.Back()
					p := old.Value.(*rollupHistoryPart)
					_ = p.iterator.Close()
					p.iterator = nil
					p.age = nil
					r.opened.Remove(old)
				}
				var err error
				part.iterator, err = newArchiveIterSelected(context.Background(), r.snapshot, nil, nil, part.descriptor)
				if err != nil {
					return err
				}
				part.age = r.opened.PushFront(part)
			} else {
				r.opened.MoveToFront(part.age)
			}
			it := part.iterator
			key := v2part.Key{Group: m.policy.Group, VShard: m.vshard, Partition: part.descriptor.Scope.Partition, Part: part.descriptor.Scope.Part, ByID: descriptorByID(part.descriptor)}
			key, ok, err := ids.key(key, m.policy.Name)
			if err != nil {
				return err
			}
			if !ok {
				continue // no ID: no ID-keyed part holds the name
			}
			key.Archive, key.BlockStart = uint32(a), first
			lower, err := v2part.Encode(key)
			if err != nil {
				return err
			}
			prefix := lower[:archivePrefixLen(lower, key.ByID)]
			valid := it.SeekLT(lower)
			if !valid || !bytes.HasPrefix(it.Key(), prefix) {
				valid = it.SeekGE(lower)
			}
			for valid && bytes.HasPrefix(it.Key(), prefix) {
				k, err := v2part.DecodeKey(it.Key())
				if err != nil {
					return err
				}
				if k.BlockStart >= last {
					break
				}
				records, err := v2part.DecodeSlots(k.Archive, k.BlockStart, it.Value())
				if err != nil {
					return err
				}
				for _, record := range records {
					if record.Slot >= first && record.Slot < last {
						if err = emit(record); err != nil {
							return err
						}
					}
				}
				valid = it.Next()
			}
			if err := it.Error(); err != nil {
				return err
			}
		}
		return nil
	}
	if err := scan(lo, min(hi, ar.Count)); err != nil {
		return nil, err
	}
	if hi > ar.Count {
		if err := scan(0, hi-ar.Count); err != nil {
			return nil, err
		}
	}
	result := make([]sealrollup.Sample, 0, len(winners))
	for _, record := range winners {
		if record.Sample.Time >= start && record.Sample.Time < end {
			result = append(result, record.Sample)
		}
	}
	slices.SortFunc(result, compareHistorySamples)
	return result, nil
}
