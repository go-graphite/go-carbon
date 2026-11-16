package v2backend

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/carbonserver"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/daemonstore"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/partsread"
	"github.com/go-graphite/go-carbon/internal/partsrun"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

type catalog struct {
	names map[string]partsread.Metric
	ids   map[uint64]string
	index *carbonserver.PartsCatalogIndex
	// A root shares disjoint, immutable size-tiered segments. Existing names
	// and IDs never move when a seal contains only point updates.
	segments []*catalog
	count    int
	retained int64
}

func (c *catalog) Lookup(name string) (partsread.Metric, bool) {
	m, ok := c.names[name]
	if ok {
		return m, true
	}
	for _, segment := range c.segments {
		if segment != nil {
			if m, ok = segment.names[name]; ok {
				return m, true
			}
		}
	}
	return partsread.Metric{}, false
}
func (c *catalog) nameForID(id uint64) (string, bool) {
	if name, ok := c.ids[id]; ok {
		return name, true
	}
	for _, segment := range c.segments {
		if segment != nil {
			if name, ok := segment.ids[id]; ok {
				return name, true
			}
		}
	}
	return "", false
}
func (c *catalog) findNames(pattern string) ([]string, []bool, error) {
	if c.index != nil {
		return c.index.Find(pattern)
	}
	var names []string
	var leafs []bool
	for _, segment := range c.segments {
		if segment == nil {
			continue
		}
		ns, ls, err := segment.index.Find(pattern)
		if err != nil {
			return nil, nil, err
		}
		names = append(names, ns...)
		leafs = append(leafs, ls...)
	}
	return names, leafs, nil
}
func (c *catalog) Find(ctx context.Context, pattern string, yield func(string) error) error {
	names, leafs, e := c.findNames(pattern)
	if e != nil {
		return e
	}
	for i, n := range names {
		if e = ctx.Err(); e != nil {
			return e
		}
		if leafs[i] {
			if e = yield(n); e != nil {
				return e
			}
		}
	}
	return nil
}
func toReadMetric(m daemonstore.Metric) partsread.Metric {
	r := partsread.Metric{AggregationMethod: m.AggregationMethod, XFilesFactor: m.XFilesFactor}
	for _, a := range m.Archives {
		r.Archives = append(r.Archives, partsread.Archive{Step: int64(a.Step), Points: int64(a.Points)})
	}
	return r
}
func toDaemonMetric(m partsread.Metric) daemonstore.Metric {
	r := daemonstore.Metric{AggregationMethod: m.AggregationMethod, XFilesFactor: m.XFilesFactor}
	for _, a := range m.Archives {
		r.Archives = append(r.Archives, daemonstore.Archive{Step: int(a.Step), Points: int(a.Points)})
	}
	return r
}
func (o *owner) publishCatalog() (*catalog, error) {
	old := o.readCatalog.Load()
	// Policies are fixed and this backend has no delete/rename path. Equal
	// cardinality therefore proves that the frozen catalogue is still current.
	if old != nil && old.count == len(o.metrics) {
		return old, nil
	}
	delta := &catalog{names: make(map[string]partsread.Metric), ids: make(map[uint64]string)}
	count := 0
	if old != nil {
		count = old.count
	}
	// Stable owner-local IDs are read handles only; durable GCP3 keys continue
	// carrying the complete portable PG/vshard/name identity.
	for name, m := range o.metrics {
		if old != nil {
			if _, exists := old.Lookup(name); exists {
				continue
			}
		}
		count++
		r := toReadMetric(m.Policy)
		r.ID = uint64(count)
		delta.names[name] = r
		delta.ids[r.ID] = name
	}
	if len(delta.names) == 0 && old != nil {
		return old, nil
	}
	c := &catalog{count: count}
	if old != nil {
		c.segments = slices.Clone(old.segments)
		c.retained = old.retained
	}
	addedBytes := int64(0)
	for name, m := range delta.names {
		addedBytes += int64(256 + len(name)*3 + len(m.Archives)*32)
	}
	c.retained += addedBytes
	// Binary size tiers bound lookup/find fanout by log2(catalogue size).
	// A merge happens only on new-name admission, never on ordinary seals.
	for len(delta.names) > 0 {
		level := bits.Len(uint(len(delta.names))) - 1
		for len(c.segments) <= level {
			c.segments = append(c.segments, nil)
		}
		prior := c.segments[level]
		if prior == nil {
			c.segments[level] = delta
			break
		}
		for name, m := range prior.names {
			delta.names[name] = m
			delta.ids[m.ID] = name
		}
		c.segments[level] = nil
	}
	metadata := make([]store.Metadata, 0, len(delta.names))
	for name, m := range delta.names {
		cfg, e := rollupConfig(toDaemonMetric(m))
		if e != nil {
			return nil, e
		}
		meta := store.Metadata{MetricConfig: store.MetricConfig{Name: name, AggregationMethod: store.AggregationMethod(cfg.Method), XFilesFactor: cfg.XFilesFactor}}
		for _, a := range m.Archives {
			meta.Retentions = append(meta.Retentions, store.Retention{Step: int(a.Step), Count: int(a.Points)})
		}
		metadata = append(metadata, meta)
	}
	var e error
	delta.index, e = carbonserver.NewPartsCatalogIndex(metadata, carbonserver.PartsIndexOptions{Trie: true, ReportFrequency: time.Minute})
	if count == 0 {
		c.index = delta.index
	}
	return c, e
}
func (o *owner) publish() error {
	started := o.s.stages.start()
	defer o.s.stages.finish(&o.s.stages.publish, started)
	c, e := o.publishCatalog()
	if e != nil {
		return e
	}
	retained := c.retained
	snap := o.db.NewSnapshot()
	source := &diskSource{snap: snap, owner: o.id, catalog: c}
	o.pub.Publish(partsread.View{Generation: o.meta.Cursor, Frontier: []uint64{o.meta.Cursor}, Catalog: c, Parts: []partsread.Source{source}, RetainedBytes: retained, Close: func() { _ = snap.Close() }})
	o.readCatalog.Store(c)
	base := &liveBase{catalog: c, cursor: o.meta.Cursor, frontiers: make(map[string]sealrollup.Frontier, len(o.metrics))}
	for n, m := range o.metrics {
		base.frontiers[n] = m.Frontier
	}
	bs := o.db.NewSnapshot()
	base.source = &diskSource{snap: bs, owner: o.id, catalog: c}
	base.pub.Publish(partsread.View{Generation: o.meta.Cursor, Catalog: c, Parts: []partsread.Source{base.source}, RetainedBytes: retained, Close: func() { _ = bs.Close() }})
	if old := o.readBase.Swap(base); old != nil {
		old.pub.Close()
	}
	return nil
}

type diskSource struct {
	snap    *pebble.Snapshot
	owner   int
	catalog *catalog
}

func (d *diskSource) Scan(ctx context.Context, id uint64, a partsread.Archive, from, until int64, yield func(partsread.Sample) error) error {
	name, ok := d.catalog.nameForID(id)
	if !ok {
		return partsread.ErrNotFound
	}
	m, _ := d.catalog.Lookup(name)
	archive := -1
	for i, c := range m.Archives {
		if c == a {
			archive = i
			break
		}
	}
	if archive < 0 {
		return errors.New("unknown archive")
	}
	prefix := refPrefix(d.owner, name)
	refs, e := d.snap.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: partsformat.PrefixEnd(prefix)})
	if e != nil {
		return e
	}
	defer refs.Close()
	n := (until - from) / a.Step
	var workspace partsrun.Workspace
	policy := toDaemonMetric(m)
	emit := func(r sealrollup.Record) error {
		if r.Archive != archive {
			return nil
		}
		slot := ((r.Sample.Time/a.Step)%a.Points - (from/a.Step)%a.Points + a.Points) % a.Points
		if slot < n {
			return yield(partsread.Sample{Time: r.Sample.Time, Bits: r.Sample.Bits, Sequence: r.Order})
		}
		return nil
	}
	for refs.First(); refs.Valid(); refs.Next() {
		if e = ctx.Err(); e != nil {
			return e
		}
		p := slices.Clone(refs.Value())
		compactPrefix := binary.BigEndian.AppendUint32(slices.Clone(p), compactArchive)
		compact, err := d.snap.NewIter(&pebble.IterOptions{LowerBound: compactPrefix, UpperBound: partsformat.PrefixEnd(compactPrefix)})
		if err != nil {
			return err
		}
		for compact.First(); compact.Valid(); compact.Next() {
			if e = ctx.Err(); e != nil {
				break
			}
			if _, e = partsformat.Decode(compact.Key()); e != nil {
				break
			}
			e = visitCompactRecords(compact.Value(), policy, &workspace, emit)
			if e != nil {
				break
			}
		}
		e = errors.Join(e, compact.Error(), compact.Close())
		if e != nil {
			return e
		}
		p = binary.BigEndian.AppendUint32(p, uint32(archive))
		ranges := physicalRanges(a, from, until)
		for _, span := range ranges {
			lower := binary.BigEndian.AppendUint64(slices.Clone(p), uint64(span[0])^(uint64(1)<<63))
			upper := binary.BigEndian.AppendUint64(slices.Clone(p), uint64(span[1])^(uint64(1)<<63))
			it, e := d.snap.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
			if e != nil {
				return e
			}
			for it.First(); it.Valid(); it.Next() {
				if e = ctx.Err(); e != nil {
					break
				}
				k, err := partsformat.Decode(it.Key())
				if err != nil {
					e = err
					break
				}
				r, err := sealrollup.DecodeRecord(archive, k.BlockStart, it.Value())
				if err != nil {
					e = err
					break
				}
				if e = emit(r); e != nil {
					break
				}
			}
			e = errors.Join(e, it.Error(), it.Close())
			if e != nil {
				return e
			}
		}
	}
	return refs.Error()
}

// physicalRanges bounds archive reads to at most two physical ring intervals.
// Ring overwrites retain the original sample timestamps; filtering by wall-clock
// timestamps here would hide the same aliases that the classic engine returns.
func physicalRanges(a partsread.Archive, from, until int64) [][2]int64 {
	n := (until - from) / a.Step
	if n >= a.Points {
		return [][2]int64{{0, a.Points}}
	}
	if n <= 0 {
		return nil
	}
	start := (from / a.Step) % a.Points
	if start < 0 {
		start += a.Points
	}
	end := start + n
	if end <= a.Points {
		return [][2]int64{{start, end}}
	}
	return [][2]int64{{start, a.Points}, {0, end - a.Points}}
}

var foundArchive = errors.New("archive found")

func (d *diskSource) HasArchive(ctx context.Context, id uint64, step int64) (bool, error) {
	name, ok := d.catalog.nameForID(id)
	if !ok {
		return false, nil
	}
	m, _ := d.catalog.Lookup(name)
	for _, a := range m.Archives {
		if a.Step == step {
			e := d.Scan(ctx, id, a, 0, a.Points*a.Step, func(partsread.Sample) error { return foundArchive })
			if errors.Is(e, foundArchive) {
				return true, nil
			}
			return false, e
		}
	}
	return false, nil
}
func (s *Store) Fetch(ctx context.Context, name string, from, until int) (*daemonstore.Series, error) {
	if e := s.readStatus(); e != nil {
		return nil, e
	}
	if s.closed.Load() {
		return nil, ErrClosed
	}
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	p, closeView, e := s.livePublication(ctx, name)
	if e != nil {
		return nil, e
	}
	defer closeView()
	if s.mixed != nil {
		return s.fetchMixedPublication(ctx, name, from, until, p)
	}
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	work := responseWork(s.catalogFor(name), name, int64(from), int64(until), s.now().Unix())
	if r, ok := s.resolved.Load(name); ok {
		c := &catalog{names: map[string]partsread.Metric{name: toReadMetric(r.(*resolvedMetric).policy)}}
		work = responseWork(c, name, int64(from), int64(until), s.now().Unix())
	}
	h, e := p.Pin(s.budget, work)
	if e != nil {
		return nil, e
	}
	// Direct callers retain the pin and response charge until Release. Serving
	// callers transfer that release to their outer serialization scope.
	x, e := h.Render(ctx, name, int64(from), int64(until), s.now().Unix())
	if e != nil || x == nil {
		h.Close()
		if errors.Is(e, partsread.ErrNotFound) {
			e = daemonstore.ErrNotFound
		}
		return nil, e
	}
	s.pins.Add(1)
	var once sync.Once
	release := func() { once.Do(func() { h.Close(); s.pins.Add(-1) }) }
	// Workspace and the detached output remain charged together.
	if int64(len(x.Values)) > work/40 {
		release()
		return nil, partsread.ErrAdmission
	}
	r := &daemonstore.Series{Metric: toDaemonMetric(x.Metric), From: int(x.From), Until: int(x.Until), Step: int(x.Step), Values: slices.Clone(x.Values), Release: release}
	daemonstore.HoldResponse(ctx, release)
	return r, nil
}

func responseWork(c *catalog, name string, from, until, now int64) int64 {
	if c == nil {
		return 1024
	}
	m, ok := c.Lookup(name)
	if !ok || len(m.Archives) == 0 || from > until {
		return 1024
	}
	ret := m.Archives[len(m.Archives)-1]
	oldest := now - ret.Step*ret.Points
	if from > now || until < oldest {
		return 1024
	}
	from = max(from, oldest)
	until = min(until, now)
	a := ret
	for _, v := range m.Archives {
		if v.Step*v.Points >= now-from {
			a = v
			break
		}
	}
	return ((until-from)/a.Step+2)*40 + 1024
}
func (s *Store) Info(ctx context.Context, name string) (daemonstore.Metric, error) {
	if e := s.readStatus(); e != nil {
		return daemonstore.Metric{}, e
	}
	if e := ctx.Err(); e != nil {
		return daemonstore.Metric{}, e
	}
	live, e := s.capture(ctx, s.owners[s.route(name)], name)
	if e != nil {
		return daemonstore.Metric{}, e
	}
	defer live.close()
	if len(live.frames) > 0 {
		m := live.frames[len(live.frames)-1].policy
		m.Archives = slices.Clone(m.Archives)
		return m, nil
	}
	var result daemonstore.Metric
	// Info uses the exact pinned policy; Render with an out-of-retention range
	// does not expose its catalog. Read it through the same immutable source.
	o := s.owners[s.route(name)]
	_ = o
	// Lookup is supplied below by an immutable per-owner publication pointer.
	c := s.catalogFor(name)
	if c == nil {
		return result, daemonstore.ErrNotFound
	}
	m, ok := c.Lookup(name)
	if !ok && s.legacyCatalog != nil {
		m, ok = s.legacyCatalog.Lookup(name)
	}
	if !ok {
		return result, daemonstore.ErrNotFound
	}
	return toDaemonMetric(m), nil
}
func (s *Store) Find(ctx context.Context, pattern string) ([]daemonstore.Match, error) {
	if e := s.readStatus(); e != nil {
		return nil, e
	}
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	var liveMetadata []store.Metadata
	for _, o := range s.owners {
		a, e := s.capture(ctx, o, "")
		if e != nil {
			return nil, e
		}
		for _, f := range a.frames {
			liveMetadata = append(liveMetadata, store.Metadata{MetricConfig: store.MetricConfig{Name: f.name}})
		}
		a.close()
	}
	liveIndex, e := carbonserver.NewPartsCatalogIndex(liveMetadata, carbonserver.PartsIndexOptions{Trie: true, ReportFrequency: time.Minute})
	if e != nil {
		return nil, e
	}
	seen := make(map[daemonstore.Match]struct{})
	var out []daemonstore.Match
	ln, ll, e := liveIndex.Find(pattern)
	if e != nil {
		return nil, e
	}
	for i, n := range ln {
		m := daemonstore.Match{Name: n, Leaf: ll[i]}
		if _, ok := seen[m]; !ok {
			seen[m] = struct{}{}
			out = append(out, m)
		}
	}
	for _, o := range s.owners {
		if e := ctx.Err(); e != nil {
			return nil, e
		}
		c := o.readCatalog.Load()
		if c == nil {
			continue
		}
		names, leafs, e := c.findNames(pattern)
		if e != nil {
			return nil, e
		}
		for i, n := range names {
			m := daemonstore.Match{Name: n, Leaf: leafs[i]}
			if _, ok := seen[m]; !ok {
				seen[m] = struct{}{}
				out = append(out, m)
				if len(out) > 1_000_000 {
					return nil, fmt.Errorf("%w: too many find names", partsread.ErrAdmission)
				}
			}
		}
	}
	if s.legacyCatalog != nil {
		names, leafs, e := s.legacyCatalog.findNames(pattern)
		if e != nil {
			return nil, e
		}
		for i, n := range names {
			m := daemonstore.Match{Name: n, Leaf: leafs[i]}
			if _, ok := seen[m]; !ok {
				seen[m] = struct{}{}
				out = append(out, m)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return !out[i].Leaf && out[j].Leaf
	})
	return out, nil
}
func (s *Store) catalogFor(name string) *catalog { return s.owners[s.route(name)].readCatalog.Load() }
