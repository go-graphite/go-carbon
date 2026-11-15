package v2backend

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
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
	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

type catalog struct {
	names map[string]partsread.Metric
	ids   map[uint64]string
	index *carbonserver.PartsCatalogIndex
}

func (c *catalog) Lookup(name string) (partsread.Metric, bool) { m, ok := c.names[name]; return m, ok }
func (c *catalog) Find(ctx context.Context, pattern string, yield func(string) error) error {
	names, leafs, e := c.index.Find(pattern)
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
func (o *owner) publish() error {
	c := &catalog{names: make(map[string]partsread.Metric, len(o.metrics)), ids: make(map[uint64]string, len(o.metrics))}
	var metadata []store.Metadata
	names := make([]string, 0, len(o.metrics))
	for n := range o.metrics {
		names = append(names, n)
	}
	sort.Strings(names)
	var retained int64
	for i, n := range names {
		m := o.metrics[n]
		r := toReadMetric(m.Policy)
		r.ID = uint64(i + 1)
		c.names[n] = r
		c.ids[r.ID] = n
		cfg, e := rollupConfig(m.Policy)
		if e != nil {
			return e
		}
		meta := store.Metadata{MetricConfig: store.MetricConfig{Name: n, AggregationMethod: store.AggregationMethod(cfg.Method), XFilesFactor: cfg.XFilesFactor}}
		for _, a := range m.Policy.Archives {
			meta.Retentions = append(meta.Retentions, store.Retention{Step: a.Step, Count: a.Points})
		}
		metadata = append(metadata, meta)
		retained += int64(256 + len(n)*3 + len(r.Archives)*32)
	}
	var e error
	c.index, e = carbonserver.NewPartsCatalogIndex(metadata, carbonserver.PartsIndexOptions{Trie: true, ReportFrequency: time.Minute})
	if e != nil {
		return e
	}
	snap := o.db.NewSnapshot()
	source := &diskSource{snap: snap, owner: o.id, catalog: c}
	o.pub.Publish(partsread.View{Generation: o.meta.Cursor, Frontier: []uint64{o.meta.Cursor}, Catalog: c, Parts: []partsread.Source{source}, RetainedBytes: retained, Close: func() { _ = snap.Close() }})
	o.readCatalog.Store(c)
	return nil
}

type diskSource struct {
	snap    *pebble.Snapshot
	owner   int
	catalog *catalog
}

func (d *diskSource) Scan(ctx context.Context, id uint64, a partsread.Archive, from, until int64, yield func(partsread.Sample) error) error {
	name, ok := d.catalog.ids[id]
	if !ok {
		return partsread.ErrNotFound
	}
	m := d.catalog.names[name]
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
	for refs.First(); refs.Valid(); refs.Next() {
		if e = ctx.Err(); e != nil {
			return e
		}
		p := slices.Clone(refs.Value())
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
				slot := ((r.Sample.Time/a.Step)%a.Points - (from/a.Step)%a.Points + a.Points) % a.Points
				if slot < n {
					if e = yield(partsread.Sample{Time: r.Sample.Time, Bits: r.Sample.Bits, Sequence: r.Order}); e != nil {
						break
					}
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
	name, ok := d.catalog.ids[id]
	if !ok {
		return false, nil
	}
	for _, a := range d.catalog.names[name].Archives {
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
	if s.closed.Load() {
		return nil, ErrClosed
	}
	if s.mixed != nil {
		return s.fetchMixed(ctx, name, from, until)
	}
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	o := s.owners[s.route(name)]
	work := responseWork(o.readCatalog.Load(), name, int64(from), int64(until), s.now().Unix())
	h, e := o.pub.Pin(s.budget, work)
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
	if e := ctx.Err(); e != nil {
		return daemonstore.Metric{}, e
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
	seen := make(map[daemonstore.Match]struct{})
	var out []daemonstore.Match
	for _, o := range s.owners {
		if e := ctx.Err(); e != nil {
			return nil, e
		}
		c := o.readCatalog.Load()
		if c == nil {
			continue
		}
		names, leafs, e := c.index.Find(pattern)
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
		names, leafs, e := s.legacyCatalog.index.Find(pattern)
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
