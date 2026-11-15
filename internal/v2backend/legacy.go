package v2backend

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-graphite/go-carbon/carbonserver"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/daemonstore"
	"github.com/go-graphite/go-carbon/internal/mixedread"
	"github.com/go-graphite/go-carbon/internal/partsread"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

func (s *Store) openLegacy() error {
	dir := s.opts.LegacyStoreDir
	if dir == "" {
		return nil
	}
	marker, e := os.ReadFile(filepath.Join(dir, "CHUNKSTORE"))
	if errors.Is(e, os.ErrNotExist) {
		// A fresh daemon may point at an empty legacy directory. Never silently
		// hide existing classic files when no read-only classic adapter exists.
		if s.opts.LegacyDataDir != "" {
			e = filepath.WalkDir(s.opts.LegacyDataDir, func(path string, d fs.DirEntry, e error) error {
				if errors.Is(e, os.ErrNotExist) {
					return nil
				}
				if e != nil {
					return e
				}
				if !d.IsDir() && strings.HasSuffix(path, ".wsp") {
					return errors.New("classic legacy history requires conversion before v2 activation")
				}
				return nil
			})
			if e != nil {
				return e
			}
		}
		return nil
	}
	if e != nil {
		return e
	}
	opts := store.Options{ReadOnly: true, ExactBlocks: true, Now: s.now}
	// Continuation formats are explicitly identified by their durable marker.
	if !strings.Contains(string(marker), "-v2\n") && !strings.Contains(string(marker), "-v4\n") {
		opts.Continuation = &whisperio.ContinuationCodec{ReuseBlockKeys: true, DirectLateWrites: true}
		opts.TimeBuckets = true
	}
	s.legacy, e = store.Open(dir, opts)
	if e != nil {
		return fmt.Errorf("read-only legacy base: %w", e)
	}
	if !s.legacy.ReadOnly() {
		return errors.New("legacy handle is writable")
	}
	metadata, e := s.legacy.List(context.Background(), "")
	if e != nil {
		return e
	}
	c := &catalog{names: make(map[string]partsread.Metric), ids: make(map[uint64]string)}
	for i, m := range metadata {
		r := partsread.Metric{ID: uint64(i + 1), AggregationMethod: m.AggregationMethod.String(), XFilesFactor: m.XFilesFactor}
		for _, a := range m.Retentions {
			r.Archives = append(r.Archives, partsread.Archive{Step: int64(a.Step), Points: int64(a.Count)})
		}
		c.names[m.Name] = r
		c.ids[r.ID] = m.Name
	}
	c.index, e = carbonserver.NewPartsCatalogIndex(metadata, carbonserver.PartsIndexOptions{Trie: true, ReportFrequency: time.Minute})
	if e != nil {
		return e
	}
	s.legacyCatalog = c
	return nil
}
func (s *Store) openMixed() error {
	if s.legacy == nil {
		return nil
	}
	a := partsread.Adapter{Select: func(name string) *partsread.Publication { return &s.owners[s.route(name)].pub }, Catalog: &s.owners[0].pub, Budget: s.budget, WorkBytes: 32 << 20, Now: s.now}
	r, e := mixedread.New(mixedread.Options{Legacy: s.legacy, LegacyCatalog: s.legacyCatalog, Parts: a, Now: s.now})
	if e != nil {
		return e
	}
	s.mixed = r
	return nil
}
func (s *Store) fetchMixed(ctx context.Context, name string, from, until int) (*daemonstore.Series, error) {
	return s.fetchMixedPublication(ctx, name, from, until, &s.owners[s.route(name)].pub)
}
func (s *Store) fetchMixedPublication(ctx context.Context, name string, from, until int, p *partsread.Publication) (*daemonstore.Series, error) {
	h, e := p.Pin(s.budget, 64<<20)
	if e != nil {
		return nil, e
	}
	s.pins.Add(1)
	var once sync.Once
	release := func() { once.Do(func() { h.Close(); s.pins.Add(-1) }) }
	var result *daemonstore.Series
	a := partsread.Adapter{Select: func(string) *partsread.Publication { return p }, Catalog: p, Budget: s.budget, WorkBytes: 32 << 20, Now: s.now}
	mixed, e := mixedread.New(mixedread.Options{Legacy: s.legacy, LegacyCatalog: s.legacyCatalog, Parts: a, Now: s.now})
	if e != nil {
		release()
		return nil, e
	}
	e = mixed.Fetch(ctx, name, int64(from), int64(until), func(x *partsread.Series) error {
		if x == nil {
			return nil
		}
		if len(x.Values) > (64<<20)/64 {
			return partsread.ErrAdmission
		}
		result = &daemonstore.Series{Metric: toDaemonMetric(x.Metric), From: int(x.From), Until: int(x.Until), Step: int(x.Step), Values: append([]float64(nil), x.Values...), Release: release}
		return nil
	})
	if e != nil || result == nil {
		release()
		if errors.Is(e, partsread.ErrNotFound) {
			e = daemonstore.ErrNotFound
		}
		return nil, e
	}
	daemonstore.HoldResponse(ctx, release)
	return result, nil
}
