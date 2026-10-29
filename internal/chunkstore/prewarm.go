package chunkstore

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"iter"
	"slices"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/phasecpu"
)

// ContinuationStateLoader is implemented by a codec that can read, through an
// io, the continuation state an update of a metric starts by reading. The
// store records what it reads (see stateView) to load that state into its
// metadata cache ahead of the update (see PrewarmMetadata).
type ContinuationStateLoader interface {
	LoadState(context.Context, ContinuationIO, Metadata, func() time.Time) error
}

// PrewarmOptions paces PrewarmMetadata.
type PrewarmOptions struct {
	// Workers read concurrently, each through its own read view and
	// iterators; zero means DefaultPrewarmWorkers.
	Workers int
	// Rate bounds the names read per second over all workers; zero leaves
	// them unpaced.
	Rate int
	// Batch is the number of names each read view and its iterators serve,
	// read in key order; zero means DefaultPrewarmBatch. A larger batch
	// shares each opened table among more names, but a name loaded later in
	// a batch is more likely to see its shard change since the view opened,
	// which drops it (see stateCache.prewarm).
	Batch int
}

const (
	DefaultPrewarmWorkers = 4
	DefaultPrewarmBatch   = 32
)

// PrewarmStats reports what PrewarmMetadata did.
type PrewarmStats struct {
	// Names were offered; Prewarmed of them were loaded, Bytes in all.
	Names, Prewarmed, Bytes int64
	// Skipped names: Cached already had an entry (a state, or a pending
	// mutation); Absent are not in the catalog; Raced saw a mutation of their
	// shard begin or end while they were read; Full found no room in their
	// shard.
	Cached, Absent, Raced, Full int64
	// Failed names could not be read.
	Failed int64
	// BudgetFull reports that the prewarm stopped because the cache was full.
	BudgetFull bool
	Duration   time.Duration
}

// Skipped is the number of names passed over without an error.
func (s PrewarmStats) Skipped() int64 { return s.Cached + s.Absent + s.Raced + s.Full }

func (s *PrewarmStats) add(o PrewarmStats) {
	s.Prewarmed += o.Prewarmed
	s.Bytes += o.Bytes
	s.Cached += o.Cached
	s.Absent += o.Absent
	s.Raced += o.Raced
	s.Full += o.Full
	s.Failed += o.Failed
}

// ErrMetadataCacheDisabled is returned by PrewarmMetadata without a cache.
var ErrMetadataCacheDisabled = errors.New("metadata cache disabled")

// prewarmFullFraction is the share of the cache's budget at which a prewarm
// stops: the budget is split among shards, and the fullest fill first.
const prewarmFullFraction = 0.97

// prewarmState lets StopBackground and Close cancel running prewarms and wait
// for them before the database closes.
type prewarmState struct {
	mu      sync.Mutex
	stopped bool
	// base is canceled by stop; every prewarm's context follows it.
	base       context.Context
	cancelBase context.CancelFunc
	running    sync.WaitGroup
}

// begin registers a prewarm and returns its context and the function that
// ends it, or false once stopped.
func (p *prewarmState) begin(ctx context.Context) (context.Context, func(), bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return nil, nil, false
	}
	if p.base == nil {
		p.base, p.cancelBase = context.WithCancel(context.Background())
	}
	ctx, cancel := context.WithCancel(ctx)
	unlink := context.AfterFunc(p.base, cancel)
	p.running.Add(1)
	return ctx, func() { unlink(); cancel(); p.running.Done() }, true
}

// stop cancels every prewarm, refuses new ones and waits for them to end.
func (p *prewarmState) stop() {
	p.mu.Lock()
	p.stopped = true
	if p.cancelBase != nil {
		p.cancelBase()
	}
	p.mu.Unlock()
	p.running.Wait()
}

// PrewarmMetadata loads the committed state of names into the metadata cache:
// the catalog metadata and the continuation state an update starts by reading
// (see ContinuationStateLoader), which every first update and fetch of a
// metric after a restart otherwise reads from the database while the cache
// warms. It reads names in batches, each in key order through one read view
// with one iterator per key range, and fills the cache through the reader
// protocol of fetches (see stateCache): a name some mutation or reader cached
// meanwhile, or whose shard a mutation touched while it was read, is passed
// over. It never evicts: a shard without room passes its names over, and the
// prewarm stops once the cache is nearly full.
//
// It returns when names are exhausted, the cache is full, ctx is done, or
// StopBackground or Close stops it; the error is ctx's, or one reported when
// the cache is disabled. Names a read fails for are counted, not returned.
func (s *Store) PrewarmMetadata(ctx context.Context, names iter.Seq[string], options PrewarmOptions) (stats PrewarmStats, err error) {
	phasecpu.Set(phasecpu.MetadataCache)
	defer phasecpu.Set(phasecpu.Other)
	started := time.Now()
	defer func() { stats.Duration = time.Since(started) }()
	cache := s.stateCache
	if cache == nil {
		return stats, ErrMetadataCacheDisabled
	}
	ctx, end, ok := s.prewarm.begin(ctx)
	if !ok {
		return stats, context.Canceled
	}
	defer end()
	workers := options.Workers
	if workers <= 0 {
		workers = DefaultPrewarmWorkers
	}
	batch := options.Batch
	if batch <= 0 {
		batch = DefaultPrewarmBatch
	}
	loader, _ := s.continuation.(ContinuationStateLoader)
	if cache.keys == nil {
		loader = nil
	}
	full := int64(float64(cache.budget()) * prewarmFullFraction)

	batches := make(chan []string, workers)
	results := make([]PrewarmStats, workers)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for names := range batches {
				var b PrewarmStats
				s.prewarmBatch(ctx, cache, loader, names, &b)
				results[w].add(b)
				// Published per batch, so the counters show progress.
				cache.prewarmed.Add(uint64(b.Prewarmed))
				cache.prewarmSkipped.Add(uint64(b.Skipped()))
				cache.prewarmFailed.Add(uint64(b.Failed))
			}
		})
	}
	// due is when the next batch may be sent. It never lies in the past, so
	// a stall is not followed by an unpaced catch-up.
	var due time.Time
	next := make([]string, 0, batch)
	send := func() bool {
		if len(next) == 0 {
			return true
		}
		select {
		case batches <- next:
		case <-ctx.Done():
			return false
		}
		sent := len(next)
		next = make([]string, 0, batch)
		if options.Rate > 0 {
			if now := time.Now(); due.Before(now) {
				due = now
			}
			due = due.Add(time.Duration(float64(sent) / float64(options.Rate) * float64(time.Second)))
			if delay := time.Until(due); delay > 0 {
				timer := time.NewTimer(delay)
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					return false
				}
			}
		}
		return true
	}
	for name := range names {
		if ctx.Err() != nil {
			break
		}
		if cache.bytes.Load() >= full {
			stats.BudgetFull = true
			break
		}
		stats.Names++
		if cache.has(s.metricStripe(name), name) {
			stats.Cached++
			cache.prewarmSkipped.Add(1)
			continue
		}
		if next = append(next, name); len(next) == batch && !send() {
			break
		}
	}
	if ctx.Err() == nil && !stats.BudgetFull {
		send()
	}
	close(batches)
	wg.Wait()
	for _, r := range results {
		stats.add(r)
	}
	return stats, ctx.Err()
}

// prewarmItem is one name of a batch being read.
type prewarmItem struct {
	name          string
	stripe, epoch uint64
	meta          Metadata
}

var (
	catalogLower      = []byte("m/")
	catalogUpper      = prefixEnd(catalogLower)
	revisionLower     = []byte{'r'}
	revisionUpper     = prefixEnd(revisionLower)
	continuationLower = []byte{'u'}
	continuationUpper = prefixEnd(continuationLower)
)

// prewarmBatch loads the states of names through one read view: the catalog
// in name order, then each metric's revision and continuation state in ID
// order, each range through one iterator whose seeks only move forward.
func (s *Store) prewarmBatch(ctx context.Context, cache *stateCache, loader ContinuationStateLoader, names []string, r *PrewarmStats) {
	if ctx.Err() != nil {
		return
	}
	slices.Sort(names)
	items := make([]prewarmItem, 0, len(names))
	for _, name := range names {
		if !s.catalogMembership.mayContain(name) {
			r.Absent++
			continue
		}
		// Each epoch is captured before the view opens, as a fetch does.
		stripe := s.metricStripe(name)
		items = append(items, prewarmItem{name: name, stripe: stripe, epoch: cache.epoch(stripe)})
	}
	if len(items) == 0 {
		return
	}
	view := s.db.NewReadView()
	defer view.Close()
	present := items[:0]
	{
		it, err := view.NewIter(&pebble.IterOptions{LowerBound: catalogLower, UpperBound: catalogUpper})
		if err != nil {
			r.Failed += int64(len(items))
			return
		}
		for _, item := range items {
			key := catalogKey(item.name)
			countStoreRead()
			if !it.SeekPrefixGE(key) || !bytes.Equal(it.Key(), key) {
				if it.Error() != nil {
					r.Failed++
				} else {
					r.Absent++
				}
				continue
			}
			m, err := decodeCatalog(item.name, it.Value())
			if err != nil {
				r.Failed++
				continue
			}
			item.meta = m
			present = append(present, item)
		}
		if err := it.Close(); err != nil {
			r.Failed += int64(len(present))
			return
		}
	}
	slices.SortFunc(present, func(a, b prewarmItem) int {
		return cmp.Or(cmp.Compare(a.meta.ID, b.meta.ID), cmp.Compare(a.meta.Generation, b.meta.Generation))
	})
	revisions, err := view.NewIter(&pebble.IterOptions{LowerBound: revisionLower, UpperBound: revisionUpper})
	if err != nil {
		r.Failed += int64(len(present))
		return
	}
	defer revisions.Close()
	var continuation *pebble.Iterator
	if loader != nil {
		if continuation, err = view.NewIter(&pebble.IterOptions{LowerBound: continuationLower, UpperBound: continuationUpper}); err != nil {
			r.Failed += int64(len(present))
			return
		}
		defer continuation.Close()
	}
	for _, item := range present {
		if ctx.Err() != nil {
			return
		}
		m := item.meta
		key := revisionKey(m)
		countStoreRead()
		if !revisions.SeekPrefixGE(key) || !bytes.Equal(revisions.Key(), key) || decodeRevision(revisions.Value(), &m) != nil {
			r.Failed++
			continue
		}
		var st *metricState
		// An update reads the continuation state only of a metric it may
		// write; for any other, the state is the metadata alone, as
		// Store.Metadata caches it.
		if m.Continuation && loader != nil && s.checkFileOwnership(m) == nil {
			io := s.newContinuationIO(ctx, view, nil, m).withBorrowedIterator(continuation)
			io.state = s.newStateView(nil, m)
			err := loader.LoadState(ctx, io, m, s.now)
			err = errors.Join(err, io.Close())
			if err != nil {
				if ctx.Err() == nil {
					r.Failed++
				}
				continue
			}
			st = io.state.final(m)
		} else {
			st = newMetricState(m).seal()
		}
		if hook := prewarmBeforeFill; hook != nil {
			hook(item.name)
		}
		switch cache.prewarm(item.stripe, item.name, item.epoch, st) {
		case prewarmFilled:
			r.Prewarmed++
			r.Bytes += st.size
		case prewarmCached:
			r.Cached++
		case prewarmRaced:
			r.Raced++
		case prewarmFull:
			r.Full++
		}
	}
}
