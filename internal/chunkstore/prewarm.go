package chunkstore

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"iter"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
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
	// Wait paces optional warming behind writeout; it runs without views.
	Wait func(context.Context) error
	// Progress reports completed catalog windows across both startup passes.
	Progress *PrewarmProgress

	// StartAfter, when non-nil, waits for input/handoff readiness before
	// doing work. Shutdown still cancels a prewarm waiting here.
	StartAfter <-chan struct{}
	// Workers read concurrently, each through its own read view and
	// iterators; zero means DefaultPrewarmWorkers.
	Workers int
	// match is set only by PrewarmPendingMetadata.
	match func(string) bool
	// Rate bounds the names read per second over all workers; zero leaves
	// them unpaced.
	Rate int
	// ByteRate bounds logical key/value bytes read per second across workers.
	// Zero leaves bytes unpaced. At most one batch per worker may burst.
	ByteRate int
	// Batch is the number of names each read view and its iterators serve,
	// read in key order; zero means DefaultPrewarmBatch. A larger batch
	// shares each opened table among more names, but a name loaded later in
	// a batch is more likely to see its shard change since the view opened,
	// which drops it (see stateCache.prewarm).
	Batch int
}

// PrewarmProgress counts completed work without retaining catalog names.
// Catalog work is measured in names, checkpoint loading in file bytes.
// Total becomes known when a catalog pass reaches its end; a pending filter
// reserves two passes, including the remaining catalog.
type PrewarmProgress struct {
	work, total, passes atomic.Uint64
	checkpointLoading   atomic.Bool
}

// CheckpointLoading keeps byte projections from releasing a cold startup gate.
func (p *PrewarmProgress) CheckpointLoading() bool {
	return p != nil && p.checkpointLoading.Load()
}

// Snapshot returns completed work and expected total (zero until known).
func (p *PrewarmProgress) Snapshot() (names, total uint64) {
	if p == nil {
		return 0, 0
	}
	return p.work.Load(), p.total.Load()
}

const DefaultPrewarmBatch = 32

// DefaultPrewarmWorkers bounds startup CPU even on a large store host.
func DefaultPrewarmWorkers() int { return prewarmWorkers(runtime.GOMAXPROCS(0)) }

func prewarmWorkers(procs int) int { return min(16, max(2, procs/12)) }

// PrewarmStats reports what PrewarmMetadata did.
type PrewarmStats struct {
	// Names were offered; Prewarmed of them were loaded, Bytes in all.
	Names, Prewarmed, Bytes int64
	// ReadBytes counts logical key/value bytes read, including refused fills.
	ReadBytes int64
	// Skipped names: Cached already had an entry (a state, or a pending
	// mutation); Absent are not in the catalog; Raced saw a mutation of their
	// shard begin or end while they were read; Full found no room in their
	// shard. Filtered names were outside the pending membership filter.
	Cached, Absent, Raced, Full, Filtered int64
	// Failed names could not be read.
	Failed int64
	// BudgetFull reports that the prewarm stopped because the cache was full.
	BudgetFull bool
	Duration   time.Duration
}

// Skipped is the number of names passed over without an error.
func (s PrewarmStats) Skipped() int64 { return s.Cached + s.Absent + s.Raced + s.Full + s.Filtered }

func (s *PrewarmStats) add(o PrewarmStats) {
	s.Prewarmed += o.Prewarmed
	s.Bytes += o.Bytes
	s.ReadBytes += o.ReadBytes
	s.Cached += o.Cached
	s.Absent += o.Absent
	s.Raced += o.Raced
	s.Full += o.Full
	s.Filtered += o.Filtered
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
// With nil names it streams disjoint catalog windows to the workers, without
// catalog point lookups or retaining a second copy of all names. Call it after the priority
// names from a restored checkpoint to cover metrics absent from the dump.
//
// It returns when names are exhausted, the cache is full, ctx is done, or
// StopBackground or Close stops it; the error is ctx's, or one reported when
// the cache is disabled. Names a read fails for are counted, not returned.
func (s *Store) PrewarmMetadata(ctx context.Context, names iter.Seq[string], options PrewarmOptions) (stats PrewarmStats, err error) {
	phasecpu.Set(phasecpu.MetadataCache)
	defer phasecpu.Set(phasecpu.Other)
	started := time.Now()
	defer func() {
		stats.Duration = time.Since(started)
		if s.stateCache != nil {
			s.stateCache.prewarmNanos.Add(uint64(stats.Duration))
		}
	}()
	cache := s.stateCache
	if cache == nil {
		return stats, ErrMetadataCacheDisabled
	}
	ctx, end, ok := s.prewarm.begin(ctx)
	if !ok {
		return stats, context.Canceled
	}
	defer end()
	if options.StartAfter != nil {
		select {
		case <-options.StartAfter:
		case <-ctx.Done():
			return stats, ctx.Err()
		}
	}
	started = time.Now()
	if names == nil {
		err = s.prewarmCatalog(ctx, options, &stats)
		return stats, err
	}
	workers := options.Workers
	if workers <= 0 {
		workers = DefaultPrewarmWorkers()
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
	bytePace := prewarmPacer{rate: options.ByteRate}
	for w := range workers {
		wg.Go(func() {
			for names := range batches {
				if options.Wait != nil && options.Wait(ctx) != nil {
					return
				}
				var b PrewarmStats
				s.prewarmBatch(ctx, cache, loader, names, &b)
				results[w].add(b)
				// Published per batch, so the counters show progress.
				cache.recordPrewarm(b)
				bytePace.wait(ctx, int(b.ReadBytes))
			}
		})
	}
	pace := prewarmPacer{rate: options.Rate}
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
		return pace.wait(ctx, sent)
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

// PrewarmPendingMetadata gives pending names first claim on cache capacity
// without a catalog seek per name. A bounded membership filter selects them
// during the ordinary catalog walk; false positives only warm extra metrics.
// The temporary filter is at most 64 MiB, independent of the catalog size.
func (s *Store) PrewarmPendingMetadata(ctx context.Context, names iter.Seq[string], options PrewarmOptions) (stats PrewarmStats, err error) {
	if s.stateCache == nil {
		return stats, ErrMetadataCacheDisabled
	}
	ctx, end, ok := s.prewarm.begin(ctx)
	if !ok {
		return stats, context.Canceled
	}
	defer end()
	if options.StartAfter != nil {
		select {
		case <-options.StartAfter:
		case <-ctx.Done():
			return stats, ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return stats, err
	}
	phasecpu.Set(phasecpu.MetadataCache)
	defer phasecpu.Set(phasecpu.Other)
	started := time.Now()
	defer func() {
		extra := max(time.Since(started)-stats.Duration, 0)
		stats.Duration += extra
		s.stateCache.prewarmNanos.Add(uint64(extra))
	}()
	var filter *catalogMembership
	if names != nil {
		var offered uint64
		for name := range names {
			if offered%DefaultPrewarmBatch == 0 && options.Wait != nil {
				if err := options.Wait(ctx); err != nil {
					return stats, err
				}
			}
			offered++
			if err := ctx.Err(); err != nil {
				return stats, err
			}
			if filter == nil {
				filter = newCatalogMembership(uint64(s.stateCache.budget() / 1500))
			}
			filter.add(name)
		}
	}
	if options.Progress != nil {
		passes := uint64(1)
		if filter != nil {
			passes = 2
		}
		options.Progress.passes.Store(passes)
	}
	if filter != nil {
		filter.ready.Store(true)
		options.match = filter.mayContain
	}
	stats, err = s.PrewarmMetadata(ctx, nil, options)
	return stats, err
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
// order, sharing an iterator for each range across the batch.
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
			r.ReadBytes += int64(len(key))
			if !it.SeekPrefixGE(key) || !bytes.Equal(it.Key(), key) {
				if it.Error() != nil {
					r.Failed++
				} else {
					r.Absent++
				}
				continue
			}
			r.ReadBytes += int64(len(it.Value()))
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
	s.prewarmItems(ctx, cache, loader, view, present, r)
}

func (s *Store) prewarmItems(ctx context.Context, cache *stateCache, loader ContinuationStateLoader, view *pebble.ReadView, present []prewarmItem, r *PrewarmStats) {
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
		r.ReadBytes += int64(len(key))
		if !revisions.SeekPrefixGE(key) || !bytes.Equal(revisions.Key(), key) {
			r.Failed++
			continue
		}
		r.ReadBytes += int64(len(revisions.Value()))
		if decodeRevision(revisions.Value(), &m) != nil {
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
			err := loader.LoadState(ctx, prewarmIO{continuationIO: io, bytes: &r.ReadBytes}, m, s.now)
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

// prewarmCatalog streams disjoint windows to bounded workers. A worker slot
// is reserved before opening each view, so a paced worker never leaves the
// scanner holding a future view. Epochs precede views; views precede pacing.
func (s *Store) prewarmCatalog(ctx context.Context, options PrewarmOptions, stats *PrewarmStats) (err error) {
	cache := s.stateCache
	loader, _ := s.continuation.(ContinuationStateLoader)
	if cache.keys == nil {
		loader = nil
	}
	batch := options.Batch
	if batch <= 0 {
		batch = DefaultPrewarmBatch
	}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	workers := options.Workers
	if workers <= 0 {
		workers = DefaultPrewarmWorkers()
	}
	type window struct {
		view  *pebble.ReadView
		items []prewarmItem
		stats PrewarmStats
	}
	if options.Progress != nil {
		options.Progress.passes.CompareAndSwap(0, 1)
	}
	windows := make(chan window)
	slots := make(chan struct{}, workers)
	results := make([]PrewarmStats, workers)
	pace := prewarmPacer{rate: options.Rate, byteRate: options.ByteRate}
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			phasecpu.Set(phasecpu.MetadataCache)
			defer phasecpu.Set(phasecpu.Other)
			for next := range windows {
				if ctx.Err() == nil && len(next.items) > 0 {
					s.prewarmItems(ctx, cache, loader, next.view, next.items, &next.stats)
				}
				if err := next.view.Close(); err != nil {
					cancel(err)
				}
				results[w].Names += next.stats.Names
				results[w].add(next.stats)
				cache.recordPrewarm(next.stats)
				if options.Progress != nil {
					options.Progress.work.Add(uint64(next.stats.Names))
				}
				pace.waitBytes(ctx, int(next.stats.Names), int(next.stats.ReadBytes))
				<-slots
			}
		})
	}
	defer func() {
		close(windows)
		wg.Wait()
		for _, r := range results {
			stats.Names += r.Names
			stats.add(r)
		}
		err = context.Cause(ctx)
	}()
	full := int64(float64(cache.budget()) * prewarmFullFraction)
	lower := catalogLower
	var catalogNames, catalogFiltered uint64
	for ctx.Err() == nil {
		if cache.bytes.Load() >= full {
			stats.BudgetFull = true
			break
		}
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
		if options.Wait != nil {
			if err := options.Wait(ctx); err != nil {
				<-slots
				return err
			}
		}
		var epochs [metricStripes]uint64
		for i := range epochs {
			epochs[i] = cache.epoch(uint64(i))
		}
		view := s.db.NewReadView()
		it, err := view.NewIter(&pebble.IterOptions{LowerBound: catalogLower, UpperBound: catalogUpper})
		if err != nil {
			view.Close()
			cancel(err)
			return err
		}
		items := make([]prewarmItem, 0, batch)
		var b PrewarmStats
		valid := it.SeekGE(lower)
		for valid && b.Names < int64(batch) && ctx.Err() == nil {
			b.Names++
			b.ReadBytes += int64(len(it.Key()) + len(it.Value()))
			name := string(it.Key()[len(catalogLower):])
			stripe := s.metricStripe(name)
			if options.match != nil && !options.match(name) {
				b.Filtered++
			} else if cache.has(stripe, name) {
				b.Cached++
			} else if m, err := decodeCatalog(name, it.Value()); err != nil {
				b.Failed++
			} else {
				items = append(items, prewarmItem{name: name, stripe: stripe, epoch: epochs[stripe], meta: m})
			}
			valid = it.Next()
		}
		err = it.Error()
		if valid {
			lower = append([]byte(nil), it.Key()...)
		}
		err = errors.Join(err, it.Close())
		if err != nil {
			view.Close()
			cancel(err)
			return err
		}
		select {
		case windows <- window{view: view, items: items, stats: b}:
		case <-ctx.Done():
			view.Close()
			return context.Cause(ctx)
		}
		catalogNames += uint64(b.Names)
		catalogFiltered += uint64(b.Filtered)
		if !valid {
			if options.Progress != nil {
				if options.match != nil && catalogFiltered == 0 {
					options.Progress.passes.Store(1)
				}
				options.Progress.total.Store(catalogNames * options.Progress.passes.Load())
			}
			break
		}
	}
	return context.Cause(ctx)
}

func (c *stateCache) recordPrewarm(b PrewarmStats) {
	c.prewarmed.Add(uint64(b.Prewarmed))
	c.prewarmBytes.Add(uint64(b.Bytes))
	c.prewarmReadBytes.Add(uint64(b.ReadBytes))
	c.prewarmSkipped.Add(uint64(b.Skipped()))
	c.prewarmFailed.Add(uint64(b.Failed))
}

// A stall never earns credit for an unpaced catch-up.
type prewarmPacer struct {
	mu             sync.Mutex
	rate, byteRate int
	due            time.Time
}

func (p *prewarmPacer) wait(ctx context.Context, names int) bool {
	return p.waitBytes(ctx, names, 0)
}

func (p *prewarmPacer) waitBytes(ctx context.Context, names, bytes int) bool {
	var interval time.Duration
	if p.rate > 0 {
		interval = time.Duration(float64(names) / float64(p.rate) * float64(time.Second))
	}
	if p.byteRate > 0 {
		interval = max(interval, time.Duration(float64(bytes)/float64(p.byteRate)*float64(time.Second)))
	}
	if interval <= 0 {
		return ctx.Err() == nil
	}
	p.mu.Lock()
	if now := time.Now(); p.due.Before(now) {
		p.due = now
	}
	p.due = p.due.Add(interval)
	delay := time.Until(p.due)
	p.mu.Unlock()
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// prewarmIO counts bytes on the codec's existing prefix/iterator read path.
// Charge reads even when a later epoch check rejects the state.
type prewarmIO struct {
	*continuationIO
	bytes *int64
}

func (io prewarmIO) Get(key []byte) ([]byte, error) {
	value, err := io.continuationIO.Get(key)
	*io.bytes += int64(len(io.prefix) + len(key) + len(value))
	return value, err
}

func (io prewarmIO) Probe(key []byte) ([]byte, error) {
	value, err := io.continuationIO.Probe(key)
	*io.bytes += int64(len(io.prefix) + len(key) + len(value))
	return value, err
}

func (io prewarmIO) Visit(prefix []byte, visit func([]byte, []byte) error) error {
	return io.continuationIO.Visit(prefix, func(key, value []byte) error {
		*io.bytes += int64(len(io.prefix) + len(key) + len(value))
		return visit(key, value)
	})
}
