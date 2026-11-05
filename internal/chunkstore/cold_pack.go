package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

// ColdPackOptions controls low-priority workers. Mode is off, exact, decimal,
// or unpack. Decimal rounding occurs only while sealing, never in hot writes.
type ColdPackOptions struct {
	Mode           string
	PackTails      bool // opt-in whole-range v9 packing of cold non-bucketed native heads
	Workers        int  // default 1; at most 64
	BytesPerSecond int64
	MinFreeBytes   uint64
	// CPUFraction bounds the duty fraction of the worker group (up to Workers *
	// CPUFraction cores). Charging the slowest worker's IO wait to the whole
	// group is conservative; asynchronous flush/compaction CPU is separate.
	CPUFraction float64
	WriteWindow time.Duration
	Interval    time.Duration
	// MaxCompactionDebt pauses maintenance while Pebble's estimated compaction
	// debt is above this many bytes. Stores that normally carry debt need more.
	MaxCompactionDebt uint64
}

func NormalizeColdPack(o ColdPackOptions) (ColdPackOptions, error) {
	if o.Mode == "" {
		o.Mode = "off"
	}
	if o.Workers == 0 {
		o.Workers = 1
	}
	if o.Workers < 1 || o.Workers > ColdPackPageSize {
		return o, errors.New("cold pack workers must be between 1 and 64")
	}
	if o.MinFreeBytes == 0 {
		o.MinFreeBytes = 1 << 30
	}
	if o.BytesPerSecond == 0 {
		o.BytesPerSecond = 1 << 20
	}
	if o.CPUFraction == 0 {
		o.CPUFraction = .02
	}
	if o.WriteWindow == 0 {
		o.WriteWindow = 24 * time.Hour
	}
	if o.Interval == 0 {
		o.Interval = time.Hour
	}
	if o.MaxCompactionDebt == 0 {
		o.MaxCompactionDebt = 64 << 20
	}
	if o.Mode != "off" && o.Mode != "exact" && o.Mode != "decimal" && o.Mode != "unpack" {
		return o, errors.New("cold pack mode must be off, exact, decimal or unpack")
	}
	if o.BytesPerSecond < 1 || math.IsNaN(o.CPUFraction) || o.CPUFraction < .001 || o.CPUFraction > 1 || o.WriteWindow < 0 || o.Interval < time.Second {
		return o, errors.New("invalid cold pack byte/CPU budget, window or interval")
	}
	return o, nil
}

const (
	ColdPackPoints         = 4096
	MaxColdPackRangePoints = 65536
	ColdPackPageSize       = 64
	// Bounds include headers, source groups and replacement batch framing. Native
	// ranges above MaxColdPackRangePoints are counted and skipped, never partly rewritten.
	coldPackReadLimit  = 2 << 20
	coldPackBatchLimit = 256 << 10
)

var ErrColdPackBudget = errors.New("cold pack operation exceeds bounded budget")

type RepackStats struct {
	Metrics, Blocks, Points, InputBytes, OutputBytes, Skipped uint64
	// Repacked is the subset of sealing work on previously thawed ranges.
	RepackedBlocks, RepackedPoints, RepackedInputBytes, RepackedOutputBytes uint64
	// ObservedPoints counts only descriptors consumed before the returned
	// cursor, even when a page reads ahead. Observed is false for unsupported
	// layouts. These fields do not describe points rewritten by maintenance.
	ObservedPoints uint64
	Observed       bool
}

type ColdPackRequest struct {
	Cursor    uint64 // codec-owned descriptor seek position
	Cutoff    time.Time
	Mode      string
	PackTails bool
}

// ContinuationRepacker examines at most 32 descriptors and rewrites at most one
// bounded physical range. All IO uses an immutable snapshot plus buffered writes.
type ContinuationRepacker interface {
	Repack(context.Context, ContinuationIO, ColdPackRequest) (RepackStats, uint64, bool, error)
}

type ColdPackStats struct {
	RepackStats
	// Blocks/Points retain their maintenance-work meaning in the status API;
	// subtract unpack work when exporting sealing-only lifecycle counters.
	UnpackedBlocks, UnpackedPoints                   uint64
	Pages, Passes, Errors, Conflicts, PressurePauses uint64
	Resumed, Checkpoints                             uint64
	MetricsVisited, PersistedMetricsVisited          uint64
	ReadBytes, WriteBytes                            uint64
	WorkerNanos, LockNanos, MaxLockNanos             uint64
	Cursor                                           string
	BlockCursor                                      uint64
	PendingMetrics                                   int
	ActiveWorkers                                    int
	Active                                           bool
	Mode, Error                                      string
	// Last completed pass, published together under mu. The timestamp is zero
	// until a pass completes (or a compatible checkpoint restores its result).
	StoredPoints, StoredMetrics uint64
	StoredAt                    int64
}

type coldPackState struct {
	mu                      sync.Mutex
	options                 ColdPackOptions
	stats                   ColdPackStats
	run                     sync.Mutex
	format                  atomic.Bool
	cleanUnpack             bool
	passFailed              bool
	passPoints, passMetrics uint64
	pending                 []*coldPackWork
	wake                    chan struct{}
	cancel                  context.CancelFunc
	done                    chan struct{}
	// Checkpoint bookkeeping is protected by run, not mu.
	checkpoint        coldPackCheckpoint
	checkpointAt      time.Time
	checkpointPresent bool
}

// Entries remain in catalog order until the entire prefix has completed. Only
// run owns these fields; each wave lends distinct entries to its workers.
type coldPackWork struct {
	name    string
	block   uint64
	done    bool
	points  uint64
	counted bool
}

// ConfigureColdPack joins an old operation before returning, so switching off on
// SIGHUP cannot leave an old policy publishing blocks. The marker is synced before
// enabling new encodings. Existing v9 stores remain readable with mode off.
func (s *Store) ConfigureColdPack(options ColdPackOptions) error {
	o, err := NormalizeColdPack(options)
	if err != nil {
		return err
	}
	if o.Mode != "off" {
		if !s.timeBuckets {
			return errors.New("cold pack requires time-bucketed continuation storage")
		}
		if _, ok := s.continuation.(ContinuationRepacker); !ok {
			return errors.New("continuation codec cannot cold pack")
		}
	}
	c := &s.coldPack
	c.run.Lock()
	defer c.run.Unlock()
	// Compare the saved marker before an enabling policy can upgrade it again.
	initial := c.options.Mode == ""
	var marker string
	if initial && s.db != nil {
		marker, err = readStoreMarker(s.fs, s.dir, "CHUNKSTORE")
		if err != nil {
			return err
		}
	}
	if o.Mode != "off" && o.Mode != "unpack" && !c.format.Load() {
		if err := replaceFormatMarker(s.fs, s.dir, formatMarkerColdPack); err != nil {
			return fmt.Errorf("fence cold pack format: %w", err)
		}
		c.format.Store(true)
	}
	c.mu.Lock()
	// An unrelated SIGHUP must not wake a paced worker and forgive its wait.
	if c.options == o {
		c.mu.Unlock()
		return nil
	}
	reset := c.options.Mode != o.Mode || c.options.WriteWindow != o.WriteWindow || c.options.PackTails != o.PackTails
	if reset {
		c.stats.Cursor, c.stats.BlockCursor = "", 0
		c.stats.MetricsVisited = 0
		c.passPoints, c.passMetrics = 0, 0
		c.pending = nil
		c.stats.PendingMetrics = 0
		c.passFailed = false
	}
	if o.Mode != "off" || c.options.Mode != "" && c.options.Mode != "off" {
		zapwriter.Logger("storage").Info("cold pack configured", zap.String("mode", o.Mode), zap.Bool("pack_tails", o.PackTails), zap.Int("workers", o.Workers), zap.Int64("bytes_per_second", o.BytesPerSecond), zap.Float64("cpu_fraction", o.CPUFraction), zap.Duration("write_window", o.WriteWindow), zap.Uint64("max_compaction_debt", o.MaxCompactionDebt))
	}
	c.cleanUnpack = false
	c.options = o
	c.stats.Mode = o.Mode
	c.mu.Unlock()
	if initial && s.db != nil {
		if err := s.loadColdPackCheckpoint(marker); err != nil {
			return err
		}
	}
	if err := s.saveColdPackCheckpoint(true); err != nil {
		return err
	}
	if c.wake != nil {
		select {
		case c.wake <- struct{}{}:
		default:
		}
	}
	return nil
}

func (s *Store) ColdPackStats() ColdPackStats {
	c := &s.coldPack
	c.mu.Lock()
	defer c.mu.Unlock()
	stats := c.stats
	return stats
}

// coldPackPage returns a bounded catalog page in name order. It seeks strictly
// after the previous key; no full-store materialization or ID sorting occurs.
func (s *Store) coldPackPage(ctx context.Context, after string) ([]string, error) {
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("m/"), UpperBound: []byte("m0")})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	valid := it.First()
	if after != "" {
		valid = it.SeekGE(append(catalogKey(after), 0))
	}
	var names []string
	for ; valid && len(names) < ColdPackPageSize; valid = it.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		names = append(names, string(it.Key()[2:]))
	}
	return names, it.Error()
}

// ColdPackStep advances a bounded amount of work. The advisory metric/block
// cursor is WAL-synced every 30 seconds at step boundaries, on pass completion,
// policy changes and clean stop. Syncing the cursor also makes earlier range
// publications durable. A crash can replay work after the last checkpoint; it
// cannot checkpoint ahead of publication. Missing/invalid state or a different
// mode, write window or format marker starts a new sweep. Deleted cursor names
// seek to the next key; new/recreated metrics and corrections behind the cursor
// are visited next pass. Every range remains independently atomic and readable.
// Each call joins a wave of at most Workers distinct metrics before publishing
// the contiguous completion watermark. A metric error is logged and counted,
// then skipped for this pass; cancellation and step-wide errors are returned.
// Only one caller may advance it.
func (s *Store) ColdPackStep(ctx context.Context) (finished bool, failure error) {
	ctx, end, ok := s.explicitMaintenance.begin(ctx)
	if !ok {
		return false, context.Canceled
	}
	defer end()
	return s.coldPackStep(ctx)
}

func (s *Store) coldPackStep(ctx context.Context) (finished bool, failure error) {
	c := &s.coldPack
	c.run.Lock()
	defer c.run.Unlock()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	c.mu.Lock()
	o := c.options
	c.stats.Active = o.Mode != "off" && o.Mode != ""
	c.mu.Unlock()
	if o.Mode == "off" || o.Mode == "" {
		return true, nil
	}
	start := time.Now()
	defer func() {
		c.mu.Lock()
		if o.Workers == 1 {
			c.stats.WorkerNanos += uint64(time.Since(start))
		}
		c.stats.Active = false
		if failure != nil {
			c.passFailed = true
			c.stats.Error = failure.Error()
			if !errors.Is(failure, context.Canceled) {
				c.stats.Errors++
			}
		}
		c.mu.Unlock()
		if err := s.saveColdPackCheckpoint(finished); err != nil {
			failure = errors.Join(failure, err)
			c.mu.Lock()
			c.passFailed = true
			c.stats.Errors++
			c.stats.Error = err.Error()
			c.mu.Unlock()
		}
	}()
	// Pressure gates are sampled once per bounded step. Never wait for a writer's
	// lock; later sweeps retry conflicts and busy stripes.
	usage, diskErr := s.fs.GetDiskUsage(s.dir)
	if diskErr != nil && !errors.Is(diskErr, pebble.ErrNotFound) && !errors.Is(diskErr, vfs.ErrUnsupported) {
		return false, fmt.Errorf("cold pack disk reserve: %w", diskErr)
	}
	if diskErr == nil && (usage.AvailBytes < o.MinFreeBytes || usage.AvailBytes-o.MinFreeBytes < uint64(coldPackReadLimit*4*o.Workers)) {
		c.mu.Lock()
		c.stats.PressurePauses++
		c.mu.Unlock()
		return false, nil
	}
	m := s.db.Metrics()
	if m.Compact.EstimatedDebt > o.MaxCompactionDebt || int64(m.MemTable.Size) > int64(s.pebbleOptions.MemTableSize)*2 || m.Levels[0].Sublevels >= int32(max(2, s.pebbleOptions.L0StopWritesThreshold/2)) {
		c.mu.Lock()
		c.stats.PressurePauses++
		c.mu.Unlock()
		return false, nil
	}
	if len(c.pending) == 0 {
		c.mu.Lock()
		cursor, block := c.stats.Cursor, c.stats.BlockCursor
		c.mu.Unlock()
		if block != 0 {
			// A serial or parallel checkpoint can retain a published descriptor
			// position within the oldest unfinished metric.
			c.pending = []*coldPackWork{{name: cursor, block: block, points: c.checkpoint.MetricPoints, counted: c.checkpoint.MetricCounted}}
		} else {
			names, err := s.coldPackPage(ctx, cursor)
			if err != nil {
				return false, err
			}
			c.mu.Lock()
			c.stats.Pages++
			if len(names) == 0 {
				c.stats.Cursor = ""
				c.stats.Passes++
				c.stats.MetricsVisited = 0
				c.stats.StoredPoints, c.stats.StoredMetrics = c.passPoints, c.passMetrics
				c.stats.StoredAt = s.now().Unix()
				c.passPoints, c.passMetrics = 0, 0
				c.cleanUnpack = o.Mode == "unpack" && !c.passFailed
				c.passFailed = false
				c.mu.Unlock()
				return true, nil
			}
			c.mu.Unlock()
			for _, name := range names {
				c.pending = append(c.pending, &coldPackWork{name: name, counted: true})
			}
		}
	}
	c.mu.Lock()
	c.cleanUnpack = false
	c.stats.PendingMetrics = 0
	for _, work := range c.pending {
		if !work.done {
			c.stats.PendingMetrics++
		}
	}
	c.mu.Unlock()

	// One bounded range per worker, then join. The wave barrier bounds memory,
	// pacing overshoot and reload latency without per-range channel traffic.
	var wg sync.WaitGroup
	var failures []error
	var failureMu sync.Mutex
	started := 0
	for _, work := range c.pending {
		if work.done {
			continue
		}
		process := func() {
			if err := s.coldPackWork(ctx, work, o); err != nil {
				failureMu.Lock()
				failures = append(failures, err)
				failureMu.Unlock()
			}
		}
		if o.Workers == 1 {
			process() // preserve the serial path without goroutine overhead
		} else {
			wg.Go(process)
		}
		started++
		if started == o.Workers {
			break
		}
	}
	wg.Wait()

	c.mu.Lock()
	defer c.mu.Unlock()
	// Retire only the completed prefix. Later successes remain in the bounded
	// page until the oldest outstanding metric finishes; a crash may replay
	// them. Failed metrics are retired too, and retried from block zero next
	// pass; passFailed prevents a partial unpack from authorizing downgrade.
	for len(c.pending) > 0 && c.pending[0].done {
		if work := c.pending[0]; work.counted {
			c.passPoints += work.points
			c.passMetrics++
		}
		c.stats.Cursor, c.stats.BlockCursor = c.pending[0].name, 0
		c.stats.MetricsVisited++
		c.pending = c.pending[1:]
	}
	if len(c.pending) > 0 && c.pending[0].block != 0 {
		c.stats.Cursor, c.stats.BlockCursor = c.pending[0].name, c.pending[0].block
	}
	c.stats.PendingMetrics = 0
	for _, work := range c.pending {
		if !work.done {
			c.stats.PendingMetrics++
		}
	}
	return false, errors.Join(failures...)
}

func (s *Store) coldPackWork(ctx context.Context, work *coldPackWork, o ColdPackOptions) error {
	c := &s.coldPack
	c.mu.Lock()
	c.stats.ActiveWorkers++
	c.mu.Unlock()
	started := time.Now()
	stats, next, done, read, written, lockTime, conflict, err := s.coldPackMetric(ctx, work.name, ColdPackRequest{Cursor: work.block, Cutoff: s.now().Add(-o.WriteWindow), Mode: o.Mode, PackTails: o.PackTails})
	if err == nil && !done && next <= work.block {
		err = errors.New("cold pack codec did not advance")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stats.ActiveWorkers--
	if o.Workers > 1 {
		c.stats.WorkerNanos += uint64(time.Since(started))
	}
	// Failed plans still consume read/worker budget. Retire a failed metric
	// without publishing its plan, so it cannot hold the rest of the pass.
	c.stats.ReadBytes += read
	c.stats.WriteBytes += written
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, context.Canceled) {
			return err
		}
		c.stats.Errors++
		c.stats.Skipped++
		c.stats.Error = fmt.Sprintf("metric %q block %d: %v", work.name, work.block, err)
		c.passFailed = true
		zapwriter.Logger("storage").Error("cold pack metric failed", zap.String("metric", work.name), zap.Uint64("block_cursor", work.block), zap.Error(err))
		work.block, work.done = 0, true
		work.counted = false
		return nil
	}
	c.stats.Metrics += stats.Metrics
	c.stats.Blocks += stats.Blocks
	c.stats.Points += stats.Points
	if o.Mode == "unpack" {
		c.stats.UnpackedBlocks += stats.Blocks
		c.stats.UnpackedPoints += stats.Points
	}
	c.stats.InputBytes += stats.InputBytes
	c.stats.OutputBytes += stats.OutputBytes
	c.stats.RepackedBlocks += stats.RepackedBlocks
	c.stats.RepackedPoints += stats.RepackedPoints
	c.stats.RepackedInputBytes += stats.RepackedInputBytes
	c.stats.RepackedOutputBytes += stats.RepackedOutputBytes
	c.stats.Skipped += stats.Skipped
	if stats.Skipped > 0 || conflict != "" {
		c.passFailed = true
		if conflict != "" {
			c.stats.Error = fmt.Sprintf("metric %q block %d: %s", work.name, work.block, conflict)
		} else {
			c.stats.Error = fmt.Sprintf("metric %q block %d: skipped %d ranges", work.name, work.block, stats.Skipped)
		}
	}
	c.stats.LockNanos += uint64(lockTime)
	c.stats.MaxLockNanos = max(c.stats.MaxLockNanos, uint64(lockTime))
	if conflict != "" {
		c.stats.Conflicts++
	}
	work.points += stats.ObservedPoints
	work.counted = work.counted && stats.Observed
	work.block, work.done = next, done
	return nil
}

// limitedRepackIO caps snapshot bytes and buffered mutations before publication.
// The adapter reads each source range before it writes; read-after-write is not
// part of this planning contract.
type limitedRepackIO struct {
	ContinuationIO
	batch *pebble.Batch
	base  *continuationIO
	read  uint64
}

func (p *limitedRepackIO) account(n int) error {
	p.read += uint64(n)
	if p.read > coldPackReadLimit {
		return ErrColdPackBudget
	}
	return nil
}
func (p *limitedRepackIO) Get(k []byte) ([]byte, error) {
	if err := p.base.ctx.Err(); err != nil {
		return nil, err
	}
	value, closer, err := p.base.reader.Get(p.base.key(k))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	// Check the borrowed value before allocating an owned copy.
	if err := p.account(len(k) + len(value)); err != nil {
		return nil, errors.Join(err, closer.Close())
	}
	owned := bytes.Clone(value)
	return owned, closer.Close()
}

func (p *limitedRepackIO) Visit(k []byte, f func([]byte, []byte) error) error {
	return p.ContinuationIO.Visit(k, func(k, v []byte) error {
		if e := p.account(len(k) + len(v)); e != nil {
			return e
		}
		return f(k, v)
	})
}
func (p *limitedRepackIO) Set(k, v []byte) error {
	if p.batch.Len()+len(k)+len(v)+32 > coldPackBatchLimit {
		return ErrColdPackBudget
	}
	return p.base.Set(k, v)
}
func (p *limitedRepackIO) Delete(k []byte) error {
	if p.batch.Len()+len(k)+32 > coldPackBatchLimit {
		return ErrColdPackBudget
	}
	return p.base.Delete(k)
}

// DeletePrefix enumerates the immutable source and buffers point deletes. A
// range tombstone here would invalidate Pebble's shared memtable fragmentation
// cache on each publication and stall unrelated reads.
func (p *limitedRepackIO) DeletePrefix(k []byte) error {
	return p.Visit(k, func(key, value []byte) error { return p.Delete(key) })
}

// ContinuationPageReader seeks within one metric-local prefix. start is an
// inclusive suffix; callbacks borrow values only until they return.
type ContinuationPageReader interface {
	VisitPage(prefix, start []byte, limit int, fn func([]byte, []byte) error) (bool, error)
}

func (p *limitedRepackIO) VisitPage(prefix, start []byte, limit int, fn func([]byte, []byte) error) (bool, error) {
	if limit < 1 || limit > 32 {
		return false, ErrColdPackBudget
	}
	lower := p.base.key(prefix)
	it, err := p.base.reader.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: prefixEnd(lower)})
	if err != nil {
		return false, err
	}
	defer it.Close()
	seek := p.base.key(start)
	for valid, n := it.SeekGE(seek), 0; valid; valid = it.Next() {
		if n == limit {
			return false, nil
		}
		n++
		if err := p.base.ctx.Err(); err != nil {
			return false, err
		}
		key, value := p.base.suffix(it.Key()), it.Value()
		if err := p.account(len(key) + len(value)); err != nil {
			return false, err
		}
		if err := fn(key, value); err != nil {
			return false, err
		}
	}
	return true, it.Error()
}

func (s *Store) coldPackMetric(ctx context.Context, name string, request ColdPackRequest) (stats RepackStats, next uint64, done bool, read, written uint64, held time.Duration, conflict string, err error) {
	snap := s.db.NewSnapshot()
	defer snap.Close()
	meta, e := metadataFrom(snap, name)
	if errors.Is(e, ErrNotFound) {
		return stats, 0, true, 0, 0, 0, "", nil
	}
	if e != nil {
		err = e
		return
	}
	if !meta.Continuation {
		done = true
		return
	}
	if err = s.checkFileOwnership(meta); err != nil {
		if errors.Is(err, ErrNotFound) {
			// Staged migration candidates remain in the raw catalog until
			// verified/published or discarded. They are not ours to rewrite.
			done, err = true, nil
		}
		return
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	base := s.newContinuationIO(ctx, snap, batch, meta)
	defer base.Close()
	plan := &limitedRepackIO{ContinuationIO: base, base: base, batch: batch}
	stats, next, done, err = s.continuation.(ContinuationRepacker).Repack(ctx, plan, request)
	read = plan.read
	if errors.Is(err, ErrColdPackBudget) {
		stats = RepackStats{Skipped: 1}
		err = nil
		done = true
		return
	}
	if err != nil || stats.Blocks == 0 {
		return
	}
	if meta.Revision == math.MaxUint64 {
		err = errors.New("metric revision exhausted")
		return
	}
	updated := meta
	updated.Revision++
	if err = batch.Set(revisionKey(updated), revisionBytes(updated), nil); err != nil {
		return
	}
	if err = ctx.Err(); err != nil {
		return
	}
	// Snapshot and encoding work are complete before taking the stripe. Revision
	// equality fences every update, deletion, transfer and generation replacement.
	unlock, ok := s.tryLockMetricMutation(name)
	if !ok {
		stats = RepackStats{}
		conflict = "metric stripe busy"
		return
	}
	locked := time.Now()
	defer func() { unlock(); held = time.Since(locked) }()
	current, e := s.metadataLocked(name)
	if errors.Is(e, ErrNotFound) || e == nil && (current.ID != meta.ID || current.Generation != meta.Generation || current.Revision != meta.Revision) {
		stats = RepackStats{}
		conflict = "metric changed during planning"
		return
	}
	if e != nil {
		err = e
		return
	}
	if time.Since(locked) > 2*time.Millisecond {
		stats = RepackStats{}
		conflict = "publication validation exceeded 2ms"
		return
	}
	if err = ctx.Err(); err != nil {
		return
	}
	if err = s.commit(batch); err != nil {
		return
	}
	written = uint64(batch.Len())
	stats.Metrics = 1
	return
}

func (s *Store) startColdPack() {
	c := &s.coldPack
	ctx, cancel := context.WithCancel(s.maintenanceCtx)
	c.cancel = cancel
	c.done = make(chan struct{})
	c.wake = make(chan struct{}, 1)
	go func() {
		defer close(c.done)
		defer s.checkpointColdPackOnStop()
		if err := s.waitMaintenance(ctx, "cold-pack"); err != nil {
			return
		}
		var pacer coldPackPacer
		timer := time.NewTimer(time.Second)
		defer timer.Stop()
		delay := time.Second
		for {
			if delay > 0 {
				select {
				case <-ctx.Done():
					return
				case <-c.wake:
				case <-timer.C:
				}
			} else {
				select {
				case <-ctx.Done():
					return
				default:
				}
			}
			before := s.ColdPackStats()
			started := time.Now()
			done, err := s.coldPackStep(ctx)
			c.mu.Lock()
			o := c.options
			c.mu.Unlock()
			after := s.ColdPackStats()
			delay = s.coldPackDelay(&pacer, before, started)
			if err != nil && !errors.Is(err, context.Canceled) {
				zapwriter.Logger("storage").Error("cold pack failed", zap.String("metric", after.Cursor), zap.Uint64("block_cursor", after.BlockCursor), zap.Error(err))
			}
			if done && o.Mode != "off" {
				zapwriter.Logger("storage").Info("cold pack pass complete", zap.String("mode", o.Mode), zap.Bool("pack_tails", o.PackTails), zap.Uint64("blocks", after.Blocks), zap.Int64("saved_bytes", int64(after.InputBytes)-int64(after.OutputBytes)), zap.Uint64("skipped", after.Skipped), zap.Uint64("conflicts", after.Conflicts))
			}
			if done || err != nil {
				delay = max(delay, o.Interval)
			}
			if after.PressurePauses > before.PressurePauses {
				delay = max(delay, time.Second)
			}
			if delay > 0 {
				timer.Reset(delay)
			}
		}
	}()
}

func (s *Store) stopColdPack() {
	if c := &s.coldPack; c.cancel != nil {
		c.cancel()
		<-c.done
	}
}

// RunColdPack completes one paced sweep. Offline tools use the same step and
// pacing policy as the background worker; callers must stop that worker first.
func (s *Store) RunColdPack(ctx context.Context) error {
	ctx, end, ok := s.explicitMaintenance.begin(ctx)
	if !ok {
		return context.Canceled
	}
	defer end()
	errorsBefore := s.ColdPackStats().Errors
	var pacer coldPackPacer
	timer := time.NewTimer(0)
	timer.Stop()
	defer timer.Stop()
	for {
		before := s.ColdPackStats()
		started := time.Now()
		done, err := s.coldPackStep(ctx)
		if err != nil || done {
			if err == nil {
				if stats := s.ColdPackStats(); stats.Errors > errorsBefore {
					err = fmt.Errorf("cold pack pass failed: %s", stats.Error)
				}
			}
			return err
		}
		delay := s.coldPackDelay(&pacer, before, started)
		if delay == 0 {
			continue
		}
		timer.Reset(delay)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// Short operations must share pacing debt: a timer per metric rounds cheap
// scans up to the platform's timer resolution and leaves both budgets unused.
// Defer waits below this quantum, allowing at most this much extra budget debt
// plus one bounded step. Real sleeps repay debt; idle time never earns credit.
const coldPackPaceQuantum = 10 * time.Millisecond

type coldPackPacer struct {
	options              ColdPackOptions
	cpuAfter, bytesAfter time.Time
}

func (s *Store) coldPackDelay(p *coldPackPacer, before ColdPackStats, started time.Time) time.Duration {
	c := &s.coldPack
	c.mu.Lock()
	o, after := c.options, c.stats
	c.mu.Unlock()
	return p.delay(o, before, after, started, time.Now())
}

func (p *coldPackPacer) delay(o ColdPackOptions, before, after ColdPackStats, started, now time.Time) time.Duration {
	if p.options != o {
		*p = coldPackPacer{options: o}
	}
	if p.cpuAfter.Before(started) {
		p.cpuAfter = started
	}
	if p.bytesAfter.Before(started) {
		p.bytesAfter = started
	}
	elapsed := now.Sub(started)
	p.cpuAfter = p.cpuAfter.Add(time.Duration(float64(elapsed) / o.CPUFraction))
	bytes := after.ReadBytes - before.ReadBytes + after.WriteBytes - before.WriteBytes
	p.bytesAfter = p.bytesAfter.Add(time.Duration(float64(bytes) / float64(o.BytesPerSecond) * float64(time.Second)))
	delay := max(p.cpuAfter.Sub(now), p.bytesAfter.Sub(now))
	if after.PressurePauses > before.PressurePauses {
		delay = max(delay, time.Second)
	}
	if delay < coldPackPaceQuantum {
		return 0
	}
	return delay
}
