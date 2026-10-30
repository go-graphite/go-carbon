package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

// ColdPackOptions controls one low-priority worker. Mode is off, exact, decimal,
// or unpack. Decimal rounding occurs only while sealing, never in hot writes.
type ColdPackOptions struct {
	Mode           string
	BytesPerSecond int64
	MinFreeBytes   uint64
	// CPUFraction bounds worker wall time / elapsed time. Charging IO wait as CPU
	// is conservative; Pebble's asynchronous flush/compaction CPU is separate.
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
}

type ColdPackRequest struct {
	Cursor uint64 // codec-owned descriptor seek position
	Cutoff time.Time
	Mode   string
}

// ContinuationRepacker examines at most 32 descriptors and rewrites at most one
// bounded physical range. All IO uses an immutable snapshot plus buffered writes.
type ContinuationRepacker interface {
	Repack(context.Context, ContinuationIO, ColdPackRequest) (RepackStats, uint64, bool, error)
}

type ColdPackStats struct {
	RepackStats
	Pages, Passes, Errors, Conflicts, PressurePauses uint64
	ReadBytes, WriteBytes                            uint64
	WorkerNanos, LockNanos, MaxLockNanos             uint64
	Cursor                                           string
	BlockCursor                                      uint64
	PendingMetrics                                   int
	Active                                           bool
	Mode, Error                                      string
}

type coldPackState struct {
	mu          sync.Mutex
	options     ColdPackOptions
	stats       ColdPackStats
	run         sync.Mutex
	format      atomic.Bool
	cleanUnpack bool
	passFailed  bool
	pending     []string
	wake        chan struct{}
	cancel      context.CancelFunc
	done        chan struct{}
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
	if c.options.Mode != o.Mode {
		c.stats.Cursor, c.stats.BlockCursor = "", 0
		c.pending = nil
		c.passFailed = false
	}
	if o.Mode != "off" || c.options.Mode != "" && c.options.Mode != "off" {
		log.Printf("cold pack configured: mode=%s bytes_per_second=%d cpu_fraction=%g write_window=%s max_compaction_debt=%d", o.Mode, o.BytesPerSecond, o.CPUFraction, o.WriteWindow, o.MaxCompactionDebt)
	}
	c.cleanUnpack = false
	c.options = o
	c.stats.Mode = o.Mode
	c.mu.Unlock()
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
	stats.PendingMetrics = len(c.pending)
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

// ColdPackStep advances a bounded amount of work. Cursor state is intentionally
// in memory: after a crash the idempotent sweep restarts, while every published
// range is independently atomic and readable. Only one caller may advance it.
func (s *Store) ColdPackStep(ctx context.Context) (finished bool, failure error) {
	c := &s.coldPack
	c.run.Lock()
	defer c.run.Unlock()
	c.mu.Lock()
	o, cursor, block := c.options, c.stats.Cursor, c.stats.BlockCursor
	c.stats.Active = o.Mode != "off"
	c.mu.Unlock()
	if o.Mode == "off" || o.Mode == "" {
		return true, nil
	}
	start := time.Now()
	defer func() {
		c.mu.Lock()
		c.stats.WorkerNanos += uint64(time.Since(start))
		c.stats.Active = false
		if failure != nil {
			c.passFailed = true
			if !errors.Is(failure, context.Canceled) {
				c.stats.Errors++
				c.stats.Error = failure.Error()
			}
		}
		c.mu.Unlock()
	}()
	// Pressure gates are sampled once per bounded step. Never wait for a writer's
	// lock; later sweeps retry conflicts and busy stripes.
	usage, diskErr := s.fs.GetDiskUsage(s.dir)
	if diskErr != nil && !errors.Is(diskErr, pebble.ErrNotFound) && !errors.Is(diskErr, vfs.ErrUnsupported) {
		return false, fmt.Errorf("cold pack disk reserve: %w", diskErr)
	}
	if diskErr == nil && (usage.AvailBytes < o.MinFreeBytes || usage.AvailBytes-o.MinFreeBytes < coldPackReadLimit*4) {
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
	if block == 0 {
		c.mu.Lock()
		empty := len(c.pending) == 0
		c.mu.Unlock()
		if empty {
			names, err := s.coldPackPage(ctx, cursor)
			if err != nil {
				return false, err
			}
			c.mu.Lock()
			c.stats.Pages++
			c.mu.Unlock()
			if len(names) == 0 {
				c.mu.Lock()
				c.stats.Cursor = ""
				c.stats.Passes++
				c.cleanUnpack = o.Mode == "unpack" && !c.passFailed
				c.passFailed = false
				c.mu.Unlock()
				return true, nil
			}
			c.mu.Lock()
			c.pending = names
			c.mu.Unlock()
		}
		c.mu.Lock()
		cursor, c.pending = c.pending[0], c.pending[1:]
		c.mu.Unlock()
	}
	c.mu.Lock()
	c.cleanUnpack = false
	c.mu.Unlock()
	stats, next, done, read, written, lockTime, conflict, err := s.coldPackMetric(ctx, cursor, ColdPackRequest{Cursor: block, Cutoff: s.now().Add(-o.WriteWindow), Mode: o.Mode})
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		// A newly selected name has not yet been published as the cursor. Keep
		// it queued across errors/cancellation rather than silently skipping it.
		if block == 0 {
			c.pending = append([]string{cursor}, c.pending...)
		}
		return false, err
	}
	c.stats.Metrics += stats.Metrics
	c.stats.Blocks += stats.Blocks
	c.stats.Points += stats.Points
	c.stats.InputBytes += stats.InputBytes
	c.stats.OutputBytes += stats.OutputBytes
	c.stats.Skipped += stats.Skipped
	if stats.Skipped > 0 || conflict {
		c.passFailed = true
	}
	c.stats.ReadBytes += read
	c.stats.WriteBytes += written
	c.stats.LockNanos += uint64(lockTime)
	c.stats.MaxLockNanos = max(c.stats.MaxLockNanos, uint64(lockTime))
	if conflict {
		c.stats.Conflicts++
	}
	c.stats.Cursor, c.stats.BlockCursor = cursor, next
	if done {
		c.stats.BlockCursor = 0
	} else if next == 0 {
		return false, errors.New("cold pack codec did not advance")
	}
	return false, nil
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

func (s *Store) coldPackMetric(ctx context.Context, name string, request ColdPackRequest) (stats RepackStats, next uint64, done bool, read, written uint64, held time.Duration, conflict bool, err error) {
	snap := s.db.NewSnapshot()
	defer snap.Close()
	meta, e := metadataFrom(snap, name)
	if errors.Is(e, ErrNotFound) {
		return stats, 0, true, 0, 0, 0, false, nil
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
		conflict = true
		return
	}
	locked := time.Now()
	defer func() { unlock(); held = time.Since(locked) }()
	current, e := s.metadataLocked(name)
	if errors.Is(e, ErrNotFound) || e == nil && (current.ID != meta.ID || current.Generation != meta.Generation || current.Revision != meta.Revision) {
		stats = RepackStats{}
		conflict = true
		return
	}
	if e != nil {
		err = e
		return
	}
	if time.Since(locked) > 2*time.Millisecond {
		stats = RepackStats{}
		conflict = true
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
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.done = make(chan struct{})
	c.wake = make(chan struct{}, 1)
	go func() {
		defer close(c.done)
		timer := time.NewTimer(time.Second)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-c.wake:
			case <-timer.C:
			}
			before := s.ColdPackStats()
			started := time.Now()
			done, err := s.ColdPackStep(ctx)
			c.mu.Lock()
			o := c.options
			c.mu.Unlock()
			after := s.ColdPackStats()
			delay := s.coldPackDelay(before, started)
			if err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("ERROR cold pack failed: %v", err)
			}
			if done && o.Mode != "off" {
				log.Printf("cold pack pass complete: mode=%s blocks=%d saved_bytes=%d skipped=%d conflicts=%d", o.Mode, after.Blocks, int64(after.InputBytes)-int64(after.OutputBytes), after.Skipped, after.Conflicts)
			}
			if done || err != nil {
				delay = max(delay, o.Interval)
			}
			if after.PressurePauses > before.PressurePauses {
				delay = max(delay, time.Second)
			}
			timer.Reset(max(delay, time.Millisecond))
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
	for {
		before := s.ColdPackStats()
		started := time.Now()
		done, err := s.ColdPackStep(ctx)
		if err != nil || done {
			return err
		}
		delay := s.coldPackDelay(before, started)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (s *Store) coldPackDelay(before ColdPackStats, started time.Time) time.Duration {
	c := &s.coldPack
	c.mu.Lock()
	o, after := c.options, c.stats
	c.mu.Unlock()
	elapsed := time.Since(started)
	delay := time.Duration(float64(elapsed) * (1/o.CPUFraction - 1))
	bytes := after.ReadBytes - before.ReadBytes + after.WriteBytes - before.WriteBytes
	delay = max(delay, time.Duration(float64(bytes)/float64(o.BytesPerSecond)*float64(time.Second))-elapsed)
	if after.PressurePauses > before.PressurePauses {
		delay = max(delay, time.Second)
	}
	return max(delay, time.Millisecond)
}
