package persister

import (
	"container/heap"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperpath"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"

	"github.com/go-graphite/go-carbon/internal/phasecpu"
)

// deferredReclaim unlinks published originals later and in batches. An unlink
// frees an inode while conversions are still opening its directory neighbours;
// XFS then forces its log for each open that touches the freed inode cluster,
// and thousands of conversions queued on those forces. Unlinking a directory's
// originals after its conversions, together, leaves one directory sync per
// batch. Ownership is durable before a job is queued, so a stop or crash only
// leaves pending cleanups that the next run resumes.
type deferredReclaim struct {
	rootMu         sync.RWMutex
	root           *os.File
	delay          time.Duration
	started        time.Time
	workers        int
	mu             sync.Mutex
	dirs           map[string]*reclaimDirectoryQueue
	ready          reclaimDirectories
	queued         atomic.Int64
	completed      atomic.Uint64
	inlineFallback atomic.Uint64
	batches        atomic.Uint64
	jobs           atomic.Uint64
	batchNanos     atomic.Uint64
	unlinkNanos    atomic.Uint64
	syncNanos      atomic.Uint64
	cleanupNanos   atomic.Uint64
	wake           chan struct{}
	stop           chan struct{}
	done           chan struct{}
}

// CompleteFileCleanup checks only Name, ID and Generation against current
// metadata. Retaining the whole publication also pinned its retention slice.
// This record is 56 bytes on 64-bit hosts, plus the name/path string contents.
// due is monotonic nanoseconds since the reclaimer started.
type reclaimJob struct {
	name, path     string
	id, generation uint64
	due            int64
}

type reclaimDirectoryQueue struct {
	path      string
	jobs      []reclaimJob
	nextBatch int64
}

type reclaimDirectories []*reclaimDirectoryQueue

func (q *reclaimDirectoryQueue) deadline() int64 { return max(q.jobs[0].due, q.nextBatch) }
func (q reclaimDirectories) Len() int            { return len(q) }
func (q reclaimDirectories) Less(i, j int) bool  { return q[i].deadline() < q[j].deadline() }
func (q reclaimDirectories) Swap(i, j int)       { q[i], q[j] = q[j], q[i] }
func (q *reclaimDirectories) Push(v any)         { *q = append(*q, v.(*reclaimDirectoryQueue)) }
func (q *reclaimDirectories) Pop() any {
	old := *q
	n := len(old) - 1
	v := old[n]
	old[n] = nil
	*q = old[:n]
	return v
}

const (
	// This limit applies per directory, rather than to a host-wide wave.
	reclaimBatch = 8192
	reclaimTick  = time.Second
	// Allow eight times the old backlog before forcing conversions back onto
	// the inline unlink/sync path. At 10k/s this holds about fourteen minutes.
	reclaimMaxQueued = 8 << 20
)

func (p *Whisper) reclaimWorkers() int {
	workers := cap(p.conversionIO)
	if workers == 0 {
		workers = 4 * runtime.GOMAXPROCS(0)
	}
	return max(64, min(workers, 512))
}

// SetDeferredReclaim delays unlinking converted originals by at least delay and
// batches them by directory; zero keeps reclamation inside each conversion.
func (p *Whisper) SetDeferredReclaim(delay time.Duration) {
	if delay <= 0 || p.reclaim != nil {
		return
	}
	// File syscalls can block OS threads. Match conversion I/O parallelism,
	// capped at 512, with a CPU-derived default when no slots were configured.
	workers := p.reclaimWorkers()
	r := &deferredReclaim{
		delay: delay, started: time.Now(), workers: workers,
		dirs: make(map[string]*reclaimDirectoryQueue), wake: make(chan struct{}, 1),
		stop: make(chan struct{}), done: make(chan struct{}),
	}
	if p.rootPath != "" {
		if fd, err := whisperpath.OpenDir(p.rootPath); err == nil {
			r.root = os.NewFile(uintptr(fd), p.rootPath)
		}
	}
	p.reclaim = r
	go p.runDeferredReclaim(r)
}

// StopDeferredReclaim stops the reclaimer. Queued originals stay pending
// cleanups for the next run.
func (p *Whisper) StopDeferredReclaim() {
	if r := p.reclaim; r != nil {
		close(r.stop)
		<-r.done
		p.reclaim = nil
	}
}

// DeferredReclaimQueued reports deferred cleanup jobs, including active batches.
func (p *Whisper) DeferredReclaimQueued() int64 {
	if r := p.reclaim; r != nil {
		return r.queued.Load()
	}
	return 0
}

// DeferredReclaimStats reports completed cleanups and directory-batch work.
// Durations sum worker time; rate(Completed) measures the actual drain rate.
type DeferredReclaimStats struct {
	Completed, InlineFallback, Batches, Jobs         uint64
	BatchNanos, UnlinkNanos, SyncNanos, CleanupNanos uint64
	Workers                                          int
}

func (p *Whisper) DeferredReclaimStats() DeferredReclaimStats {
	if r := p.reclaim; r != nil {
		return DeferredReclaimStats{
			Completed: r.completed.Load(), InlineFallback: r.inlineFallback.Load(),
			Batches: r.batches.Load(), Jobs: r.jobs.Load(), BatchNanos: r.batchNanos.Load(),
			UnlinkNanos: r.unlinkNanos.Load(), SyncNanos: r.syncNanos.Load(), CleanupNanos: r.cleanupNanos.Load(),
			Workers: r.workers,
		}
	}
	return DeferredReclaimStats{}
}

// push reserves capacity under the queue lock, including jobs already being
// reclaimed. Concurrent publications cannot overrun the cap.
func (r *deferredReclaim) push(job reclaimJob) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.queued.Load() >= reclaimMaxQueued {
		return false
	}
	job.due = time.Since(r.started).Nanoseconds() + int64(r.delay)
	r.append(job)
	r.queued.Add(1)
	return true
}

// append is called with mu held. A directory is in the heap only while idle;
// arrivals during its batch wait for the same worker to finish its sync.
func (r *deferredReclaim) append(job reclaimJob) {
	dir := filepath.Dir(job.path)
	q := r.dirs[dir]
	if q == nil {
		q = &reclaimDirectoryQueue{path: dir, jobs: []reclaimJob{job}}
		r.dirs[dir] = q
		heap.Push(&r.ready, q)
		return
	}
	// A busy metric's retry may arrive behind publications with a longer delay.
	// Keep each directory FIFO without letting it block another directory.
	if n := len(q.jobs); n > 0 {
		job.due = max(job.due, q.jobs[n-1].due)
	}
	q.jobs = append(q.jobs, job)
}

// due detaches one directory's due prefix without copying the remaining jobs.
// It examines only due jobs and the heap root, never the delayed backlog.
func (r *deferredReclaim) due(now time.Time) (*reclaimDirectoryQueue, []reclaimJob) {
	r.mu.Lock()
	defer r.mu.Unlock()
	deadline := now.Sub(r.started).Nanoseconds()
	if len(r.ready) == 0 || r.ready[0].deadline() > deadline {
		return nil, nil
	}
	q := heap.Pop(&r.ready).(*reclaimDirectoryQueue)
	n := 0
	for n < len(q.jobs) && n < reclaimBatch && q.jobs[n].due <= deadline {
		n++
	}
	jobs := q.jobs[:n:n]
	q.jobs = q.jobs[n:]
	if len(q.jobs) == 0 {
		q.jobs = nil
	}
	return q, jobs
}

func (r *deferredReclaim) finish(q *reclaimDirectoryQueue) {
	r.mu.Lock()
	if len(q.jobs) == 0 {
		delete(r.dirs, q.path)
	} else {
		// Continue full due batches immediately. Accumulate smaller batches
		// until the next tick instead of syncing each trickle of newly due
		// jobs while other workers keep waking the dispatcher.
		now := time.Since(r.started).Nanoseconds()
		q.nextBatch = 0
		if len(q.jobs) < reclaimBatch || q.jobs[reclaimBatch-1].due > now {
			q.nextBatch = (now/int64(reclaimTick) + 1) * int64(reclaimTick)
		}
		heap.Push(&r.ready, q)
		select {
		case r.wake <- struct{}{}:
		default:
		}
	}
	r.mu.Unlock()
}

func (p *Whisper) runDeferredReclaim(r *deferredReclaim) {
	phasecpu.Set(phasecpu.Reclaim)
	defer phasecpu.Set(phasecpu.Other)
	type batch struct {
		dir  *reclaimDirectoryQueue
		jobs []reclaimJob
	}
	work := make(chan batch)
	var workers sync.WaitGroup
	for range r.workers {
		workers.Go(func() {
			for b := range work {
				p.reclaimDirectory(r, b.dir.path, b.jobs)
				r.finish(b.dir)
			}
		})
	}
	defer func() {
		close(work)
		workers.Wait()
		r.rootMu.Lock()
		if r.root != nil {
			r.root.Close()
			r.root = nil
		}
		r.rootMu.Unlock()
		close(r.done)
	}()
	ticker := time.NewTicker(reclaimTick)
	defer ticker.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-ticker.C:
		case <-r.wake:
		}
		for {
			dir, jobs := r.due(time.Now())
			if dir == nil {
				break
			}
			select {
			case <-r.stop:
				return
			case work <- batch{dir: dir, jobs: jobs}:
			}
		}
	}
}

// reclaimDirectory unlinks one directory's originals, syncs the directory once
// and only then records the cleanups as complete.
func (p *Whisper) reclaimDirectory(r *deferredReclaim, dir string, jobs []reclaimJob) {
	started := time.Now()
	released := len(jobs)
	defer func() {
		r.queued.Add(-int64(released))
		r.batches.Add(1)
		r.jobs.Add(uint64(len(jobs)))
		r.batchNanos.Add(uint64(time.Since(started)))
		clear(jobs)
	}()
	logger := zapwriter.Logger("storage")
	ctx := context.Background()
	unlinked := jobs[:0]
	for _, job := range jobs {
		// A concurrent resume of the same metric owns its cleanup.
		if _, busy := p.converting.LoadOrStore(job.name, struct{}{}); busy {
			r.mu.Lock()
			job.due = time.Since(r.started).Nanoseconds() + int64(reclaimTick)
			r.append(job)
			r.mu.Unlock()
			released--
			continue
		}
		err := removeOriginals(job.path)
		p.converting.Delete(job.name)
		if err != nil {
			logger.Warn("deferred reclaim failed; cleanup stays pending", zap.String("metric", job.name), zap.Error(err))
			continue
		}
		unlinked = append(unlinked, job)
	}
	r.unlinkNanos.Add(uint64(time.Since(started)))
	if len(unlinked) == 0 {
		return
	}
	synced := time.Now()
	err := p.reclaimedDirectories.sync(dir, p.rootPath)
	r.syncNanos.Add(uint64(time.Since(synced)))
	if err != nil {
		logger.Warn("deferred reclaim directory sync failed; cleanups stay pending", zap.String("directory", dir), zap.Error(err))
		return
	}
	// One unpaced ancestor chain per synced directory batch. The batch and
	// reclaim I/O workers already bound the namespace work.
	if err := p.pruneOriginalDirectory(context.Background(), dir); err != nil {
		logger.Warn("prune reclaimed directory", zap.String("directory", dir), zap.Error(err))
	}

	completing := time.Now()
	for _, job := range unlinked {
		metadata := store.Metadata{MetricConfig: store.MetricConfig{Name: job.name}, ID: job.id, Generation: job.generation}
		if err := p.incrementalStore.CompleteFileCleanup(ctx, metadata); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Warn("deferred reclaim completion failed; cleanup stays pending", zap.String("metric", job.name), zap.Error(err))
			}
		} else {
			r.completed.Add(1)
		}
	}
	r.cleanupNanos.Add(uint64(time.Since(completing)))
}

// removeOriginals unlinks a metric's original files, refusing nonregular ones.
func removeOriginals(path string) error {
	for _, original := range originalPaths(path) {
		info, err := os.Lstat(original)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return errors.New("refuse to reclaim a nonregular original: " + original)
		}
		if err := os.Remove(original); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}
