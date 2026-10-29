package persister

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestDeferredReclaimUnlinksAfterDelayAndCompletesCleanup(t *testing.T) {
	t.Parallel()
	p, _, db, _ := incrementalPersisterFixture(t)
	defer db.Close()
	ctx := context.Background()
	old := time.Now().Add(-30 * 24 * time.Hour)
	if err := os.Chtimes(filepath.Join(p.rootPath, "metric.wsp"), old, old); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(p.rootPath, "metric.wsp"))
	if err != nil {
		t.Fatal(err)
	}
	wantActivity := info.ModTime()
	p.SetDeferredReclaim(200 * time.Millisecond)
	defer p.StopDeferredReclaim()
	path, err := p.incrementalPath("metric")
	if err != nil {
		t.Fatal(err)
	}
	result, err := p.ConvertFile(ctx, "metric")
	if err != nil {
		t.Fatal(err)
	}
	if result.UnlinkedBytes == 0 {
		t.Fatal("scheduled original bytes not reported")
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("original unlinked before the delay: %v", err)
	}
	if m, err := db.Metadata(ctx, "metric"); err != nil || m.FileMigration == store.FileReclaimed {
		t.Fatalf("cleanup completed before unlinking: %+v %v", m, err)
	}
	if p.DeferredReclaimQueued() != 1 {
		t.Fatalf("queued = %d", p.DeferredReclaimQueued())
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		m, err := db.Metadata(ctx, "metric")
		if err == nil && m.FileMigration == store.FileReclaimed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("deferred cleanup did not complete: %+v %v", m, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	m, err := db.Metadata(ctx, "metric")
	if err != nil || !m.LastUpdate.Equal(wantActivity) {
		t.Fatalf("deferred reclaim changed activity: %s want %s, %v", m.LastUpdate, wantActivity, err)
	}
	assertOriginalsReclaimed(t, p, "metric")
	if p.DeferredReclaimQueued() != 0 {
		t.Fatalf("queued = %d after reclaim", p.DeferredReclaimQueued())
	}
}

func TestDeferredReclaimStopLeavesPendingCleanupForResume(t *testing.T) {
	p, _, db, _ := incrementalPersisterFixture(t)
	defer db.Close()
	ctx := context.Background()
	p.SetDeferredReclaim(time.Hour)
	if _, err := p.ConvertFile(ctx, "metric"); err != nil {
		t.Fatal(err)
	}
	p.StopDeferredReclaim()
	pending, err := db.PendingFileCleanup(ctx, 10)
	if err != nil || len(pending) != 1 || pending[0].Name != "metric" {
		t.Fatalf("pending cleanup = %+v %v", pending, err)
	}
	// The next run resumes it inline.
	result, err := p.ConvertFile(ctx, "metric")
	if err != nil || !result.PreviouslyOwned {
		t.Fatalf("resume: %+v %v", result, err)
	}
	assertOriginalsReclaimed(t, p, "metric")
	if m, err := db.Metadata(ctx, "metric"); err != nil || m.FileMigration != store.FileReclaimed {
		t.Fatalf("resumed cleanup incomplete: %+v %v", m, err)
	}
	if _, err := db.PendingFileCleanup(ctx, 10); err != nil && !errors.Is(err, store.ErrNotFound) {
		t.Fatal(err)
	}
}

func TestDeferredReclaimFallsBackInlineWhenBacklogIsFull(t *testing.T) {
	p, _, db, _ := incrementalPersisterFixture(t)
	defer db.Close()
	ctx := context.Background()
	p.SetDeferredReclaim(time.Hour)
	defer p.StopDeferredReclaim()
	p.reclaim.queued.Store(reclaimMaxQueued)
	if _, err := p.ConvertFile(ctx, "metric"); err != nil {
		t.Fatal(err)
	}
	// A full backlog must not grow: this conversion reclaimed inline.
	assertOriginalsReclaimed(t, p, "metric")
	if m, err := db.Metadata(ctx, "metric"); err != nil || m.FileMigration != store.FileReclaimed {
		t.Fatalf("inline fallback left cleanup pending: %+v %v", m, err)
	}
	if p.DeferredReclaimStats().InlineFallback != 1 {
		t.Fatal("inline fallback not counted")
	}
	if got := p.reclaim.queued.Load(); got != reclaimMaxQueued {
		t.Fatalf("queued = %d", got)
	}
}

func reclaimTestQueue() *deferredReclaim {
	return &deferredReclaim{
		started: time.Now(), dirs: make(map[string]*reclaimDirectoryQueue),
		wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{}),
	}
}

func TestDeferredReclaimDueGroupsDirectoriesAndKeepsFutureJobs(t *testing.T) {
	r := reclaimTestQueue()
	// The delayed directory must not hide already-due work in another one.
	r.append(reclaimJob{name: "future", path: "future/a.wsp", due: int64(time.Hour)})
	for i := 0; i < 2*reclaimBatch+1; i++ {
		r.append(reclaimJob{name: fmt.Sprint(i), path: "hot/a.wsp", due: 0})
	}
	r.append(reclaimJob{name: "later", path: "hot/b.wsp", due: int64(time.Hour)})
	q, jobs := r.due(r.started)
	if q == nil || q.path != "hot" || len(jobs) != reclaimBatch {
		t.Fatalf("first batch: %v %d", q, len(jobs))
	}
	// A running directory cannot be assigned again, even with new arrivals.
	if next, _ := r.due(r.started); next != nil {
		t.Fatal("directory assigned concurrently")
	}
	r.finish(q)
	q, jobs = r.due(r.started)
	if q == nil || len(jobs) != reclaimBatch {
		t.Fatal("full due batch waited for the next tick")
	}
	r.finish(q)
	if next, _ := r.due(r.started); next != nil {
		t.Fatal("partial batch did not accumulate until the next tick")
	}
	q, jobs = r.due(r.started.Add(reclaimTick))
	if q == nil || len(jobs) != 1 || jobs[0].name != fmt.Sprint(2*reclaimBatch) {
		t.Fatalf("second batch: %v %v", q, jobs)
	}
	r.finish(q)
	if next, _ := r.due(r.started); next != nil {
		t.Fatal("future job reclaimed early")
	}
	for range 2 {
		q, jobs = r.due(r.started.Add(time.Hour))
		if q == nil || len(jobs) != 1 {
			t.Fatal("future job lost")
		}
		r.finish(q)
	}
	if len(r.dirs) != 0 || len(r.ready) != 0 {
		t.Fatal("empty directories retained")
	}
}

func TestDeferredReclaimAdmissionReservesCapacity(t *testing.T) {
	r := reclaimTestQueue()
	r.queued.Store(reclaimMaxQueued - 1)
	var admitted atomic.Int64
	var writers sync.WaitGroup
	for i := range 64 {
		writers.Go(func() {
			if r.push(reclaimJob{name: fmt.Sprint(i), path: "hot/a.wsp"}) {
				admitted.Add(1)
			}
		})
	}
	writers.Wait()
	if admitted.Load() != 1 || r.queued.Load() != reclaimMaxQueued {
		t.Fatalf("admitted=%d queued=%d", admitted.Load(), r.queued.Load())
	}
	q, jobs := r.due(time.Now().Add(time.Hour))
	if q == nil || len(jobs) != 1 {
		t.Fatal("reservation lost")
	}
	if r.push(reclaimJob{path: "other/a.wsp"}) {
		t.Fatal("active batch did not count against cap")
	}
}

// Only the durable store completion is stubbed: directory batching, source
// unlinking and syncing use the real temp filesystem. The existing migration
// crash tests exercise completion against the real store.
type reclaimCompletionStore struct {
	IncrementalMetricStore
	completed atomic.Int64
	total     int64
	done      chan struct{}
	complete  func(store.Metadata) error
}

func (s *reclaimCompletionStore) CompleteFileCleanup(_ context.Context, m store.Metadata) error {
	if s.complete != nil {
		if err := s.complete(m); err != nil {
			return err
		}
	}
	if s.completed.Add(1) == s.total && s.done != nil {
		close(s.done)
	}
	return nil
}

func TestDeferredReclaimBusyMetricRetriesWithoutBlockingOtherDirectories(t *testing.T) {
	r := reclaimTestQueue()
	db := &reclaimCompletionStore{}
	p := &Whisper{rootPath: t.TempDir(), incrementalStore: db, reclaim: r}
	busy := reclaimJob{name: "busy", path: filepath.Join(p.rootPath, "busy.wsp"), id: 1, generation: 2}
	if !r.push(busy) {
		t.Fatal("queue rejected job")
	}
	p.converting.Store("busy", struct{}{})
	q, jobs := r.due(time.Now())
	p.reclaimDirectory(r, q.path, jobs)
	r.finish(q)
	if r.queued.Load() != 1 || db.completed.Load() != 0 {
		t.Fatal("busy metric dropped")
	}
	otherDir := filepath.Join(p.rootPath, "other")
	if err := os.Mkdir(otherDir, 0700); err != nil {
		t.Fatal(err)
	}
	if !r.push(reclaimJob{name: "other", path: filepath.Join(otherDir, "a.wsp")}) {
		t.Fatal("queue rejected other directory")
	}
	q, jobs = r.due(time.Now())
	if q == nil || q.path != otherDir {
		t.Fatal("retry blocked other directory")
	}
	p.reclaimDirectory(r, q.path, jobs)
	r.finish(q)
	p.converting.Delete("busy")
	q, jobs = r.due(time.Now().Add(reclaimTick))
	if q == nil || len(jobs) != 1 || jobs[0].id != 1 || jobs[0].generation != 2 {
		t.Fatal("retry lost identity")
	}
	p.reclaimDirectory(r, q.path, jobs)
	r.finish(q)
	stats := p.DeferredReclaimStats()
	if r.queued.Load() != 0 || stats.Completed != 2 || stats.Batches != 3 || stats.Jobs != 3 || stats.BatchNanos == 0 || stats.SyncNanos == 0 || stats.CleanupNanos == 0 {
		t.Fatalf("stats=%+v queued=%d", stats, r.queued.Load())
	}
}

func TestDeferredReclaimSyncFailureLeavesCleanupPending(t *testing.T) {
	p, _, db, _ := incrementalPersisterFixture(t)
	defer db.Close()
	p.SetDeferredReclaim(time.Hour)
	defer p.StopDeferredReclaim()
	ctx := context.Background()
	if _, err := p.ConvertFile(ctx, "metric"); err != nil {
		t.Fatal(err)
	}
	q, jobs := p.reclaim.due(time.Now().Add(2 * time.Hour))
	if q == nil || len(jobs) != 1 {
		t.Fatal("missing deferred job")
	}
	// ENOTDIR makes the sync fail after the real originals were unlinked.
	blocker := filepath.Join(p.rootPath, "blocker")
	if err := os.WriteFile(blocker, nil, 0600); err != nil {
		t.Fatal(err)
	}
	p.reclaimDirectory(p.reclaim, filepath.Join(blocker, "child"), jobs)
	p.reclaim.finish(q)
	assertOriginalsReclaimed(t, p, "metric")
	pending, err := db.PendingFileCleanup(ctx, 10)
	if err != nil || len(pending) != 1 || p.DeferredReclaimStats().Completed != 0 {
		t.Fatalf("completion preceded sync: %+v %v", pending, err)
	}
	if _, err := p.ConvertFile(ctx, "metric"); err != nil {
		t.Fatal(err)
	}
	pending, err = db.PendingFileCleanup(ctx, 10)
	if err != nil || len(pending) != 0 {
		t.Fatalf("cleanup did not resume: %+v %v", pending, err)
	}
}

func TestDeferredReclaimWorkersFollowIOSlots(t *testing.T) {
	for _, slots := range []int{0, 32, 128, 2048} {
		p := &Whisper{}
		p.SetConversionIOSlots(slots)
		p.SetDeferredReclaim(time.Hour)
		want := slots
		if want == 0 {
			want = 4 * runtime.GOMAXPROCS(0)
		}
		want = max(64, min(want, 512))
		got := p.DeferredReclaimStats().Workers
		p.StopDeferredReclaim()
		if got != want {
			t.Fatalf("slots=%d workers=%d want=%d", slots, got, want)
		}
	}
}

func TestDeferredReclaimKeepsUpWith10KProducer(t *testing.T) {
	const count = 20000
	const dirs = 256
	root := t.TempDir()
	jobs := make([]reclaimJob, count)
	var setup sync.WaitGroup
	for dir := range dirs {
		setup.Go(func() {
			path := filepath.Join(root, fmt.Sprintf("d%d", dir))
			if err := os.Mkdir(path, 0700); err != nil {
				t.Error(err)
				return
			}
			for i := dir; i < count; i += dirs {
				name := fmt.Sprintf("d%d.m%d", dir, i)
				path := filepath.Join(path, fmt.Sprintf("m%d.wsp", i))
				// Exercise idempotent cleanup with mostly absent originals. Creating
				// and unlinking 10k inodes is a filesystem benchmark, not a
				// portable scheduler test; the directory benchmark covers it.
				if i == dir {
					if err := os.WriteFile(path, nil, 0600); err != nil {
						t.Error(err)
						return
					}
				}
				jobs[i] = reclaimJob{name: name, path: path, id: uint64(i + 1), generation: 1}
			}
		})
	}
	setup.Wait()
	if t.Failed() {
		return
	}
	db := &reclaimCompletionStore{total: count, done: make(chan struct{})}
	p := &Whisper{rootPath: root, incrementalStore: db}
	p.SetConversionIOSlots(512)
	p.SetDeferredReclaim(time.Millisecond)
	defer p.StopDeferredReclaim()
	started := time.Now()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for i := 0; i < count; i += 500 {
		<-ticker.C
		for _, job := range jobs[i : i+500] {
			if !p.reclaim.push(job) {
				t.Fatal("producer fell back inline")
			}
		}
	}
	produced := time.Since(started)
	select {
	case <-db.done:
	case <-time.After(5 * time.Second):
		t.Fatalf("reclaim fell behind 10k/s producer: completed=%d queued=%d stats=%+v", db.completed.Load(), p.DeferredReclaimQueued(), p.DeferredReclaimStats())
	}
	// Stop joins the last batch so counters and cleared queue are observable.
	r := p.reclaim
	p.StopDeferredReclaim()
	if r.completed.Load() != count || r.queued.Load() != 0 || r.inlineFallback.Load() != 0 {
		t.Fatalf("completed=%d queued=%d fallback=%d", r.completed.Load(), r.queued.Load(), r.inlineFallback.Load())
	}
	for _, job := range jobs {
		if _, err := os.Lstat(job.path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("original not reclaimed: %s %v", job.path, err)
		}
	}
	t.Logf("producer: %d jobs in %s; drain: %s; batches=%d jobs/batch=%.1f batch-latency=%s unlink-worker-time=%s sync-worker-time=%s", count, produced, time.Since(started), r.batches.Load(), float64(r.jobs.Load())/float64(r.batches.Load()), time.Duration(r.batchNanos.Load()/r.batches.Load()), time.Duration(r.unlinkNanos.Load()), time.Duration(r.syncNanos.Load()))
}

func TestDeferredReclaimSlowDirectoryDoesNotStallOtherWorkers(t *testing.T) {
	root := t.TempDir()
	r := reclaimTestQueue()
	r.workers = 2
	p := &Whisper{rootPath: root, reclaim: r}
	entered, release := make(chan struct{}), make(chan struct{})
	fast := make(chan string, 2)
	p.incrementalStore = &reclaimCompletionStore{complete: func(m store.Metadata) error {
		if m.Name == "slow" {
			close(entered)
			<-release
		} else {
			fast <- m.Name
		}
		return nil
	}}
	for _, name := range []string{"slow", "fast", "last"} {
		dir := filepath.Join(root, name)
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if !r.push(reclaimJob{name: name, path: filepath.Join(dir, "m.wsp")}) {
			t.Fatal("queue full")
		}
	}
	go p.runDeferredReclaim(r)
	defer p.StopDeferredReclaim()
	defer close(release)
	r.wake <- struct{}{}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("slow directory never started")
	}
	for range 2 {
		select {
		case <-fast:
		case <-time.After(3 * time.Second):
			t.Fatal("one directory stalled unrelated batches")
		}
	}
}
