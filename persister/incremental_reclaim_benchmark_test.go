package persister

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
	"unsafe"
)

func BenchmarkDeferredReclaimDue(b *testing.B) {
	for _, queued := range []int{8192, 1 << 20} {
		for _, ready := range []bool{false, true} {
			b.Run(fmt.Sprintf("queued=%d/ready=%t", queued, ready), func(b *testing.B) {
				now := time.Now()
				r := &deferredReclaim{started: now}
				deadline := int64(time.Hour)
				if ready {
					deadline = -int64(time.Hour)
				}
				jobs := make([]reclaimJob, queued)
				for i := range jobs {
					jobs[i].due = deadline
				}
				q := &reclaimDirectoryQueue{path: "directory", jobs: jobs}
				r.ready = reclaimDirectories{q}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					q.jobs = jobs
					r.ready = append(r.ready[:0], q)
					_, _ = r.due(now)
				}
				b.ReportMetric(float64(unsafe.Sizeof(reclaimJob{})), "job-bytes")
			})
		}
	}
}

func BenchmarkDeferredReclaimConverting(b *testing.B) {
	var converting sync.Map
	names := make([]string, 8192)
	for i := range names {
		names[i] = fmt.Sprintf("metric.%d", i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		name := names[i%len(names)]
		if _, loaded := converting.LoadOrStore(name, struct{}{}); loaded {
			b.Fatal("busy")
		}
		converting.Delete(name)
	}
}

func BenchmarkDeferredReclaimDirectory(b *testing.B) {
	const count = 1024
	for _, tc := range []struct{ workers, dirs int }{{64, 1}, {64, 64}, {64, 1024}, {512, 1024}} {
		b.Run(fmt.Sprintf("workers=%d/dirs=%d", tc.workers, tc.dirs), func(b *testing.B) {
			dirs := tc.dirs
			root := b.TempDir()
			p := &Whisper{rootPath: root}
			jobs := make([]reclaimJob, count)
			for i := range jobs {
				dir := filepath.Join(root, fmt.Sprintf("d%04d", i%dirs))
				jobs[i] = reclaimJob{name: fmt.Sprintf("d%d.m%d", i%dirs, i), path: filepath.Join(dir, fmt.Sprintf("m%d.wsp", i))}
			}
			b.ReportAllocs()
			b.ResetTimer()
			var batches, unlinkNanos, syncNanos uint64
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				for dir := 0; dir < dirs; dir++ {
					if err := os.MkdirAll(filepath.Join(root, fmt.Sprintf("d%04d", dir)), 0700); err != nil {
						b.Fatal(err)
					}
				}
				for _, job := range jobs {
					f, err := os.Create(job.path)
					if err != nil {
						b.Fatal(err)
					}
					if err := f.Close(); err != nil {
						b.Fatal(err)
					}
				}
				r := reclaimTestQueue()
				r.workers = tc.workers
				db := &reclaimCompletionStore{total: count, done: make(chan struct{})}
				p.incrementalStore = db
				r.queued.Store(count)
				b.StartTimer()
				for _, job := range jobs {
					r.append(job)
				}
				go p.runDeferredReclaim(r)
				r.wake <- struct{}{}
				<-db.done
				close(r.stop)
				<-r.done
				batches += r.batches.Load()
				unlinkNanos += r.unlinkNanos.Load()
				syncNanos += r.syncNanos.Load()
			}
			b.ReportMetric(float64(count*b.N)/b.Elapsed().Seconds(), "jobs/s")
			b.ReportMetric(float64(count*b.N)/float64(batches), "jobs/batch")
			b.ReportMetric(float64(unlinkNanos)/float64(count*b.N), "unlink-ns/job")
			b.ReportMetric(float64(syncNanos)/float64(batches), "sync-ns/batch")
		})
	}
}
