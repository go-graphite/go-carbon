//go:build darwin || linux

package main

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
)

func pendingKnown(metric string) bool {
	_, host, _ := strings.Cut(metric, ".host")
	id, _, _ := strings.Cut(host, ".")
	i, err := strconv.Atoi(id)
	check(err)
	return i%1470 != 0 // about 680 new read-index names per million
}

// Build the predecessor's actual compact cache dump and captured input file
// in a separate process. Its cost is not restart CPU in the successor.
func buildPending(dir string, metrics, saved int) {
	started, used := time.Now(), cpu()
	checkpoint := filepath.Join(dir, "pending-cache")
	check(os.Mkdir(checkpoint, 0700))
	previous := cache.New()
	previous.SetMaxSize(10_000_000_000)
	data := make([]points.Point, saved-1)
	for i := range data {
		data[i] = points.Point{Timestamp: base + 30 + int64(i), Value: float64(i)}
	}
	for i := range metrics {
		previous.Add(&points.Points{Metric: name(i), Data: data})
	}
	builder := recovery.NewConcurrentBuilder(func() func(string) bool { return pendingKnown }, 8)
	builder.Reserve(metrics)
	dump, err := recovery.NewWriterFormat(filepath.Join(checkpoint, recovery.CacheName(recovery.FormatBlocks, "1.2.bin")), 0, 1<<20, builder, recovery.FormatBlocks)
	check(err)
	check(dump.WriteSegments(cache.ShardCount, 8, func(i int, out *recovery.Segment) error { return previous.DumpShardsCompactTo(i, i+1, out) }))
	cacheFile, err := dump.Close()
	check(err)
	previous.Stop()
	wal, err := recovery.NewWriterFormat(filepath.Join(checkpoint, recovery.InputName(recovery.FormatCompact, "1.2.bin")), 1, recovery.InputFileBuffer, builder, recovery.FormatCompact)
	check(err)
	for i := range metrics {
		check(wal.WritePoints(points.OnePoint(name(i), 10000, base+30+int64(saved-2))))
	}
	inputFile, err := wal.Close()
	check(err)
	indexFile, err := recovery.WriteBlocksIndex(checkpoint, builder)
	check(err)
	check(recovery.Publish(checkpoint, dir, cacheFile, inputFile, indexFile, "bench-read-index"))
	emit(map[string]any{"mode": "pending-fixture", "metrics": metrics, "points": int64(metrics) * int64(saved), "bytes": cacheFile.Size + inputFile.Size + indexFile.Size, "wall": time.Since(started).Seconds(), "cpu": cpu() - used})
}

type pendingBacklog struct {
	core                 *cache.Cache
	log                  *inputlog.Log
	bundle               *recovery.Bundle
	stop                 chan struct{}
	done                 chan struct{}
	once                 sync.Once
	metrics, saved, live int
}

func openPending(core *cache.Cache, db *store.Store, config backlogConfig, metrics int, window time.Duration) *pendingBacklog {
	started, used := time.Now(), cpu()
	bundle, err := recovery.OpenBundle(filepath.Join(config.pendingDir, "pending-cache"), config.pendingDir)
	check(err)
	check(core.AttachPendingRecoveryWithPreparedNames(bundle))
	log, err := inputlog.Open(filepath.Join(config.pendingDir, "ingest-log"), inputlog.Options{SyncInterval: time.Second, MaxAge: 2 * window})
	check(err)
	core.EnableInputLog()
	_, err = log.Replay(core.AddRecovered)
	check(err)
	log.Start(core, cache.ShardCount)
	stats, err := db.LoadMetadataCheckpoint(context.Background(), nil)
	check(err)
	emit(map[string]any{"mode": "pending-open", "points": bundle.Points(), "metrics": bundle.Metrics(), "new_names": bundle.NewNameCount(), "bytes": bundle.Bytes(), "checkpoint_valid": stats.Valid, "wall": time.Since(started).Seconds(), "cpu": cpu() - used})
	return &pendingBacklog{core: core, log: log, bundle: bundle, stop: make(chan struct{}), done: make(chan struct{}), metrics: metrics, saved: config.savedPoints, live: config.gatePoints}
}

// Workers are already waiting on an empty queue before held claims arrive.
// Live input is paced in 4096-metric chunks, including while the gate waits.
func (r *pendingBacklog) start(gate time.Duration) {
	go func() {
		defer close(r.done)
		time.Sleep(150 * time.Millisecond)
		stamp := base + 30 + int64(r.saved-2)
		r.core.AddPoint([]byte(name(1)), 20000, stamp)
		time.Sleep(150 * time.Millisecond)
		loaded := make(chan struct{})
		go func() {
			defer close(loaded)
			if err := r.core.RecoverPending(r.stop, 0); err != nil {
				return
			}
			for i := 0; i < r.metrics; i += 4096 {
				r.core.AddRecovered(points.OnePoint(name(i), 20000, stamp))
			}
			for i := 1; i < r.metrics; i += 4096 {
				r.core.AddRestored(points.OnePoint(name(i), 20001, stamp))
			}
		}()
		started := time.Now()
		duration := gate * 9 / 10
		for round := 0; round < 3; round++ {
			data := make([]points.Point, r.live/3)
			for j := range data {
				k := round*len(data) + j
				data[j] = points.Point{Timestamp: stamp + 1 + int64(k), Value: float64(30000 + k)}
			}
			for start := 0; start < r.metrics; start += 4096 {
				due := started.Add(time.Duration((float64(round) + float64(start)/float64(r.metrics)) / 3 * float64(duration)))
				if delay := time.Until(due); delay > 0 {
					timer := time.NewTimer(delay)
					select {
					case <-timer.C:
					case <-r.stop:
						timer.Stop()
						<-loaded
						return
					}
				}
				for i := start; i < min(start+4096, r.metrics); i++ {
					r.core.Add(&points.Points{Metric: name(i), Data: data})
				}
			}
		}
		<-loaded
		check(r.log.Sync())
		emit(map[string]any{"mode": "pending-intake-done", "points": int64(r.metrics) * int64(r.live), "wall": time.Since(started).Seconds(), "cache_points": r.core.Size()})
	}()
}

func (r *pendingBacklog) close() {
	r.once.Do(func() { close(r.stop); <-r.done; check(r.log.Close()); check(r.bundle.Close()) })
}

func (r *pendingBacklog) verify(db *store.Store) {
	if r.core.PendingOutstanding() != 0 {
		panic("confirmed cache retained pending source")
	}
	check(r.core.FinishPendingRecovery())
	for i := range min(r.metrics, 16) {
		snapshot, err := db.Snapshot(context.Background(), name(i))
		check(err)
		found := 0
		last := base + 30 + int64(r.saved-2)
		for _, p := range snapshot.Archives[0].Points {
			if p.Timestamp < base+30 || p.Timestamp > last+int64(r.live) {
				continue
			}
			want := float64(p.Timestamp - base - 30)
			if p.Timestamp == last {
				want = 10000
				if i%4096 == 0 {
					want = 20000
				}
				if i%4096 == 1 {
					want = 20001
				}
			}
			if p.Timestamp > last {
				want = 30000 + float64(p.Timestamp-last-1)
			}
			if p.Value != want {
				panic("pending restore changed value/order")
			}
			found++
		}
		if found != r.saved-1+r.live {
			panic("pending restore points lost")
		}
	}
}
