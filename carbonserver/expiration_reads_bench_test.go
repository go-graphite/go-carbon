package carbonserver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgryski/go-expirecache"
	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"go.uber.org/zap"
)

// Opt in with an independent copy of TestPrepareExpirationScaleStore's fixture.
// HTTP handlers run in process; a ticker admits reads independently of latency.
func BenchmarkExpirationReadLatency(b *testing.B) {
	directory := os.Getenv("EXPIRATION_READ_DIR")
	if directory == "" {
		b.Skip("set EXPIRATION_READ_DIR to a disposable >=1M-metric fixture")
	}
	const now = 1790000100
	ctx := context.Background()
	db, err := store.Open(directory, store.Options{ExactBlocks: true, CacheSize: 32 << 20, MetadataCacheSize: 32 << 20, MemTableSize: 256 << 20, CompactionWorkers: 2, SyncInterval: time.Hour, Now: func() time.Time { return time.Unix(now, 0) }})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	high, err := db.CatalogHighWatermark(ctx)
	if err != nil || high < 1_000_000 {
		b.Fatalf("need >=1M metrics: %d %v", high, err)
	}
	names := make([]string, 256)
	for i := range names {
		names[i] = fmt.Sprintf("hot.group%02d.metric%02d", i/4, i%4)
		_, err := db.Create(ctx, store.MetricConfig{Name: names[i], Retentions: []store.Retention{{Step: 60, Count: 1440}}, AggregationMethod: store.Average})
		if err != nil {
			b.Fatal(err)
		}
		if err := db.UpdateMany(ctx, names[i], []store.Point{{Timestamp: now - 60, Value: 42}}); err != nil {
			b.Fatal(err)
		}
	}
	// Settle the hot data too: otherwise deletion's first flush changes reads
	// from memtable hits into SST reads halfway through the comparison.
	if err := db.Compact(); err != nil {
		b.Fatal(err)
	}
	l := NewCarbonserverListener(cache.New().Get)
	l.logger, l.accessLogger = zap.NewNop(), zap.NewNop()
	l.SetWhisperData(b.TempDir())
	l.SetMetricStore(db)
	l.SetTrieIndex(true)
	l.fileListCache = filepath.Join(b.TempDir(), "files.gz")
	l.fileListCacheVersion = FLCVersion2
	l.SetMaxGlobs(100)
	l.SetMaxMetricsGlobbed(100)
	l.maxMetricsRendered = 100
	l.queryCache = expireCache{ec: expirecache.New(16 << 20)}
	l.SetQueryCacheEnabled(true)
	l.SetFindCacheEnabled(true)
	l.SetGlobCacheEnabled(true)
	if err := l.RefreshMetricStoreIndex(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = l.CurrentFileIndex().trieIdx.snapshot.close() }()
	request := func(i int, cold bool) (string, error) {
		name := names[(i/20)%len(names)]
		kind := "exact"
		handler := l.renderHandler
		from, until := now-3600, now
		if cold {
			until -= i % 60000 // unique key, constant response length
			from = until - 3600
		}
		url := fmt.Sprintf("/render/?format=json&target=%s&from=%d&until=%d", name, from, until)
		switch i % 20 {
		case 0:
			kind, handler = "info", l.infoHandler
			url = "/info/?format=json&target=" + name
		case 1:
			kind, handler = "find", l.findHandler
			url = fmt.Sprintf("/metrics/find/?format=json&query=hot.group%02d.*", (i/20)%64)
		default:
			if i%2 == 0 {
				kind = "wildcard"
				url = fmt.Sprintf("/render/?format=json&target=hot.group%02d.*&from=%d&until=%d", (i/20)%64, from, until)
			}
			if cold {
				kind = "miss-" + kind
			}
		}
		response := httptest.NewRecorder()
		handler(response, httptest.NewRequest(http.MethodGet, url, http.NoBody))
		if response.Code != http.StatusOK {
			return kind, fmt.Errorf("%s: status %d: %s", url, response.Code, response.Body.String())
		}
		return kind, nil
	}
	for i := range 20 * len(names) {
		if _, err := request(i, false); err != nil {
			b.Fatal(err)
		}
	}
	duration := 8 * time.Second
	if seconds, _ := strconv.Atoi(os.Getenv("EXPIRATION_READ_SECONDS")); seconds > 0 {
		duration = time.Duration(seconds) * time.Second
	}
	type sample struct {
		kind  string
		delay time.Duration
		err   error
	}
	next := 0
	measure := func(phase string, stop <-chan struct{}) {
		jobs := make(chan int, 4096)
		samples := make(chan sample, 128)
		collected := make(chan struct{})
		byKind := make(map[string][]time.Duration)
		var readErr error
		go func() {
			defer close(collected)
			for s := range samples {
				if s.err != nil {
					readErr = s.err
				}
				byKind[s.kind] = append(byKind[s.kind], s.delay)
				byKind["all"] = append(byKind["all"], s.delay)
			}
		}()
		var workers sync.WaitGroup
		for range 16 {
			workers.Go(func() {
				for i := range jobs {
					started := time.Now()
					kind, err := request(i, i%20 == 2 || i%20 == 3)
					samples <- sample{kind, time.Since(started), err}
				}
			})
		}
		hits, misses := atomic.LoadUint64(&l.metrics.QueryCacheHit), atomic.LoadUint64(&l.metrics.QueryCacheMiss)
		ticker := time.NewTicker(time.Second / 600)
		defer ticker.Stop()
		started := time.Now()
	loop:
		for {
			select {
			case <-stop:
				break loop
			case <-ticker.C:
				jobs <- next
				next++
			}
		}
		close(jobs)
		workers.Wait()
		close(samples)
		<-collected
		if readErr != nil {
			b.Fatal(readErr)
		}
		for _, kind := range []string{"all", "exact", "wildcard", "miss-exact", "miss-wildcard", "find", "info"} {
			delays := byKind[kind]
			slices.Sort(delays)
			if len(delays) != 0 {
				b.Logf("%s %s n=%d p50=%s p99=%s max=%s", phase, kind, len(delays), delays[len(delays)/2], delays[(len(delays)-1)*99/100], delays[len(delays)-1])
			}
		}
		hits, misses = atomic.LoadUint64(&l.metrics.QueryCacheHit)-hits, atomic.LoadUint64(&l.metrics.QueryCacheMiss)-misses
		b.Logf("%s seconds=%.3f reads/s=%.1f render-hit=%.2f%%", phase, time.Since(started).Seconds(), float64(len(byKind["all"]))/time.Since(started).Seconds(), 100*float64(hits)/float64(hits+misses))
	}
	b.ResetTimer()
	timed := func() <-chan struct{} {
		done := make(chan struct{})
		time.AfterFunc(duration, func() { close(done) })
		return done
	}
	measure("off", timed())
	deleteCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	finished := make(chan struct{})
	refreshed := make(chan struct{})
	var dirty atomic.Bool
	var refreshErr error
	go func() {
		defer close(refreshed)
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-finished:
				return
			case <-ticker.C:
				if dirty.Swap(false) {
					started := time.Now()
					if refreshErr = l.RefreshMetricStoreIndex(); refreshErr != nil {
						return
					}
					b.Logf("index refresh=%s", time.Since(started))
				}
			}
		}
	}()
	var deleteErr error
	var deleted atomic.Int64
	var deleteWall time.Duration
	go func() {
		defer close(finished)
		started := time.Now()
		defer func() { deleteWall = time.Since(started) }()
		jobs := make(chan []store.Metadata, 8)
		var workers sync.WaitGroup
		var failures sync.Once
		fail := func(err error) {
			if !errors.Is(err, context.DeadlineExceeded) {
				failures.Do(func() { deleteErr = err })
			}
		}
		for range 8 {
			workers.Go(func() {
				for page := range jobs {
					results, err := db.DeleteBatchIfUnchanged(deleteCtx, page)
					if err != nil {
						fail(err)
						continue
					}
					for _, err := range results {
						if err != nil {
							fail(err)
						} else {
							deleted.Add(1)
						}
					}
					dirty.Store(true)
				}
			})
		}
		limit, _ := strconv.Atoi(os.Getenv("EXPIRATION_READ_DELETES"))
		if limit == 0 {
			limit = int(min(high, 1500000))
		}
		after := ""
		for left := limit; left > 0; {
			page, err := db.ListPage(deleteCtx, "aggregations.", after, min(512, left))
			if err != nil {
				fail(err)
				break
			}
			if len(page) == 0 {
				fail(errors.New("expiration fixture exhausted"))
				break
			}
			after = page[len(page)-1].Name
			jobs <- page
			left -= len(page)
		}
		close(jobs)
		workers.Wait()
	}()
	measure("on", finished)
	<-refreshed
	if deleteErr != nil {
		b.Fatal(deleteErr)
	}
	if refreshErr != nil {
		b.Fatal(refreshErr)
	}
	b.Logf("deleted=%d seconds=%.3f deletes/s=%.1f", deleted.Load(), deleteWall.Seconds(), float64(deleted.Load())/deleteWall.Seconds())
	measure("recovery", timed())
}
