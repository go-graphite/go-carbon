package carbon

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime/pprof"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/carbonserver"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// Opt in with an independent copy of TestPrepareExpirationScaleStore's fixture.
// Real HTTP reads and the production expirer run against the same Pebble store.
// Every live response is compared byte-for-byte with its pre-delete oracle.
func BenchmarkExpirationHTTPReadLatency(b *testing.B) {
	directory := os.Getenv("EXPIRATION_READ_DIR")
	if directory == "" {
		b.Skip("set EXPIRATION_READ_DIR to a disposable >=1M-metric fixture")
	}
	if b.N != 1 {
		b.Fatal("expiration consumes its fixture; use -benchtime=1x")
	}
	const now = 1790200100
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
	l := carbonserver.NewCarbonserverListener(cache.New().Get)
	l.SetWhisperData(b.TempDir())
	l.SetMetricStore(db)
	l.SetTrieIndex(true)
	l.SetTrigramIndex(false)
	l.SetFileListCache(filepath.Join(b.TempDir(), "files.gz"))
	l.SetFileListCacheVersion(int(carbonserver.FLCVersion2))
	l.SetMaxGlobs(100)
	l.SetMaxMetricsGlobbed(100)
	l.SetMaxMetricsRendered(100)
	l.SetQueryCacheEnabled(os.Getenv("EXPIRATION_READ_UNCACHED") != "1")
	l.SetQueryCacheSizeMB(16)
	l.SetFindCacheEnabled(true)
	l.SetGlobCacheEnabled(true)
	l.SetScanFrequency(time.Hour)
	if err := l.Listen("127.0.0.1:0"); err != nil {
		b.Fatal(err)
	}
	defer l.Stop()
	deadline := time.Now().Add(time.Minute)
	for l.CurrentFileIndex() == nil {
		if time.Now().After(deadline) {
			b.Fatal("initial index timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}
	client := &http.Client{Timeout: 30 * time.Second}
	baseURL := "http://" + l.HTTPListener().Addr().String()
	oracle := make(map[string][32]byte)
	oracleBody := make(map[string]string)
	var oracleMu sync.Mutex
	checking := false
	request := func(i int, cold bool) (string, error) {
		i %= 5120
		name := names[(i/20)%len(names)]
		kind := "exact"
		from, until := now-3600, now
		if cold {
			until -= i % 59 // keep the live point within every range
			from = until - 3600
		}
		url := fmt.Sprintf("/render/?format=json&target=%s&from=%d&until=%d", name, from, until)
		switch i % 20 {
		case 0:
			kind = "info"
			url = "/info/?format=json&target=" + name
		case 1:
			kind = "find"
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
		response, err := client.Get(baseURL + url)
		if err != nil {
			return kind, err
		}
		body, readErr := io.ReadAll(response.Body)
		closeErr := response.Body.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			return kind, err
		}
		if response.StatusCode != http.StatusOK {
			return kind, fmt.Errorf("%s status %d: %s", url, response.StatusCode, body)
		}
		// Parallel render workers may return series in any order. Sort whole
		// series records, preserving every timestamp, absence flag and value byte.
		var envelope map[string]json.RawMessage
		recordsJSON := body
		if len(body) > 0 && body[0] == '{' {
			if err := json.Unmarshal(body, &envelope); err != nil {
				return kind, err
			}
			recordsJSON = envelope["metrics"]
		}
		if len(recordsJSON) > 0 && recordsJSON[0] == '[' {
			var records []json.RawMessage
			if err := json.Unmarshal(recordsJSON, &records); err != nil {
				return kind, err
			}
			slices.SortFunc(records, func(a, b json.RawMessage) int { return bytes.Compare(a, b) })
			body, err = json.Marshal(records)
			if envelope != nil {
				envelope["metrics"] = body
				body, err = json.Marshal(envelope)
			}
			if err != nil {
				return kind, err
			}
		}
		digest := sha256.Sum256(body)
		oracleMu.Lock()
		defer oracleMu.Unlock()
		if checking {
			if digest != oracle[url] {
				return kind, fmt.Errorf("live response changed: %s\nbefore=%s\nafter=%s", url, oracleBody[url], body)
			}
		} else {
			oracle[url] = digest
			oracleBody[url] = string(body)
		}

		return kind, nil
	}
	for i := range 20 * len(names) {
		for _, cold := range []bool{false, true} {
			if _, err := request(i, cold); err != nil {
				b.Fatal(err)
			}
		}
	}
	checking = true
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
	p99s := make(map[string]time.Duration)
	measure := func(phase string, stop <-chan struct{}) {
		type arrival struct {
			n  int
			at time.Time
		}
		jobs := make(chan arrival, 4096)
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
				pprof.Do(ctx, pprof.Labels("expiration-phase", phase, "work", "read"), func(context.Context) {
					for j := range jobs {
						kind, err := request(j.n, j.n%20 == 2 || j.n%20 == 3)
						samples <- sample{kind, time.Since(j.at), err}
					}
				})
			})
		}
		ticker := time.NewTicker(time.Second / 600)
		defer ticker.Stop()
		fsBefore := db.Stats().FS
		started := time.Now()
		nextArrival := started.Add(time.Second / 600)
	loop:
		for {
			select {
			case <-stop:
				break loop
			case <-ticker.C:
				// Retain intended arrival times, including ticks lost to GC,
				// descheduling and queued requests (no coordinated omission).
				for due := time.Now(); !nextArrival.After(due); nextArrival = nextArrival.Add(time.Second / 600) {
					jobs <- arrival{next, nextArrival}
					next++
				}
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
				if kind == "all" {
					p99s[phase] = delays[(len(delays)-1)*99/100]
				}
				b.Logf("%s %s n=%d p50=%s p99=%s max=%s", phase, kind, len(delays), delays[len(delays)/2], delays[(len(delays)-1)*99/100], delays[len(delays)-1])
			}
		}
		b.Logf("%s seconds=%.3f reads/s=%.1f", phase, time.Since(started).Seconds(), float64(len(byKind["all"]))/time.Since(started).Seconds())
		fsAfter := db.Stats().FS
		b.Logf("%s table-reads=%d table-read-bytes=%d", phase, fsAfter.Ops[store.FSRead][store.FSTable]-fsBefore.Ops[store.FSRead][store.FSTable], fsAfter.Bytes[store.FSRead][store.FSTable]-fsBefore.Bytes[store.FSRead][store.FSTable])
	}
	b.ResetTimer()
	timed := func() <-chan struct{} {
		done := make(chan struct{})
		time.AfterFunc(duration, func() { close(done) })
		return done
	}
	measure("off", timed())
	deleteCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	finished := make(chan struct{})
	refreshed := make(chan struct{})
	var dirty atomic.Bool
	var refreshErr error
	go func() {
		defer close(refreshed)
		refreshInterval := 30 * time.Second
		if seconds, _ := strconv.Atoi(os.Getenv("EXPIRATION_READ_REFRESH_SECONDS")); seconds > 0 {
			refreshInterval = time.Duration(seconds) * time.Second
		}
		ticker := time.NewTicker(refreshInterval)
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
	var deleteWall time.Duration
	stats := &expirationStats{}
	limit, _ := strconv.Atoi(os.Getenv("EXPIRATION_READ_DELETES"))
	if limit == 0 {
		limit = 500000
	}
	rate, _ := strconv.Atoi(os.Getenv("EXPIRATION_READ_DELETE_RATE"))
	if rate == 0 {
		rate = 50000
	}
	workers, _ := strconv.Atoi(os.Getenv("EXPIRATION_READ_DELETE_WORKERS"))
	if workers == 0 {
		workers = 4
	}
	r := &metricExpirer{db: db, now: func() time.Time { return time.Unix(now, 0) }, expiration: 24 * time.Hour,
		rate: 100000, deleteRate: rate, deleteWorkers: workers, maxDeletes: limit, maxL0: 6, maxDebt: 32 << 30, stats: stats,
		notify: func() { dirty.Store(true) }}
	go func() {
		defer close(finished)
		pprof.Do(deleteCtx, pprof.Labels("work", "delete"), func(ctx context.Context) {
			started := time.Now()
			deleteErr = r.sweep(ctx)
			deleteWall = time.Since(started)
		})
	}()
	measure("on", finished)
	<-refreshed
	if deleteErr != nil {
		b.Fatal(deleteErr)
	}
	if refreshErr != nil {
		b.Fatal(refreshErr)
	}
	if stats.deleted.Load() != uint64(limit) {
		b.Fatalf("deleted=%d want=%d guarded=%d", stats.deleted.Load(), limit, stats.guarded.Load())
	}
	b.ReportMetric(float64(p99s["on"])/float64(p99s["off"]), "p99-ratio")
	b.ReportMetric(float64(stats.deleted.Load())/deleteWall.Seconds(), "deletes/s")
	b.Logf("deleted=%d seconds=%.3f deletes/s=%.1f", stats.deleted.Load(), deleteWall.Seconds(), float64(stats.deleted.Load())/deleteWall.Seconds())
	measure("recovery", timed())
}
