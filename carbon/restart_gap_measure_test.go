package carbon

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lomik/zapwriter"

	"github.com/go-graphite/go-carbon/points"
)

// TestMeasureRestartGap replays a dump restart of an incrementally migrating
// shared store with a compact checkpoint, in one process: the persister
// creates the store, an identical cache is seeded after it stops, capped
// live input continues throughout the dump, and a successor starts on the
// same directories. It reports the time from input closing to the successor's
// receivers starting, without the
// process exit and exec a real restart adds, and the stop and start steps in
// that gap. Run it explicitly, with metrics, points per metric and seconds
// of input before the stop:
//
//	CARBON_MEASURE_GAP=2000000,40,10 go test -run TestMeasureRestartGap -v ./carbon/
func TestMeasureRestartGap(t *testing.T) {
	spec := strings.Split(os.Getenv("CARBON_MEASURE_GAP"), ",")
	if len(spec) != 3 {
		t.Skip("set CARBON_MEASURE_GAP=metrics,points,seconds to run")
	}
	metrics, _ := strconv.Atoi(spec[0])
	perMetric, _ := strconv.Atoi(spec[1])
	seconds, _ := strconv.Atoi(spec[2])
	if metrics <= 0 || perMetric <= 0 || seconds <= 0 {
		t.Fatal("CARBON_MEASURE_GAP requires positive metrics,points,seconds")
	}
	defer zapwriter.Test()()
	path, cfg := incrementalAppFixture(t)
	cfg.Dump.Enabled, cfg.Dump.Path, cfg.Dump.Format = true, filepath.Join(t.TempDir(), "dump"), "compact"
	if err := os.MkdirAll(cfg.Dump.Path, 0755); err != nil {
		t.Fatal(err)
	}
	cfg.Cache.MaxSize = 0
	cfg.Whisper.Workers = 8
	cfg.Whisper.MaxUpdatesPerSecond = 0
	cfg.Whisper.StoreCacheSize = 2 << 30
	cfg.Whisper.StoreMemTableSize = 256 << 20
	cfg.Whisper.StoreMemTableMaxUnflushed = 4 << 30
	cfg.Common.MetricInterval = &Duration{time.Hour}
	if err := os.WriteFile(cfg.Whisper.SchemasFilename, []byte("[default]\npattern = .*\nretentions = 1s:2h\n"), 0600); err != nil {
		t.Fatal(err)
	}
	writeSharedConfig(t, path, cfg)
	app := startIncrementalApp(t, path)
	// The first scan writes a file list without delaying publication for a
	// snapshot. A second start maps that saved list, as a steady-state host
	// does; an hourly scan interval would otherwise leave this fixture on its
	// initial heap index throughout the measurement.
	if !app.Carbonserver.HasMappedIndex() {
		app.Stop()
		app = startIncrementalApp(t, path)
	}

	name := func(i int) string { return fmt.Sprintf("dc.cluster%03d.host%05d.metric_%d", i%300, i%40000, i) }
	base := time.Now().Unix() - 3600
	feed := func(point func(i int) *points.Points) {
		var wg sync.WaitGroup
		for w := range 8 {
			wg.Go(func() {
				for i := w; i < metrics; i += 8 {
					app.Cache.Add(point(i))
				}
			})
		}
		wg.Wait()
	}
	// Create every metric, as a store in steady state holds them.
	started := time.Now()
	feed(func(i int) *points.Points { return points.OnePoint(name(i), 1, base) })
	for !app.Cache.IsEmpty() {
		time.Sleep(100 * time.Millisecond)
	}
	waitMigration(t, func() bool { return app.Carbonserver.HasMappedIndex() })
	t.Logf("created %d metrics in %v", metrics, time.Since(started).Round(time.Millisecond))

	// Hold the same starting cache in both versions. Persistence has already
	// written the same store; it is stopped before filling this cache so a
	// faster candidate cannot change the amount of work at the boundary.
	app.Lock()
	app.Persister.Stop()
	app.Persister = nil
	app.Unlock()
	batch := func(i, round int) *points.Points {
		p := &points.Points{Metric: name(i), Data: make([]points.Point, perMetric)}
		for j := range p.Data {
			p.Data[j] = points.Point{Value: float64(j), Timestamp: base + 1 + int64(round*perMetric+j)}
		}
		return p
	}
	feed(func(i int) *points.Points { return batch(i, 0) })
	t.Logf("seeded cache with %d points", app.Cache.Size())

	// Cap live input at 1M points/s, in 10ms batches. An unlimited producer
	// makes the cache and input file depend on the candidate's throughput,
	// confounding the scheduling change with different amounts of recovery.
	stopFeed := make(chan struct{})
	var inputClosed time.Time
	fed := make(chan int64, 1)
	go func() {
		var sent int64
		defer func() { fed <- sent }()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopFeed:
				return
			case <-ticker.C:
				for range max(1, 1_000_000/(100*perMetric)) {
					select {
					case <-stopFeed:
						return
					default:
					}
					app.Cache.Add(batch(int(sent%int64(metrics)), 1+int(sent/int64(metrics))))
					sent++
				}
			}
		}
	}()
	var sent int64
	app.FlushTraces = func() { // input closes: no point may follow
		close(stopFeed)
		sent = <-fed
		inputClosed = time.Now()
	}
	time.Sleep(time.Duration(seconds) * time.Second)
	cacheAtStop, memtables := app.Cache.Size(), app.MetricStore.Stats().MemTableBytes
	stopStarted := time.Now()
	if err := app.DumpStop(); err != nil {
		t.Fatal(err)
	}
	app.FlushTraces = nil
	stopReturned := time.Now()
	app.ReleaseForHandoff()
	released := time.Now()
	app.Stop() // memory only in a real restart: the process exits
	exited := time.Since(released)

	next := New(path)
	if err := next.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	startBegan := time.Now()
	if err := next.Start(); err != nil {
		t.Fatal(err)
	}
	ready := time.Now()
	defer next.Stop()
	logs := zapwriter.TestString()
	field := func(message, key string) string {
		for _, line := range strings.Split(logs, "\n") {
			if !strings.Contains(line, message) {
				continue
			}
			if m := regexp.MustCompile(`"` + key + `": ?([^,}]+)`).FindStringSubmatch(line); m != nil {
				return m[1]
			}
		}
		return "-"
	}
	gap := ready.Sub(inputClosed) - exited
	t.Logf("input sent %d batches; at the stop the cache held %d points and the store %d MB of memtables", sent, cacheAtStop, memtables>>20)
	t.Logf("stop: %v to input closed, %v input closed to stop returned, %v release",
		inputClosed.Sub(stopStarted).Round(time.Millisecond), stopReturned.Sub(inputClosed).Round(time.Millisecond), released.Sub(stopReturned).Round(time.Millisecond))
	t.Logf("cache dump: %s bytes in %s s; index write %s s",
		field("cache dump finished", "bytes"), field("cache dump finished", "runtime"), field("compact checkpoint index written", "runtime"))
	t.Logf("flush with input active: %s s; flush with input closed: %s s (flushes %s)",
		field("shared store flushed with input active", "runtime"), field("shared store flushed with reads available", "runtime"), field("shared store flushed with reads available", "flushes"))
	t.Logf("store close: membership %s s, pebble %s s; background stop with input active %s s",
		field("shared store closed", "membership"), field("shared store closed", "pebble"), field("background work stopped", "runtime"))
	t.Logf("successor: store open %s s; checkpoint open %s s (verified in %s s, from %s s before it was needed), index wait %s s; start %v",
		field("shared store opened", "pebble"), field("serving reads from pending checkpoint", "checkpoint_open_time"),
		field("serving reads from pending checkpoint", "checkpoint_verify_time"), field("serving reads from pending checkpoint", "checkpoint_opened_early"),
		field("serving reads from pending checkpoint", "index_wait_time"), ready.Sub(startBegan).Round(time.Millisecond))
	t.Logf("input unavailable %v (without the predecessor's in-process teardown of %v)", gap.Round(time.Millisecond), exited.Round(time.Millisecond))
}
