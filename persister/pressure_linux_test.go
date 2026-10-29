package persister

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/helper"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/points"
	whisper "github.com/go-graphite/go-whisper"
)

type pressurePoint struct {
	metric uint32
	at     time.Duration
	lag    int64
	value  float64
}

// This pressure diagnostic uses the real writeout queue and workers.
// It is not a replacement for the byte-weighted file-mirror correctness oracle.
func TestNativeProductionQueuePressure(t *testing.T) {
	dir, trace, output := os.Getenv("GRAPHITE_PRESSURE_DB"), os.Getenv("GRAPHITE_PRESSURE_TRACE"), os.Getenv("GRAPHITE_PRESSURE_OUTPUT")
	if dir == "" || trace == "" || output == "" {
		t.Skip("requires private full database and captured production trace")
	}
	if !strings.Contains(filepath.Clean(dir), "/production-replay-lab-v1/") || !strings.Contains(filepath.Clean(output), "/production-replay-lab-v1/") {
		t.Fatal("pressure may only use private laboratory paths")
	}
	number := func(name string, fallback int) int {
		s := os.Getenv(name)
		if s == "" {
			return fallback
		}
		v, err := strconv.Atoi(s)
		if err != nil || v < 1 {
			t.Fatalf("invalid %s", name)
		}
		return v
	}
	seconds, rate := number("GRAPHITE_PRESSURE_SECONDS", 180), number("GRAPHITE_PRESSURE_POINTS_PER_SECOND", 1000000)
	workers, step := number("GRAPHITE_PRESSURE_COMPACTIONS", 1), number("GRAPHITE_PRESSURE_L0_STEP", 10)
	openFiles := number("GRAPHITE_PRESSURE_MAX_OPEN_FILES", 1000)
	if seconds < 30 || seconds > 600 || rate > 4000000 || workers > 64 || step > 64 {
		t.Fatal("pressure budget exceeds bounds")
	}
	names, partitions, cycle, traceCount := loadPressureTrace(t, trace, rate)
	if err := os.Mkdir(output, 0700); err != nil {
		t.Fatal(err)
	}
	opts := store.Options{ExactBlocks: true, CacheSize: 256 << 20, MemTableSize: 64 << 20, SyncInterval: time.Second, CompactionWorkers: workers, L0CompactionConcurrency: step, MaxOpenFiles: openFiles}
	db, err := store.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	oldProcs := runtime.GOMAXPROCS(192)
	defer runtime.GOMAXPROCS(oldProcs)
	c := cache.New()
	c.SetMaxSize(400000000)
	c.SetBloomSize(50000000)
	strategy := os.Getenv("GRAPHITE_PRESSURE_WRITE_STRATEGY")
	if strategy == "" {
		strategy = "noop"
	}
	if strategy != "noop" && strategy != "lexical" {
		t.Fatal("invalid pressure write strategy")
	}
	if err = c.SetWriteStrategy(strategy); err != nil {
		t.Fatal(err)
	}
	if err = c.SetWriteoutBatching(8, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	var sent, confirmed, confirmedUpdates, readCount, readFailures atomic.Uint64
	var starts sync.Map
	var latencyMu sync.Mutex
	var latencies []time.Duration
	pop := func(name string) (*points.Points, bool) {
		p, ok := c.PopNotConfirmed(name)
		if ok && helper.HashString(name)&63 == 0 {
			starts.Store(p, time.Now())
		}
		return p, ok
	}
	confirm := func(p *points.Points) {
		if began, ok := starts.LoadAndDelete(p); ok {
			latencyMu.Lock()
			latencies = append(latencies, time.Since(began.(time.Time)))
			latencyMu.Unlock()
		}
		confirmed.Add(uint64(len(p.Data)))
		confirmedUpdates.Add(1)
		c.Confirm(p)
	}
	compressed := true
	p := NewWhisper(filepath.Join(output, "unused-files"), WhisperSchemas{{Name: "pressure-new-metrics", Pattern: regexp.MustCompile(".*"), Retentions: whisper.MustParseRetentionDefs("1s:2d"), Compressed: &compressed}}, NewWhisperAggregation(), c.WriteoutQueue().Get, pop, confirm, c.Pop)
	p.SetRequeue(c.Requeue)
	p.SetBacklogged(c.Backlogged)
	p.SetWriteoutReady(c.WriteoutReady)
	p.SetWorkers(96)
	p.SetMaxUpdatesPerSecond(162500)
	p.SetMetricStore(db)
	p.Start()
	stopped := false
	defer func() {
		if !stopped {
			p.Stop()
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reconciliation := startPressureReconciliation(t, ctx, p, db, output)
	defer func() { cancel(); reconciliation.group.Wait() }()
	began := time.Now()
	base := began.Unix()
	var before, after syscall.Rusage
	if err = syscall.Getrusage(syscall.RUSAGE_SELF, &before); err != nil {
		t.Fatal(err)
	}
	var producers sync.WaitGroup
	for _, partition := range partitions {
		producers.Go(func() {
			local := uint64(0)
			defer func() { sent.Add(local) }()
			for repeat := 0; ; repeat++ {
				for i, point := range partition {
					at := time.Duration(repeat)*cycle + point.at
					if at >= time.Duration(seconds)*time.Second || ctx.Err() != nil {
						return
					}
					if i&1023 == 0 {
						if wait := time.Until(began.Add(at)); wait > 0 {
							timer := time.NewTimer(wait)
							select {
							case <-timer.C:
							case <-ctx.Done():
								timer.Stop()
								return
							}
						}
					}
					c.AddPoint(names[point.metric], point.value, base+int64(at/time.Second)-point.lag-10)
					local++
					if local >= 4096 {
						sent.Add(local)
						local = 0
					}
				}
			}
		})
	}
	var readLatencies []time.Duration
	var readMu sync.Mutex
	var readers sync.WaitGroup
	readers.Go(func() {
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for index := 0; ; index++ {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			name := string(names[(index*7919)%len(names)])
			start := time.Now()
			_, e := db.Fetch(ctx, name, int(base)-720, int(time.Now().Unix())-1)
			if e == nil {
				readCount.Add(1)
				readMu.Lock()
				readLatencies = append(readLatencies, time.Since(start))
				readMu.Unlock()
			} else if !errors.Is(e, context.Canceled) {
				readFailures.Add(1)
			}
		}
	})
	oracle := []byte("native_pressure.storage_exact.sequence")
	var oraclePoints atomic.Uint64
	producers.Go(func() {
		for i := 1; i <= seconds; i++ {
			timer := time.NewTimer(time.Until(began.Add(time.Duration(i) * time.Second)))
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			c.AddPoint(oracle, float64(i), base-10+int64(i))
			oraclePoints.Add(1)
			sent.Add(1)
		}
	})
	statsFile, err := os.Create(filepath.Join(output, "samples.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer statsFile.Close()
	initial := db.Stats()
	var rows []pressureSample
	tick := time.NewTicker(time.Second)
	for time.Since(began) < time.Duration(seconds)*time.Second {
		<-tick.C
		row := pressureSample{Time: time.Since(began).Seconds(), Sent: sent.Load(), Confirmed: confirmed.Load(), Updates: confirmedUpdates.Load(), Cache: int64(c.Size()), Stats: db.Stats()}
		row.ReconciledMetrics, row.ReconciledPoints, row.ReconcileErrors = reconciliation.metrics.Load(), reconciliation.points.Load(), reconciliation.failures.Load()
		rows = append(rows, row)
		data, _ := json.Marshal(row)
		_, _ = statsFile.Write(append(data, '\n'))
		if c.Size() > 180000000 {
			break
		}
	}
	tick.Stop()
	cancel()
	producers.Wait()
	readers.Wait()
	reconciliation.group.Wait()
	intakeSeconds := time.Since(began).Seconds()
	var intakeUsage syscall.Rusage
	if err = syscall.Getrusage(syscall.RUSAGE_SELF, &intakeUsage); err != nil {
		t.Fatal(err)
	}
	sentAtStop, confirmedAtStop, updatesAtStop := sent.Load(), confirmed.Load(), confirmedUpdates.Load()
	// Preserve every admitted point. A failed baseline may raise only its
	// cleanup budget after the measured intake window; this is reported.
	rescued := false
	drainBegan := time.Now()
	for confirmed.Load() != sent.Load() && time.Since(drainBegan) < 180*time.Second {
		if time.Since(drainBegan) > 30*time.Second && workers == 1 && !rescued {
			if err = db.SetCompactionWorkers(8); err != nil {
				t.Fatal(err)
			}
			rescued = true
		}
		time.Sleep(time.Second)
	}
	if confirmed.Load() != sent.Load() {
		fullMirrorWriteJSON(t, filepath.Join(output, "FAILED.json"), map[string]any{
			"completed": false, "write_strategy": strategy, "read_errors": readFailures.Load(), "reason": "queue drain deadline", "compaction_workers": workers, "l0_step": step,
			"max_open_files": openFiles, "target_points_per_second": rate, "intake_seconds": intakeSeconds,
			"sent": sentAtStop, "confirmed_during_intake": confirmedAtStop, "confirmed_at_deadline": confirmed.Load(),
			"cpu_seconds_during_intake": pressureCPU(intakeUsage) - pressureCPU(before), "peak_rss_bytes_linux": intakeUsage.Maxrss * 1024,
			"rescued_after_measurement": rescued, "last_stats": db.Stats(), "samples": rows,
		})
		t.Fatalf("queue did not drain: sent=%d confirmed=%d", sent.Load(), confirmed.Load())
	}
	p.Stop()
	stopped = true
	if err = db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err = syscall.Getrusage(syscall.RUSAGE_SELF, &after); err != nil {
		t.Fatal(err)
	}
	final := db.Stats()
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = store.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	series, err := db.Fetch(context.Background(), string(oracle), int(base)-12, int(base)+seconds+1)
	if err != nil {
		t.Fatal(err)
	}
	got := map[int]float64{}
	if series != nil {
		for i, value := range series.Values {
			if !math.IsNaN(value) {
				got[series.FromTime+i*series.Step] = value
			}
		}
	}
	for i := uint64(1); i <= oraclePoints.Load(); i++ {
		if value, ok := got[int(base)-10+int(i)]; !ok || math.Float64bits(value) != math.Float64bits(float64(i)) {
			t.Fatal("queued oracle point changed or disappeared after reopen")
		}
	}
	latencyMu.Lock()
	writeP99 := pressureP99(latencies)
	latencyMu.Unlock()
	readMu.Lock()
	readP99 := pressureP99(readLatencies)
	readMu.Unlock()
	fullMirrorWriteJSON(t, filepath.Join(output, "RESULT.json"), map[string]any{"completed": true, "write_strategy": strategy, "read_errors": readFailures.Load(), "max_open_files": openFiles, "cpu_seconds_during_intake": pressureCPU(intakeUsage) - pressureCPU(before), "reconciled_metrics": reconciliation.metrics.Load(), "reconciled_points": reconciliation.points.Load(), "reconcile_errors": reconciliation.failures.Load(), "compaction_workers": workers, "l0_step": step, "trace_names": len(names), "trace_points": traceCount, "timestamp_safety_padding_seconds": 10, "target_points_per_second": rate, "requested_seconds": seconds, "intake_seconds": intakeSeconds, "sent": sentAtStop, "confirmed_during_intake": confirmedAtStop, "confirmed_updates_during_intake": updatesAtStop, "confirmed_updates_after_drain": confirmedUpdates.Load(), "confirmed_after_drain": confirmed.Load(), "drain_seconds": time.Since(drainBegan).Seconds(), "rescued_after_measurement": rescued, "cpu_seconds_including_drain": pressureCPU(after) - pressureCPU(before), "peak_rss_bytes_linux": after.Maxrss * 1024, "write_p99_seconds_sampled_pop_to_confirm": writeP99, "read_p99_seconds": readP99, "successful_reads": readCount.Load(), "oracle_reopen_points": oraclePoints.Load(), "initial_stats": initial, "final_stats": final, "samples": rows, "scope": "Full private database; real captured name/value/lag pool replayed at declared arrival rate through production Cache/WriteoutQueue and96persister workers with declared dispatch order and8point/2second batching, natural flush/compaction during intake. Shared-only engine pressure, not original-file mirroring or actual client request mix. Read plan100/s; deterministic control stream verified after drain/reopen. Optional reconciliation captures originals read-only and replaces only private histories; its pressure result does not prove real input survival. Byte-weighted full-history file-mirror proof remains separate."})
}

type pressureSample struct {
	Time                                                 float64
	Sent, Confirmed, Updates                             uint64
	Cache                                                int64
	Stats                                                store.Stats
	ReconciledMetrics, ReconciledPoints, ReconcileErrors uint64
}

func pressureP99(values []time.Duration) float64 {
	if len(values) == 0 {
		return 0
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return values[len(values)*99/100].Seconds()
}

func loadPressureTrace(t *testing.T, path string, rate int) ([][]byte, [][]pressurePoint, time.Duration, uint64) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	var header [36]byte
	if _, err = io.ReadFull(r, header[:]); err != nil || string(header[:8]) != "GTREPL01" {
		t.Fatal("invalid trace")
	}
	n, count := binary.LittleEndian.Uint32(header[8:12]), binary.LittleEndian.Uint64(header[12:20])
	first, last := int64(binary.LittleEndian.Uint64(header[20:28])), int64(binary.LittleEndian.Uint64(header[28:36]))
	span := last - first
	if n == 0 || n > 50000000 || count == 0 || count > 100000000 || span <= 0 {
		t.Fatal("trace exceeds bounds")
	}
	names := make([][]byte, n)
	for i := range names {
		var size uint32
		if err = binary.Read(r, binary.LittleEndian, &size); err != nil || size == 0 || size > 1<<20 {
			t.Fatal("invalid metric name")
		}
		names[i] = make([]byte, size)
		if _, err = io.ReadFull(r, names[i]); err != nil {
			t.Fatal(err)
		}
	}
	cycle := time.Duration(float64(count) / float64(rate) * float64(time.Second))
	parts := make([][]pressurePoint, 16)
	var raw [28]byte
	for range count {
		if _, err = io.ReadFull(r, raw[:]); err != nil {
			t.Fatal(err)
		}
		at := int64(binary.LittleEndian.Uint64(raw[:8]))
		if at >= first {
			at -= first
		}
		index := binary.LittleEndian.Uint32(raw[8:12])
		ts := int64(binary.LittleEndian.Uint64(raw[12:20]))
		if index >= n || at < 0 || at > span {
			t.Fatal("invalid trace record")
		}
		scaled := time.Duration(float64(at) / float64(span) * float64(cycle))
		if scaled >= cycle {
			scaled = cycle - 1
		}
		point := pressurePoint{metric: index, at: scaled, lag: (first+at)/int64(time.Second) - ts, value: math.Float64frombits(binary.LittleEndian.Uint64(raw[20:28]))}
		parts[index%16] = append(parts[index%16], point)
	}
	if _, err = r.ReadByte(); !errors.Is(err, io.EOF) {
		t.Fatal("trailing trace data")
	}
	for _, part := range parts {
		sort.SliceStable(part, func(i, j int) bool { return part[i].at < part[j].at })
	}
	return names, parts, cycle, count
}

func pressureCPU(r syscall.Rusage) float64 {
	return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
}
