package chunkstore

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/helper"
)

type productionReplayMetric struct {
	name   string
	points []Point
}

// This diagnosis retains full database cardinality and the captured per-key
// point order. It is an engine comparison, not an application/cutover gate.
func TestNativeProductionEngineReplay(t *testing.T) {
	dir, trace, output := os.Getenv("GRAPHITE_ENGINE_DB"), os.Getenv("GRAPHITE_ENGINE_TRACE"), os.Getenv("GRAPHITE_ENGINE_OUTPUT")
	if dir == "" || trace == "" || output == "" {
		t.Skip("requires private full database and captured ingress")
	}
	if !strings.Contains(filepath.Clean(dir), "/production-replay-lab-v1/") {
		t.Fatal("replay may only open its private lab copy")
	}
	metrics, now, pointCount, err := loadProductionEngineTrace(trace)
	if err != nil {
		t.Fatal(err)
	}
	// Keep the same sampled reads and digest positions while changing only
	// dispatch order, so locality can be evaluated without selecting easier keys.
	keyOrder := os.Getenv("GRAPHITE_ENGINE_KEY_ORDER")
	order := make([]int, len(metrics))
	for i := range order {
		order[i] = i
	}
	switch keyOrder {
	case "", "noop":
	case "lexical":
	default:
		t.Fatal("invalid key order")
	}
	if err = os.Mkdir(output, 0700); err != nil {
		t.Fatal(err)
	}
	oldProcs := runtime.GOMAXPROCS(192)
	defer runtime.GOMAXPROCS(oldProcs)
	readBudget := func(name string, fallback int64) int64 {
		raw := os.Getenv(name)
		if raw == "" {
			return fallback
		}
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || value < 1 {
			t.Fatalf("invalid %s", name)
		}
		return value
	}
	cacheBytes := readBudget("GRAPHITE_ENGINE_CACHE_BYTES", 128<<20)
	memtableBytes := readBudget("GRAPHITE_ENGINE_MEMTABLE_BYTES", 64<<20)
	passes := int(readBudget("GRAPHITE_ENGINE_PASSES", 4))
	openFiles := int(readBudget("GRAPHITE_ENGINE_MAX_OPEN_FILES", 1000))
	if passes > 16 {
		t.Fatal("replay pass count exceeds bound")
	}
	opts := Options{ExactBlocks: true, CacheSize: cacheBytes, MemTableSize: uint64(memtableBytes), MaxOpenFiles: openFiles, SyncInterval: time.Second, Now: func() time.Time { return now }}
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()
	initialStats := db.Stats()
	ctx := context.Background()
	var summaries []map[string]any
	for pass := 0; pass < passes; pass++ {
		prof, e := os.Create(filepath.Join(output, fmt.Sprintf("pass-%d.cpu.pprof", pass)))
		if e != nil {
			t.Fatal(e)
		}
		if e = pprof.StartCPUProfile(prof); e != nil {
			t.Fatal(e)
		}
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		var ruBefore, ruAfter syscall.Rusage
		if e = syscall.Getrusage(syscall.RUSAGE_SELF, &ruBefore); e != nil {
			t.Fatal(e)
		}
		var written, absent, reads atomic.Uint64
		digests := make([][32]byte, (len(metrics)+31)/32)
		latencies := make([]time.Duration, len(digests))
		errChan := make(chan error, 96)
		jobs := make(chan int)
		var workers sync.WaitGroup
		began := time.Now()
		// Charge ordering to every measured pass, including rebuilding the
		// unsorted index. The queue optimization must pay for its own sort.
		for i := range order {
			order[i] = i
		}
		if keyOrder == "lexical" {
			sort.Slice(order, func(i, j int) bool { return metrics[order[i]].name < metrics[order[j]].name })
		}
		for range 96 {
			workers.Go(func() {
				for index := range jobs {
					rec := &metrics[index]
					// These calls model the real mirror's preparation/policy checks;
					// its filesystem writes and source clock fences are not exercised.
					_, e := db.Metadata(ctx, rec.name)
					if errors.Is(e, ErrNotFound) {
						absent.Add(1)
						continue
					}
					if e == nil {
						_, e = db.Metadata(ctx, rec.name)
					}
					if e == nil {
						e = db.UpdateMany(ctx, rec.name, rec.points)
					}
					if e != nil {
						select {
						case errChan <- e:
						default:
						}
						continue
					}
					written.Add(uint64(len(rec.points)))
					if index%32 == 0 {
						started := time.Now()
						series, e := db.Fetch(ctx, rec.name, int(now.Unix())-121, int(now.Unix())-1)
						latencies[index/32] = time.Since(started)
						if e != nil {
							select {
							case errChan <- e:
							default:
							}
						} else {
							digests[index/32] = productionSeriesDigest(series)
							reads.Add(1)
						}
					}
				}
			})
		}
		for _, index := range order {
			jobs <- index
		}
		close(jobs)
		workers.Wait()
		close(errChan)
		for e := range errChan {
			t.Error(e)
		}
		if t.Failed() {
			t.FailNow()
		}
		writeSeconds := time.Since(began).Seconds()
		if err = db.Flush(); err != nil {
			t.Fatal(err)
		}
		// Include natural compaction rather than forcing a rewrite of the whole
		// 824 GB database. A bounded wait reports whether background work drains.
		deadline := time.Now().Add(2 * time.Minute)
		for db.Stats().CompactionsInProgress != 0 && time.Now().Before(deadline) {
			time.Sleep(100 * time.Millisecond)
		}
		totalSeconds := time.Since(began).Seconds()
		if e = syscall.Getrusage(syscall.RUSAGE_SELF, &ruAfter); e != nil {
			t.Fatal(e)
		}
		runtime.ReadMemStats(&after)
		pprof.StopCPUProfile()
		if e = prof.Close(); e != nil {
			t.Fatal(e)
		}
		h := sha256.New()
		for _, digest := range digests {
			_, _ = h.Write(digest[:])
		}
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		cpu := func(r syscall.Rusage) float64 {
			return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
		}
		row := map[string]any{"pass": pass, "write_seconds": writeSeconds, "seconds_including_flush_and_compaction_wait": totalSeconds, "cpu_seconds": cpu(ruAfter) - cpu(ruBefore), "allocated_bytes": after.TotalAlloc - before.TotalAlloc, "allocations": after.Mallocs - before.Mallocs, "peak_rss_bytes_linux": ruAfter.Maxrss * 1024, "written_points": written.Load(), "missing_metrics": absent.Load(), "reads": reads.Load(), "read_p99_seconds": latencies[len(latencies)*99/100].Seconds(), "read_fingerprint_sha256": hex.EncodeToString(h.Sum(nil)), "stats": db.Stats()}
		summaries = append(summaries, row)
		writeProductionReplayJSON(t, filepath.Join(output, "PROGRESS.json"), map[string]any{"passes": summaries, "initial_stats": initialStats})
		t.Logf("pass %d: %d points in %.3fs, CPU %.3fs", pass, written.Load(), totalSeconds, row["cpu_seconds"])
	}
	finalStats := db.Stats()
	if finalStats.Compactions <= initialStats.Compactions {
		t.Fatal("no natural compaction observed; replay is incomplete")
	}
	// Reopen the private database and compare every sampled reply again. The
	// digest excludes revision/activity because those are checked separately.
	wantDigest := summaries[len(summaries)-1]["read_fingerprint_sha256"]
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db = nil
	db, err = Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	for index := 0; index < len(metrics); index += 32 {
		var digest [32]byte
		_, e := db.Metadata(ctx, metrics[index].name)
		if e == nil {
			series, e := db.Fetch(ctx, metrics[index].name, int(now.Unix())-121, int(now.Unix())-1)
			if e != nil {
				t.Fatal(e)
			}
			digest = productionSeriesDigest(series)
		} else if !errors.Is(e, ErrNotFound) {
			t.Fatal(e)
		}
		_, _ = h.Write(digest[:])
	}
	if hex.EncodeToString(h.Sum(nil)) != wantDigest {
		t.Fatal("sampled replies changed after flush, compaction and reopen")
	}
	writeProductionReplayJSON(t, filepath.Join(output, "RESULT.json"), map[string]any{"passed": true, "trace_metrics": len(metrics), "trace_points": pointCount, "cache_bytes": cacheBytes, "memtable_bytes": memtableBytes, "clock_unix": now.Unix(), "initial_stats": initialStats, "final_stats": finalStats, "passes": summaries, "reopen_fingerprint_sha256": wantDigest, "scope": "Matched private full-database engine replay, captured per-key values/order, 96 writers, one read per32 names, fixed admission clock, natural compaction and reopen. Excludes original-file mirroring, actual read request mix and application queue; not byte-weighted fleet or cutover qualification."})
}

func loadProductionEngineTrace(path string) ([]productionReplayMetric, time.Time, uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, time.Time{}, 0, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	var header [36]byte
	if _, err = io.ReadFull(r, header[:]); err != nil || string(header[:8]) != "GTREPL01" {
		return nil, time.Time{}, 0, errors.New("invalid trace header")
	}
	n := binary.LittleEndian.Uint32(header[8:12])
	count := binary.LittleEndian.Uint64(header[12:20])
	if n == 0 || n > 10000000 || count > 100000000 {
		return nil, time.Time{}, 0, errors.New("trace exceeds bound")
	}
	metrics := make([]productionReplayMetric, n)
	for i := range metrics {
		var length uint32
		if err = binary.Read(r, binary.LittleEndian, &length); err != nil || length == 0 || length > 1<<20 {
			return nil, time.Time{}, 0, errors.New("invalid metric length")
		}
		name := make([]byte, length)
		if _, err = io.ReadFull(r, name); err != nil {
			return nil, time.Time{}, 0, err
		}
		metrics[i].name = string(name)
	}
	var record [28]byte
	for i := uint64(0); i < count; i++ {
		if _, err = io.ReadFull(r, record[:]); err != nil {
			return nil, time.Time{}, 0, err
		}
		index := binary.LittleEndian.Uint32(record[8:12])
		if index >= n {
			return nil, time.Time{}, 0, errors.New("invalid metric index")
		}
		metrics[index].points = append(metrics[index].points, Point{Timestamp: int64(binary.LittleEndian.Uint64(record[12:20])), Value: math.Float64frombits(binary.LittleEndian.Uint64(record[20:28]))})
	}
	if _, err = r.ReadByte(); !errors.Is(err, io.EOF) {
		return nil, time.Time{}, 0, errors.New("trailing trace data")
	}
	sort.Slice(metrics, func(i, j int) bool {
		a, b := helper.HashString(metrics[i].name), helper.HashString(metrics[j].name)
		if a&1023 != b&1023 {
			return a&1023 < b&1023
		}
		return metrics[i].name < metrics[j].name
	})
	return metrics, time.Unix(0, int64(binary.LittleEndian.Uint64(header[28:36]))).Add(10 * time.Second), count, nil
}

func productionSeriesDigest(series *Series) [32]byte {
	h := sha256.New()
	if series != nil {
		_, _ = h.Write([]byte(series.Metadata.Name))
		for _, v := range []int{series.FromTime, series.UntilTime, series.Step} {
			_ = binary.Write(h, binary.LittleEndian, int64(v))
		}
		for _, v := range series.Values {
			_ = binary.Write(h, binary.LittleEndian, math.Float64bits(v))
		}
	}
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest
}

func writeProductionReplayJSON(t *testing.T, path string, value any) {
	t.Helper()
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, append(b, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
