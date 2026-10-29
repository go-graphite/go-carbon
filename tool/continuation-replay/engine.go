package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"syscall"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/go-graphite/go-carbon/persister"
	whisper "github.com/go-graphite/go-whisper"
	"golang.org/x/sync/errgroup"
)

type replayEngine struct {
	mode, root                string
	plan                      replayPlan
	clock                     time.Time
	db                        *store.Store
	options                   store.Options
	budget                    int
	budgetSecond              int64
	compactions, budgetMisses uint64
}

func sidecarPath(path string) string { return whisper.OutOfOrderSidecarPath(path) }

func openReplayEngine(mode, root string, plan replayPlan) (*replayEngine, error) {
	e := &replayEngine{mode: mode, root: root, plan: plan, clock: time.Unix(0, plan.StartNS)}
	originals := filepath.Join(root, "originals")
	if err := os.Mkdir(originals, 0700); err != nil {
		return nil, err
	}
	capture, captureContext := errgroup.WithContext(context.Background())
	capture.SetLimit(4)
	for _, row := range plan.Rows {
		capture.Go(func() error {
			if err := captureContext.Err(); err != nil {
				return err
			}
			var allocated int64
			for _, path := range []string{filepath.Join(plan.SourceRoot, row.Path), sidecarPath(filepath.Join(plan.SourceRoot, row.Path))} {
				info, err := os.Stat(path)
				if errors.Is(err, os.ErrNotExist) && path != filepath.Join(plan.SourceRoot, row.Path) {
					continue
				}
				if err != nil {
					return err
				}
				if !info.Mode().IsRegular() {
					return errors.New("nonregular corpus input")
				}
				stat, ok := info.Sys().(*syscall.Stat_t)
				if !ok {
					return errors.New("physical source allocation is unavailable")
				}
				allocated += stat.Blocks * 512
			}
			if allocated != row.AllocatedBytes {
				return errors.New("source allocation differs from the sampling manifest")
			}
			// A whole missing/changed row fails the run. Selection never depends on
			// which histories are convenient to copy or which replies look populated.
			return whisperio.Capture(captureContext, filepath.Join(plan.SourceRoot, row.Path), filepath.Join(originals, row.Path), false)
		})
	}
	if err := capture.Wait(); err != nil {
		return nil, err
	}
	if mode == "reference" {
		return e, nil
	}
	if mode != "candidate" {
		return nil, errors.New("unknown engine")
	}
	e.options = store.Options{Continuation: &whisperio.ContinuationCodec{}, ExactBlocks: true, IncrementalSource: originals,
		CacheSize: plan.CacheBytes, MemTableSize: plan.MemtableBytes, SyncInterval: time.Second, Now: func() time.Time { return e.clock }}
	var err error
	e.db, err = store.Open(filepath.Join(root, "shared"), e.options)
	if err != nil {
		return nil, err
	}
	p := persister.NewWhisper(originals, nil, persister.NewWhisperAggregation(), nil, nil, nil, nil)
	p.SetFLock(true)
	p.SetMetricStore(e.db)
	if err := p.SetIncrementalMigration(e.db, filepath.Join(root, "shared")); err != nil {
		_ = e.db.Close()
		return nil, err
	}
	conversion, conversionContext := errgroup.WithContext(context.Background())
	conversion.SetLimit(4)
	for _, row := range plan.Rows {
		conversion.Go(func() error {
			_, err := p.ConvertFile(conversionContext, row.Metric)
			return err
		})
	}
	if err := conversion.Wait(); err != nil {
		_ = e.db.Close()
		return nil, err
	}
	return e, nil
}

func (e *replayEngine) allowCompaction() bool {
	second := e.clock.Unix()
	if second != e.budgetSecond {
		e.budgetSecond, e.budget = second, 500
	}
	if e.budget == 0 {
		e.budgetMisses++
		return false
	}
	e.budget--
	return true
}

func (e *replayEngine) original(index int) string {
	return filepath.Join(e.root, "originals", e.plan.Rows[index].Path)
}

func (e *replayEngine) write(op replayOperation) (err error) {
	row := e.plan.Rows[op.Metric]
	if e.mode == "candidate" {
		points := make([]store.Point, len(op.Points))
		for i, p := range op.Points {
			points[i] = store.Point{Timestamp: p.Timestamp, Value: math.Float64frombits(p.Bits)}
		}
		return e.db.UpdateManyMaintained(context.Background(), row.Metric, points, store.ContinuationMaintenance{MinBytes: 64 << 10, Allow: e.allowCompaction,
			Result: func(merged bool, err error) {
				if merged {
					e.compactions++
				}
			}})
	}
	w, err := whisper.OpenWithOptions(e.original(op.Metric), &whisper.Options{Compressed: true, OutOfOrder: true, FLock: true, Now: func() time.Time { return e.clock }})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, w.Close()) }()
	points := make([]*whisper.TimeSeriesPoint, len(op.Points))
	for i, p := range op.Points {
		points[i] = &whisper.TimeSeriesPoint{Time: int(p.Timestamp), Value: math.Float64frombits(p.Bits)}
	}
	if err := w.UpdateMany(points); err != nil {
		return err
	}
	if path := w.OutOfOrderPath(); path != "" {
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		allocated := info.Size()
		if stat, ok := info.Sys().(*syscall.Stat_t); ok {
			allocated = stat.Blocks * 512
		}
		if allocated >= 64<<10 && e.allowCompaction() {
			merged, err := w.MergeOutOfOrderWithOptions(whisper.OutOfOrderMergeOptions{})
			if err != nil {
				return err
			}
			if merged {
				e.compactions++
			}
		}
	}
	return nil
}

func (e *replayEngine) read(op replayOperation) (*store.Series, error) {
	if e.mode == "candidate" {
		return e.db.Fetch(context.Background(), e.plan.Rows[op.Metric].Metric, op.From, op.Until)
	}
	readOnly := os.O_RDONLY
	w, err := whisper.OpenWithOptions(e.original(op.Metric), &whisper.Options{OutOfOrder: true, OpenFileFlag: &readOnly, Now: func() time.Time { return e.clock }})
	if err != nil {
		return nil, err
	}
	defer w.Close()
	s, err := w.Fetch(op.From, op.Until)
	if err != nil || s == nil {
		return nil, err
	}
	return &store.Series{FromTime: s.FromTime(), UntilTime: s.UntilTime(), Step: s.Step(), Values: s.Values()}, nil
}

func seriesHash(series *store.Series) string {
	h := sha256.New()
	if series == nil {
		_, _ = h.Write([]byte{0})
	} else {
		_, _ = h.Write([]byte{1})
		for _, v := range []int{series.FromTime, series.UntilTime, series.Step, len(series.Values)} {
			_ = binary.Write(h, binary.LittleEndian, int64(v))
		}
		for _, v := range series.Values {
			_ = binary.Write(h, binary.LittleEndian, math.Float64bits(v))
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

func usage() (syscall.Rusage, error) {
	var r syscall.Rusage
	err := syscall.Getrusage(syscall.RUSAGE_SELF, &r)
	return r, err
}
func cpu(r syscall.Rusage) float64 {
	return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
}
func percentile(values []time.Duration) float64 {
	if len(values) == 0 {
		return 0
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return values[(len(values)-1)*99/100].Seconds()
}

func writeAllocationProfile(path string) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	return errors.Join(pprof.Lookup("allocs").WriteTo(f, 0), f.Close())
}

func runEngine(mode, root string, plan replayPlan, operations string, paced, profile bool) (err error) {
	e, err := openReplayEngine(mode, root, plan)
	if err != nil {
		return err
	}
	defer func() {
		if e.db != nil {
			err = errors.Join(err, e.db.Close())
		}
	}()
	readsFile, err := os.OpenFile(filepath.Join(root, "READS.jsonl"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer readsFile.Close()
	encoder := json.NewEncoder(readsFile)
	queries := make(map[[3]int]replayOperation)
	var readCount, pointsCount, writes uint64
	var readTimes, writeTimes []time.Duration
	var before syscall.Rusage
	var statsStart, statsEnd *store.Stats
	var memBefore runtime.MemStats
	var measuredStart time.Time
	var cpuProfile *os.File
	defer func() {
		if cpuProfile != nil {
			pprof.StopCPUProfile()
			err = errors.Join(err, cpuProfile.Close())
		}
	}()
	measuring := false
	started := time.Now()
	err = eachOperation(operations, plan, func(ordinal int, op replayOperation) error {
		if paced {
			if wait := time.Duration(op.AtNS-plan.StartNS) - time.Since(started); wait > 0 {
				time.Sleep(wait)
			}
		}
		e.clock = time.Unix(0, op.AtNS)
		if !measuring && op.AtNS >= plan.WarmupUntilNS {
			var err error
			if profile {
				if err := writeAllocationProfile(filepath.Join(root, "ALLOC-START.pb.gz")); err != nil {
					return err
				}
				cpuProfile, err = os.OpenFile(filepath.Join(root, "CPU.pb.gz"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
				if err != nil {
					return err
				}
				if err := pprof.StartCPUProfile(cpuProfile); err != nil {
					_ = cpuProfile.Close()
					cpuProfile = nil
					return err
				}
			}
			before, err = usage()
			if err != nil {
				return err
			}
			runtime.ReadMemStats(&memBefore)
			measuring = true
			measuredStart = time.Now()
			if e.db != nil {
				stats := e.db.Stats()
				statsStart = &stats
			}
		}
		opStarted := time.Now()
		if op.Kind == "write" {
			if err := e.write(op); err != nil {
				return fmt.Errorf("write %d: %w", ordinal, err)
			}
			if measuring {
				writes++
				pointsCount += uint64(len(op.Points))
				writeTimes = append(writeTimes, time.Since(opStarted))
			}
		} else {
			s, err := e.read(op)
			if err != nil {
				return err
			}
			readDuration := time.Since(opStarted)
			if err := encoder.Encode(struct {
				Ordinal int
				Hash    string
			}{ordinal, seriesHash(s)}); err != nil {
				return err
			}
			key := [3]int{op.Metric, op.From, op.Until}
			if _, exists := queries[key]; !exists && len(queries) >= 100000 {
				return errors.New("distinct read query bound reached")
			}
			queries[key] = op
			if measuring {
				readCount++
				readTimes = append(readTimes, readDuration)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !measuring || readCount == 0 || writes == 0 {
		return errors.New("measured replay must contain reads and writes")
	}
	if paced {
		if wait := time.Duration(plan.EndNS-plan.StartNS) - time.Since(started); wait > 0 {
			time.Sleep(wait)
		}
	}
	if e.db != nil {
		if err := e.db.Flush(); err != nil {
			return err
		}
		deadline := time.Now().Add(2 * time.Minute)
		for e.db.Stats().CompactionsInProgress != 0 && time.Now().Before(deadline) {
			time.Sleep(100 * time.Millisecond)
		}
		if e.db.Stats().CompactionsInProgress != 0 {
			return errors.New("background compaction did not drain")
		}
	}
	if e.db != nil {
		stats := e.db.Stats()
		statsEnd = &stats
	}
	measuredSeconds := time.Since(measuredStart).Seconds()
	after, err := usage()
	if err != nil {
		return err
	}
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)
	if cpuProfile != nil {
		pprof.StopCPUProfile()
		err := cpuProfile.Close()
		cpuProfile = nil
		if err != nil {
			return err
		}
		if err := writeAllocationProfile(filepath.Join(root, "ALLOC-END.pb.gz")); err != nil {
			return err
		}
	}
	if e.db != nil {
		if err := e.db.Close(); err != nil {
			return err
		}
		e.db = nil
		e.db, err = store.Open(filepath.Join(root, "shared"), e.options)
		if err != nil {
			return err
		}
	}
	e.clock = time.Unix(0, plan.EndNS)
	// This is a correctness phase, outside the measured resource interval. Every
	// existing sidecar gets a matching final fold on both engines; the resulting
	// headers, block points and OOO slots are independently checked by the parent.
	for index, row := range plan.Rows {
		if e.db != nil {
			if err := e.db.UpdateManyMaintained(context.Background(), row.Metric, nil, store.ContinuationMaintenance{Allow: func() bool { return true }}); err != nil {
				return err
			}
		} else {
			w, err := whisper.OpenWithOptions(e.original(index), &whisper.Options{OutOfOrder: true, Now: func() time.Time { return e.clock }})
			if err != nil {
				return err
			}
			if w.IsCompressed() {
				err = w.MergeOutOfOrder()
			}
			err = errors.Join(err, w.Close())
			if err != nil {
				return err
			}
		}
	}
	if e.db != nil {
		if err := e.db.Close(); err != nil {
			return err
		}
		e.db = nil
		e.db, err = store.Open(filepath.Join(root, "shared"), e.options)
		if err != nil {
			return err
		}
	}
	keys := make([][3]int, 0, len(queries))
	for k := range queries {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		for n := range 3 {
			if keys[i][n] != keys[j][n] {
				return keys[i][n] < keys[j][n]
			}
		}
		return false
	})
	reopened := make([]string, 0, len(keys))
	for _, key := range keys {
		s, err := e.read(queries[key])
		if err != nil {
			return err
		}
		reopened = append(reopened, seriesHash(s))
	}
	if err := writeJSON(filepath.Join(root, "REOPEN.json"), reopened); err != nil {
		return err
	}
	peak := after.Maxrss
	if runtime.GOOS == "linux" {
		peak *= 1024
	}
	result := map[string]any{"mode": mode, "pid": os.Getpid(), "measured_reads": readCount, "measured_writes": writes, "measured_points": pointsCount, "measured_wall_seconds": measuredSeconds, "cpu_seconds_including_compaction": cpu(after) - cpu(before), "allocated_bytes": memAfter.TotalAlloc - memBefore.TotalAlloc, "peak_rss_bytes_process_lifetime": peak, "read_p99_seconds": percentile(readTimes), "write_p99_seconds": percentile(writeTimes), "ooo_compactions": e.compactions, "compaction_budget_misses": e.budgetMisses, "paced": paced, "workers": 1, "performance_qualified": false, "engine_measured_start": statsStart, "engine_measured_end": statsEnd, "cpu_scope": "Includes operation decoding and validation hashes; diagnostic figures are not a production performance comparison."}
	if e.db != nil {
		result["engine_after_verification"] = e.db.Stats()
	}
	return writeJSON(filepath.Join(root, "RESULT.json"), result)
}
