package persister

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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	carboncache "github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/helper"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/go-graphite/go-carbon/points"
	whisper "github.com/go-graphite/go-whisper"
	"golang.org/x/sys/unix"
)

type fullMirrorRow struct {
	Metric, Path string
	Points       []struct {
		Timestamp int64
		Bits      uint64
	}
	AllocatedBytes       int64
	InclusionProbability float64
	BaselineID           uint64
}

// Run only on private copies. The cadence and timestamp translations are
// identical for the file reference and the mirrored writer; no live endpoint
// or original file is changed. Full DB cardinality is retained for mirroring.
func TestNativeFullDatabaseMirrorReplay(t *testing.T) {
	dir, source, manifest, output, mode := os.Getenv("GRAPHITE_FULLMIRROR_DB"), os.Getenv("GRAPHITE_FULLMIRROR_SOURCE"), os.Getenv("GRAPHITE_FULLMIRROR_MANIFEST"), os.Getenv("GRAPHITE_FULLMIRROR_OUTPUT"), os.Getenv("GRAPHITE_FULLMIRROR_MODE")
	if source == "" || manifest == "" || output == "" || mode == "" {
		t.Skip("requires private byte-weighted corpus and full database")
	}
	if !strings.Contains(filepath.Clean(output), "/production-replay-lab-v1/") || !strings.HasSuffix(filepath.Clean(source), "/full-baseline/cwhisper-copy") {
		t.Fatal("requires private replay output and immutable retained source")
	}
	if mode != "files" && mode != "mirror" {
		t.Fatal("invalid mode")
	}
	if mode == "mirror" && !strings.Contains(filepath.Clean(dir), "/production-replay-lab-v1/") {
		t.Fatal("requires private full database")
	}
	var corpus struct {
		Rows            []fullMirrorRow
		OriginalIDLimit uint64
	}
	raw, err := os.ReadFile(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &corpus); err != nil {
		t.Fatal(err)
	}
	if len(corpus.Rows) < 100 || corpus.OriginalIDLimit == 0 {
		t.Fatal("invalid corpus")
	}
	readNumber := func(name string, fallback int64) int64 {
		raw := os.Getenv(name)
		if raw == "" {
			return fallback
		}
		v, e := strconv.ParseInt(raw, 10, 64)
		if e != nil || v < 1 {
			t.Fatalf("invalid %s", name)
		}
		return v
	}
	baseClock := readNumber("GRAPHITE_FULLMIRROR_CLOCK", 0)
	if baseClock == 0 {
		t.Fatal("requires captured admission clock")
	}
	realClock := os.Getenv("GRAPHITE_FULLMIRROR_REAL_CLOCK") == "1"
	timestampShift := readNumber("GRAPHITE_FULLMIRROR_TIMESTAMP_SHIFT", 0)
	logicalStep := readNumber("GRAPHITE_FULLMIRROR_LOGICAL_STEP", 3)
	cadence := 3 * time.Second
	if realClock {
		// Sweep one complete wall-second phase over the 32 rounds. An exact
		// three-second cadence can repeatedly miss the admission boundary.
		cadence = 3031 * time.Millisecond
	}
	cacheBytes, memtableBytes := readNumber("GRAPHITE_FULLMIRROR_CACHE", 320<<20), readNumber("GRAPHITE_FULLMIRROR_MEMTABLE", 64<<20)
	if err = os.Mkdir(output, 0700); err != nil {
		t.Fatal(err)
	}
	original := filepath.Join(output, "original")
	jobs := make(chan int)
	errChan := make(chan error, 16)
	var copies sync.WaitGroup
	for range 16 {
		copies.Go(func() {
			for index := range jobs {
				row := &corpus.Rows[index]
				if !filepath.IsLocal(row.Path) {
					select {
					case errChan <- errors.New("nonlocal corpus path"):
					default:
					}
					continue
				}
				for _, suffix := range []string{"", ".ooo"} {
					from, to := filepath.Join(source, row.Path)+suffix, filepath.Join(original, row.Path)+suffix
					if e := cloneFullMirrorFile(from, to); e != nil && !(suffix != "" && os.IsNotExist(e)) {
						select {
						case errChan <- e:
						default:
						}
						break
					}
				}
			}
		})
	}
	for i := range corpus.Rows {
		jobs <- i
	}
	close(jobs)
	copies.Wait()
	close(errChan)
	for e := range errChan {
		t.Error(e)
	}
	if t.Failed() {
		t.FailNow()
	}
	oldProcs := runtime.GOMAXPROCS(192)
	defer runtime.GOMAXPROCS(oldProcs)
	var clock atomic.Int64
	clock.Store(baseClock)
	var holdClock atomic.Bool
	oldNow := whisper.Now
	whisper.Now = func() time.Time {
		if realClock && !holdClock.Load() {
			return time.Now()
		}
		return time.Unix(clock.Load(), 0)
	}
	defer func() { whisper.Now = oldNow }()
	ctx := context.Background()
	opts := store.Options{ExactBlocks: true, CacheSize: cacheBytes, MemTableSize: uint64(memtableBytes), SyncInterval: time.Second, Now: whisper.Now}
	opts.CompactionWorkers = int(readNumber("GRAPHITE_FULLMIRROR_COMPACTIONS", 1))
	opts.L0CompactionConcurrency = int(readNumber("GRAPHITE_FULLMIRROR_L0_STEP", 10))
	opts.MaxOpenFiles = int(readNumber("GRAPHITE_FULLMIRROR_MAX_OPEN_FILES", 1000))
	var db *profileMirrorStore
	var initialStats store.Stats
	initialGenerations := make(map[string]uint64, len(corpus.Rows))
	sampledDB := os.Getenv("GRAPHITE_FULLMIRROR_SAMPLED_DB") == "1"
	resetCohort := os.Getenv("GRAPHITE_FULLMIRROR_RESET_COHORT") == "1"
	if sampledDB && resetCohort {
		t.Fatal("sampled DB and existing full DB reset are mutually exclusive")
	}
	if mode == "mirror" {
		opened, e := store.Open(dir, opts)
		if e != nil {
			t.Fatal(e)
		}
		db = &profileMirrorStore{Store: opened}
		defer func() {
			if db != nil {
				_ = db.Close()
			}
		}()
		for _, row := range corpus.Rows {
			if resetCohort {
				m, err := db.Metadata(ctx, row.Metric)
				if err != nil || m.ID != row.BaselineID || m.ID > corpus.OriginalIDLimit {
					t.Fatal("existing full DB does not match the captured corpus IDs")
				}
			}
			if sampledDB || resetCohort {
				snapshot, err := whisperio.ReadSnapshotIn(ctx, row.Metric, filepath.Join(original, row.Path), output)
				if err != nil {
					t.Fatal(err)
				}
				if sampledDB {
					_, err = db.CreateFromSnapshot(ctx, snapshot)
				} else {
					_, err = db.ReconcileSnapshot(ctx, snapshot)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			m, e := db.Metadata(ctx, row.Metric)
			if e != nil || !sampledDB && (m.ID != row.BaselineID || m.ID > corpus.OriginalIDLimit) || !resetCohort && (m.Generation != 1 || m.Revision != 1) {
				t.Fatalf("initial baseline mismatch for %s: %+v, %v", row.Metric, m, e)
			}
			initialGenerations[row.Metric] = m.Generation
		}
		if sampledDB {
			if err := db.Compact(); err != nil {
				t.Fatal(err)
			}
		}
		initialStats = db.Stats()
	}
	c := carboncache.New()
	var confirmed atomic.Uint64
	p := newOOOTestPersister(t, original, &fakeCache{})
	p.pop = c.PopNotConfirmed
	p.popConfirm = c.Pop
	p.confirm = func(v *points.Points) { c.Confirm(v); confirmed.Add(uint64(len(v.Data))) }
	p.SetRequeue(c.Requeue)
	p.SetFLock(true)
	p.outOfOrder.ticker.Stop()
	oooThreshold := readNumber("GRAPHITE_FULLMIRROR_OOO_THRESHOLD", 65536)
	p.EnableOutOfOrder(500, oooThreshold)
	p.outOfOrder.ticker = helper.NewHardThrottleTicker(500)
	defer p.outOfOrder.ticker.Stop()
	if db != nil {
		p.SetMetricStore(db)
		p.SetMirrorFiles(true)
		p.SetMirrorImportMissing(true)
		p.SetMirrorState(&MirrorState{ScratchDir: output})
	}
	const rounds = 32
	warmups := int(readNumber("GRAPHITE_FULLMIRROR_WARMUPS", 0))
	latencies := make([]time.Duration, rounds*len(corpus.Rows))
	readDigests := make([][32]byte, rounds*((len(corpus.Rows)+15)/16))
	var expected uint64
	for _, row := range corpus.Rows {
		expected += uint64(len(row.Points)) * uint64(rounds+warmups)
	}
	var before, after runtime.MemStats
	var ruBefore, ruAfter syscall.Rusage
	prof, err := os.Create(filepath.Join(output, "cpu.pprof"))
	if err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	overruns := 0
	for round := -warmups; round < rounds; round++ {
		if round == 0 {
			runtime.GC()
			runtime.ReadMemStats(&before)
			if err = syscall.Getrusage(syscall.RUSAGE_SELF, &ruBefore); err != nil {
				t.Fatal(err)
			}
			if db != nil {
				initialStats = db.Stats()
			}
			if err = pprof.StartCPUProfile(prof); err != nil {
				t.Fatal(err)
			}
			began = time.Now()
		}
		deadline := began.Add(time.Duration(round+1) * cadence)
		if round < 0 {
			deadline = time.Now().Add(cadence)
		}
		clock.Store(baseClock + int64(round+warmups)*logicalStep)
		jobs := make(chan int)
		failures := make(chan error, 96)
		var workers sync.WaitGroup
		for range 96 {
			workers.Go(func() {
				for index := range jobs {
					row := &corpus.Rows[index]
					data := make([]points.Point, len(row.Points))
					for i, point := range row.Points {
						data[i] = points.Point{Timestamp: point.Timestamp + timestampShift + int64(round+warmups)*logicalStep, Value: math.Float64frombits(point.Bits)}
					}
					c.Add(&points.Points{Metric: row.Metric, Data: data})
					started := time.Now()
					p.store(row.Metric)
					if round >= 0 {
						latencies[round*len(corpus.Rows)+index] = time.Since(started)
					}
					if index%16 == 0 {
						series, e := readFullMirrorSeries(filepath.Join(original, row.Path), int(clock.Load())-121, int(clock.Load())-1)
						if e != nil {
							select {
							case failures <- e:
							default:
							}
						} else if round >= 0 {
							readDigests[round*((len(corpus.Rows)+15)/16)+index/16] = fullMirrorSeriesDigest(series)
						}
					}
				}
			})
		}
		for i := range corpus.Rows {
			jobs <- i
		}
		close(jobs)
		workers.Wait()
		close(failures)
		for e := range failures {
			t.Error(e)
		}
		if t.Failed() {
			t.FailNow()
		}
		if db != nil {
			if err = db.Flush(); err != nil {
				t.Fatal(err)
			}
		}
		if time.Now().After(deadline) {
			if round >= 0 {
				overruns++
			}
		} else {
			time.Sleep(time.Until(deadline))
		}
		if round >= 0 && round%4 == 3 {
			fullMirrorWriteJSON(t, filepath.Join(output, "PROGRESS.json"), map[string]any{"rounds_completed": round + 1, "confirmed_points": confirmed.Load(), "elapsed_seconds": time.Since(began).Seconds()})
		}
	}
	if db != nil {
		until := time.Now().Add(2 * time.Minute)
		for db.Stats().CompactionsInProgress != 0 && time.Now().Before(until) {
			time.Sleep(100 * time.Millisecond)
		}
	}
	elapsed := time.Since(began)
	if err = syscall.Getrusage(syscall.RUSAGE_SELF, &ruAfter); err != nil {
		t.Fatal(err)
	}
	runtime.ReadMemStats(&after)
	pprof.StopCPUProfile()
	if err = prof.Close(); err != nil {
		t.Fatal(err)
	}
	if confirmed.Load() != expected || c.Len() != 0 {
		t.Fatalf("writes not confirmed: got %d of %d, pending metrics %d", confirmed.Load(), expected, c.Len())
	}
	h := sha256.New()
	for _, d := range readDigests {
		_, _ = h.Write(d[:])
	}
	writeFingerprint := hex.EncodeToString(h.Sum(nil))
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	fullMirrorWriteJSON(t, filepath.Join(output, "TIMED-PENDING-CORRECTNESS.json"), map[string]any{"passed": false, "correctness_pending": true, "mode": mode, "confirmed_points": confirmed.Load(), "seconds": elapsed.Seconds(), "cpu_seconds": float64(ruAfter.Utime.Sec+ruAfter.Stime.Sec-ruBefore.Utime.Sec-ruBefore.Stime.Sec) + float64(ruAfter.Utime.Usec+ruAfter.Stime.Usec-ruBefore.Utime.Usec-ruBefore.Stime.Usec)/1e6, "peak_rss_bytes_linux": ruAfter.Maxrss * 1024, "read_fingerprint_sha256": writeFingerprint})
	if realClock {
		// Writes and natural compaction above use the moving wall clock. Hold
		// one clock for the subsequent paired oracle so a read pair itself
		// cannot straddle the oldest-retention cutoff on different calls.
		clock.Store(time.Now().Unix())
		holdClock.Store(true)
	}
	var exactHistories, exactPoints atomic.Uint64
	var fullRetentionReads atomic.Uint64
	finalStats := store.Stats{}
	if db != nil {
		finalStats = db.Stats()
		if finalStats.Compactions <= initialStats.Compactions {
			t.Fatal("no natural shared compaction observed")
		}
		jobs := make(chan int)
		failures := make(chan error, 8)
		var validators sync.WaitGroup
		for range 8 {
			validators.Go(func() {
				for index := range jobs {
					row := &corpus.Rows[index]
					want, e := whisperio.ReadSnapshotIn(ctx, row.Metric, filepath.Join(original, row.Path), output)
					if e == nil {
						var got store.Snapshot
						got, e = db.Snapshot(ctx, row.Metric)
						if e == nil {
							e = store.EqualSnapshots(want, got)
						}
					}
					if e != nil {
						select {
						case failures <- fmt.Errorf("metric index %d: %w", index, e):
						default:
						}
						continue
					}
					exactHistories.Add(1)
					for _, a := range want.Archives {
						exactPoints.Add(uint64(len(a.Points)))
					}
				}
			})
		}
		for i := range corpus.Rows {
			jobs <- i
		}
		close(jobs)
		validators.Wait()
		close(failures)
		for e := range failures {
			t.Error(e)
		}
		if t.Failed() {
			t.FailNow()
		}
		if err = db.Close(); err != nil {
			t.Fatal(err)
		}
		db = nil
		opened, e := store.Open(dir, opts)
		if e != nil {
			t.Fatal(e)
		}
		db = &profileMirrorStore{Store: opened}
		readJobs := make(chan int)
		readErrors := make(chan error, 8)
		var readers sync.WaitGroup
		for range 8 {
			readers.Go(func() {
				for index := range readJobs {
					row := &corpus.Rows[index]
					metadata, e := db.Metadata(ctx, row.Metric)
					if e != nil {
						select {
						case readErrors <- e:
						default:
						}
						continue
					}
					fromTimes := []int{int(clock.Load()) - 121}
					for _, retention := range metadata.Retentions {
						fromTimes = append(fromTimes, int(clock.Load())-retention.MaxRetention()+retention.Step-1)
					}
					for _, from := range fromTimes {
						until := int(clock.Load()) - 1
						want, e := readFullMirrorSeries(filepath.Join(original, row.Path), from, until)
						if e == nil {
							var got *store.Series
							got, e = db.Fetch(ctx, row.Metric, from, until)
							if e == nil && fullMirrorSeriesDigest(want) != fullMirrorSeriesDigest(got) {
								e = fmt.Errorf("full-retention read mismatch at metric index %d from %d", index, from)
							}
						}
						if e != nil {
							select {
							case readErrors <- e:
							default:
							}
						} else {
							fullRetentionReads.Add(1)
						}
					}
				}
			})
		}
		for index := range corpus.Rows {
			readJobs <- index
		}
		close(readJobs)
		readers.Wait()
		close(readErrors)
		for e := range readErrors {
			t.Error(e)
		}
		if t.Failed() {
			t.FailNow()
		}
	}
	cpu := func(r syscall.Rusage) float64 {
		return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
	}
	replacements := uint64(0)
	for _, row := range corpus.Rows {
		if db != nil {
			m, e := db.Metadata(ctx, row.Metric)
			if e != nil {
				t.Fatal(e)
			}
			replacements += m.Generation - initialGenerations[row.Metric]
		}
	}
	fullMirrorWriteJSON(t, filepath.Join(output, "RESULT.json"), map[string]any{"passed": true, "sampled_database": sampledDB, "cohort_reset_before_replay": resetCohort, "real_admission_clock": realClock, "paired_read_clock_held_after_replay": realClock, "timestamp_shift_seconds": timestampShift, "logical_step_seconds": logicalStep, "planned_clock_base": baseClock, "mode": mode, "metrics": len(corpus.Rows), "rounds": rounds, "warmup_rounds": warmups, "measured_points": expected * rounds / uint64(rounds+warmups), "round_cadence_seconds": cadence.Seconds(), "cadence_overruns": overruns, "seconds": elapsed.Seconds(), "cpu_seconds": cpu(ruAfter) - cpu(ruBefore), "allocated_bytes": after.TotalAlloc - before.TotalAlloc, "allocations": after.Mallocs - before.Mallocs, "peak_rss_bytes_linux": ruAfter.Maxrss * 1024, "confirmed_points": confirmed.Load(), "write_p99_seconds": latencies[len(latencies)*99/100].Seconds(), "read_fingerprint_sha256": writeFingerprint, "exact_full_histories": exactHistories.Load(), "exact_history_points": exactPoints.Load(), "full_retention_reopen_reads": fullRetentionReads.Load(), "full_source_replacements": replacements, "ooo_threshold_bytes": oooThreshold, "ooo_compactions": p.oooCompactions, "ooo_compact_errors": p.oooCompactErrors, "cache_bytes": cacheBytes, "memtable_bytes": memtableBytes, "initial_stats": initialStats, "final_stats": finalStats, "scope": "Byte-weighted real-ingress source cohort; sampled_database identifies a standalone DB seeded with the same complete sampled histories instead of full-node cardinality; real persister mirror/OOO/flock and original-file reads at matched reported cadence, 96 writers, captured values with the reported logical timestamp/admission-clock step each round, natural shared compaction and exact whole-history/full-retention public-read reopen oracle. Predetermined reads; real_admission_clock identifies moving wall-clock writes/compaction, followed by one held clock for the paired read oracle. Excludes full app queue/actual client request mix; live qualification still required."})
}

func cloneFullMirrorFile(from, to string) error {
	in, err := os.Open(from)
	if err != nil {
		return err
	}
	defer in.Close()
	if err = os.MkdirAll(filepath.Dir(to), 0700); err != nil {
		return err
	}
	out, err := os.OpenFile(to, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	err = unix.IoctlFileClone(int(out.Fd()), int(in.Fd()))
	return errors.Join(err, out.Close())
}

func readFullMirrorSeries(path string, from, until int) (*store.Series, error) {
	w, err := whisper.OpenWithOptions(path, &whisper.Options{Compressed: true, FLock: true, OutOfOrder: true})
	if err != nil {
		return nil, err
	}
	s, e := w.Fetch(from, until)
	err = errors.Join(e, w.Close())
	if err != nil || s == nil {
		return nil, err
	}
	return &store.Series{FromTime: s.FromTime(), UntilTime: s.UntilTime(), Step: s.Step(), Values: s.Values()}, nil
}

func fullMirrorSeriesDigest(series *store.Series) [32]byte {
	h := sha256.New()
	var word [8]byte
	if series != nil {
		for _, v := range []int{series.FromTime, series.UntilTime, series.Step} {
			binary.LittleEndian.PutUint64(word[:], uint64(int64(v)))
			_, _ = h.Write(word[:])
		}
		for _, v := range series.Values {
			binary.LittleEndian.PutUint64(word[:], math.Float64bits(v))
			_, _ = h.Write(word[:])
		}
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func fullMirrorWriteJSON(t *testing.T, path string, value any) {
	t.Helper()
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, append(b, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
