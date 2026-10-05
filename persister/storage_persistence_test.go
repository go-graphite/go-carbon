package persister

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/helper"
	whisper "github.com/go-graphite/go-whisper"
)

func TestStorageFetchEdges(t *testing.T) {
	now := storageTestClock(t)
	c := storageConfig("metric", "1s:1m,10s:10m,60s:1h", whisper.Sum, 0.5)
	for _, kind := range storageBackends[1:] {
		t.Run(kind, func(t *testing.T) {
			oracle := newStorageBackend(t, "classic", now)
			candidate := newStorageBackend(t, kind, now)
			for _, s := range []*storageBackend{oracle, candidate} {
				storageMust(t, s.create(c))
			}
			for _, populated := range []bool{false, true} {
				if populated {
					for _, s := range []*storageBackend{oracle, candidate} {
						storageMust(t, s.update(c.Name, storagePoints(storageEpoch-50, 50, 1)))
					}
				}
				for _, age := range []int{-1, 0, 1, 59, 60, 61, 599, 600, 601, 3599, 3600, 3601} {
					for _, width := range []int{0, 1, 9, 10, 11, 60} {
						t.Run(fmt.Sprintf("populated=%v/age=%d/width=%d", populated, age, width), func(t *testing.T) {
							from := storageEpoch - age
							want, err := oracle.fetch(c.Name, from, from+width)
							storageMust(t, err)
							got, err := candidate.fetch(c.Name, from, from+width)
							storageMust(t, err)
							if diff := storageSeriesDiff(want, got); diff != "" {
								t.Fatal(diff)
							}
						})
					}
				}
			}
		})
	}
}

func TestStorageRandomizedParity(t *testing.T) {
	now := storageTestClock(t)
	for _, kind := range []string{"cwhisper-ooo"} {
		for _, seed := range []int64{1, 4271, 82597} {
			for _, method := range []whisper.AggregationMethod{whisper.Average, whisper.Sum, whisper.Last, whisper.Max, whisper.Min, whisper.First} {
				t.Run(fmt.Sprintf("%s/seed=%d/%s", kind, seed, method), func(t *testing.T) {
					now.Store(storageEpoch)
					// A fixed seed lets failures replay the same write order and timestamps.
					// skipcq: GSC-G404
					rng := rand.New(rand.NewSource(seed))
					oracle := newStorageBackend(t, "classic", now)
					candidate := newStorageBackend(t, kind, now)
					c := storageConfig("metric", "1s:1m,10s:10m,60s:1h", method, 0.5)
					storageMust(t, oracle.create(c))
					storageMust(t, candidate.create(c))
					for round := 0; round < 20; round++ {
						now.Add(int64(rng.Intn(20)))
						batch := make([]whisper.TimeSeriesPoint, 1+rng.Intn(24))
						for i := range batch {
							batch[i] = whisper.TimeSeriesPoint{Time: int(now.Load()) - rng.Intn(750), Value: float64(rng.Intn(201) - 100)}
						}
						if round%3 == 0 {
							batch = append(batch, whisper.TimeSeriesPoint{Time: batch[0].Time, Value: 999})
						}
						storageMust(t, oracle.update(c.Name, batch))
						storageMust(t, candidate.update(c.Name, batch))
						storageCompare(t, oracle, candidate, c, fmt.Sprintf("seed=%d round=%d now=%d batch=%v", seed, round, now.Load(), batch))
						if round%5 == 4 {
							storageMust(t, candidate.compact([]string{c.Name}))
							storageMust(t, candidate.reopen())
							storageCompare(t, oracle, candidate, c, fmt.Sprintf("seed=%d round=%d compacted", seed, round))
						}
					}
				})
			}
		}
	}
}

func TestStorageCircularSlotWriteOrder(t *testing.T) {
	now := storageTestClock(t)
	for _, kind := range storageBackends[1:] {
		for _, schema := range []string{"1s:1m", "1s:10s,10s:1m,60s:1h"} {
			for _, order := range []string{"correction-first", "future-first", "future-only"} {
				if kind == "cwhisper" && order != "future-only" {
					continue // Plain cwhisper does not support historical corrections.
				}
				t.Run(fmt.Sprintf("%s/%s/%s", kind, schema, order), func(t *testing.T) {
					now.Store(storageEpoch)
					oracle := newStorageBackend(t, "classic", now)
					candidate := newStorageBackend(t, kind, now)
					c := storageConfig("metric", schema, whisper.Average, 0)
					storageMust(t, oracle.create(c))
					storageMust(t, candidate.create(c))
					initial := []whisper.TimeSeriesPoint{{Time: storageEpoch - 50, Value: 7}, {Time: storageEpoch - 40, Value: 8}}
					correction := []whisper.TimeSeriesPoint{{Time: storageEpoch - 50, Value: 11}}
					future := []whisper.TimeSeriesPoint{{Time: storageEpoch + 10, Value: 9}}
					batches := [][]whisper.TimeSeriesPoint{initial, future, correction}
					switch order {
					case "correction-first":
						batches = [][]whisper.TimeSeriesPoint{initial, correction, future}
					case "future-only":
						batches = [][]whisper.TimeSeriesPoint{initial, future}
					}
					for i, batch := range batches {
						storageMust(t, oracle.update(c.Name, batch))
						storageMust(t, candidate.update(c.Name, batch))
						storageCompare(t, oracle, candidate, c, fmt.Sprintf("batch=%d", i))
					}
					now.Add(20)
					storageCompare(t, oracle, candidate, c, "future-now-visible")
					storageMust(t, candidate.compact([]string{c.Name}))
					storageMust(t, candidate.reopen())
					storageCompare(t, oracle, candidate, c, "compacted-and-reopened")
				})
			}
		}
	}
}

func storageRecoveryBatches(kind string) [][]whisper.TimeSeriesPoint {
	result := [][]whisper.TimeSeriesPoint{storagePoints(storageEpoch-200, 64, 1), storagePoints(storageEpoch-100, 64, 1)}
	if kind != "cwhisper" {
		result = append(result, storagePoints(storageEpoch-136, 36, 1))
	}
	return result
}

// This helper exits after acknowledged writes without running any defer or cleanup.
func TestStorageCrashWriter(t *testing.T) {
	dir := os.Getenv("GO_CARBON_STORAGE_CRASH_DIR")
	if dir == "" {
		t.Skip("subprocess helper")
	}
	now := storageTestClock(t)
	s := &storageBackend{kind: os.Getenv("GO_CARBON_STORAGE_CRASH_BACKEND"), dir: dir, now: now}
	storageMust(t, s.open())
	for i := 0; i < 4; i++ {
		name := fmt.Sprintf("metric-%d", i)
		storageMust(t, s.create(storageConfig(name, "1s:10m", whisper.Average, 0.5)))
		for _, batch := range storageRecoveryBatches(s.kind) {
			storageMust(t, s.update(name, batch))
		}
	}
	// Bypass test cleanup and deferred closes to simulate process termination.
	// skipcq: RVV-A0003
	os.Exit(23)
}

func TestStorageCrashRecovery(t *testing.T) {
	now := storageTestClock(t)
	for _, kind := range storageBackends {
		modes := []string{"unclosed"}
		for _, mode := range modes {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				dir := t.TempDir()
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStorageCrashWriter$")
				cmd.Env = append(os.Environ(), "GO_CARBON_STORAGE_CRASH_DIR="+dir, "GO_CARBON_STORAGE_CRASH_BACKEND="+kind)
				output, err := cmd.CombinedOutput()
				var exitErr *exec.ExitError
				if !errors.As(err, &exitErr) || exitErr.ExitCode() != 23 {
					t.Fatalf("writer did not reach the acknowledged-write exit: %v\n%s", err, output)
				}
				candidate := &storageBackend{kind: kind, dir: dir, now: now}
				storageMust(t, candidate.open())
				t.Cleanup(func() { storageMust(t, candidate.close()) })
				oracle := newStorageBackend(t, "classic", now)
				for i := 0; i < 4; i++ {
					c := storageConfig(fmt.Sprintf("metric-%d", i), "1s:10m", whisper.Average, 0.5)
					storageMust(t, oracle.create(c))
					for _, batch := range storageRecoveryBatches(kind) {
						storageMust(t, oracle.update(c.Name, batch))
					}
					storageCompare(t, oracle, candidate, c, "crash-recovery")
					for _, s := range []*storageBackend{oracle, candidate} {
						storageMust(t, s.update(c.Name, storagePoints(storageEpoch-10, 8, 1)))
					}
					storageMust(t, candidate.compact([]string{c.Name}))
					storageMust(t, candidate.reopen())
					storageCompare(t, oracle, candidate, c, "post-recovery-write-and-compaction")
				}
			})
		}
	}
}

func TestStorageConcurrentMetrics(t *testing.T) {
	now := storageTestClock(t)
	for _, kind := range storageBackends {
		t.Run(kind, func(t *testing.T) {
			candidate := newStorageBackend(t, kind, now)
			oracle := newStorageBackend(t, "classic", now)
			var wg sync.WaitGroup
			for metric := 0; metric < 8; metric++ {
				c := storageConfig(fmt.Sprintf("metric-%d", metric), "1s:10m", whisper.Average, 0.5)
				storageMust(t, candidate.create(c))
				storageMust(t, oracle.create(c))
				wg.Go(func() {
					for batch := 0; batch < 8; batch++ {
						input := storagePoints(storageEpoch-256+batch*8, 8, 1)
						for i := range input {
							input[i].Value += float64(metric * 100)
						}
						for _, s := range []*storageBackend{oracle, candidate} {
							if err := s.update(c.Name, input); err != nil {
								t.Errorf("metric=%s batch=%d: %v", c.Name, batch, err)
								return
							}
						}
						v, err := candidate.fetch(c.Name, storageEpoch-256, storageEpoch)
						if err != nil || v == nil {
							t.Errorf("read during writes: series=%v err=%v", v, err)
							return
						}
					}
				})
			}
			wg.Wait()
			storageMust(t, candidate.reopen())
			for metric := 0; metric < 8; metric++ {
				c := storageConfig(fmt.Sprintf("metric-%d", metric), "1s:10m", whisper.Average, 0.5)
				storageCompare(t, oracle, candidate, c, "concurrent-metrics-reopened")
			}
		})
	}
}

// Drive the real persister path too: direct library parity alone cannot catch
// configuration wiring or an acknowledgement that drops an uncommitted batch.
func TestStoragePersisterRoundTrip(t *testing.T) {
	now := storageTestClock(t)
	for _, kind := range storageBackends {
		t.Run(kind, func(t *testing.T) {
			candidate := newStorageBackend(t, kind, now)
			oracle := newStorageBackend(t, "classic", now)
			c := storageConfig("metric", "1s:10m", whisper.Average, 0.5)
			storageMust(t, oracle.create(c))
			cache := &fakeCache{}
			p := NewWhisper(candidate.dir, WhisperSchemas{{Name: "all", Pattern: regexp.MustCompile(".*"), Retentions: whisper.NewRetentionsNoPointer(c.Retentions)}}, NewWhisperAggregation(), nil, cache.pop, cache.confirm, cache.pop)
			p.SetRequeue(cache.requeue)
			p.SetFLock(true)
			p.SetCompressed(kind == "cwhisper" || kind == "cwhisper-ooo")
			if kind == "cwhisper-ooo" {
				p.EnableOutOfOrder(1, 1<<30)
				p.outOfOrder.ticker = helper.NewHardThrottleTicker(1)
				t.Cleanup(p.outOfOrder.ticker.Stop)
			}
			count := 0
			for _, batch := range storageRecoveryBatches(kind) {
				storageMust(t, oracle.update(c.Name, batch))
				for _, point := range batch {
					cache.add(c.Name, int64(point.Time), point.Value)
					count++
				}
				p.store(c.Name)
				if cache.confirmedPoints() != count || cache.notConfirmedPoints() != 0 {
					t.Fatalf("acknowledgements: confirmed=%d want=%d pending=%d", cache.confirmedPoints(), count, cache.notConfirmedPoints())
				}
			}
			storageMust(t, candidate.reopen())
			storageCompare(t, oracle, candidate, c, "persister-reopened")
			v, err := candidate.fetch(c.Name, storageEpoch-200, storageEpoch-199)
			storageMust(t, err)
			if v == nil || len(v.values) != 1 || math.IsNaN(v.values[0]) {
				t.Fatal("persisted query is empty")
			}
		})
	}
}
