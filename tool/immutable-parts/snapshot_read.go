package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	pr "github.com/go-graphite/go-carbon/internal/partsread"
)

// snapshotCoreSource adapts the offline owner's raw, immutable part format.
// It scans every retained time partition of a metric, so a newer physical ring
// slot outside the query range cannot resurrect an overwritten old timestamp.
// Production wiring must obtain retained partitions from the pinned manifest.
type snapshotCoreSource struct {
	snap       *pebble.Snapshot
	partitions []int64
}

func (s *snapshotCoreSource) iter(id uint64, part int64) (*pebble.Iterator, error) {
	var lo, hi [21]byte
	lo[0] = 1
	binary.BigEndian.PutUint64(lo[1:9], uint64(part)^(1<<63))
	binary.BigEndian.PutUint64(lo[9:17], id)
	copy(hi[:], lo[:])
	binary.BigEndian.PutUint32(hi[17:], 1)
	return s.snap.NewIter(&pebble.IterOptions{LowerBound: lo[:], UpperBound: hi[:]})
}
func (s *snapshotCoreSource) HasArchive(ctx context.Context, id uint64, step int64) (bool, error) {
	if step != 1 {
		return false, fmt.Errorf("raw core source has no step%d", step)
	}
	for _, part := range s.partitions {
		if e := ctx.Err(); e != nil {
			return false, e
		}
		it, e := s.iter(id, part)
		if e != nil {
			return false, e
		}
		found := it.First()
		e = errors.Join(it.Error(), it.Close())
		if e != nil || found {
			return found, e
		}
	}
	return false, nil
}
func (s *snapshotCoreSource) Scan(ctx context.Context, id uint64, a pr.Archive, from, until int64, yield func(pr.Sample) error) error {
	if a.Step != 1 {
		return fmt.Errorf("raw core source has no step%d", a.Step)
	}
	n := (until - from) / a.Step
	for _, part := range s.partitions {
		if e := ctx.Err(); e != nil {
			return e
		}
		it, e := s.iter(id, part)
		if e != nil {
			return e
		}
		for ok := it.First(); ok; ok = it.Next() {
			if e = ctx.Err(); e != nil {
				break
			}
			v := it.Value()
			if len(v)%24 != 0 {
				e = fmt.Errorf("invalid raw block length%d", len(v))
				break
			}
			for len(v) > 0 {
				ts := int64(binary.LittleEndian.Uint64(v))
				slot := ((ts/a.Step)%a.Points - (from/a.Step)%a.Points + a.Points) % a.Points
				if slot < 0 {
					slot += a.Points
				}
				if slot < n {
					e = yield(pr.Sample{Time: ts, Bits: binary.LittleEndian.Uint64(v[8:]), Sequence: binary.LittleEndian.Uint64(v[16:])})
					if e != nil {
						break
					}
				}
				v = v[24:]
			}
			if e != nil {
				break
			}
		}
		e = errors.Join(e, it.Error(), it.Close())
		if e != nil {
			return e
		}
	}
	return nil
}

type snapshotReadStats struct {
	count, empty, failed, admission, points uint64
	maxLatencyUS                            uint64
	bins                                    [100002]uint64
	firstError                              string
}

func (s *snapshotReadStats) record(d time.Duration) {
	us := uint64((d + time.Microsecond - 1) / time.Microsecond)
	s.maxLatencyUS = max(s.maxLatencyUS, us)
	s.bins[min(us, uint64(len(s.bins)-1))]++
	s.count++
}
func (s *snapshotReadStats) quantile(q float64) uint64 {
	target := uint64(float64(s.count) * q)
	var n uint64
	for i, c := range s.bins {
		n += c
		if n >= max(target, 1) {
			if i == len(s.bins)-1 {
				return s.maxLatencyUS
			}
			return uint64(i)
		}
	}
	return 0
}

// runSnapshotReadBenchmark uses the exact same synthetic producer/core benchmark
// as the write-only experiment. Readers pin the frozen head + sealed DB view.
// All snapshots are released in the end sentinel before the owner DBs close.
func runSnapshotReadBenchmark(out string, cfg coreConfig, duration time.Duration, rate, metrics uint64, settle time.Duration, readers int) error {
	if readers < 0 || metrics == 0 {
		return os.ErrInvalid
	}
	pubs := make([]*pr.Publication, cfg.Shards)
	for i := range pubs {
		pubs[i] = &pr.Publication{}
	}
	cat := pr.CatalogFuncs{
		LookupFunc: func(name string) (pr.Metric, bool) {
			id, e := strconv.ParseUint(strings.TrimPrefix(name, "metric."), 10, 64)
			return pr.Metric{ID: id, Archives: []pr.Archive{{Step: 1, Points: 86400}}}, e == nil && id < metrics && strings.HasPrefix(name, "metric.")
		},
		FindFunc: func(ctx context.Context, pattern string, yield func(string) error) error {
			if pattern != "metric.*" {
				return fmt.Errorf("benchmark catalog supports metric.* only")
			}
			for id := uint64(0); id < metrics; id++ {
				if e := ctx.Err(); e != nil {
					return e
				}
				if e := yield(fmt.Sprintf("metric.%d", id)); e != nil {
					return e
				}
			}
			return nil
		},
	}
	var partitions []int64
	for p := corePartition(1800000000, cfg.PartitionSeconds); p <= corePartition(1800000000+int64(duration.Seconds())+2, cfg.PartitionSeconds); p++ {
		partitions = append(partitions, p)
	}
	budget := pr.NewBudget(512 << 20)
	for _, p := range pubs {
		p.Publish(pr.View{Catalog: cat})
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	done := make(chan *snapshotReadStats, readers)
	start := time.Now()
	warmup := 3 * time.Second
	for worker := 0; worker < readers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			stats := new(snapshotReadStats)
			defer func() { done <- stats }()
			select {
			case <-ctx.Done():
				return
			case <-time.After(warmup):
			}
			rng := uint64(worker + 1)
			adapter := pr.Adapter{Select: func(name string) *pr.Publication { m, _ := cat.Lookup(name); return pubs[coreShard(m.ID, cfg.Shards)] }, Catalog: pubs[0], Budget: budget, WorkBytes: 4 << 20, Now: func() time.Time { return time.Unix(1800000000+int64(time.Since(start).Seconds()), 0) }}
			for ctx.Err() == nil {
				rng ^= rng << 13
				rng ^= rng >> 7
				rng ^= rng << 17
				id := rng % metrics
				name := fmt.Sprintf("metric.%d", id)
				now := adapter.Now().Unix()
				width := int64(3600)
				if rng%4 == 0 {
					width = 86400
				}
				t := time.Now()
				err := adapter.Fetch(ctx, name, now-width, now, func(s *pr.Series) error {
					if s == nil {
						stats.empty++
						return nil
					}
					var populated uint64
					for _, v := range s.Values {
						if v == v {
							populated++
						}
					}
					if populated == 0 {
						stats.empty++
					}
					stats.points += populated
					return nil
				})
				if errors.Is(err, context.Canceled) {
					return
				}
				if err != nil {
					stats.failed++
					if errors.Is(err, pr.ErrAdmission) {
						stats.admission++
					}
					if stats.firstError == "" {
						stats.firstError = err.Error()
					}
				}
				stats.record(time.Since(t))
			}
		}(worker)
	}
	var stop sync.Once
	var readingSeconds float64
	shutdown := func() {
		stop.Do(func() {
			readingSeconds = max(0, time.Since(start).Seconds()-warmup.Seconds())
			cancel()
			wg.Wait()
			for _, p := range pubs {
				p.Close()
			}
		})
	}
	defer shutdown()
	hook := func(shard int, snap *pebble.Snapshot, head []row) error {
		if shard < 0 {
			shutdown()
			return nil
		}
		records := make([]pr.Record, len(head))
		for i, r := range head {
			records[i] = pr.Record{ID: r.ID, Step: 1, Sample: pr.Sample{Time: r.Time, Bits: r.Bits, Sequence: r.Seq}}
		}
		frozen := pr.Freeze(records)
		source := &snapshotCoreSource{snap: snap, partitions: partitions}
		pubs[shard].Publish(pr.View{Catalog: cat, Head: []pr.Source{frozen}, Parts: []pr.Source{source}, RetainedBytes: frozen.RetainedBytes() + 65536, Close: func() { _ = snap.Close() }})
		return nil
	}
	err := coreBenchmarkWithHook(out, cfg, duration, rate, metrics, settle, hook)
	shutdown()
	var total snapshotReadStats
	for range readers {
		s := <-done
		total.maxLatencyUS = max(total.maxLatencyUS, s.maxLatencyUS)
		total.count += s.count
		total.empty += s.empty
		total.failed += s.failed
		total.admission += s.admission
		total.points += s.points
		for i, n := range s.bins {
			total.bins[i] += n
		}
		if total.firstError == "" {
			total.firstError = s.firstError
		}
	}
	rateReads := float64(0)
	if readingSeconds > 0 {
		rateReads = float64(total.count) / readingSeconds
	}
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"stage": "snapshot_reads", "when_cet": time.Now().In(time.FixedZone("CET", 3600)).Format(time.RFC3339), "readers": readers, "reads": total.count, "reads_per_second": rateReads, "active_seconds": readingSeconds, "warmup_seconds": warmup.Seconds(), "read_p50_us_upper": total.quantile(.5), "read_p99_us_upper": total.quantile(.99), "latency_over_100ms": total.bins[len(total.bins)-1], "empty_reads": total.empty, "failed_reads": total.failed, "admission_rejections": total.admission, "first_error": total.firstError, "populated_slots": total.points, "read_budget_bytes": 512 << 20, "budget_after_close": budget.Used(), "workload": "synthetic_preidentified_rows_with_populated_raw_1h_1d_renders"})
	return err
}
