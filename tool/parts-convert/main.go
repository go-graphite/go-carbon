//go:build linux || darwin

// parts-convert is an offline synthetic qualification tool. It never opens a
// legacy store or connects to a running daemon. Production source/routing
// adapters must be certified separately before using the package online.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/partsconvert"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	"golang.org/x/sys/unix"
)

type config struct {
	dir                                                               string
	workers, shards, metrics, points, archives, chunkMiB, blockPoints int
	minFreeGiB                                                        uint64
	partitionSeconds                                                  int64
}
type result struct {
	Synthetic                                                                                  bool
	Workers, Shards, MetricsPerShard, PointsPerMetric, Archives                                int
	Points, SSTBytes, IngestL0Bytes, CompactionBytes, L0Files, DebtBytes                       uint64
	Seconds, CPUSeconds, Cores, PointsPerSecond, TBPerHour, LogicalMBPerSecond, SSTMBPerSecond float64
	VerifyWorkerSeconds                                                                        float64
	FinalAllocatedBytes                                                                        uint64
	GOMAXPROCS                                                                                 int
}

func main() {
	c := config{}
	baseline := flag.Bool("compare-coldpack", false, "compare actual legacy cold-pack and converted SST bytes on identical stored slots")
	flag.StringVar(&c.dir, "dir", "", "new, private output directory (required)")
	flag.IntVar(&c.workers, "workers", 1, "parallel vshards/physical Pebble owners")
	flag.IntVar(&c.shards, "shards", 16, "number of routing vshards to convert")
	flag.IntVar(&c.metrics, "metrics-per-shard", 100, "canonical metrics per vshard")
	flag.IntVar(&c.points, "points-per-metric", 10080, "stored raw slots per metric")
	flag.IntVar(&c.archives, "archives", 1, "number of existing archives (1 to 4)")
	flag.IntVar(&c.chunkMiB, "chunk-mib", 32, "bounded sort buffer per worker")
	flag.IntVar(&c.blockPoints, "block-points", 4096, "stored slots per compressed archive block")
	flag.Int64Var(&c.partitionSeconds, "partition-seconds", 86400, "synthetic time partition width; larger widths trade expiry granularity for compression")
	flag.Uint64Var(&c.minFreeGiB, "min-free-gib", 8, "free space left for other users")
	flag.Parse()
	if *baseline {
		if c.dir == "" || c.shards < 1 || c.metrics < 1 || c.points < 1 || c.archives < 1 || c.archives > 4 {
			fmt.Fprintln(os.Stderr, "ERROR: invalid comparison configuration")
			os.Exit(1)
		}
		r, err := compare(context.Background(), c)
		if err != nil {
			fmt.Fprintln(os.Stderr, "ERROR:", err)
			os.Exit(1)
		}
		if err = json.NewEncoder(os.Stdout).Encode(r); err != nil {
			fmt.Fprintln(os.Stderr, "ERROR:", err)
			os.Exit(1)
		}
		return
	}
	r, err := run(context.Background(), c)
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
	if err = json.NewEncoder(os.Stdout).Encode(r); err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
}

type measuredTarget struct {
	partsconvert.PebbleTarget
	verifyNS atomic.Int64
}

func (t *measuredTarget) Verify(ctx context.Context, tables []partsconvert.Table) error {
	start := time.Now()
	err := t.PebbleTarget.Verify(ctx, tables)
	t.verifyNS.Add(time.Since(start).Nanoseconds())
	return err
}

func run(ctx context.Context, c config) (result, error) {
	r := result{Synthetic: true, Workers: c.workers, Shards: c.shards, MetricsPerShard: c.metrics, PointsPerMetric: c.points, Archives: c.archives, GOMAXPROCS: runtime.GOMAXPROCS(0)}
	if c.dir == "" || c.workers < 1 || c.shards < 1 || c.shards > partsformat.VirtualShards || c.metrics < 1 || c.points < 1 || c.archives < 1 || c.archives > 4 || c.chunkMiB < 1 || c.chunkMiB > 1024 || c.minFreeGiB > (^uint64(0)>>30) {
		return r, errors.New("invalid synthetic conversion configuration")
	}
	// A new-only path avoids accidentally benchmarking over an unrelated DB.
	if err := os.Mkdir(c.dir, 0750); err != nil {
		return r, err
	}
	guard := &partsconvert.SpaceGuard{Path: c.dir, MinFree: c.minFreeGiB << 30}
	start := time.Now()
	cpuStart := cpuSeconds()
	var next atomic.Int64
	var l0, points, sst, debt, compaction, verifyNS atomic.Uint64
	var wg sync.WaitGroup
	errCh := make(chan error, c.workers)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	identity := fmt.Sprintf("synthetic-stored-v2:%d:%d:%d:%d:%d:%d", c.shards, c.metrics, c.points, c.archives, max(1, c.blockPoints), c.partitionSeconds)
	for worker := 0; worker < c.workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			dir := filepath.Join(c.dir, fmt.Sprintf("owner-%04d", worker))
			opts := &pebble.Options{MemTableSize: 16 << 20, MaxConcurrentCompactions: func() int { return 2 }}
			opts.Experimental.TableCacheShards = 1
			db, err := pebble.Open(dir, opts)
			if err != nil {
				errCh <- err
				cancel()
				return
			}
			target := &measuredTarget{PebbleTarget: partsconvert.PebbleTarget{DB: db, DestinationID: identity + fmt.Sprint(worker), OnIngest: func(s pebble.IngestOperationStats) { l0.Add(s.ApproxIngestedIntoL0Bytes) }}}
			defer func() {
				m := db.Metrics()
				debt.Add(uint64(max(0, m.Compact.EstimatedDebt)))
				for _, level := range m.Levels {
					compaction.Add(level.BytesCompacted)
				}
				if err := db.Close(); err != nil {
					errCh <- err
					cancel()
				}
				verifyNS.Add(uint64(target.verifyNS.Load()))
			}()
			for {
				index := next.Add(1) - 1
				if index >= int64(c.shards) || ctx.Err() != nil {
					return
				}
				u := partsconvert.Unit{Group: "synthetic", VShard: uint16(index)}
				source := synthetic(c, u, identity)
				converter := partsconvert.Converter{Directory: filepath.Join(dir, "conversion"), Source: source, Target: target, Space: guard, ChunkBytes: c.chunkMiB << 20, MaxTables: 100000}
				receipt, err := converter.Run(ctx, u)
				if err != nil {
					errCh <- err
					cancel()
					return
				}
				points.Add(receipt.Points)
				sst.Add(receipt.OutputBytes)
			}
		}(worker)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return r, err
		}
	}
	r.Seconds = time.Since(start).Seconds()
	r.CPUSeconds = cpuSeconds() - cpuStart
	r.Cores = r.CPUSeconds / r.Seconds
	r.Points = points.Load()
	r.SSTBytes = sst.Load()
	r.IngestL0Bytes = l0.Load()
	r.CompactionBytes = compaction.Load()
	r.DebtBytes = debt.Load()
	r.PointsPerSecond = float64(r.Points) / r.Seconds
	r.TBPerHour = float64(r.SSTBytes) * 3600 / r.Seconds / 1e12
	r.LogicalMBPerSecond = float64(r.Points) * 16 / r.Seconds / 1e6
	r.SSTMBPerSecond = float64(r.SSTBytes) / r.Seconds / 1e6
	r.VerifyWorkerSeconds = float64(verifyNS.Load()) / 1e9
	seen := make(map[[2]uint64]bool)
	err := filepath.Walk(c.dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		var st unix.Stat_t
		if err := unix.Stat(path, &st); err != nil {
			return err
		}
		id := [2]uint64{uint64(st.Dev), uint64(st.Ino)}
		if !seen[id] {
			seen[id] = true
			r.FinalAllocatedBytes += uint64(st.Blocks) * 512
		}
		return nil
	})
	return r, err
}

func synthetic(c config, u partsconvert.Unit, identity string) partsconvert.ArchiveSource {
	blockPoints := c.blockPoints
	if blockPoints == 0 {
		blockPoints = 4096
	}
	partitionSeconds := c.partitionSeconds
	if partitionSeconds == 0 {
		partitionSeconds = 86400
	}
	return partsconvert.ArchiveSource{SnapshotID: identity, PartitionSeconds: partitionSeconds, BlockPoints: blockPoints, EmptyPartition: 20000,
		UpperBound: func(context.Context, partsconvert.Unit) (uint64, error) {
			// Covers incompressible values, keys, per-table overhead and policy.
			n := uint64(c.metrics) * uint64(c.points) * uint64(c.archives)
			if n > math.MaxUint64/64 {
				return 0, errors.New("synthetic size overflow")
			}
			return n*64 + (64 << 20), nil
		},
		ScanMetrics: func(ctx context.Context, _ partsconvert.Unit, yield func(partsconvert.StoredMetric) error) error {
			found := 0
			for candidate := uint64(0); found < c.metrics; candidate++ {
				if err := ctx.Err(); err != nil {
					return err
				}
				name := partsformat.ID(fmt.Sprintf("synthetic.namespace.service.host.metric.%016x", candidate))
				if partsformat.Route(name) != u.VShard {
					continue
				}
				m := partsconvert.StoredMetric{Name: name, Group: partsformat.Group(u.Group), Method: 1, XFilesFactorBits: math.Float32bits(.5)}
				seed := sha256.Sum256([]byte(name))
				x := uint64(0)
				for _, b := range seed[:8] {
					x = x<<8 | uint64(b)
				}
				for a := 0; a < c.archives; a++ {
					step := uint32(60)
					count := c.points
					for i := 0; i < a; i++ {
						step *= 10
						count = max(1, count/4)
					}
					ar := partsconvert.StoredArchive{Step: step, Slots: uint32(count), Points: make([]partsconvert.StoredPoint, count)}
					for i := range ar.Points {
						x ^= x << 13
						x ^= x >> 7
						x ^= x << 17
						bits := math.Float64bits(float64(x%100000) / 100)
						if i%2 == 0 {
							bits = x
						} // high entropy half; includes arbitrary IEEE bits
						ar.Points[i] = partsconvert.StoredPoint{Timestamp: 1728000000 + int64(i)*int64(step), ValueBits: bits}
					}
					m.Archives = append(m.Archives, ar)
				}
				if err := yield(m); err != nil {
					return err
				}
				found++
			}
			return nil
		}}
}

func cpuSeconds() float64 {
	var r unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_SELF, &r); err != nil {
		return math.NaN()
	}
	return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
}
