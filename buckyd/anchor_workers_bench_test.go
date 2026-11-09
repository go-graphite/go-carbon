package buckyd

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

// anchorLatencyModel replays production per-name preparation latency (p50
// 0.7 ms, p90 88 ms, p99 354 ms; mostly read waits, the host stayed idle) as
// sleeps, by log-linear interpolation between measured quantiles.
func anchorLatencyModel(n int) []time.Duration {
	knots := []struct{ q, ms float64 }{{0, 0.1}, {0.5, 0.7}, {0.89, 4}, {0.9, 88}, {0.98, 110}, {0.99, 354}, {1, 420}}
	r := rand.New(rand.NewPCG(1, 2))
	out := make([]time.Duration, n)
	for i := range out {
		q := r.Float64()
		k := 1
		for knots[k].q < q {
			k++
		}
		a, b := knots[k-1], knots[k]
		f := (q - a.q) / (b.q - a.q)
		out[i] = time.Duration(math.Exp(math.Log(a.ms)+f*(math.Log(b.ms)-math.Log(a.ms))) * float64(time.Millisecond))
	}
	return out
}

func BenchmarkArchiveAnchorCensusLatencyModel(b *testing.B) {
	latency := anchorLatencyModel(maxAnchorNames * 256)
	sorted := slices.Sorted(slices.Values(latency))
	var sum time.Duration
	for _, d := range latency {
		sum += d
	}
	b.Logf("model per-name: p50 %v p90 %v p99 %v mean %v", sorted[len(sorted)/2], sorted[len(sorted)*9/10], sorted[len(sorted)*99/100], sum/time.Duration(len(latency)))
	names := make([]string, maxAnchorNames)
	for i := range names {
		names[i] = fmt.Sprintf("m%02d", i)
	}
	repair := func(batch []time.Duration) func(context.Context, string) (chunkstore.AnchorRepairResult, error) {
		return func(_ context.Context, name string) (chunkstore.AnchorRepairResult, error) {
			var i int
			_, _ = fmt.Sscanf(name, "m%d", &i)
			time.Sleep(batch[i])
			return chunkstore.AnchorRepairResult{}, nil
		}
	}
	for _, c := range anchorBenchCases {
		b.Run(c.String(), func(b *testing.B) {
			runAnchorCensus(b, c, func(request int) {
				offset := request * maxAnchorNames % len(latency)
				anchorRows(context.Background(), names, c.workers, repair(latency[offset:offset+maxAnchorNames]))
			})
		})
	}
}

// clients > 1 models a census driver keeping that many requests in flight,
// which needs max-maintenance-requests >= clients.
type anchorBenchCase struct{ clients, workers int }

func (c anchorBenchCase) String() string {
	return fmt.Sprintf("clients=%d/workers=%d", c.clients, c.workers)
}

var anchorBenchCases = []anchorBenchCase{{1, 1}, {1, 8}, {1, 16}, {1, 32}, {2, 8}, {4, 8}, {8, 8}}

func runAnchorCensus(b *testing.B, c anchorBenchCase, request func(int)) {
	var next atomic.Int64
	var wg sync.WaitGroup
	b.ResetTimer()
	for range c.clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := int(next.Add(1)) - 1; j < b.N; j = int(next.Add(1)) - 1 {
				request(j)
			}
		}()
	}
	wg.Wait()
	b.ReportMetric(float64(b.N*maxAnchorNames)/b.Elapsed().Seconds(), "names/s")
}

// A real store with a heavy-tailed mix per 32-name dry-run request: 26 small
// classic files (half misplaced), 4 medium and 2 large dense archives.
func BenchmarkArchiveAnchorCensusFixture(b *testing.B) {
	db := anchorStore(b)
	var metrics []anchorMetric
	for i := range maxAnchorNames {
		m := anchorMetric{name: fmt.Sprintf("census.b%02d", i), layout: []string{"relative", "absolute"}[i%2], shift: int64(i % 30)}
		switch {
		case i%16 == 7:
			m.heavy = 200000
		case i%8 == 3:
			m.heavy = 20000
		}
		metrics = append(metrics, m)
	}
	importAnchorFixture(b, db, metrics)
	names := make([]string, len(metrics))
	for i, m := range metrics {
		names[i] = m.name
	}
	for _, m := range metrics[:8] {
		start := time.Now()
		if _, err := db.RepairArchiveAnchors(context.Background(), m.name, false); err != nil {
			b.Fatal(err)
		}
		b.Logf("%s heavy=%d: %v", m.name, m.heavy, time.Since(start))
	}
	for _, c := range anchorBenchCases {
		b.Run(c.String(), func(b *testing.B) {
			runAnchorCensus(b, c, func(int) {
				rows := anchorRows(context.Background(), names, c.workers, func(ctx context.Context, name string) (chunkstore.AnchorRepairResult, error) {
					return db.RepairArchiveAnchors(ctx, name, false)
				})
				if len(rows) != len(names) || rows[0].Error != "" {
					b.Errorf("rows %d %+v", len(rows), rows[0])
				}
			})
		})
	}
}
