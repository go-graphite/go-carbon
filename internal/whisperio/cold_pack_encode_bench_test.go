package whisperio

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"syscall"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// Opt-in real cold-pack path: equal value-family sensitivity, including the
// cold-pack yield corpus's integer/fractional formulas, three complete sweeps.
func TestColdPackEncodeThroughput(t *testing.T) {
	if os.Getenv("SEALED_ENCODE_BENCH") != "1" {
		t.Skip("local throughput measurement")
	}
	cpu := func() int64 {
		var u syscall.Rusage
		if err := syscall.Getrusage(syscall.RUSAGE_SELF, &u); err != nil {
			t.Fatal(err)
		}
		return (u.Utime.Sec+u.Stime.Sec)*1e9 + int64(u.Utime.Usec+u.Stime.Usec)*1000
	}
	for _, mode := range []string{"exact", "decimal"} {
		for trial := range 3 {
			const count = 12288
			now := time.Unix(coldBase+count*60+3600, 0)
			opts := coldOptions(&ContinuationCodec{ReuseBlockKeys: true})
			opts.Now = func() time.Time { return now }
			opts.CacheSize = 8 << 20
			opts.MemTableSize = 8 << 20
			db, err := store.Open(t.TempDir(), opts)
			if err != nil {
				t.Fatal(err)
			}
			db.StopBackground()
			ctx := context.Background()
			var before []store.Snapshot
			for series := range 40 {
				kind := series % 10
				name := fmt.Sprintf("cold.%03d", series)
				if _, err := db.Create(ctx, store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 60, Count: 20000}}, AggregationMethod: store.Sum}); err != nil {
					t.Fatal(err)
				}
				p := make([]store.Point, count)
				r := rand.New(rand.NewSource(int64(series + 42)))
				counter := 0
				for i := range p {
					v := float64((i*17 + int(r.Int31n(7))) % 1000)
					switch kind {
					case 0:
						v = 7
					case 1:
						v = float64(1000000 + i*123)
					case 2, 3:
						counter += r.Intn(128)
						if kind == 3 && i%997 == 996 {
							counter = 0
						}
						v = float64(counter)
					case 4:
						v = float64(r.Intn(1000000) - 500000)
					case 5:
						v = math.Round(10000 + 1000*math.Sin(float64(i)*.01))
					case 7:
						v = 1000 + math.Sin(float64(i)*.013)*31.123456789 + float64(r.Int31n(1000))/997
					case 8:
						v = float64(r.Intn(100000)) / 100
					case 9:
						v = math.Float64frombits(0x3ff0000000000000 | r.Uint64()>>12)
					}
					p[i] = store.Point{Timestamp: coldBase + int64(i*60), Value: v}
				}
				if err := db.UpdateMany(ctx, name, p); err != nil {
					t.Fatal(err)
				}
				snap, err := db.Snapshot(ctx, name)
				if err != nil {
					t.Fatal(err)
				}
				before = append(before, snap)
			}
			if err := db.Compact(); err != nil {
				t.Fatal(err)
			}
			now = now.Add(3 * 24 * time.Hour)
			if err := db.ConfigureColdPack(store.ColdPackOptions{Mode: mode, WriteWindow: 24 * time.Hour, CPUFraction: 1, BytesPerSecond: 1 << 40}); err != nil {
				t.Fatal(err)
			}
			runtime.GC()
			var a, z runtime.MemStats
			runtime.ReadMemStats(&a)
			start := time.Now()
			startCPU := cpu()
			var latencies []int64
			for n := 0; ; n++ {
				ts := time.Now()
				done, err := db.ColdPackStep(ctx)
				latencies = append(latencies, time.Since(ts).Nanoseconds())
				if err != nil {
					t.Fatal(err)
				}
				if done && db.ColdPackStats().Points == 40*count {
					break
				}
				if n > 10000 {
					t.Fatal("no completion")
				}
			}
			cpuNS := cpu() - startCPU
			wallNS := time.Since(start).Nanoseconds()
			runtime.ReadMemStats(&z)
			st := db.ColdPackStats()
			if st.Points != 40*count {
				t.Fatal("incomplete sealing")
			}
			for i, snap := range before {
				got, err := db.Snapshot(ctx, fmt.Sprintf("cold.%03d", i))
				if err != nil {
					t.Fatal(err)
				}
				coldEqual(t, snap, got, mode == "decimal")
			}
			sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
			t.Logf("THROUGHPUT mode=%s trial=%d points=%d blocks=%d input=%d output=%d conflicts=%d cpu_ns=%d wall_ns=%d alloc_bytes=%d allocs=%d step_p50_ns=%d step_p99_ns=%d", mode, trial, st.Points, st.Blocks, st.InputBytes, st.OutputBytes, st.Conflicts, cpuNS, wallNS, z.TotalAlloc-a.TotalAlloc, z.Mallocs-a.Mallocs, latencies[len(latencies)/2], latencies[min(len(latencies)-1, len(latencies)*99/100)])
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
}
