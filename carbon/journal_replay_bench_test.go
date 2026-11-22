//go:build unix

package carbon

import (
	"fmt"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
	"github.com/klauspost/compress/zstd"
	"github.com/zeebo/xxh3"
)

type journalBenchSource struct {
	mu     sync.Mutex
	shards [cache.ShardCount][]inputlog.Entry
}

func (s *journalBenchSource) TakeLogEntries(shard int, _ uint64, spare []inputlog.Entry) []inputlog.Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.shards[shard]
	s.shards[shard] = spare[:0]
	return out
}
func (*journalBenchSource) LogLowWater() uint64         { return 0 }
func (*journalBenchSource) RelogBefore(uint64, int) int { return 0 }
func (*journalBenchSource) LogDropped() uint64          { return 0 }

// The fixture stays private. Measure the real journal/cache/cursor integration,
// including admission/name resolution and the normal replay worker topology.
func BenchmarkJournalReplayFixture(b *testing.B) {
	path := os.Getenv("CAPTURE_BATCH_FIXTURE")
	if path == "" {
		b.Skip("set CAPTURE_BATCH_FIXTURE")
	}
	wire, err := os.ReadFile(path)
	if err != nil {
		b.Fatal(err)
	}
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		b.Fatal(err)
	}
	raw, n, err := recovery.DecodeCaptureFrame(dec, wire, nil)
	dec.Close()
	if err != nil {
		b.Fatal(err)
	}
	var records []*points.Points
	if err = points.ReadBinaryBytesBorrowed(raw, func(name []byte, data []points.Point) {
		records = append(records, &points.Points{Metric: string(name), Data: append([]points.Point(nil), data...)})
	}); err != nil {
		b.Fatal(err)
	}
	const cycles = 256
	dir := b.TempDir()
	opts := inputlog.Options{SyncInterval: time.Hour, SegmentSize: 16 << 20, MaxAge: 2 * time.Hour}
	log, err := inputlog.Open(dir, opts)
	if err != nil {
		b.Fatal(err)
	}
	src := &journalBenchSource{}
	log.Start(src, cache.ShardCount)
	want := make(map[string][]points.Point)
	refs := make(map[string]*inputlog.StreamRef)
	for range cycles {
		for _, p := range records {
			ref := refs[p.Metric]
			if ref == nil {
				ref = inputlog.NewStreamRef(p.Metric)
				refs[p.Metric] = ref
			}
			shard := xxh3.HashString(p.Metric) & (cache.ShardCount - 1)
			for _, v := range p.Data {
				src.shards[shard] = append(src.shards[shard], ref.Entry(v.Value, v.Timestamp))
			}
			want[p.Metric] = append(want[p.Metric], p.Data...)
		}
		if err := log.Sync(); err != nil {
			b.Fatal(err)
		}
	}
	log.Kill()
	cpu := func() float64 {
		var r syscall.Rusage
		_ = syscall.Getrusage(syscall.RUSAGE_SELF, &r)
		return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
	}
	var cpuSeconds float64
	b.ReportAllocs()
	b.ResetTimer()
	b.StopTimer()
	for range b.N {
		core := cache.New()
		core.SetMaxSize(0)
		core.SetRecoveryMemory(func() (uint64, bool) { return 128 << 30, true })
		for i := range 200000 {
			core.AddRecovered(points.OnePoint(fmt.Sprintf("resident.metric.%09d", i), 1, 1700000000))
		}
		log, err = inputlog.Open(dir, opts)
		if err != nil {
			b.Fatal(err)
		}
		core.EnableInputLog()
		app := New("")
		app.Config = NewConfig()
		app.Config.InputLog.Path = dir
		app.startupInput = &startupInput{}
		app.inputLog.Store(log)
		runtime.GC()
		var sampled chan struct{}
		if path := os.Getenv("JOURNAL_REPLAY_STACKS"); path != "" {
			sampled = make(chan struct{})
			go func() {
				defer close(sampled)
				time.Sleep(2 * time.Millisecond)
				file, err := os.Create(path)
				if err == nil {
					_ = pprof.Lookup("goroutine").WriteTo(file, 2)
					_ = file.Close()
				}
			}()
		}
		before := cpu()
		b.StartTimer()
		err = app.replayInputLogStopped(core, nil)
		b.StopTimer()
		cpuSeconds += cpu() - before
		if sampled != nil {
			<-sampled
		}
		if err != nil {
			b.Fatal(err)
		}
		log.Kill()
		if core.Size() != int64(200000+n*cycles) {
			b.Fatal("count", core.Size(), n)
		}
		for name, expected := range want {
			got := core.Get(name)
			if len(got) != len(expected) {
				b.Fatal("metric count", name, len(got), len(expected))
			}
			for i, p := range got {
				w := expected[i]
				if p.Timestamp != w.Timestamp || math.Float64bits(p.Value) != math.Float64bits(w.Value) {
					b.Fatal("point order/bits", name, i)
				}
			}
		}
	}
	total := float64(b.N * n * cycles)
	b.ReportMetric(cpuSeconds*1e6/total, "cpu-s/Mpt")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/total, "ns/pt")
	b.ReportMetric(float64(n*cycles), "points/op")
}
