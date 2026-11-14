package cache

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

// Compare this receipt with the previous implementation. It covers independently
// durable input, several streams, reverse source order, requeue, and exact values.
func TestReplaySourceDifferential(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	var streams [3]RecoveryStream
	sources := make([]*RecoverySource, 12)
	for i := range sources {
		sources[i] = streams[i%len(streams)].Next()
	}
	names := make([]string, 128)
	for i := range names {
		names[i] = fmt.Sprintf("%s.%d", strings.Repeat("shared.prefix.", 25), i)
	}
	digest := sha256.New()
	put := func(v uint64) { var buf [8]byte; binary.LittleEndian.PutUint64(buf[:], v); digest.Write(buf[:]) }
	record := func(p *points.Points) {
		digest.Write([]byte(p.Metric))
		for _, d := range p.Data {
			put(math.Float64bits(d.Value))
			put(uint64(d.Timestamp))
		}
	}
	rng := rand.New(rand.NewPCG(8144, 1298))
	bits := []uint64{0, 1 << 63, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000042, 1, 0x000fffffffffffff}
	for i := range 12000 {
		name := names[rng.IntN(len(names))]
		p := points.OnePoint(name, math.Float64frombits(bits[i%len(bits)]), int64(i%71-35))
		if i%2 == 0 {
			c.AddRecoveredFrom(p, sources[rng.IntN(len(sources))])
		} else {
			c.AddCapturedFrom(p, sources[rng.IntN(len(sources))])
		}
		if i%19 == 0 {
			p, ok := c.PopNotConfirmed(name)
			if !ok {
				t.Fatal("missing batch")
			}
			if i%3 == 0 {
				c.Requeue(p)
			} else {
				record(p)
				c.Confirm(p)
			}
		}
		if i%31 == 0 {
			for _, s := range sources {
				put(uint64(s.Pending()))
			}
		}
	}
	for _, name := range names {
		if p, ok := c.PopNotConfirmed(name); ok {
			record(p)
			c.Confirm(p)
		}
	}
	for _, s := range sources {
		if s.Pending() != 0 {
			t.Fatal("retained source", s.Pending())
		}
		put(uint64(s.Pending()))
	}
	put(uint64(c.Size()))
	result := fmt.Sprintf("%x\n", digest.Sum(nil))
	if result != "63bf7d7a0523b6ac2eaadb15ea940575e0248327a9966ed79543eaf6965b1b29\n" {
		t.Fatalf("point/source receipt changed: %s", result)
	}
	if path := os.Getenv("CAPTURE_REPLAY_ORACLE"); path != "" {
		if err := os.WriteFile(path, []byte(result), 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Log(result)
}

// Opt-in production-cardinality replay admission/application. Seeded random
// received names defeat interned-string equality and fit no CPU cache. Fixture
// construction is excluded; repeated arrivals share an ordered durable source.
var replayFixture struct {
	sync.Once
	cache  *Cache
	names  []string
	source *RecoverySource
}

func BenchmarkCaptureReplayResident(b *testing.B) {
	n, _ := strconv.Atoi(os.Getenv("CAPTURE_REPLAY_NAMES"))
	if n == 0 {
		b.Skip("set CAPTURE_REPLAY_NAMES=20000000")
	}
	replayFixture.Do(func() {
		c := New()
		c.SetMaxSize(0)
		names := make([]string, n)
		var stream RecoveryStream
		source := stream.Next()
		var wg sync.WaitGroup
		prefix := strings.Repeat("namespace.", 24)
		for worker := range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := worker; i < n; i += 8 {
					names[i] = fmt.Sprintf("service.metric.%08d.%s", i, prefix)
					c.AddRecoveredFrom(&points.Points{Metric: names[i], Data: []points.Point{{Value: 1, Timestamp: 1700000000}, {Value: 2, Timestamp: 1700000001}}}, source)
				}
			}()
		}
		wg.Wait()
		runtime.GC()
		replayFixture.cache, replayFixture.names, replayFixture.source = c, names, source
	})
	c, names, source := replayFixture.cache, replayFixture.names, replayFixture.source
	before := c.Size()
	b.ReportAllocs()
	b.ResetTimer()
	cpu := compactCPUTime()
	var workers atomic.Uint64
	b.RunParallel(func(pb *testing.PB) {
		var buf [320]byte
		state := workers.Add(1)
		var p points.Points
		data := [1]points.Point{{Value: 3, Timestamp: 1700000002}}
		p.Data = data[:]
		for pb.Next() {
			state = state*6364136223846793005 + 1
			name := names[(state>>32)%uint64(n)]
			copy(buf[:], name)
			raw := buf[:len(name)]
			c.RecoveryMetricBytes(raw)
			p.Metric = c.RecoveryMetricName(raw)
			c.AddRecoveredFrom(&p, source)
		}
	})
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "points/s")
	b.ReportMetric(float64(compactCPUTime()-cpu)/float64(b.N), "cpu-ns/point")
	if c.Size() != before+int64(b.N) {
		b.Fatal("replay point count", c.Size())
	}
}

func TestReplayDemandCountsDurableIngress(t *testing.T) {
	c := New()
	c.AddRecovered(points.OnePoint("checkpoint", 1, 1))
	c.AddRestored(points.OnePoint("dump", 1, 1))
	var stream RecoveryStream
	source := stream.Next()
	c.AddRecoveredFrom(points.OnePoint("capture", 2, 2), source)
	c.AddCapturedFrom(points.OnePoint("capture", 3, 3), source)
	p, ok := c.PopNotConfirmed("capture")
	if !ok {
		t.Fatal("missing capture")
	}
	c.Requeue(p)
	s := c.WriteoutState()
	if s.ReplayPoints != 2 || s.LivePoints != 0 {
		t.Fatalf("replayed/restored attribution: %+v", s)
	}
}
