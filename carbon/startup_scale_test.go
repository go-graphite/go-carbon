package carbon

import (
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"runtime/metrics"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver"
	"github.com/go-graphite/go-carbon/receiver/tcp"
	"github.com/lomik/zapwriter"
)

func captureFixture(t testing.TB) (*startupInput, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "input.1.2.tail.bin")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	s, err := openStartupInput(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = New("")
	opts := tcp.NewOptions()
	opts.Listen = "127.0.0.1:0"
	ro, err := receiver.WithProtocol(opts, "tcp")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := receiver.New("tcp", ro, func(*points.Points) {})
	if err != nil {
		t.Fatal(err)
	}
	original := raw.(*tcp.TCP)
	address := original.Addr().String()
	h, err := original.Detach()
	if err != nil {
		t.Fatal(err)
	}
	opts.Listen = address
	s.receiver, err = tcp.NewInheritedCapture("tcp", opts, h, s)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		s.receiver.Stop()
		_ = s.close()
		if s.inherited != nil {
			closeTCPHandoff(s.inherited)
		}
	})
	return s, address
}

func TestStartupCaptureBounds(t *testing.T) {
	for _, reason := range []string{"duration", "bytes", "capacity"} {
		t.Run(reason, func(t *testing.T) {
			defer zapwriter.Test()()
			s, _ := captureFixture(t)
			limit, maxBytes := time.Hour, int64(1<<30)
			switch reason {
			case "duration":
				limit = time.Millisecond
			case "bytes":
				maxBytes = 1
			case "capacity":
				s.requestFallback()
			}
			// Force a byte-limit crossing with a valid complete record.
			p := points.OnePoint("bound.metric", 1, 1)
			if err := s.shards[0].Write(p.AppendBinary(nil), 1); err != nil {
				t.Fatal(err)
			}
			s.startMonitor(limit, maxBytes)
			select {
			case <-s.sealed:
				if !s.fallback.Load() || s.sealErr != nil || s.inherited == nil {
					t.Fatal("fallback did not retain sockets", s.sealErr)
				}
			case <-time.After(time.Second):
				t.Fatal("capture bound did not seal independently of app startup")
			}
			var count int
			for _, shard := range s.shards {
				n, err := replayCaptureShard(&startupShardReader{s: s, shard: shard}, func(*points.Points) {})
				if err != nil {
					t.Fatal(err)
				}
				count += n
			}
			if count != 1 {
				t.Fatal("fallback lost or duplicated input", count)
			}
		})
	}
}

// Bounded, opt-in real TCP workload. It retains about 1.2 GB for one second at
// 10 M points/s, then verifies every value bit/timestamp and occurrence, rather
// than just comparing a receiver counter. The configured cache starts full.
func TestStartupCaptureProductionRate(t *testing.T) {
	if os.Getenv("CAPTURE_SCALE") != "1" {
		t.Skip("set CAPTURE_SCALE=1")
	}
	const clients, slots, batch, rate = 190, 64, 512, 10000000
	seconds := 1
	if v, err := strconv.Atoi(os.Getenv("CAPTURE_SCALE_SECONDS")); err == nil && v > 0 {
		seconds = v
	}
	if seconds > 3 {
		t.Fatal("bounded fixture permits at most three seconds")
	}
	perClient := (rate*seconds + clients - 1) / clients
	total := perClient * clients
	s, address := captureFixture(t)
	t.Logf("policy: %+v", s.policy)
	// Restore 32 M points to the configured limit in compact cache form,
	// without allocating a 512 MB source slice. This tests a full cache while
	// keeping the laptop fixture small.
	core := cache.New()
	const restored = 32000000
	core.SetMaxSize(restored)
	old := &points.Points{Metric: "restored.full", Data: make([]points.Point, 10000)}
	for i := range old.Data {
		old.Data[i] = points.Point{Timestamp: 1700000000 + int64(i), Value: 1}
	}
	for range restored / len(old.Data) {
		core.AddRecovered(old)
	}
	if core.Size() != restored || core.HasRecoveryCapacity(1) {
		t.Fatal("cache is not full")
	}
	conns := make([]net.Conn, clients)
	for i := range conns {
		c, err := net.Dial("tcp", address)
		if err != nil {
			t.Fatal(err)
		}
		conns[i] = c
	}
	names := make([][]string, clients)
	for i := range names {
		for j := range slots {
			names[i] = append(names[i], fmt.Sprintf("servers.client%03d.slot%02d.application.requests.%s", i, j, strings.Repeat("x", 55)))
		}
	}
	// Pre-encode a bounded repeated batch outside the timed region, as a real
	// relay already has its outgoing bytes. In-loop fmt benchmarks the generator.
	payloads := make([][]byte, clients)
	endings := make([][]byte, clients)
	for client := range clients {
		for seq := range batch {
			payloads[client] = fmt.Appendf(payloads[client], "%s %d %d\n", names[client][seq%slots], seq, 1700000000+seq/slots)
		}
		for seq := range perClient % batch {
			endings[client] = fmt.Appendf(endings[client], "%s %d %d\n", names[client][seq%slots], seq, 1700000000+seq/slots)
		}
	}
	s.startMonitor(time.Hour, 1<<40)
	start := time.Now().Add(100 * time.Millisecond)
	var wg sync.WaitGroup
	var late [clients]time.Duration
	for client, conn := range conns {
		wg.Go(func() {
			defer conn.Close()
			_ = conn.SetWriteDeadline(start.Add(time.Duration(seconds)*time.Second + 20*time.Second))
			for seq := 0; seq < perClient; seq += batch {
				target := start.Add(time.Duration(float64(seq) / float64(rate/clients) * float64(time.Second)))
				if d := time.Until(target); d > 0 {
					time.Sleep(d)
				}
				raw := payloads[client]
				if seq+batch > perClient {
					raw = endings[client]
				}
				if _, err := conn.Write(raw); err != nil {
					t.Error(err)
					return
				}
				if d := time.Since(target); d > late[client] {
					late[client] = d
				}
			}
		})
	}
	wg.Wait()
	sentDuration := time.Since(start)
	deadline := time.Now().Add(20 * time.Second)
	for s.points.Load() < int64(total) {
		if time.Now().After(deadline) {
			t.Fatalf("capture stalled: %d/%d", s.points.Load(), total)
		}
		time.Sleep(time.Millisecond)
	}
	captureDuration := time.Since(start)
	// The cache remains full when replay begins. A simulated confirmation of the
	// old persisted history releases space after pressure has been exercised.
	replayStart := time.Now()
	app := &App{startupInput: s}
	t.Cleanup(func() {
		if app.inheritedInput != nil {
			app.inheritedInput.Close()
		}
	})
	finish := make(chan error, 1)
	go func() { finish <- app.finishStartupInput(core) }()
	time.AfterFunc(100*time.Millisecond, func() { core.Pop("restored.full") })
	// A persistence confirmation frees restored history; all captured records
	// remain in cache for exact accounting as well as independent spool decoding.

	select {
	case err := <-finish:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("parallel replay did not converge")
	}
	replayDuration := time.Since(replayStart)
	// s.close closed descriptors, so read the same legacy files ordinary crash
	// recovery sees. Compare the exact multiset and per-metric correction order.
	expected := make(map[string]int, clients*slots)
	for client := range names {
		for slot, name := range names[client] {
			expected[name] = slot
		}
	}
	var verifyWG sync.WaitGroup
	verified := make([]int, len(s.shards))
	for shardIndex, shard := range s.shards {
		verifyWG.Go(func() {
			next := make(map[string]int)
			if err := points.ReadFromFile(shard.path, func(p *points.Points) {
				slot, known := expected[p.Metric]
				if !known {
					t.Errorf("unexpected metric %q", p.Metric)
					return
				}
				want, ok := next[p.Metric]
				if !ok {
					want = slot
				}
				for _, d := range p.Data {
					if math.Float64bits(d.Value) != math.Float64bits(float64(want%batch)) || d.Timestamp != 1700000000+int64((want%batch)/slots) || want >= perClient {
						t.Errorf("bad/duplicate record %s seq %d point %+v", p.Metric, want, d)
						return
					}
					want += slots
					verified[shardIndex]++
				}
				next[p.Metric] = want
			}); err != nil {
				t.Error(err)
			}
		})
	}
	verifyWG.Wait()
	count := 0
	for _, n := range verified {
		count += n
	}
	if count != total || s.points.Load() != int64(total) || s.replayedPoints.Load() != int64(total) || core.Size() != int64(total) {
		t.Fatal("multiset mismatch", count, total, s.points.Load(), s.replayedPoints.Load(), core.Size())
	}
	var maxLate time.Duration
	for _, d := range late {
		maxLate = max(maxLate, d)
	}
	t.Logf("CPUs=%d clients=%d offered=%d/s names=%d bytes=%d sent=%v drained=%v capture=%.3fGB/s %.0fpts/s replay=%v %.0fpts/s max_sender_lag=%v fallback=%v exact_records=%d", runtime.NumCPU(), clients, rate, len(names[0][0]), s.size.Load(), sentDuration, captureDuration, float64(s.size.Load())/captureDuration.Seconds()/1e9, float64(total)/captureDuration.Seconds(), replayDuration, float64(total)/replayDuration.Seconds(), maxLate, s.fallback.Load(), count)
	if captureDuration > time.Duration(seconds)*time.Second+100*time.Millisecond {
		t.Errorf("capture backpressured the %.1f M/s offered load", float64(rate)/1e6)
	}
}

// Isolates independent append streams from parser/generator cost. Retained data
// is bounded to about 1 GiB; fsync completion is measured separately.
func TestStartupCaptureRawBandwidth(t *testing.T) {
	if os.Getenv("CAPTURE_SCALE") != "1" {
		t.Skip("set CAPTURE_SCALE=1")
	}
	s, _ := captureFixture(t)
	const records = 8192
	raw := make([]byte, 0, 1<<20)
	for i := range records {
		raw = points.OnePoint(strings.Repeat("n", 100), float64(i), 1700000000).AppendBinary(raw)
	}
	var wg sync.WaitGroup
	start := time.Now()
	for _, shard := range s.shards {
		wg.Go(func() {
			for range 32 {
				if err := shard.Write(raw, records); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	duration := time.Since(start)
	s.receiver.Stop()
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	durable := time.Since(start)
	t.Logf("bytes=%d appends=%.3fGB/s durable=%.3fGB/s append_time=%v durable_time=%v", s.size.Load(), float64(s.size.Load())/duration.Seconds()/1e9, float64(s.size.Load())/durable.Seconds()/1e9, duration, durable)
	// Read every byte back through independent production decoders.
	start = time.Now()
	counts := make([]int, len(s.shards))
	for i, shard := range s.shards {
		wg.Go(func() {
			f, err := os.Open(shard.path)
			if err != nil {
				t.Error(err)
				return
			}
			defer f.Close()
			counts[i], err = replayCaptureShard(io.Reader(f), func(*points.Points) {})
			if err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	count := 0
	for _, n := range counts {
		count += n
	}
	if int64(count) != s.points.Load() {
		t.Fatal(count, s.points.Load())
	}
	t.Logf("parallel_decode=%.0fpts/s %.3fGB/s records=%d", float64(count)/time.Since(start).Seconds(), float64(s.size.Load())/time.Since(start).Seconds()/1e9, count)
}

func mutexWaitSeconds() float64 {
	samples := []metrics.Sample{{Name: "/sync/mutex/wait/total:seconds"}}
	metrics.Read(samples)
	return samples[0].Value.Float64()
}

// Start with one third of a production second of backlog, then keep 190 producers
// offering 10 M points/s while the real parallel decoders add to a full cache.
// Catch-up must occur while capture is still active, with a measured rate margin
// and a mutex budget. Comparing only a sealed-file replay rate misses starvation.
func TestStartupReplayMarginWith190Writers(t *testing.T) {
	if os.Getenv("CAPTURE_SCALE") != "1" {
		t.Skip("set CAPTURE_SCALE=1")
	}
	const writers, batch, rate = 190, 512, 10000000
	s, _ := captureFixture(t)
	raw := make([][]byte, len(s.shards))
	for i := range raw {
		name := fmt.Sprintf("seed.shard%03d.%s", i, strings.Repeat("x", 86))
		for j := range batch {
			raw[i] = points.OnePoint(name, float64(j), 1700000000+int64(j)).AppendBinary(raw[i])
		}
	}
	var seedWG sync.WaitGroup
	for i, shard := range s.shards {
		seedWG.Go(func() {
			for range (rate / 3) / len(s.shards) / batch {
				if err := shard.Write(raw[i], batch); err != nil {
					t.Error(err)
				}
			}
		})
	}
	seedWG.Wait()
	seed := s.points.Load()
	core := cache.New()
	core.SetMaxSize(1000000)
	p := &points.Points{Metric: "restored.full", Data: make([]points.Point, 10000)}
	for range 100 {
		core.AddRecovered(p)
	}
	if core.HasRecoveryCapacity(1) {
		t.Fatal("cache did not start full")
	}
	s.startMonitor(time.Hour, 1<<40)
	counts := make([]atomic.Int64, len(s.shards))
	var readers sync.WaitGroup
	start := time.Now().Add(50 * time.Millisecond)
	waitBefore := mutexWaitSeconds()
	for i, shard := range s.shards {
		readers.Go(func() {
			if d := time.Until(start); d > 0 {
				time.Sleep(d)
			}
			local := int64(0)
			n, err := replayCaptureShardBuffer(&startupShardReader{s: s, shard: shard}, s.pacedReplay(func(p *points.Points) {
				core.AddRecovered(p)
				local += int64(len(p.Data))
				if local%1024 == 0 {
					counts[i].Store(local)
				}
			}), s.policy.readBuffer)
			counts[i].Store(int64(n))
			if err != nil {
				t.Error(err)
			}
		})
	}
	var producers sync.WaitGroup
	for worker := range writers {
		producers.Go(func() {
			for j := range (rate/writers + batch - 1) / batch {
				target := start.Add(time.Duration(float64(j*batch) / float64(rate/writers) * float64(time.Second)))
				if d := time.Until(target); d > 0 {
					time.Sleep(d)
				}
				shard := worker % len(s.shards)
				if err := s.shards[shard].Write(raw[shard], batch); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	caught := false
	var margin, captureRate, replayRate float64
	deadline := start.Add(950 * time.Millisecond)
	for time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
		var replayed int64
		for i := range counts {
			replayed += counts[i].Load()
		}
		captured := s.points.Load() - seed
		if captured > rate/10 && seed+captured-replayed <= int64(len(s.shards))*2048 {
			duration := time.Since(start).Seconds()
			captureRate = float64(captured) / duration
			replayRate = float64(replayed) / duration
			margin = replayRate / captureRate
			caught = true
			break
		}
	}
	producers.Wait()
	producerDuration := time.Since(start)
	if producerDuration > 1100*time.Millisecond {
		t.Errorf("190 producers fell behind 10 M/s: %v", producerDuration)
	}
	s.requestFallback() // stop this synthetic capture without opening live input
	readers.Wait()
	duration := time.Since(start).Seconds()
	waited := mutexWaitSeconds() - waitBefore
	var count int64
	for i := range counts {
		count += counts[i].Load()
	}
	if count != s.points.Load() || core.Size() != count+1000000 {
		t.Fatal("lost/duplicate records", count, s.points.Load(), core.Size())
	}
	t.Logf("writers=%d full_offer=%d/s seed=%d capture=%.0fpts/s replay=%.0fpts/s margin=%.2fx caught_while_capturing=%v mutex_wait_seconds=%.3f wall_seconds=%.3f mutex_wait_per_wall=%.3f", writers, rate, seed, captureRate, replayRate, margin, caught, waited, duration, waited/duration)
	if !caught || margin < 1.25 {
		t.Errorf("replay needs >=1.25x measured capture rate and convergence during active input")
	}
	if waited/duration > 10 {
		t.Errorf("capture+replay exceeded 10 mutex-wait seconds per wall second: %.3f", waited/duration)
	}
}
