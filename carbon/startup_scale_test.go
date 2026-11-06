package carbon

import (
	"fmt"
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
	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver"
	"github.com/go-graphite/go-carbon/receiver/tcp"
	"github.com/lomik/zapwriter"
)

func captureFixture(t testing.TB) (*startupInput, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "input.1.2.tail.bin")
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
	t.Cleanup(func() { s.receiver.Stop(); _ = s.close() })
	return s, address
}
func visitCapture(s *startupInput, visit func(*points.Points)) error {
	if err := points.ReadFromFile(s.path, visit); err != nil {
		return err
	}
	paths, err := filepath.Glob(s.path + ".capture-*.tail.bin")
	if err != nil {
		return err
	}
	for _, path := range paths {
		if _, err := recovery.ReadCaptureFile(path, visit); err != nil {
			return err
		}
	}
	return nil
}

type testAdmission struct{ allowed atomic.Bool }

func (a *testAdmission) HasRecoveryCapacity(int) bool { return a.allowed.Load() }
func awaitCapture(t testing.TB, condition func() bool) {
	t.Helper()
	end := time.Now().Add(10 * time.Second)
	for !condition() {
		if time.Now().After(end) {
			t.Fatal("capture condition timed out")
		}
		time.Sleep(time.Millisecond)
	}
}
func TestStartupCaptureContinuesUnderAdmissionPressure(t *testing.T) {
	defer zapwriter.Test()()
	s, _ := captureFixture(t)
	admission := &testAdmission{}
	core := cache.New()
	s.startReplay(&startupSink{admission: admission, add: core.AddRecoveredFrom, canRetire: func() bool { return true }}, func() error { return nil })
	sh := s.shards[0]
	for i := range 100 {
		if err := sh.Write(points.OnePoint("same.metric", float64(i), 1).AppendBinary(nil), 1); err != nil {
			t.Fatal(err)
		}
	}
	if s.points.Load() != 100 || s.replayedPoints.Load() != 0 || s.paused.Load() {
		t.Fatal("admission paused or dropped capture")
	}
	admission.allowed.Store(true)
	awaitCapture(t, func() bool { return s.replayedPoints.Load() == 100 && sh.live.Load() })
	// Alternate admission after a FIFO promotion. New corrections must stay behind
	// the backlog even when the partition returns to capture mode.
	admission.allowed.Store(false)
	for i := 100; i < 200; i++ {
		if err := sh.Write(points.OnePoint("same.metric", float64(i), 1).AppendBinary(nil), 1); err != nil {
			t.Fatal(err)
		}
	}
	admission.allowed.Store(true)
	awaitCapture(t, func() bool { return s.replayedPoints.Load() == 200 })
	got := core.Get("same.metric")
	if len(got) != 200 {
		t.Fatal(len(got))
	}
	for i, d := range got {
		if d.Value != float64(i) || d.Timestamp != 1 {
			t.Fatal("same-timestamp FIFO correction changed", i, d)
		}
	}
}
func TestStartupCaptureSegmentRetirement(t *testing.T) {
	s, _ := captureFixture(t)
	s.policy.segmentBytes = 64
	admission := &testAdmission{}
	admission.allowed.Store(true)
	core := cache.New()
	s.startReplay(&startupSink{admission: admission, add: core.AddRecoveredFrom, canRetire: func() bool { return true }}, func() error { return nil })
	sh := s.shards[0]
	for i := range 10 {
		if err := sh.Write(points.OnePoint("ordered.metric", float64(i), 1).AppendBinary(nil), 1); err != nil {
			t.Fatal(err)
		}
	}
	awaitCapture(t, func() bool { return s.replayedPoints.Load() == 10 })
	s.reclaim()
	if s.retiredBytes.Load() != 0 {
		t.Fatal("unconfirmed segment removed")
	}
	p, ok := core.PopNotConfirmed("ordered.metric")
	if !ok {
		t.Fatal("no metric")
	}
	core.Requeue(p)
	s.reclaim()
	if s.retiredBytes.Load() != 0 {
		t.Fatal("requeued segment removed")
	}
	p, _ = core.PopNotConfirmed("ordered.metric")
	core.Confirm(p)
	s.reclaim()
	if s.retiredBytes.Load() == 0 {
		t.Fatal("confirmed prefix retained")
	}
	if sh.first.Load() == nil || sh.first.Load() != sh.current.Load() {
		t.Fatal("retirement passed current append file")
	}
}
func mutexWaitSeconds() float64 {
	s := []metrics.Sample{{Name: "/sync/mutex/wait/total:seconds"}}
	metrics.Read(s)
	return s[0].Value.Float64()
}

// Real TCP, unique value/timestamp oracle, full restored cache, and a persister
// capped at one million points/s. A 120s run retains compressed source frames on
// disk; no in-memory list grows with input. Persistence receipts are bitsets.
func TestStartupCaptureProductionRate(t *testing.T) {
	if os.Getenv("CAPTURE_SCALE") != "1" {
		t.Skip("set CAPTURE_SCALE=1")
	}
	seconds := 3
	if n, e := strconv.Atoi(os.Getenv("CAPTURE_SCALE_SECONDS")); e == nil && n > 0 {
		seconds = n
	}
	if seconds > 180 {
		t.Fatal("local disk safety: maximum 180 seconds")
	}
	const clients, slots, batch, rate, limit, persistRate = 190, 16, 512, 10000000, 2000000, 1000000
	perClient := (rate*seconds + clients - 1) / clients
	total := perClient * clients
	s, address := captureFixture(t)
	s.startMonitor(time.Hour, 1<<40)
	core := cache.New()
	core.SetMaxSize(limit)
	keys := make([]string, 0, clients*slots+1000)
	for i := range 1000 {
		name := fmt.Sprintf("restored.%d", i)
		keys = append(keys, name)
		core.AddRecovered(&points.Points{Metric: name, Data: make([]points.Point, limit/1000)})
	}
	if core.Size() != limit || core.HasRecoveryCapacity(1) {
		t.Fatal("cache not restored to its limit")
	}
	names := make([][]string, clients)
	for i := range clients {
		for j := range slots {
			name := fmt.Sprintf("servers.client%03d.slot%02d.application.requests.%s", i, j, strings.Repeat("x", 55))
			names[i] = append(names[i], name)
			keys = append(keys, name)
		}
	}
	persisted := make([]uint64, (total+63)/64)
	seen := make([]atomic.Uint64, len(persisted))
	var persistedCount atomic.Int64
	var restoredCount int64
	var invalid atomic.Bool
	check := func(name string, d points.Point) (int, bool) {
		if len(name) != len(names[0][0]) || !strings.HasPrefix(name, "servers.client") {
			invalid.Store(true)
			return 0, false
		}
		client, e := strconv.Atoi(name[14:17])
		if e != nil || client >= clients {
			invalid.Store(true)
			return 0, false
		}
		seq := int(d.Value)
		if seq < 0 || seq >= perClient || math.Float64bits(d.Value) != math.Float64bits(float64(seq)) || d.Timestamp != 1700000000+int64(seq/slots) || name != names[client][seq%slots] {
			invalid.Store(true)
			return 0, false
		}
		return client*perClient + seq, true
	}
	stopPersist := make(chan struct{})
	persistDone := make(chan struct{})
	start := time.Now().Add(100 * time.Millisecond)
	go func() {
		defer close(persistDone)
		index := 0
		for {
			select {
			case <-stopPersist:
				return
			default:
			}
			budget := int64(max(0, time.Since(start).Seconds())*persistRate) - persistedCount.Load() - restoredCount
			if budget <= 0 {
				time.Sleep(time.Millisecond)
				continue
			}
			name := keys[index%len(keys)]
			index++
			p, ok := core.PopNotConfirmed(name)
			if !ok {
				if index%len(keys) == 0 {
					time.Sleep(time.Millisecond)
				}
				continue
			}
			if strings.HasPrefix(name, "restored.") {
				restoredCount += int64(len(p.Data))
			} else {
				for _, d := range p.Data {
					if id, valid := check(name, d); valid {
						bit := uint64(1) << uint(id%64)
						if persisted[id/64]&bit != 0 {
							invalid.Store(true)
						}
						persisted[id/64] |= bit
					}
				}
				persistedCount.Add(int64(len(p.Data)))
			}
			core.Confirm(p)
		}
	}()
	s.startReplay(&startupSink{admission: core, add: core.AddRecoveredFrom, canRetire: func() bool { return true }}, func() error { return nil })
	conns := make([]net.Conn, clients)
	for i := range conns {
		var err error
		conns[i], err = net.Dial("tcp", address)
		if err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	var late [clients]time.Duration
	before := mutexWaitSeconds()
	for client, conn := range conns {
		wg.Go(func() {
			defer conn.Close()
			_ = conn.SetWriteDeadline(start.Add(time.Duration(seconds+30) * time.Second))
			raw := make([]byte, 0, batch*128)
			for seq := 0; seq < perClient; {
				target := start.Add(time.Duration(float64(seq) * clients / rate * float64(time.Second)))
				if d := time.Until(target); d > 0 {
					time.Sleep(d)
				}
				raw = raw[:0]
				end := min(perClient, seq+batch)
				for ; seq < end; seq++ {
					raw = append(raw, names[client][seq%slots]...)
					raw = append(raw, ' ')
					raw = strconv.AppendInt(raw, int64(seq), 10)
					raw = append(raw, ' ')
					raw = strconv.AppendInt(raw, 1700000000+int64(seq/slots), 10)
					raw = append(raw, '\n')
				}
				n, err := conn.Write(raw)
				if err != nil || n != len(raw) {
					t.Errorf("sender: %d/%d %v", n, len(raw), err)
					return
				}
				late[client] = max(late[client], time.Since(target))
			}
		})
	}
	wg.Wait()
	sendTime := time.Since(start)
	awaitCapture(t, func() bool { return s.points.Load() == int64(total) })
	captureTime := time.Since(start)
	s.receiver.Stop()
	s.pauseReplay()
	close(stopPersist)
	<-persistDone
	var peak runtime.MemStats
	runtime.ReadMemStats(&peak)
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	wait := mutexWaitSeconds() - before
	paths, err := filepath.Glob(s.path + ".capture-*.tail.bin")
	if err != nil {
		t.Fatal(err)
	}
	var pathIndex atomic.Int64
	var retained atomic.Int64
	for range s.policy.shards {
		wg.Go(func() {
			for {
				index := int(pathIndex.Add(1) - 1)
				if index >= len(paths) {
					return
				}
				n, err := recovery.ReadCaptureFileBorrowed(paths[index], func(name []byte, data []points.Point) {
					for _, d := range data {
						if id, valid := check(string(name), d); valid {
							bit := uint64(1) << uint(id%64)
							if seen[id/64].Or(bit)&bit != 0 {
								invalid.Store(true)
							}
						}
					}
				})
				if err != nil {
					t.Error(err)
				}
				retained.Add(int64(n))
			}
		})
	}
	wg.Wait()
	var union int
	for id := range total {
		if (seen[id/64].Load()|persisted[id/64])&(uint64(1)<<uint(id%64)) != 0 {
			union++
		}
	}
	var lag time.Duration
	for _, v := range late {
		lag = max(lag, v)
	}
	t.Logf("seconds=%d cpus=%d clients=%d offered=%d/s name_bytes=%d send=%v drained=%v raw_GB_s=%.3f captured=%d replayed=%d persisted=%d retained_records=%d exact_union=%d spool_bytes=%d retired_bytes=%d cache_points=%d cache_bytes=%d heap_sys=%d sender_max_lag=%v mutex_wait_per_wall=%.3f paused=%v", seconds, runtime.NumCPU(), clients, rate, len(names[0][0]), sendTime, captureTime, float64(s.rawBytes.Load())/captureTime.Seconds()/1e9, s.points.Load(), s.replayedPoints.Load(), persistedCount.Load(), retained.Load(), union, s.size.Load()-s.retiredBytes.Load(), s.retiredBytes.Load(), core.Size(), core.EstimatedBytes(), peak.HeapSys, lag, wait/captureTime.Seconds(), s.paused.Load())
	if invalid.Load() || union != total {
		t.Fatal("exact multiset receipts failed", union, total, invalid.Load())
	}
	if captureTime > time.Duration(seconds)*time.Second+500*time.Millisecond || lag > time.Second {
		t.Error("offered load backpressured")
	}
	if s.paused.Load() || s.fallback.Load() {
		t.Error("cache pressure paused capture")
	}
	if core.Size() > limit+int64(s.policy.shards)*20000 {
		t.Error("unbounded cache", core.Size())
	}
	if persistedCount.Load()+restoredCount > int64(captureTime.Seconds()*2e6)+limit {
		t.Error("simulated persister too fast")
	}
	if wait/captureTime.Seconds() > 10 {
		t.Error("mutex wait budget exceeded")
	}
}

// With an admission grant, seed a finite backlog while all 190 append clients
// continue at production rate. Replay must catch up before producers finish.
func TestStartupCaptureReplayMargin(t *testing.T) {
	if os.Getenv("CAPTURE_SCALE") != "1" {
		t.Skip("set CAPTURE_SCALE=1")
	}
	s, _ := captureFixture(t)
	const writers, rate, batch = 190, 10000000, 1024
	raw := make([][]byte, len(s.shards))
	for i := range raw {
		for j := range batch {
			raw[i] = points.OnePoint(fmt.Sprintf("partition.%03d.%s", i, strings.Repeat("n", 86)), float64(j), 1).AppendBinary(raw[i])
		}
	}
	var wg sync.WaitGroup
	for i, sh := range s.shards {
		wg.Go(func() {
			for range (rate / 2) / len(s.shards) / batch {
				if err := sh.Write(raw[i], batch); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	seed := s.points.Load()
	core := cache.New()
	core.SetMaxSize(1000000)
	core.AddRecovered(&points.Points{Metric: "restored.full", Data: make([]points.Point, 1000000)})
	admission := &testAdmission{}
	admission.allowed.Store(true)
	start := time.Now().Add(100 * time.Millisecond)
	before := mutexWaitSeconds()
	s.startReplay(&startupSink{admission: admission, add: core.AddRecoveredFrom, canRetire: func() bool { return false }}, func() error { time.Sleep(time.Until(start)); return nil })
	for worker := range writers {
		wg.Go(func() {
			for seq := 0; seq < rate/writers; seq += batch {
				target := start.Add(time.Duration(float64(seq) * writers / rate * float64(time.Second)))
				if d := time.Until(target); d > 0 {
					time.Sleep(d)
				}
				i := worker % len(s.shards)
				if err := s.shards[i].Write(raw[i], batch); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	caught := false
	var margin, capRate, repRate float64
	for time.Now().Before(start.Add(900 * time.Millisecond)) {
		time.Sleep(time.Millisecond)
		captured := s.points.Load() - seed
		replayed := s.replayedPoints.Load()
		if captured > rate/10 && seed+captured-replayed < int64(len(s.shards)*batch) {
			elapsed := time.Since(start).Seconds()
			capRate = float64(captured) / elapsed
			repRate = float64(replayed) / elapsed
			margin = repRate / capRate
			caught = true
			break
		}
	}
	wg.Wait()
	duration := time.Since(start)
	awaitCapture(t, func() bool { return s.points.Load() == s.replayedPoints.Load() })
	s.pauseReplay()
	wait := (mutexWaitSeconds() - before) / duration.Seconds()
	t.Logf("writers=%d seed=%d capture=%.0f/s replay=%.0f/s margin=%.2fx caught_during_capture=%v producer_time=%v mutex_wait_per_wall=%.3f", writers, seed, capRate, repRate, margin, caught, duration, wait)
	if !caught || margin < 1.25 || duration > 1200*time.Millisecond || wait > 10 {
		t.Fatal("concurrent replay margin or mutex budget failed")
	}
	if core.Size() != s.points.Load()+1000000 {
		t.Fatal("lost or duplicated replay", core.Size(), s.points.Load())
	}
}
func TestStartupCaptureRawBandwidth(t *testing.T) {
	if os.Getenv("CAPTURE_SCALE") != "1" {
		t.Skip("set CAPTURE_SCALE=1")
	}
	s, _ := captureFixture(t)
	var raw []byte
	const batch = 8192
	for i := range batch {
		raw = points.OnePoint(fmt.Sprintf("servers.host%06d.application.requests.%s", i, strings.Repeat("n", 65)), float64(i), 1700000000).AppendBinary(raw)
	}
	var wg sync.WaitGroup
	start := time.Now()
	for _, sh := range s.shards {
		wg.Go(func() {
			for range 32 {
				if err := sh.Write(raw, batch); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	appended := time.Since(start)
	s.receiver.Stop()
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	durable := time.Since(start)
	t.Logf("raw_bytes=%d physical_bytes=%d append_GB_s=%.3f through_sync_GB_s=%.3f append=%v through_sync=%v", s.rawBytes.Load(), s.size.Load(), float64(s.rawBytes.Load())/appended.Seconds()/1e9, float64(s.rawBytes.Load())/durable.Seconds()/1e9, appended, durable)
	var count int64
	if err := visitCapture(s, func(p *points.Points) { count += int64(len(p.Data)) }); err != nil {
		t.Fatal(err)
	}
	if count != s.points.Load() {
		t.Fatal(count, s.points.Load())
	}
}
