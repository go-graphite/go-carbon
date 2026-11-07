//go:build unix

package carbon

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/points"
)

// Keep this trace compatible with the sb3 source so the same test can be run
// through an overlay against both revisions. FIFO promotion is driven by the
// real append/replay owners; the receipts include pre- and post-promotion input.
func TestReplayFixBitDifferential(t *testing.T) {
	s, err := openStartupInputWithPolicy(filepath.Join(t.TempDir(), "input.1.2.tail.bin"), chooseStartupPolicy(4, 1<<30, 1<<30))
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	core := cache.New()
	core.SetMaxSize(0)
	core.SetRecoveryMemory(func() (uint64, bool) { return 24 << 30, true })
	names := make([]string, 128)
	for i := range names {
		names[i] = fmt.Sprintf("capture.bits.%03d.%s", i, strings.Repeat("x", 83))
	}
	bits := []uint64{0, 1 << 63, 0x7ff8000000000042, 0x7ff0000000000000, 0xfff0000000000000, 1, 0x7fefffffffffffff}
	want := make(map[string][]points.Point)
	write := func(round int) {
		for shard, sh := range s.shards {
			var raw []byte
			count := 0
			for i := shard; i < len(names); i += len(s.shards) {
				p := points.OnePoint(names[i], math.Float64frombits(bits[(round+i)%len(bits)]), int64(1700000000+round/3-i%5))
				raw = p.AppendBinary(raw)
				want[p.Metric] = append(want[p.Metric], p.Data...)
				count++
			}
			if err := sh.Write(raw, count); err != nil {
				t.Fatal(err)
			}
		}
	}
	for i := range 8 {
		write(i)
	}
	s.startReplay(&startupSink{admission: core, add: core.AddRecoveredFrom, liveAdd: core.AddCapturedFrom, canRetire: func() bool { return false }}, func() error { return nil })
	awaitCapture(t, func() bool {
		for _, sh := range s.shards {
			if !sh.live.Load() {
				return false
			}
		}
		return true
	})
	for i := 8; i < 32; i++ {
		write(i)
	}
	awaitCapture(t, func() bool { return s.replayedPoints.Load() == 32*int64(len(names)) })
	s.pauseReplay()
	h := sha256.New()
	var scratch [16]byte
	sort.Strings(names)
	for _, name := range names {
		batch, ok := core.PopNotConfirmed(name)
		if !ok || len(batch.Data) != len(want[name]) {
			t.Fatalf("missing or duplicate receipt for %s", name)
		}
		fmt.Fprintf(h, "%d:%s:", len(name), name)
		for i, p := range batch.Data {
			expected := want[name][i]
			if p.Timestamp != expected.Timestamp || math.Float64bits(p.Value) != math.Float64bits(expected.Value) {
				t.Fatalf("bit/order mismatch %s[%d]: %+v versus %+v", name, i, p, expected)
			}
			binary.LittleEndian.PutUint64(scratch[:8], uint64(p.Timestamp))
			binary.LittleEndian.PutUint64(scratch[8:], math.Float64bits(p.Value))
			h.Write(scratch[:])
		}
		core.Confirm(batch)
	}
	if !core.IsEmpty() {
		t.Fatal("unexpected extra points")
	}
	t.Logf("points=%d names=%d sha256=%x", 32*len(names), len(names), h.Sum(nil))
}

// Opt-in real cache + compressed spool + replay run. Seeded history keeps the
// observed bytes/point below the controller's 64-byte floor, as on the affected
// stores. Each input record is one point for a 100-byte metric name.
func TestReplayFixProductionShape(t *testing.T) {
	if os.Getenv("REPLAY_FIX_SCALE") != "1" {
		t.Skip("set REPLAY_FIX_SCALE=1")
	}
	keys, rate := 25000000, 4000000
	if v, err := strconv.Atoi(os.Getenv("REPLAY_FIX_KEYS")); err == nil && v > 0 {
		keys = v
	}
	if v, err := strconv.Atoi(os.Getenv("REPLAY_FIX_RATE")); err == nil && v > 0 {
		rate = v
	}
	core := cache.New()
	core.SetMaxSize(0)
	core.SetRecoveryMemory(func() (uint64, bool) { return 24 << 30, true })
	name := func(i int) string { return fmt.Sprintf("production.%08d.%s", i, strings.Repeat("x", 80)) }
	started := time.Now()
	workers := min(16, runtime.GOMAXPROCS(0))
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Go(func() {
			data := make([]points.Point, 32)
			for j := range data {
				data[j] = points.Point{Value: float64(j), Timestamp: int64(1700000000 + j)}
			}
			for i := worker; i < keys; i += workers {
				core.AddRecovered(&points.Points{Metric: name(i), Data: data})
			}
		})
	}
	wg.Wait()
	t.Logf("seed names=%d name_bytes=%d history_points=%d estimated_bytes=%d duration=%v", keys, len(name(0)), core.Size(), core.EstimatedBytes(), time.Since(started))
	p := chooseStartupPolicy(workers, 24<<30, 1<<40)
	p.segmentBytes = 32 << 20
	s, err := openStartupInputWithPolicy(filepath.Join(t.TempDir(), "input.1.2.tail.bin"), p)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	const batch = 4096
	// Sweep the whole universe, rather than repeatedly hitting a small hot set.
	// Keep generation outside the measured concurrent append/replay interval.
	raw := make([][][]byte, len(s.shards))
	for shard := range raw {
		wg.Go(func() {
			for base := 0; base+batch <= keys/len(s.shards); base += batch {
				frame := make([]byte, 0, batch*120)
				for j := range batch {
					i := (base+j)*len(s.shards) + shard
					frame = points.OnePoint(name(i), float64(i), 1700000100).AppendBinary(frame)
				}
				raw[shard] = append(raw[shard], frame)
			}
		})
	}
	wg.Wait()
	t.Logf("prepared frames=%d raw_universe_names=%d", len(raw[0])*len(raw), len(raw[0])*len(raw)*batch)
	perShardSeed := max(1, rate/2/len(s.shards)/batch)
	for i, sh := range s.shards {
		wg.Go(func() {
			for seq := range perShardSeed {
				if err := sh.Write(raw[i][seq%len(raw[i])], batch); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	seed := s.points.Load()
	start := time.Now().Add(50 * time.Millisecond)
	s.startReplay(&startupSink{admission: core, add: core.AddRecoveredFrom, liveAdd: core.AddCapturedFrom, canRetire: func() bool { return false }}, func() error { time.Sleep(time.Until(start)); return nil })
	var offered atomic.Int64
	for i, sh := range s.shards {
		wg.Go(func() {
			for n := 0; n < 2*rate/len(s.shards); n += batch {
				target := start.Add(time.Duration(float64(n*len(s.shards)) / float64(rate) * float64(time.Second)))
				if d := time.Until(target); d > 0 {
					time.Sleep(d)
				}
				offered.Add(batch)
				if err := sh.Write(raw[i][(perShardSeed+n/batch)%len(raw[i])], batch); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	var peak, ratio, sustained float64
	var caught bool
	previous, at := int64(0), start
	deadline := start.Add(20 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		now, replayed := time.Now(), s.replayedPoints.Load()
		if d := now.Sub(at).Seconds(); d > 0 {
			peak = max(peak, float64(replayed-previous)/d)
		}
		previous, at = replayed, now
		if replayed == seed+offered.Load() && offered.Load() >= int64(rate/4) {
			caught = true
			ratio = float64(replayed) / float64(offered.Load())
			sustained = float64(replayed) / time.Since(start).Seconds()
			break
		}
	}
	wg.Wait()
	producerTime := time.Since(start)
	awaitCapture(t, func() bool { return s.points.Load() == s.replayedPoints.Load() })
	live := 0
	for _, sh := range s.shards {
		if sh.live.Load() {
			live++
		}
	}
	window, remaining, rejected := core.RecoveryCapacity()
	t.Logf("keys=%d offered_target=%d/s offered=%d seed=%d producer_duration=%v achieved_input=%.0f/s peak_replay_50ms=%.0f/s sustained_replay_to_catchup=%.0f/s catchup_ratio=%.3f caught=%v backlog=%d live=%d/%d window=%d remaining=%d rejections=%d", keys, rate, offered.Load(), seed, producerTime, float64(offered.Load())/producerTime.Seconds(), peak, sustained, ratio, caught, s.points.Load()-s.replayedPoints.Load(), live, len(s.shards), window, remaining, rejected)
	if core.Size() != int64(keys)*32+s.points.Load() {
		t.Fatal("lost or duplicate replay", core.Size(), s.points.Load())
	}
	if !caught || sustained < float64(rate)*1.5 || producerTime > 2500*time.Millisecond {
		t.Fatal("production-shape throughput/convergence gate failed")
	}
}
