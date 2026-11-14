package persister

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/points"
)

func TestWarmReservationsIndependentNames(t *testing.T) {
	p := &Whisper{warmPending: &warmReservations{}}
	first := "held"
	other := ""
	for i := 0; ; i++ {
		other = fmt.Sprint("other", i)
		if p.warmPending.forName(first) != p.warmPending.forName(other) {
			break
		}
	}
	held := p.warmPending.forName(first)
	held.mu.Lock()
	done := make(chan bool, 1)
	go func() { done <- p.claimWarmName(other); p.finishWarmName(other) }()
	select {
	case ok := <-done:
		held.mu.Unlock()
		if !ok {
			t.Fatal("independent name refused")
		}
	case <-time.After(time.Second):
		held.mu.Unlock()
		<-done
		t.Fatal("unrelated name waited for a reservation shard")
	}
}

func TestWarmReservationsExcludeSameName(t *testing.T) {
	p := &Whisper{warmPending: &warmReservations{}}
	var active, overlaps atomic.Int64
	var wg sync.WaitGroup
	for range 64 {
		wg.Go(func() {
			for range 1000 {
				if !p.claimWarmName("same") {
					continue
				}
				if active.Add(1) != 1 {
					overlaps.Add(1)
				}
				active.Add(-1)
				p.finishWarmName("same")
			}
		})
	}
	wg.Wait()
	if overlaps.Load() != 0 {
		t.Fatal("same name concurrently reserved")
	}
	if !p.claimWarmName("same") {
		t.Fatal("reservation leaked")
	}
}

// Compare complete decoded write batches through ordinary and warm-first
// worker scheduling, including duplicate snapshots, corrections and NaN bits.
func TestWarmReservationsPreservePointBits(t *testing.T) {
	run := func(warm bool) map[string][]points.Point {
		c := cache.New()
		source := make(chan string, 768)
		for i := range 256 {
			name := fmt.Sprint("warm", i)
			for j, bits := range []uint64{0, 1, 0x8000000000000000, 0x7ff8000000000012, 0x3ff0000000000001} {
				c.AddPoint([]byte(name), math.Float64frombits(bits), int64(j%2))
			}
			for range 3 {
				source <- name
			}
		}
		p := &Whisper{recv: func(exit chan bool) string {
			select {
			case name := <-source:
				return name
			case <-exit:
				return ""
			}
		}}
		p.SetWriteoutBatchReceiver(func() string {
			select {
			case name := <-source:
				return name
			default:
				return ""
			}
		})
		p.SetWorkers(32)
		p.SetMetricStore(&controlledWarmer{}) // All names are metadata-cached.
		p.SetWarmFirst(warm)
		got := make(map[string][]points.Point)
		var mu sync.Mutex
		done := make(chan struct{})
		p.SetMockStore(func() (StoreFunc, func()) {
			return func(name string) {
				if batch, ok := c.PopNotConfirmed(name); ok {
					mu.Lock()
					got[name] = append(got[name], batch.Data...)
					if len(got) == 256 {
						close(done)
					}
					mu.Unlock()
					c.Confirm(batch)
				}
			}, nil
		})
		if err := p.Start(); err != nil {
			t.Fatal(err)
		}
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			p.Stop()
			t.Fatal("writeout stalled")
		}
		p.Stop()
		return got
	}
	want, got := run(false), run(true)
	for name, values := range want {
		actual := got[name]
		if len(actual) != len(values) {
			t.Fatalf("%s: got %d points, want %d", name, len(actual), len(values))
		}
		for i, point := range values {
			if actual[i].Timestamp != point.Timestamp || math.Float64bits(actual[i].Value) != math.Float64bits(point.Value) {
				t.Fatalf("%s point %d changed", name, i)
			}
		}
	}
}
