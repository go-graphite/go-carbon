package lockset

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/contentiontest"
)

func TestDoesNotHoldSiblingWhileWaiting(t *testing.T) {
	var mus [2]sync.Mutex
	mus[1].Lock()
	done := make(chan struct{})
	go func() { Lock(mus[:], []uint64{0, 1}); mus[1].Unlock(); mus[0].Unlock(); close(done) }()
	// Give the contender time to reach the occupied stripe. Then repeatedly
	// require the independent stripe, while that other stripe stays blocked.
	time.Sleep(time.Millisecond)
	deadline := time.Now().Add(time.Second)
	for range 100 {
		for !mus[0].TryLock() {
			if time.Now().After(deadline) {
				mus[1].Unlock()
				<-done
				t.Fatal("batch held independent stripe while waiting on occupied sibling")
			}
			runtime.Gosched()
		}
		mus[0].Unlock()
	}
	mus[1].Unlock()
	<-done
}

func TestOverlappingSetsMakeProgress(t *testing.T) {
	var mus [8]sync.Mutex
	var counts [8]int
	var wg sync.WaitGroup
	for w := range 256 {
		wg.Go(func() {
			ids := []uint64{uint64(w % 8), uint64((w + 1) % 8), uint64((w + 2) % 8)}
			for range 100 {
				Lock(mus[:], ids)
				for _, i := range ids {
					counts[i]++
				}
				for _, i := range ids {
					mus[i].Unlock()
				}
			}
		})
	}
	wg.Wait()
	for _, n := range counts {
		if n != 9600 {
			t.Fatal(counts)
		}
	}
}

// Paired benchmark uses the former acquisition algorithm as an in-process
// control, with identical producer count and a blocked storage stripe.
func BenchmarkConvoy(b *testing.B) {
	for _, ordered := range []bool{true, false} {
		b.Run(fmt.Sprintf("ordered%v", ordered), func(b *testing.B) {
			var totalWait, foreground time.Duration
			var elapsed time.Duration
			for range b.N {
				var mus [2]sync.Mutex
				mus[1].Lock()
				done := make(chan struct{})
				go func() {
					if ordered {
						mus[0].Lock()
						mus[1].Lock()
					} else {
						Lock(mus[:], []uint64{0, 1})
					}
					mus[1].Unlock()
					mus[0].Unlock()
					close(done)
				}()
				time.Sleep(time.Millisecond)
				before, at := contentiontest.WaitSeconds(), time.Now()
				timer := time.AfterFunc(15*time.Millisecond, mus[1].Unlock)
				var wg sync.WaitGroup
				for range 256 {
					wg.Go(func() {
						for range 64 {
							mus[0].Lock()
							mus[0].Unlock()
						}
					})
				}
				wg.Wait()
				foreground += time.Since(at)
				<-done
				timer.Stop()
				elapsed += time.Since(at)
				totalWait += time.Duration((contentiontest.WaitSeconds() - before) * 1e9)
			}
			b.ReportMetric(float64(totalWait)/float64(elapsed), "wait-s/s")
			b.ReportMetric(float64(foreground.Nanoseconds())/float64(b.N), "foreground-ns")
		})
	}
}
