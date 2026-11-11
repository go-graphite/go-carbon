package cache

import (
	"fmt"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

func TestPendingRecoveryUsesMemoryAdmissionAboveHalfCache(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := New()
		c.SetMaxSize(6) // Five saved points fit the full limit, but not half.
		c.SetRecoveryMemory(func() (uint64, bool) { return 16 << 20, true })
		if err := c.AttachPendingRecovery(pendingFixture(t)); err != nil {
			t.Fatal(err)
		}
		stop := make(chan struct{})
		defer close(stop)
		done := make(chan error, 1)
		go func() { done <- c.RecoverPending(stop, 0) }()
		synctest.Wait()
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatal("saved history still waits at the legacy half-cache limit")
		}
		if c.Size() != 5 || c.PendingOutstanding() != 2 {
			t.Fatal("saved history was not loaded and tracked", c.Size(), c.PendingOutstanding())
		}
		window, remaining, rejections := c.RecoveryCapacity()
		if window == 0 || remaining >= window || rejections != 0 {
			t.Fatal("pending claims did not reserve memory", window, remaining, rejections)
		}
		if err := c.FinishPendingRecovery(); err == nil {
			t.Fatal("loading retired unpersisted history")
		}
		for _, name := range []string{"metric", "other"} {
			p, ok := c.PopNotConfirmed(name)
			if !ok {
				t.Fatal("saved metric missing", name)
			}
			c.Confirm(p)
		}
		if err := c.FinishPendingRecovery(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestPendingRecoverySkipsLiveClaimsUnderPressure(t *testing.T) {
	for _, memory := range []bool{false, true} {
		for _, preclaimed := range []bool{false, true} {
			t.Run(fmt.Sprintf("memory=%t/preclaimed=%t", memory, preclaimed), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					c := New()
					c.SetMaxSize(1)
					if memory {
						c.SetRecoveryMemory(func() (uint64, bool) { return 0, true })
					}
					if err := c.AttachPendingRecovery(pendingFixture(t)); err != nil {
						t.Fatal(err)
					}
					c.AddRecovered(points.OnePoint("busy", 1, 1))
					claim := func() {
						// Accepted input claims its saved history before appending
						// the new point, even while the loader has no capacity.
						c.AddRecovered(points.OnePoint("metric", 9, 1))
						c.AddRecovered(points.OnePoint("other", 10, 2))
					}
					if preclaimed {
						claim()
					}
					stop := make(chan struct{})
					defer close(stop)
					done := make(chan error, 1)
					go func() { done <- c.RecoverPending(stop, 0) }()
					synctest.Wait()
					if !preclaimed {
						if !c.RecoveryBlocked() {
							t.Fatal("loader did not wait for capacity")
						}
						claim()
						// The retry must observe a live claim without waiting
						// for the cache to drain below its admission ceiling.
						time.Sleep(10 * time.Millisecond)
						synctest.Wait()
					}
					select {
					case err := <-done:
						if err != nil {
							t.Fatal(err)
						}
					default:
						t.Fatal("completed claims still wait for recovery capacity")
					}
					want := []points.Point{{Value: 1, Timestamp: 1}, {Value: 2, Timestamp: 2}, {Value: 3, Timestamp: 1}, {Value: 9, Timestamp: 1}}
					if got := c.Get("metric"); !reflect.DeepEqual(got, want) {
						t.Fatal("saved/live correction ordering changed", got)
					}
					if c.Size() != 8 || c.PendingOutstanding() != 3 {
						t.Fatal("claim skipped or duplicated history", c.Size(), c.PendingOutstanding())
					}
				})
			})
		}
	}
}

func TestPendingRecoveryPreservesAdmissionLimitsAndCancellation(t *testing.T) {
	for _, limit := range []string{"points", "bytes", "headroom", "legacy"} {
		t.Run(limit, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := New()
				c.SetRecoveryMemory(func() (uint64, bool) { return 16 << 20, true })
				if err := c.AttachPendingRecovery(pendingFixture(t)); err != nil {
					t.Fatal(err)
				}
				c.AddRecovered(&points.Points{Metric: "busy", Data: make([]points.Point, 3)})
				switch limit {
				case "points":
					c.SetMaxSize(4)
				case "bytes":
					c.SetMaxBytes(1)
				case "headroom":
					c.SetRecoveryMemory(func() (uint64, bool) { return 0, true })
				case "legacy":
					c.SetRecoveryMemory(nil)
					c.SetMaxSize(6)
				}
				stop := make(chan struct{})
				done := make(chan error, 1)
				go func() { done <- c.RecoverPending(stop, 0) }()
				synctest.Wait()
				close(stop)
				if err := <-done; err == nil {
					t.Fatal("recovery bypassed its admission limit")
				}
				if !c.RecoveryBlocked() || c.Size() != 3 || c.pending.Load().remaining.Load() != 2 {
					t.Fatal("denied recovery allocated or lost saved history")
				}
				if len(c.Get("metric")) != 3 || len(c.Get("other")) != 2 {
					t.Fatal("cancellation lost readable saved history")
				}
			})
		})
	}
}
