package helper

import (
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
)

func doTestThrottleTicker(perSecond int, wait time.Duration, hard bool) (bw int) {
	// start := time.Now()
	w := time.After(wait)

	ticker := newThrottleTicker(perSecond, hard)
	defer ticker.Stop()

LOOP:
	for {
		select {
		case <-w:
			break LOOP
		case keep := <-ticker.C:
			if keep {
				bw++
			}
		}
	}
	// stop := time.Now()

	return bw
}

func TestThrottleChan(t *testing.T) {
	// perSecondTable := []int{100, 1000, 10000, 100000, 200000, 400000}
	perSecondTable := []int{100, 1000, 10000, 100000, 200000}

	for _, perSecond := range perSecondTable {
		synctest.Test(t, func(t *testing.T) {
			bw := doTestThrottleTicker(perSecond, time.Second, false)
			max := float64(perSecond) * 1.10
			min := float64(perSecond) * 0.90
			assert.True(t, float64(bw) >= min, fmt.Sprintf("perSecond: %d, bw: %d", perSecond, bw))
			assert.True(t, float64(bw) <= max, fmt.Sprintf("perSecond: %d, bw: %d", perSecond, bw))
		})
	}
}

func TestHardThrottleChan(t *testing.T) {
	perSecondTable := []int{100, 1000, 10000, 100000, 200000}

	for _, perSecond := range perSecondTable {
		synctest.Test(t, func(t *testing.T) {
			// A little less than a second, as the hard throttle front-loads
			// its payloads
			kept := doTestThrottleTicker(perSecond, 900*time.Millisecond, true)
			max := float64(perSecond) * 1.05
			min := float64(perSecond) * 0.95
			assert.True(t, float64(kept) >= min, fmt.Sprintf("perSecond: %d, kept: %d", perSecond, kept))
			assert.True(t, float64(kept) <= max, fmt.Sprintf("perSecond: %d, kept: %d", perSecond, kept))
		})
	}
}

func TestSoftThrottleDoesNotBankASecondAcrossConsumerStall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ticker := NewThrottleTickerWithBurst(1000, 40)
		defer ticker.Stop()
		time.Sleep(time.Second)
		deadline := time.After(100 * time.Millisecond)
		count := 0
		for {
			select {
			case <-ticker.C:
				count++
			case <-deadline:
				// 100 newly due permits, 40 saved ones and at most one
				// chunk already blocked on the stalled consumer.
				if count < 100 || count > 142 {
					t.Fatalf("stall changed the rate or banked excess credit: %d", count)
				}
				return
			}
		}
	})
}

func TestSoftThrottleAccountsForCoalescedTicks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var clock atomic.Int64
		now := func() time.Time { return time.Unix(0, clock.Load()) }
		ticks := make(chan time.Time)
		permits := make(chan bool, 40)
		exit, done := make(chan bool), make(chan struct{})
		go func() {
			softThrottleTicks(permits, 1000, 40, ticks, now, exit)
			close(done)
		}()
		defer func() { close(exit); <-done }()
		synctest.Wait()
		for _, elapsed := range []time.Duration{10 * time.Millisecond, 20 * time.Millisecond} {
			clock.Add(int64(elapsed))
			// A scheduler delivers one tick even though several periods passed.
			ticks <- now()
			synctest.Wait()
			want := int(elapsed / time.Millisecond)
			if got := len(permits); got != want {
				t.Fatalf("coalesced ticks lost configured rate: got=%d want=%d", got, want)
			}
			for range want {
				<-permits
			}
		}
	})
}

func TestSoftThrottleDelayedPublisherCannotExceedBurst(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var clock atomic.Int64
		now := func() time.Time { return time.Unix(0, clock.Load()) }
		ticks, permits := make(chan time.Time), make(chan bool, 40)
		exit, done := make(chan bool), make(chan struct{})
		go func() {
			softThrottleTicks(permits, 1000, 40, ticks, now, exit)
			close(done)
		}()
		defer func() { close(exit); <-done }()
		synctest.Wait()
		clock.Store(int64(10 * time.Second))
		ticks <- now()
		synctest.Wait()
		if got := len(permits); got != 40 {
			t.Fatalf("delayed publisher credit=%d want=40", got)
		}
		for range 40 {
			<-permits
		}
		// A second timer delivery at the same clock must not recover the
		// other9960permits: the long delay was already capped and discarded.
		ticks <- now()
		synctest.Wait()
		if len(permits) != 0 {
			t.Fatal("publisher retained credit beyond the configured burst")
		}
	})
}

func TestSoftThrottleStopsWhileConsumerBlocked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var clock atomic.Int64
		now := func() time.Time { return time.Unix(0, clock.Load()) }
		ticks := make(chan time.Time)
		exit, done := make(chan bool), make(chan struct{})
		go func() {
			softThrottleTicks(make(chan bool), 1000, 40, ticks, now, exit)
			close(done)
		}()
		synctest.Wait()
		clock.Store(int64(10 * time.Millisecond))
		ticks <- now()
		synctest.Wait()
		close(exit)
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Fatal("blocked permit delivery ignored cancellation")
		}
	})
}

func TestSoftThrottleOldBufferAndDelayedCreditShareBurst(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var clock atomic.Int64
		now := func() time.Time { return time.Unix(0, clock.Load()) }
		ticks, permits := make(chan time.Time), make(chan bool, 40)
		exit, done := make(chan bool), make(chan struct{})
		go func() {
			softThrottleTicks(permits, 1000, 40, ticks, now, exit)
			close(done)
		}()
		defer func() { close(exit); <-done }()
		synctest.Wait()
		clock.Store(int64(20 * time.Millisecond))
		ticks <- now()
		synctest.Wait()
		if len(permits) != 20 {
			t.Fatal("fixture did not accumulate20permits")
		}
		// The publisher is delayed while20old permits remain buffered. The
		// consumer resumes just before its next tick: there is room for40,
		// but at most20new permits may join those20old saved permits.
		clock.Add(int64(time.Second))
		for range 20 {
			<-permits
		}
		ticks <- now()
		synctest.Wait()
		if got := len(permits); got != 20 {
			t.Fatalf("old buffer and delayed publisher double-banked credit: got=%d want=20", got)
		}
	})
}
