package helper

import (
	"math"
	"time"
)

// ThrottleTicker is a ticker that can be used for hard or soft rate-limiting.
//
//   - A soft rate limiter will send a message on C at the actual rate that is
//     specified.
//
//   - A hard rate limiter makes up to ratePerSec 'true' messages available at
//     once each second. It sends nothing after that budget is consumed.
type ThrottleTicker struct {
	Stoppable
	C chan bool
}

// NewThrottleTicker returns a new soft throttle ticker.
func NewThrottleTicker(ratePerSec int) *ThrottleTicker {
	return newThrottleTicker(ratePerSec, false)
}

// NewThrottleTickerWithBurst bounds unused soft-rate credit to burst messages.
// A stalled consumer can also resume the single chunk already being sent.
func NewThrottleTickerWithBurst(ratePerSec, burst int) *ThrottleTicker {
	return newThrottleTickerBurst(ratePerSec, false, burst)
}

// NewHardThrottleTicker returns a new hard throttle ticker.
func NewHardThrottleTicker(ratePerSec int) *ThrottleTicker {
	return newThrottleTicker(ratePerSec, true)
}

func newThrottleTicker(ratePerSec int, hard bool) *ThrottleTicker {
	return newThrottleTickerBurst(ratePerSec, hard, ratePerSec)
}

func newThrottleTickerBurst(ratePerSec int, hard bool, burst int) *ThrottleTicker {
	burst = min(ratePerSec, max(1, burst))
	t := &ThrottleTicker{
		C: make(chan bool, burst),
	}

	t.Start()

	if ratePerSec <= 0 {
		close(t.C)
		return t
	}

	if hard {
		t.Go(hardThrottle(t.C, ratePerSec))
	} else {
		t.Go(softThrottle(t.C, ratePerSec, burst))
	}

	return t
}

func softThrottle(throttle chan bool, ratePerSec, burst int) func(chan bool) {
	return func(exit chan bool) {
		defer close(throttle)

		delimeter := ratePerSec

		if ratePerSec > 1000 {
			minRemainder := ratePerSec

			for i := 100; i < 1000; i++ {
				if ratePerSec%i < minRemainder {
					delimeter = i
					minRemainder = ratePerSec % delimeter
				}
			}
		}

		step := time.Duration(1e9/delimeter) * time.Nanosecond

		ticker := time.NewTicker(step)
		defer ticker.Stop()
		softThrottleTicks(throttle, ratePerSec, burst, ticker.C, time.Now, exit)
	}
}

// The explicit clock/tick boundary lets tests reproduce a delayed publisher
// without depending on host CPU scheduling or a wall-clock throughput gate.
func softThrottleTicks(throttle chan bool, rate, burst int, ticks <-chan time.Time, now func() time.Time, exit chan bool) {
	last, credit := now(), 0.
	space := max(0, burst-len(throttle))
	for {
		select {
		case <-ticks:
		case <-exit:
			return
		}
		at := now()
		// Keep the pre-wait free capacity: permits already buffered at that
		// boundary and catch-up credit together cannot exceed burst, even if
		// the consumer drains the old buffer before this publisher resumes.
		credit = math.Min(float64(space), credit+at.Sub(last).Seconds()*float64(rate))
		last = at
		for credit >= 1 {
			select {
			case throttle <- true:
				credit--
				continue
			case <-exit:
				return
			default:
			}
			// Consumer backpressure must not earn scheduling catch-up credit.
			select {
			case throttle <- true:
				credit--
				last = now()
			case <-exit:
				return
			}
		}
		space = max(0, burst-len(throttle))
	}
}

func hardThrottle(notThrottle chan bool, ratePerSec int) func(chan bool) {
	return func(exit chan bool) {
		defer close(notThrottle)

		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		refill := func() bool {
			missing := ratePerSec - len(notThrottle)
			for i := 0; i < missing; i++ {
				select {
				case notThrottle <- true:
				case <-exit:
					return false
				}
			}
			return true
		}

		if !refill() {
			return
		}

		for {
			select {
			case <-ticker.C:
				if !refill() {
					return
				}
			case <-exit:
				return
			}
		}
	}
}
