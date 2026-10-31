package helper

import (
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
	t := &ThrottleTicker{
		C: make(chan bool, min(ratePerSec, max(1, burst))),
	}

	t.Start()

	if ratePerSec <= 0 {
		close(t.C)
		return t
	}

	if hard {
		t.Go(hardThrottle(t.C, ratePerSec))
	} else {
		t.Go(softThrottle(t.C, ratePerSec))
	}

	return t
}

func softThrottle(throttle chan bool, ratePerSec int) func(chan bool) {
	return func(exit chan bool) {
		defer close(throttle)

		delimeter := ratePerSec
		chunk := 1

		if ratePerSec > 1000 {
			minRemainder := ratePerSec

			for i := 100; i < 1000; i++ {
				if ratePerSec%i < minRemainder {
					delimeter = i
					minRemainder = ratePerSec % delimeter
				}
			}

			chunk = ratePerSec / delimeter
		}

		step := time.Duration(1e9/delimeter) * time.Nanosecond

		ticker := time.NewTicker(step)
		defer ticker.Stop()

	LOOP:
		for {
			select {
			case <-ticker.C:
				for i := 0; i < chunk; i++ {
					select {
					case throttle <- true:
					// pass
					case <-exit:
						break LOOP
					}
				}
			case <-exit:
				break LOOP
			}
		}
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
