package persister

import (
	"time"

	"github.com/lomik/go-carbon/helper"
)

// ThrottleTicker is a ticker that can be used for hard or soft rate-limiting.
//
// * A soft rate limiter will send a message on C at the actual rate that is
//   specified.
//
// * A hard rate limiter may send arbitrarily many messages on C every second,
//   but it will send the value 'true' with the first ratePerSec ones, and
//   'false' with all subsequent ones, until the next second. It is up to the
//   user to decide what to do in each case.
type ThrottleTicker struct {
	helper.Stoppable
	C chan bool
}

// NewThrottleTicker returns a new soft throttle ticker.
func NewThrottleTicker(ratePerSec int) *ThrottleTicker {
	return newThrottleTicker(ratePerSec, false)
}

// NewSoftThrottleTicker returns a new soft throttle ticker.
func NewSoftThrottleTicker(ratePerSec int) *ThrottleTicker {
	return newThrottleTicker(ratePerSec, false)
}

// NewHardThrottleTicker returns a new hard throttle ticker.
func NewHardThrottleTicker(ratePerSec int) *ThrottleTicker {
	return newThrottleTicker(ratePerSec, true)
}

func newThrottleTicker(ratePerSec int, hard bool) *ThrottleTicker {
	t := &ThrottleTicker{
		C: make(chan bool, ratePerSec),
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
					//pass
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

func hardThrottle(throttle chan bool, ratePerSec int) func(chan bool) {
	return func(exit chan bool) {
		defer close(throttle)

		tick := time.Now().Add(time.Second)
		sent := 0
		for {
			select {
			case throttle <- sent < ratePerSec:
				if time.Now().Before(tick) {
					// Let's not overflow in case computers get wonderfully
					// better, however improbable that is
					if sent < ratePerSec {
						sent++
					}
				} else {
					sent = 0
					tick = time.Now().Add(time.Second)
				}

			case <-exit:
				return
			}
		}
	}
}
