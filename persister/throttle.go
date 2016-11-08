package persister

import (
	"time"

	"github.com/lomik/go-carbon/helper"
)

type ThrottleTicker struct {
	helper.Stoppable
	C chan bool
}

func NewThrottleTicker(ratePerSec int) *ThrottleTicker {
	t := &ThrottleTicker{
		C: make(chan bool, ratePerSec),
	}

	t.Start()

	if ratePerSec <= 0 {
		close(t.C)
		return t
	}

	t.Go(func(exit chan bool) {
		defer close(t.C)

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
					case t.C <- true:
					//pass
					case <-exit:
						break LOOP
					}
				}
			case <-exit:
				break LOOP
			}
		}
	})

	return t
}
