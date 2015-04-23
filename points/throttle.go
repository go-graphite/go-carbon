package points

import (
	"time"
)

// ThrottleChan creates new points channel with limited throughput
func ThrottleChan(in chan *Points, ratePerSec int) chan *Points {
	out := make(chan *Points, cap(in))

	step := time.Duration(1e9/ratePerSec) * time.Nanosecond

	go func() {
		var p *Points
		var opened bool

		defer close(out)

		// start flight
		throttleTicker := time.NewTicker(step)
		defer throttleTicker.Stop()

		for {
			<-throttleTicker.C
			if p, opened = <-in; !opened {
				return
			}
			out <- p
		}
	}()

	return out
}
