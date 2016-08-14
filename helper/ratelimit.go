package helper

import "time"

type Ratelimiter struct {
	excess time.Duration
	last   time.Time
}

func NewRatelimiter() Ratelimiter {
	return Ratelimiter{
		excess: 0,
		last:   time.Time{},
	}
}

func (l *Ratelimiter) Tick(ratePerSecond int) (sleepTime time.Duration) {
	if ratePerSecond == 0 {
		return 0
	}

	if (l.last == time.Time{}) { // first tick
		l.last = time.Now()
		return 0
	}

	now := time.Now()
	elapsed := now.Sub(l.last)

	if elapsed < 0 { // GO time is not monotonic
		elapsed = -elapsed
	}

	rps := time.Duration(ratePerSecond)
	excess := l.excess - rps*elapsed + time.Second

	if excess < 0 {
		excess = 0
	}

	l.excess = excess
	l.last = now

	sleepTime = (excess / rps)

	// don't return very short sleeps. it will produce correct result on average nevertheless
	if sleepTime < time.Duration(10)*time.Millisecond {
		sleepTime = 0
	}

	return sleepTime
}

func (l *Ratelimiter) TickSleep(ratePerSecond int) {
	sleepTime := l.Tick(ratePerSecond)
	if sleepTime > 0 {
		time.Sleep(sleepTime)
	}
}
