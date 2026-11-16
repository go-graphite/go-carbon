package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"
)

// replayPacer is private to one stream. It reserves time before each batch,
// never banking catch-up credit while storage is stalled. It changes timing
// only: source timestamps, sequence numbers and apply boundaries stay intact.
type replayPacer struct {
	rate    uint64
	streams int
	next    time.Time
}

func (p *replayPacer) deadline(now time.Time, rows uint64) time.Time {
	if p.next.IsZero() {
		p.next = now
	}
	d := time.Duration(float64(rows) * float64(p.streams) / float64(p.rate) * float64(time.Second))
	p.next = p.next.Add(max(d, time.Nanosecond))
	if p.next.Before(now) {
		p.next = now
	}
	return p.next
}

func (p *replayPacer) wait(stop <-chan struct{}, rows uint64) error {
	if p.rate == 0 || rows == 0 {
		return nil
	}
	timer := time.NewTimer(time.Until(p.deadline(time.Now(), rows)))
	defer timer.Stop()
	select {
	case <-stop:
		return context.Canceled
	case <-timer.C:
		return nil
	}
}

func replayRateEnv(name string, fallback uint64) (uint64, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}
	rate, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || rate == 0 {
		return 0, fmt.Errorf("invalid %s: expected positive points/s", name)
	}
	return rate, nil
}
