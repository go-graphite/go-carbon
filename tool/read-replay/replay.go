package main

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"time"
)

func runReplay(ctx context.Context, c config, requests []request, s schedule, report summary, client *http.Client, out io.Writer) summary {
	report.Started = time.Now()
	deadline := report.Started.Add(c.Duration)
	minuteStart := report.Started
	minuteEnd := minuteStart.Add(time.Minute)
	var total, minute accumulator
	var window rolling
	encoder := json.NewEncoder(out)
	flush := func(end time.Time) {
		row := interval{minuteStart, end, minute.snapshot()}
		report.Minutes = append(report.Minutes, row)
		_ = encoder.Encode(struct {
			Type string `json:"type"`
			interval
		}{"minute", row})
		minute = accumulator{}
		minuteStart, minuteEnd = end, end.Add(time.Minute)
	}
	flushUntil := func(now time.Time) {
		for !now.Before(minuteEnd) {
			flush(minuteEnd)
		}
	}
	requestCtx, cancelRequests := context.WithCancel(context.Background())
	defer cancelRequests()
	results := make(chan result, c.Concurrency)
	inFlight := 0
	collect := func(r result) {
		now := time.Now()
		flushUntil(now)
		inFlight--
		total.add(r)
		minute.add(r)
		window.add(now, r)
	}
	// In fixed-rate mode, actual starts also respect that rate after a late wake.
	minGap := time.Duration(math.Ceil(float64(time.Second) / c.MaxRPS))
	minGap = max(minGap, s.fixed)
	var lastSend time.Time
	timer := time.NewTimer(0)
	defer timer.Stop()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for report.Reason == "" {
		now := time.Now()
		flushUntil(now)
		switch {
		case ctx.Err() != nil:
			report.Reason = "signal"
		case !now.Before(deadline):
			report.Reason = "duration"
		default:
			report.Reason = window.reason(now, c)
		}
		if report.Reason != "" {
			break
		}
		due := report.Started.Add(s.next())
		// Wait for the actual-send gap too. Dropping a slot just because timer
		// jitter made it a few microseconds early would halve a fixed rate.
		wake := due
		if !lastSend.IsZero() {
			wake = maxTime(wake, lastSend.Add(minGap))
		}
		timer.Reset(max(time.Duration(0), min(wake.Sub(now), deadline.Sub(now))))
		select {
		case <-ctx.Done():
			report.Reason = "signal"
		case r := <-results:
			collect(r)
		case <-tick.C:
		case <-timer.C:
			now = time.Now()
			// Report minute boundaries before launching, independent of whether
			// the minute tick or send timer won the select at the same instant.
			flushUntil(now)
			now = time.Now()
			if ctx.Err() != nil || !now.Before(deadline) {
				continue
			}
			var capped uint64
			if wake.After(due) {
				capped = uint64(max(int64(0), s.latest(wake.Sub(report.Started))-s.pos))
			}
			index, missed := s.take(now.Sub(report.Started))
			total.DroppedCap += capped
			minute.DroppedCap += capped
			total.DroppedLate += missed - capped
			minute.DroppedLate += missed - capped
			if !lastSend.IsZero() && now.Sub(lastSend) < minGap {
				total.DroppedCap++
				minute.DroppedCap++
				continue
			}
			if inFlight >= c.Concurrency {
				total.DroppedBusy++
				minute.DroppedBusy++
				continue
			}
			lastSend = now
			inFlight++
			total.Started++
			minute.Started++
			go func(r request) { results <- send(requestCtx, client, c.Target, r) }(requests[index])
		}
	}
	report.Stopped = time.Now()
	window.prune(report.Stopped)
	report.SafetyWindow = safetySnapshot{Count: window.hist.count, Failed: window.failed, P99: milliseconds(window.hist.percentile(.99))}
	if window.hist.count > 0 {
		report.SafetyWindow.ErrorRatio = float64(window.failed) / float64(window.hist.count)
	}
	// Signals and safety stops stop scheduling immediately, but allow existing
	// body reads to finish. The request context is cancelled at the drain limit.
	drain := time.NewTimer(5 * time.Second)
	defer drain.Stop()
draining:
	for inFlight > 0 {
		select {
		case r := <-results:
			collect(r)
		case <-tick.C:
			flushUntil(time.Now())
		case <-drain.C:
			cancelRequests()
			break draining
		}
	}
	report.Ended = time.Now()
	report.Undrained = inFlight
	flushUntil(report.Ended)
	if report.Ended.After(minuteStart) {
		flush(report.Ended)
	}
	report.Total = total.snapshot()
	_ = encoder.Encode(struct {
		Type      string `json:"type"`
		Reason    string `json:"stop_reason"`
		Undrained int    `json:"undrained"`
		metrics
	}{"summary", report.Reason, report.Undrained, report.Total})
	return report
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
