package main

import (
	"math"
	"math/bits"
	"time"
)

// Logarithmic histogram with 128 subdivisions per power of two, in ns.
// Quantiles use bucket upper bounds (less than 0.8% relative error); max is exact.
type histogram struct {
	bins  [64 * 128]uint64
	count uint64
	max   time.Duration
}

func bucket(d time.Duration) int {
	n := uint64(max(d, 0))
	if n < 128 {
		return int(n)
	}
	e := bits.Len64(n) - 1
	return (e-6)*128 + int((n>>uint(e-7))&127)
}

func bucketUpper(b int) time.Duration {
	if b < 128 {
		return time.Duration(b)
	}
	shift := uint(b/128 - 1)
	n := uint64(129+b%128)<<shift - 1
	return time.Duration(min(n, math.MaxInt64))
}

func (h *histogram) add(d time.Duration) {
	h.bins[bucket(d)]++
	h.count++
	h.max = max(h.max, d)
}

func (h *histogram) percentile(q float64) time.Duration {
	if h.count == 0 {
		return 0
	}
	rank := uint64(math.Ceil(q * float64(h.count)))
	var n uint64
	for b, count := range h.bins {
		n += count
		if n >= rank {
			return min(bucketUpper(b), h.max)
		}
	}
	return 0
}

type distribution struct {
	Count uint64  `json:"count"`
	P50   float64 `json:"p50_ms"`
	P90   float64 `json:"p90_ms"`
	P99   float64 `json:"p99_ms"`
	P999  float64 `json:"p99_9_ms"`
	Max   float64 `json:"max_ms"`
}

func milliseconds(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func (h *histogram) distribution() distribution {
	return distribution{h.count, milliseconds(h.percentile(.5)), milliseconds(h.percentile(.9)), milliseconds(h.percentile(.99)), milliseconds(h.percentile(.999)), milliseconds(h.max)}
}

type result struct {
	Latency, Runtime time.Duration
	Code             int
	Bytes            int64
	Error, Timeout   bool
	FromCache        bool
}

func (r result) failed() bool { return r.Error || r.Timeout || r.Code < 200 || r.Code >= 300 }

type metrics struct {
	Started       uint64         `json:"started"`
	Count         uint64         `json:"count"`
	Failed        uint64         `json:"failed"`
	HTTPCodes     map[int]uint64 `json:"http_codes"`
	HTTPErrors    uint64         `json:"http_errors"`
	Errors        uint64         `json:"errors"`
	Timeouts      uint64         `json:"timeouts"`
	Bytes         int64          `json:"bytes"`
	FromCache     uint64         `json:"logged_from_cache"`
	DroppedLate   uint64         `json:"dropped_late"`
	DroppedCap    uint64         `json:"dropped_max_rps"`
	DroppedBusy   uint64         `json:"dropped_concurrency"`
	Latency       distribution   `json:"latency"`
	LoggedRuntime distribution   `json:"logged_runtime"`
}

type accumulator struct {
	metrics
	latency, runtime histogram
}

func (a *accumulator) add(r result) {
	a.Count++
	if r.failed() {
		a.Failed++
	}
	a.Bytes += r.Bytes
	if a.HTTPCodes == nil {
		a.HTTPCodes = make(map[int]uint64)
	}
	if r.Code > 0 {
		a.HTTPCodes[r.Code]++
		if r.Code < 200 || r.Code >= 300 {
			a.HTTPErrors++
		}
	}
	if r.Error {
		a.Errors++
	}
	if r.Timeout {
		a.Timeouts++
	}
	if r.FromCache {
		a.FromCache++
	}
	a.latency.add(r.Latency)
	a.runtime.add(r.Runtime)
}

func (a *accumulator) snapshot() metrics {
	m := a.metrics
	m.Latency = a.latency.distribution()
	m.LoggedRuntime = a.runtime.distribution()
	return m
}

type observation struct {
	at     time.Time
	bucket int
	failed bool
}

type rolling struct {
	items  []observation
	head   int
	failed uint64
	hist   histogram
}

func (w *rolling) add(now time.Time, r result) {
	w.items = append(w.items, observation{now, bucket(r.Latency), r.failed()})
	w.hist.add(r.Latency)
	if r.failed() {
		w.failed++
	}
}

func (w *rolling) prune(now time.Time) {
	cutoff := now.Add(-time.Minute)
	for w.head < len(w.items) && !w.items[w.head].at.After(cutoff) {
		x := w.items[w.head]
		w.hist.bins[x.bucket]--
		w.hist.count--
		if x.failed {
			w.failed--
		}
		w.head++
	}
	if w.head > 0 && w.head >= len(w.items)/2 {
		w.items = w.items[:copy(w.items, w.items[w.head:])]
		w.head = 0
	}
}

func (w *rolling) reason(now time.Time, c config) string {
	w.prune(now)
	if w.hist.count == 0 {
		return ""
	}
	if float64(w.failed)/float64(w.hist.count) > c.MaxErrorRatio {
		return "max_error_ratio"
	}
	if w.hist.percentile(.99) > c.AbortP99 {
		return "abort_p99"
	}
	return ""
}
