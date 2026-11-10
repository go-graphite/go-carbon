package carbon

import (
	"math"
	"runtime/metrics"
	"strconv"

	"github.com/go-graphite/go-carbon/helper"
	"github.com/prometheus/client_golang/prometheus"
)

const mutexWaitMetric = "/sync/mutex/wait/total:seconds"

// Each read owns its sample storage: HTTP scrapes and Graphite collection can
// run concurrently. Reading these runtime counters never enters an app lock.
func runtimeWaitSamples() []metrics.Sample {
	s := []metrics.Sample{{Name: mutexWaitMetric}, {Name: "/sched/latencies:seconds"}}
	metrics.Read(s)
	return s
}

var (
	mutexWaitDesc        = prometheus.NewDesc("go_sync_mutex_wait_seconds_total", "Estimated cumulative goroutine seconds waiting for Go mutexes, including runtime locks. rate() gives wait seconds per wall second, not CPU utilization.", nil, nil)
	schedulerLatencyDesc = prometheus.NewDesc("go_sched_latency_seconds", "Runtime sampled time goroutines spent runnable before execution. Histogram sum is an estimate from bucket midpoints.", nil, nil)
)

type runtimeWaitCollector struct{}

func (runtimeWaitCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- mutexWaitDesc
	ch <- schedulerLatencyDesc
}

func (runtimeWaitCollector) Collect(ch chan<- prometheus.Metric) {
	s := runtimeWaitSamples()
	ch <- prometheus.MustNewConstMetric(mutexWaitDesc, prometheus.CounterValue, s[0].Value.Float64())
	h := s[1].Value.Float64Histogram()
	buckets := make(map[float64]uint64)
	count, sum := schedulerHistogram(h, func(upper float64, _ uint64, count uint64) { buckets[upper] = count })
	ch <- prometheus.MustNewConstHistogram(schedulerLatencyDesc, count, sum, buckets)
}

func schedulerHistogram(h *metrics.Float64Histogram, bucket func(upper float64, ns uint64, count uint64)) (count uint64, sum float64) {
	// Coarsen the runtime's logarithmic buckets to powers of two. +Inf is
	// implicit; upper bounds are exclusive in runtime/metrics (inclusive in Prom).
	for i, n := range h.Counts {
		count += n
		lo, hi := h.Buckets[i], h.Buckets[i+1]
		if !math.IsInf(hi, 1) {
			if lo >= 0 {
				sum += (lo + hi) / 2 * float64(n)
			}
			nanos := hi * 1e9
			if nanos >= 1 && nanos < math.MaxInt64 {
				ns := uint64(math.Round(nanos))
				if ns&(ns-1) == 0 {
					bucket(math.Nextafter(hi, math.Inf(-1)), ns-1, count)
				}
			}
		}
	}
	return
}

func runtimeWaitStat(send helper.StatCallback) {
	s := runtimeWaitSamples()
	send("mutexWaitSecondsTotal", s[0].Value.Float64())
	count, sum := schedulerHistogram(s[1].Value.Float64Histogram(), func(_ float64, ns uint64, count uint64) {
		send("schedulerLatencyBuckets.le"+strconv.FormatUint(ns, 10)+"Ns", float64(count))
	})
	send("schedulerLatencySamplesTotal", float64(count))
	send("schedulerLatencySecondsTotal", sum)
}
