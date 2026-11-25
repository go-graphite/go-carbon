package carbonserver

import (
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/helper"
	"github.com/go-graphite/go-carbon/persister"
	"github.com/go-graphite/go-carbon/points"
)

// Set GO_CARBON_QUOTA_BENCH_FILE to replay a storage-quotas.conf; otherwise
// use anonymous rules with the same shape. The first seven rules are the
// baseline; the remaining 2,566 rules constrain metrics only.
func metricOnlyBenchmarkRules(b *testing.B) []*Quota {
	b.Helper()
	if file := os.Getenv("GO_CARBON_QUOTA_BENCH_FILE"); file != "" {
		parsed, err := persister.ReadWhisperQuotas(file)
		if err != nil {
			b.Fatal(err)
		}
		rules := make([]*Quota, len(parsed))
		for i, q := range parsed {
			rules[i] = &Quota{Pattern: q.Pattern, Namespaces: q.Namespaces, Metrics: q.Metrics,
				LogicalSize: q.LogicalSize, PhysicalSize: q.PhysicalSize, DataPoints: q.DataPoints,
				Throughput: q.Throughput, DroppingPolicy: ParseQuotaDroppingPolicy(q.DroppingPolicy),
				StatMetricPrefix: q.StatMetricPrefix}
		}
		if len(rules) != 2573 {
			b.Fatalf("got %d rules, want 2573", len(rules))
		}
		return rules
	}
	var rules []*Quota
	for _, pattern := range []string{"*", "aggregations", "aggregations.minutely", "aggregations.secondly", "aggregations.minutely.*", "aggregations.minutely.mobile_apps", "/"} {
		rules = append(rules, &Quota{Pattern: pattern, Metrics: math.MaxInt64, Throughput: math.MaxInt64})
	}
	for i := range 2566 {
		pattern := fmt.Sprintf("aggregations.secondly.namespace_%04d", i)
		if i%5 == 0 {
			pattern += ".child"
		}
		rules = append(rules, &Quota{Pattern: pattern, Metrics: 26500, DroppingPolicy: QDPNone})
	}
	return rules
}

// BenchmarkQuotaMetricOnly measures one point per cache admission callback.
// 80% of names match the extra rules, 20% use the original namespaces; names
// have 6-12 components. Both cache hits and re-admissions of indexed metrics
// are measured. Index/config construction and route warmup are not timed.
func BenchmarkQuotaMetricOnly(b *testing.B) {
	rules := metricOnlyBenchmarkRules(b)
	const n = 32768
	names := make([]string, n)
	for i := range names {
		prefix := rules[7+(i*7919)%2566].Pattern
		if i%5 == 0 {
			prefix = "aggregations.minutely.mobile_apps"
		}
		depth := max(6+i%7, strings.Count(prefix, ".")+2)
		names[i] = prefix + "." + strings.Repeat("component.", depth-strings.Count(prefix, ".")-2) + fmt.Sprintf("value_%05d", i)
	}
	for _, count := range []int{7, 2573} {
		for _, cachedRoute := range []bool{false, true} {
			for _, inCache := range []bool{true, false} {
				b.Run(fmt.Sprintf("rules=%d/route=%t/inCache=%t", count, cachedRoute, inCache), func(b *testing.B) {
					ti := newTrie(".wsp", 0, nil)
					for _, name := range names {
						if _, err := ti.insert("/"+strings.ReplaceAll(name, ".", "/")+".wsp", 0, 0, 0, 0); err != nil {
							b.Fatal(err)
						}
					}
					if _, err := ti.applyQuotas(time.Minute, rules[:count]...); err != nil {
						b.Fatal(err)
					}
					listener := NewCarbonserverListener(nil)
					listener.UpdateFileIndex(&fileIndex{trieIdx: ti})
					states, hashes := make([]any, n), make([]uint64, n)
					ps := points.OnePoint("", 1, 1)
					for i, name := range names {
						ps.Metric, hashes[i] = name, helper.HashString(name)
						listener.ShouldThrottleMetricWithHash(ps, true, hashes[i], &states[i])
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := range b.N {
						j := i & (n - 1)
						ps.Metric = names[j]
						var throttled bool
						if cachedRoute {
							if !inCache {
								// A newly admitted cache item has no saved route.
								states[j] = nil
								}
							throttled = listener.ShouldThrottleMetricWithHash(ps, inCache, hashes[j], &states[j])
						} else {
							throttled = listener.ShouldThrottleMetric(ps, inCache)
						}
						if throttled {
							b.Fatal("unexpected throttle")
						}
					}
				})
			}
		}
	}
}
