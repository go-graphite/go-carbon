// Package loadgen generates a synthetic metric mix for input log tests and
// measurements: container-style names of 60-200 bytes sharing long per-pod
// prefixes, and values with the temporal correlation of real series.
package loadgen

import (
	"fmt"
	"math/rand/v2"
	"strings"
)

// Value kinds, by share of metrics: counters 40%, gauges 20%, mostly
// constant 25%, noisy floats 15%.
const (
	counter = iota
	gauge
	constant
	noisy
)

// Mix is a set of metrics with their current values.
type Mix struct {
	Names  [][]byte
	kinds  []uint8
	values []float64
}

var (
	clusters = []string{"eu-west-prod-a", "eu-west-prod-b", "eu-north-prod-a", "eu-central-prod-c", "eu-west-staging",
		"eu-north-staging", "eu-central-prod-a", "eu-west-prod-c"}
	apps = []string{"search", "availability-cache", "pricing-engine", "checkout-flow-orchestrator", "geo", "reviews-aggregator",
		"payments-gateway", "inventory-sync-worker", "auth", "content-delivery-edge-proxy", "ranking", "notifications-dispatcher"}
	containers = []string{"app", "istio-proxy", "log-shipper"}
	families   = []string{"cpu.usage_seconds_total", "cpu.throttled_seconds_total", "memory.rss_bytes", "memory.working_set_bytes",
		"network.receive_bytes_total", "network.transmit_bytes_total", "fs.writes_bytes_total", "process.open_fds", "gc.pause_seconds.p99"}
	routes = []string{"api_v1_search", "api_v1_hotels_availability_by_destination", "api_v2_pricing_quote", "health",
		"api_v1_reviews_summary_for_property", "api_v3_orders_confirm_with_payment", "internal_metrics"}
	routeFamilies = []string{"requests.count", "requests.duration_seconds.p50", "requests.duration_seconds.p99", "errors.count"}
	statuses      = []string{"2xx", "3xx", "4xx", "5xx"}
)

// New returns n metrics. Each pod has a fixed set of metrics, so names share
// long prefixes as in production.
func New(n int, seed uint64) *Mix {
	r := rand.New(rand.NewPCG(seed, 2))
	m := &Mix{Names: make([][]byte, 0, n), kinds: make([]uint8, n), values: make([]float64, n)}
	for len(m.Names) < n {
		app := apps[r.IntN(len(apps))]
		pod := fmt.Sprintf("containers.%s.%s-ns-%03d.%s-%010x-%05x", clusters[r.IntN(len(clusters))], app, r.IntN(60), app, r.Uint64()&(1<<40-1), r.IntN(1<<20))
		for _, container := range containers {
			for _, family := range families {
				m.add(fmt.Sprintf("%s.%s.%s", pod, container, family))
			}
			if container != "app" {
				continue
			}
			for _, route := range routes {
				for _, family := range routeFamilies {
					m.add(fmt.Sprintf("%s.%s.http_server.route.%s.status.%s.%s", pod, container, route, statuses[r.IntN(len(statuses))], family))
				}
			}
		}
	}
	m.Names = m.Names[:n]
	for i := range m.kinds {
		switch k := r.IntN(20); {
		case k < 8:
			m.kinds[i] = counter
		case k < 12:
			m.kinds[i] = gauge
		case k < 17:
			m.kinds[i] = constant
		default:
			m.kinds[i] = noisy
		}
		m.values[i] = float64(r.IntN(1000))
	}
	return m
}

func (m *Mix) add(name string) {
	if len(m.Names) < cap(m.Names) {
		m.Names = append(m.Names, []byte(strings.Clone(name)))
	}
}

// Next advances metric i and returns its value. Callers that share a Mix must
// use disjoint metrics and their own r.
func (m *Mix) Next(i int, r *rand.Rand) float64 {
	v := m.values[i]
	switch m.kinds[i] {
	case counter:
		v += float64(r.IntN(100))
	case gauge:
		v += (r.Float64() - 0.5) * 10
	case constant:
		if r.IntN(100) == 0 {
			v = float64(r.IntN(10))
		}
	default:
		v = r.Float64() * 1000
	}
	m.values[i] = v
	return v
}

// MeanNameLength reports the average name length.
func (m *Mix) MeanNameLength() float64 {
	total := 0
	for _, name := range m.Names {
		total += len(name)
	}
	return float64(total) / float64(len(m.Names))
}
