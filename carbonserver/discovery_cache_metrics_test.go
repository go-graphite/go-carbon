package carbonserver

import (
	"context"
	"testing"
	"time"

	protov2 "github.com/go-graphite/protocol/carbonapi_v2_pb"
	prom "github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

func TestDiscoveryCachePrometheusRequests(t *testing.T) {
	t.Parallel()
	for _, protocol := range []string{"http", "grpc"} {
		t.Run(protocol, func(t *testing.T) {
			listener := newDiscoveryCacheTestListener(t)
			registry := prom.NewRegistry()
			listener.InitPrometheus(registry)
			const query = "aggregations.secondly._meta.by_queue_and_host.*.*.build_sha.*"
			for i := 0; i < 2; i++ {
				var err error
				if protocol == "http" {
					_, _, err = listener.findResponse(context.Background(), zap.NewNop(), time.Now(), jsonFormat, "json", []string{query})
				} else {
					_, err = listener.Find(context.Background(), &protov2.GlobRequest{Query: query})
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			families, err := registry.Gather()
			if err != nil {
				t.Fatal(err)
			}
			counts := map[string]float64{}
			for _, family := range families {
				if family.GetName() != "cache_requests_total" {
					continue
				}
				for _, metric := range family.Metric {
					kind, hit := "", ""
					for _, label := range metric.Label {
						switch label.GetName() {
						case "type":
							kind = label.GetValue()
						case "hit":
							hit = label.GetValue()
						}
					}
					if kind == "find" {
						counts[hit] = metric.GetCounter().GetValue()
					}
				}
			}
			if counts["false"] != 1 || counts["true"] != 1 {
				t.Fatalf("find cache counts=%v; want one miss and one hit", counts)
			}
		})
	}
}
