package persister

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// TestPrometheusPersisterReplacement preserves histogram observations across
// replacement persisters, including the wrapped registerer used by the app.
func TestPrometheusPersisterReplacement(t *testing.T) {
	for _, wrapped := range []bool{false, true} {
		name := "plain"
		if wrapped {
			name = "wrapped"
		}
		t.Run(name, func(t *testing.T) {
			registry := prometheus.NewPedanticRegistry()
			var registerer prometheus.Registerer = registry
			if wrapped {
				registerer = prometheus.WrapRegistererWithPrefix("carbon_",
					prometheus.WrapRegistererWith(prometheus.Labels{"instance": "test"}, registry))
			}
			for i := 1; i <= 3; i++ {
				p := new(Whisper)
				p.InitPrometheus(registerer)
				p.prometheus.outOfOrderWriteLag(time.Duration(i) * time.Second)
				families, err := registry.Gather()
				if err != nil {
					t.Fatal(err)
				}
				if len(families) != 1 || len(families[0].Metric) != 1 {
					t.Fatalf("unexpected metric families: %v", families)
				}
				histogram := families[0].Metric[0].GetHistogram()
				if histogram.GetSampleCount() != uint64(i) || histogram.GetSampleSum() != float64(i*(i+1)/2) {
					t.Fatalf("observations reset on replacement %d: %v", i, histogram)
				}
			}
		})
	}
}
