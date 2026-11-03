package downsize

import "testing"

func TestLatencyIdleEnvelope(t *testing.T) {
	h := hostLimit{latencyBaseline: &backlogBaseline{Min: 100, Median: 500, Max: 12691}}
	for _, await := range []float64{.5, 12.690667, 3, 12.690667} {
		if s := h.observeLatency(hostSample{DiskAwaitMS: await, DiskUtil: 1.2}, false); s.Soft != "" {
			t.Fatal(s)
		}
	}
	for i := range 3 {
		s := h.observeLatency(hostSample{DiskAwaitMS: 16, DiskUtil: 1.2}, false)
		if (s.Soft != "") != (i == 2) {
			t.Fatal(i, s)
		}
	}
	// The same original 2ms floor applies at high utilization when a quiet
	// measured baseline permits it; a single spike does not stop admission.
	h = hostLimit{latencyBaseline: &backlogBaseline{Max: 500}}
	for i := range 3 {
		s := h.observeLatency(hostSample{DiskAwaitMS: 3, DiskUtil: 95}, false)
		if (s.Soft != "") != (i == 2) || s.Limit != 2 {
			t.Fatal(i, s)
		}
	}
}
