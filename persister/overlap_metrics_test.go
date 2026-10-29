package persister

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestOverlapUpdatesMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	p := NewWhisper("", nil, nil, nil, nil, nil, nil)
	p.InitPrometheus(reg)
	// A reloaded persister registers again on the same registry.
	NewWhisper("", nil, nil, nil, nil, nil, nil).InitPrometheus(reg)
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[string]bool{}
	for _, family := range families {
		if family.GetName() != "persister_overlap_updates_total" {
			continue
		}
		for _, m := range family.GetMetric() {
			for _, label := range m.GetLabel() {
				if label.GetName() == "kind" {
					kinds[label.GetValue()] = true
				}
			}
		}
	}
	for _, kind := range []string{"batch_slot", "buffered_rollup", "sidecar_slot", "future", "stored_slot"} {
		if !kinds[kind] {
			t.Errorf("persister_overlap_updates_total{kind=%q} missing, got %v", kind, kinds)
		}
	}
	results := map[string]map[string]bool{"persister_rollup_corrections_total": {}, "persister_sidecar_slot_merges_total": {}, "persister_sidecar_folds_total": {}, "persister_ooo_direct_sidecar_folds_total": {}}
	for _, family := range families {
		if seen, ok := results[family.GetName()]; ok {
			for _, m := range family.GetMetric() {
				for _, label := range m.GetLabel() {
					if label.GetName() == "result" {
						seen[label.GetValue()] = true
					}
				}
			}
		}
	}
	for name, seen := range results {
		for _, result := range []string{"in_place", "rewrite_unsupported", "rewrite_extend", "rewrite_overflow", "rewrite_late_batch", "rewrite_tail"} {
			if !seen[result] {
				t.Errorf("%s{result=%q} missing, got %v", name, result, seen)
			}
		}
	}
	late := map[string]map[string]bool{"persister_ooo_direct_writes_total": {}, "persister_ooo_direct_points_total": {}}
	for _, family := range families {
		if seen, ok := late[family.GetName()]; ok {
			for _, m := range family.GetMetric() {
				for _, label := range m.GetLabel() {
					if label.GetName() == "result" {
						seen[label.GetValue()] = true
					}
				}
			}
		}
	}
	for name, seen := range late {
		for _, result := range []string{"direct", "sidecar_unsupported", "sidecar_unfolded", "sidecar_overflow", "sidecar_tail", "sidecar_coarse"} {
			if !seen[result] {
				t.Errorf("%s{result=%q} missing, got %v", name, result, seen)
			}
		}
	}
	fallbacks := map[string]bool{}
	for _, family := range families {
		if family.GetName() != "persister_rewrite_block_fallbacks_total" {
			continue
		}
		for _, m := range family.GetMetric() {
			for _, label := range m.GetLabel() {
				if label.GetName() == "kind" {
					fallbacks[label.GetValue()] = true
				}
			}
		}
	}
	for _, kind := range []string{"exact", "aligned"} {
		if !fallbacks[kind] {
			t.Errorf("persister_rewrite_block_fallbacks_total{kind=%q} missing, got %v", kind, fallbacks)
		}
	}
	stats := map[string]float64{}
	p.sendOverlapUpdates(func(name string, value float64) { stats[name] = value })
	if _, ok := stats["overlapUpdates.buffered_rollup"]; !ok || len(stats) != 5 {
		t.Errorf("unexpected overlap stats %v", stats)
	}
}
