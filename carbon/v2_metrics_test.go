package carbon

import (
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
)

func TestV2PrometheusOptInAndLifecycle(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			app := v2TestApp(t, t.TempDir())
			app.Config.Prometheus.Enabled = enabled
			if err := app.Start(); err != nil {
				t.Fatal(err)
			}
			defer app.Stop()
			conn, err := net.Dial("tcp", app.v2.server.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			_, err = fmt.Fprintf(conn, "metrics.canary 1 %d\n", time.Now().Unix())
			_ = conn.Close()
			if err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(3 * time.Second)
			for app.v2.server.Stats().Accepted < 1 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			families, err := app.PromRegistry.Gather()
			if err != nil {
				t.Fatal(err)
			}
			seen := map[string]bool{}
			for _, family := range families {
				name := family.GetName()
				if !strings.HasPrefix(name, "v2_") {
					continue
				}
				if !enabled {
					t.Fatalf("disabled metrics exported %s", name)
				}
				seen[name] = true
				if name == "v2_ingest_points_total" && family.Metric[0].Counter.GetValue() != 1 {
					t.Fatalf("accepted point not counted: %v", family)
				}
				if name == "v2_parts" && len(family.Metric) != 7 {
					t.Fatalf("expected seven bounded level labels: %v", family)
				}
			}
			if enabled {
				for _, name := range []string{"v2_ingest_points_total", "v2_committed_points_total", "v2_parts", "v2_seal_lag_seconds", "v2_rollup_lag_seconds", "v2_read_duration_seconds", "v2_dual_write_enabled", "v2_sealed_parts", "v2_merge_over_budget_seals_total", "v2_merge_runs_total", "v2_part_gc_pending_parts", "v2_part_gc_removed_parts_total", "v2_part_gc_unlinked_bytes_total", "v2_catalog_base_file_errors_total", "v2_catalog_base_heap_bytes"} {
					if !seen[name] {
						t.Errorf("missing %s", name)
					}
				}
			}
			stats := map[string]float64{}
			app.v2.catalog.Stat(func(k string, v float64) { stats[k] = v })
			if v, ok := stats["baseFileErrors"]; !ok || v != 0 || len(stats) != 2 {
				t.Fatalf("catalog stats: %v", stats)
			}
			app.Stop()
			// Collection after Stop must not access the closed Pebble handles.
			if _, err := app.PromRegistry.Gather(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
