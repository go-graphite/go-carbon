package carbon

import (
	"context"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/stagetime"
	"github.com/go-graphite/go-carbon/persister"
	"github.com/go-graphite/go-carbon/points"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestMigrationCollectorExportsFetchStages(t *testing.T) {
	now := time.Unix(100000, 0)
	db := expirationTestDB(t, &now)
	m := createExpirationMetric(t, db, "timed.fetch")
	if _, err := db.Fetch(context.Background(), m.Name, int(now.Unix())-60, int(now.Unix())); err != nil {
		t.Fatal(err)
	}
	reg := prometheus.NewRegistry()
	reg.MustRegister(&migrationCollector{app: &App{MetricStore: db}})
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	timings := db.FetchTimings()
	want := map[string]store.FetchTiming{"body": timings.Body, "read_view_close": timings.ReadViewClose, "iterator_close": timings.IteratorClose}
	found := 0
	for _, family := range families {
		name := family.GetName()
		if name != "pebble_fetch_stage_seconds_total" && name != "pebble_fetch_stage_calls_total" {
			continue
		}
		found++
		if len(family.Metric) != len(want) {
			t.Fatalf("%s: wrong number of stages", name)
		}
		for _, metric := range family.Metric {
			stage := metric.GetLabel()[0].GetValue()
			timing, ok := want[stage]
			if !ok {
				t.Fatalf("unexpected stage %q", stage)
			}
			value := float64(timing.Calls)
			if name == "pebble_fetch_stage_seconds_total" {
				value = float64(timing.Nanoseconds) / 1e9
			}
			if metric.GetCounter().GetValue() != value {
				t.Fatalf("%s{%s}: got %v, want %v", name, stage, metric.GetCounter().GetValue(), value)
			}
		}
	}
	if found != 2 || timings.Body.Calls != 1 || timings.ReadViewClose.Calls != 1 || timings.IteratorClose.Calls != 0 {
		t.Fatalf("missing fetch counters: families=%d timings=%+v", found, timings)
	}
}

func TestMigrationCollectorReportsStoreAndMigration(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	gather := func(app *App) map[string]float64 {
		t.Helper()
		reg := prometheus.NewRegistry()
		reg.MustRegister(&migrationCollector{app: app})
		families, err := reg.Gather()
		if err != nil {
			t.Fatal(err)
		}
		values := make(map[string]float64)
		for _, f := range families {
			m := f.GetMetric()[0]
			if c := m.GetCounter(); c != nil {
				values[f.GetName()] = c.GetValue()
			} else {
				values[f.GetName()] = m.GetGauge().GetValue()
			}
		}
		return values
	}
	app := &App{}
	values := gather(app)
	if _, ok := values["pebble_disk_bytes"]; ok {
		t.Fatal("store metrics reported without a store")
	}
	if v, ok := values["store_migration_running"]; !ok || v != 0 {
		t.Fatalf("migration gauge missing or set: %v %v", v, ok)
	}
	db, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, CompactionRate: 123456})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	app.MetricStore = db
	m := &incrementalStoreMigration{maximum: 8}
	m.metrics.Store(5)
	m.unlinked.Store(100)
	m.archived.Store(20)
	app.incrementalMigration.Store(m)
	values = gather(app)
	for name, want := range map[string]float64{"store_migration_running": 1, "store_migration_converted_total": 5, "store_migration_reclaimed_bytes_total": 120, "store_migration_workers_max": 8, "store_ooo_inventory_ready": 1, "store_ooo_sidecars": 0, "store_ooo_sidecar_points": 0, "store_ooo_sidecar_oldest_timestamp_seconds": 0} {
		if values[name] != want {
			t.Fatalf("%s = %v, want %v", name, values[name], want)
		}
	}
	m.phase = "shared"
	values = gather(app)
	if values["store_migration_running"] != 0 || values["store_migration_converted_total"] != 5 {
		t.Fatalf("finished run: running = %v, converted = %v; want 0 and 5", values["store_migration_running"], values["store_migration_converted_total"])
	}
	m.phase = "files"
	if values["pebble_compaction_rate_bytes"] != 123456 {
		t.Fatal("compaction rate metric differs from configured budget")
	}
	if _, ok := values["pebble_disk_bytes"]; !ok {
		t.Fatal("store metrics missing with an open store")
	}
	for _, name := range []string{"pebble_compaction_rate_bytes", "pebble_compaction_pacer_target_rate_bytes", "pebble_compaction_pacer_auto", "pebble_compaction_pacer_bypass", "pebble_compaction_pacer_bytes_total", "pebble_compaction_pacer_wait_seconds_total", "pebble_flush_rate_bytes", "pebble_flush_pacer_target_rate_bytes", "pebble_flush_pacer_auto", "pebble_flush_pacer_bypass", "pebble_flush_pacer_bytes_total", "pebble_flush_pacer_wait_seconds_total", "pebble_compactions_total", "pebble_fs_ops_total", "pebble_fs_bytes_total", "pebble_block_cache_bytes", "pebble_table_cache_entries", "pebble_level_compaction_write_bytes_total", "pebble_level_compaction_read_bytes_total", "pebble_level_bytes", "store_metadata_cache_prewarmed_total", "store_metadata_cache_prewarm_bytes_total", "store_metadata_cache_prewarm_read_bytes_total", "store_metadata_cache_prewarm_duration_seconds_total", "store_relayout_bytes_total", "store_relayout_progress_ratio", "store_relayout_rate_bytes", "store_migration_sidecar_folds_total"} {
		if _, ok := values[name]; !ok {
			t.Fatalf("%s missing with an open store", name)
		}
	}
	if err := db.SetCompactionRate(-1); err != nil {
		t.Fatal(err)
	}
	m.active.Store(1)
	db.SetCompactionPacingConversionSource(func() (int64, uint64) { return m.active.Load(), m.metrics.Load() })
	values = gather(app)
	for name, want := range map[string]float64{"pebble_compaction_pacer_auto": 1, "pebble_compaction_pacer_bypass": 1, "pebble_compaction_rate_bytes": 0, "pebble_compaction_pacer_target_rate_bytes": 8 << 20} {
		if values[name] != want {
			t.Fatalf("%s = %v, want %v", name, values[name], want)
		}
	}
	reg := prometheus.NewRegistry()
	reg.MustRegister(&migrationCollector{app: app})
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	counts := make(map[string]float64)
	for _, family := range families {
		if family.GetName() == "pebble_compaction_pacer_bypasses_total" {
			for _, metric := range family.GetMetric() {
				counts[metric.GetLabel()[0].GetValue()] = metric.GetCounter().GetValue()
			}
		}
	}
	if counts["warmup"] != 1 || counts["conversion"] != 1 {
		t.Fatalf("bypass counters lost reason labels: %v", counts)
	}
}

func TestRelayoutProgress(t *testing.T) {
	for _, c := range []struct {
		r    store.RelayoutStats
		want float64
	}{
		{store.RelayoutStats{}, 0},
		{store.RelayoutStats{Cursor: 25, Ceiling: 100}, 0.25},
		{store.RelayoutStats{Cursor: 120, Ceiling: 100}, 1},
		{store.RelayoutStats{Done: true}, 1},
	} {
		if got := relayoutProgress(c.r); got != c.want {
			t.Fatalf("relayoutProgress(%+v) = %v, want %v", c.r, got, c.want)
		}
	}
}

// Persister stage timing, the update histogram and memory gauges are exported.
func TestCollectorReportsPersisterTimingsAndMemory(t *testing.T) {
	c := cache.New()
	c.Add(points.OnePoint("timings.metric", 1, 100))
	app := &App{Cache: c, Persister: persister.NewWhisper(t.TempDir(), nil, nil, nil, nil, nil, nil)}
	stagetime.ObserveUpdate(3 * time.Millisecond)
	reg := prometheus.NewRegistry()
	reg.MustRegister(&migrationCollector{app: app})
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	found := make(map[string]*dto.MetricFamily)
	for _, f := range families {
		found[f.GetName()] = f
	}
	for _, name := range []string{"persister_stage_seconds_total", "persister_stage_calls_total", "persister_update_duration_seconds", "persister_update_seconds_max", "cache_points", "cache_metrics", "cache_estimated_bytes"} {
		if found[name] == nil {
			t.Fatalf("%s missing", name)
		}
	}
	if got := len(found["persister_stage_seconds_total"].GetMetric()); got != int(stagetime.NumStages) {
		t.Fatalf("stage series %d, want %d", got, stagetime.NumStages)
	}
	if got := found["cache_points"].GetMetric()[0].GetGauge().GetValue(); got != 1 {
		t.Fatalf("cache_points %v, want 1", got)
	}
	if got := found["cache_estimated_bytes"].GetMetric()[0].GetGauge().GetValue(); got != float64(c.EstimatedBytes()) {
		t.Fatalf("cache_estimated_bytes %v, want %d", got, c.EstimatedBytes())
	}
	if found["persister_update_duration_seconds"].GetMetric()[0].GetHistogram().GetSampleCount() == 0 {
		t.Fatal("update histogram empty")
	}
	if _, ok := found["store_migration_working_bytes_reserved"]; ok {
		t.Fatal("migration working bytes reported without a migration")
	}
}
