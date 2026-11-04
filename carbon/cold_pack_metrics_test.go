package carbon

import (
	"context"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestCollectorColdPackLifecycle(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1700120040, 0)
	db, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: &whisperio.ContinuationCodec{}, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.StopBackground()
	cfg := store.MetricConfig{Name: "lifecycle", Retentions: []store.Retention{{Step: 60, Count: 10080}}, AggregationMethod: store.Average}
	if _, err := db.Create(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	points := make([]store.Point, 1800)
	for i := range points {
		points[i] = store.Point{Timestamp: 1700000040 + int64(i*60), Value: float64(i % 7)}
	}
	if err := db.UpdateMany(ctx, cfg.Name, points); err != nil {
		t.Fatal(err)
	}
	app := &App{}
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(&migrationCollector{app: app})
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == "store_cold_pack_thaws_total" || family.GetName() == "store_cold_pack_repacked_blocks_total" {
			t.Fatal("lifecycle counters emitted without a store")
		}
	}
	app.MetricStore = db
	check := func() {
		t.Helper()
		st, thaws := db.ColdPackStats(), whisperio.ColdPackThaws()
		want := map[string]float64{
			"store_cold_pack_blocks_total":                float64(st.Blocks - st.UnpackedBlocks),
			"store_cold_pack_points_total":                float64(st.Points - st.UnpackedPoints),
			"store_cold_pack_repacked_blocks_total":       float64(st.RepackedBlocks),
			"store_cold_pack_repacked_points_total":       float64(st.RepackedPoints),
			"store_cold_pack_repacked_input_bytes_total":  float64(st.RepackedInputBytes),
			"store_cold_pack_repacked_output_bytes_total": float64(st.RepackedOutputBytes),
			"store_cold_pack_thawed_points_total":         float64(thaws.Points),
			"store_cold_pack_thawed_bytes_total":          float64(thaws.Bytes),
		}
		reasons := map[string]uint64{"append": thaws.Appends, "patch": thaws.Patches, "rewrite": thaws.Rewrites, "unpack": thaws.Unpacks}
		families, err := reg.Gather()
		if err != nil {
			t.Fatal(err)
		}
		for _, family := range families {
			name := family.GetName()
			if value, ok := want[name]; ok {
				if family.GetType() != dto.MetricType_COUNTER || len(family.Metric) != 1 || len(family.Metric[0].GetLabel()) != 0 || family.Metric[0].GetCounter().GetValue() != value {
					t.Fatalf("%s: got %v want %v", name, family, value)
				}
				delete(want, name)
			}
			if name == "store_cold_pack_thaws_total" {
				if family.GetType() != dto.MetricType_COUNTER || len(family.Metric) != 4 {
					t.Fatalf("thaw counter shape: %v", family)
				}
				for _, metric := range family.Metric {
					labels := metric.GetLabel()
					if len(labels) != 1 || labels[0].GetName() != "reason" {
						t.Fatalf("thaw labels: %v", labels)
					}
					value, ok := reasons[labels[0].GetValue()]
					if !ok || metric.GetCounter().GetValue() != float64(value) {
						t.Fatalf("thaw reason: %v", metric)
					}
					delete(reasons, labels[0].GetValue())
				}
			}
		}
		if len(want)+len(reasons) != 0 {
			t.Fatalf("missing counters: %v %v", want, reasons)
		}
	}
	check()
	for _, mode := range []string{"exact", "unpack", "exact"} {
		if err := db.ConfigureColdPack(store.ColdPackOptions{Mode: mode, CPUFraction: 1, BytesPerSecond: 1 << 40, WriteWindow: time.Second}); err != nil {
			t.Fatal(err)
		}
		if err := db.RunColdPack(ctx); err != nil {
			t.Fatal(err)
		}
		check()
	}
	if st := db.ColdPackStats(); st.RepackedBlocks == 0 || st.RepackedPoints == 0 || st.RepackedInputBytes == 0 || st.RepackedOutputBytes == 0 || st.UnpackedBlocks == 0 {
		t.Fatalf("fixture did not exercise repacking/unpacking: %+v", st)
	}
}
