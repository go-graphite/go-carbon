package carbon

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type statusColdCodec struct {
	whisperio.ContinuationCodec
	before func(context.Context) (store.RepackStats, error)
}

func (c *statusColdCodec) Repack(ctx context.Context, _ store.ContinuationIO, _ store.ColdPackRequest) (store.RepackStats, uint64, bool, error) {
	stats, err := c.before(ctx)
	return stats, 0, true, err
}

func TestCollectorColdPackFailureLatch(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1700120040, 0)
	codec := &statusColdCodec{}
	db, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: codec,
		CacheSize: 1 << 20, MemTableSize: 1 << 20, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.StopBackground()
	if _, err := db.Create(ctx, store.MetricConfig{Name: "status", Retentions: []store.Retention{{Step: 60, Count: 10080}}, AggregationMethod: store.Average}); err != nil {
		t.Fatal(err)
	}
	if err := db.ConfigureColdPack(store.ColdPackOptions{Mode: "exact"}); err != nil {
		t.Fatal(err)
	}
	if err := db.ConfigureColdPack(store.ColdPackOptions{Mode: "unpack", CPUFraction: 1, BytesPerSecond: 1 << 40, MinFreeBytes: 1}); err != nil {
		t.Fatal(err)
	}
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(&migrationCollector{app: &App{MetricStore: db}})
	var conflicts, skipped, failures uint64
	for pass, reason := range []string{"conflict", "skip", "failure", "conflict", "skip", "clean"} {
		t.Run(fmt.Sprint(pass+1)+"-"+reason, func(t *testing.T) {
			codec.before = func(ctx context.Context) (store.RepackStats, error) {
				switch reason {
				case "conflict":
					// Mutate after the maintenance snapshot was taken. Revision
					// validation must reject publication regardless of timing.
					err := db.UpdateMany(ctx, "status", []store.Point{{Timestamp: now.Unix() - 60, Value: float64(pass)}})
					return store.RepackStats{Blocks: 1}, err
				case "skip":
					return store.RepackStats{}, store.ErrColdPackBudget
				case "failure":
					return store.RepackStats{}, errors.New("injected repack failure")
				default:
					return store.RepackStats{}, nil
				}
			}
			err := db.RunColdPack(ctx)
			if (err != nil) != (reason == "failure") {
				t.Fatalf("pass result: %v", err)
			}
			switch reason {
			case "conflict":
				conflicts++
			case "skip":
				skipped++
			case "failure":
				failures++
				skipped++
			}
			st := db.ColdPackStats()
			if st.Passes != uint64(pass+1) || st.Conflicts != conflicts || st.Skipped != skipped || st.Errors != failures {
				t.Fatalf("pass did not finish and retry: %+v", st)
			}
			if failures == 0 && st.Error != "" || failures != 0 && !strings.Contains(st.Error, "injected repack failure") {
				t.Errorf("failure field must retain only real errors: %+v", st)
			}
			families, err := reg.Gather()
			if err != nil {
				t.Fatal(err)
			}
			failed := 0.0
			if failures != 0 {
				failed = 1
			}
			want := map[string]float64{"store_cold_pack_failed": failed, "store_cold_pack_errors_total": float64(failures),
				"store_cold_pack_conflicts_total": float64(conflicts), "store_cold_pack_skipped_total": float64(skipped)}
			for _, family := range families {
				if expected, ok := want[family.GetName()]; ok {
					metric := family.Metric[0]
					got := metric.GetCounter().GetValue()
					if family.GetType() == dto.MetricType_GAUGE {
						got = metric.GetGauge().GetValue()
					}
					if got != expected {
						t.Errorf("%s = %v, want %v", family.GetName(), got, expected)
					}
					delete(want, family.GetName())
				}
			}
			if len(want) != 0 {
				t.Errorf("missing metrics: %v", want)
			}
			err = db.DowngradeColdPack(ctx)
			if reason == "clean" {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, store.ErrColdPackIncomplete) {
				t.Fatalf("incomplete pass authorized downgrade: %v", err)
			}
		})
	}
}

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
