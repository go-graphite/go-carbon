package carbon

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/v2convert"
	"github.com/go-graphite/go-carbon/internal/v2part"
	"github.com/go-graphite/go-carbon/internal/v2read"
	"github.com/go-graphite/go-carbon/persister"
)

type frozenV2Classic struct{ snapshot chunkstore.Snapshot }

func (f frozenV2Classic) Snapshot(context.Context, string) (chunkstore.Snapshot, error) {
	return f.snapshot, nil
}

func TestV2MultiarchiveBaselineTCPRestart(t *testing.T) {
	root := t.TempDir()
	now := time.Now().Unix()
	now -= now % 60
	aggregation := filepath.Join(root, "aggregation.conf")
	if err := os.WriteFile(aggregation, []byte("[all]\npattern = .*\nxFilesFactor = 0\naggregationMethod = average\n"), 0600); err != nil {
		t.Fatal(err)
	}
	baselinePath := ""
	setup := func() *App {
		app := v2TestApp(t, root)
		var err error
		app.Config.Whisper.Schemas[0].Retentions, err = persister.ParseRetentionDefs("1s:15m,1m:2h")
		if err != nil {
			t.Fatal(err)
		}
		app.Config.Whisper.Aggregation, err = persister.ReadWhisperAggregation(aggregation)
		if err != nil {
			t.Fatal(err)
		}
		app.Config.Whisper.StorageBackend = "pebble-chunk"
		app.Config.Whisper.StoreDir = filepath.Join(root, "classic")
		app.Config.Whisper.StoreEncoding = "exact"
		app.Config.V2.BaselinePath = baselinePath
		return app
	}
	app := setup()
	native, err := chunkstore.Open(app.Config.Whisper.StoreDir, chunkstore.Options{ExactBlocks: true, Now: func() time.Time { return time.Unix(now, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"union.multi", "union.unseeded"} {
		if _, err := native.Create(t.Context(), chunkstore.MetricConfig{Name: name, Retentions: []chunkstore.Retention{{Step: 1, Count: 900}, {Step: 60, Count: 120}}, AggregationMethod: chunkstore.Average}); err != nil {
			t.Fatal(err)
		}
		if err := native.UpdateMany(t.Context(), name, []chunkstore.Point{{Timestamp: now - 59, Value: 2}, {Timestamp: now - 58, Value: 4}}); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := native.Snapshot(t.Context(), "union.multi")
	if err != nil {
		t.Fatal(err)
	}
	if err := native.Close(); err != nil {
		t.Fatal(err)
	}
	plan := v2convert.NativeCheckpointPlan{Version: 1, Now: now, Names: []string{"union.multi"}, Baseline: v2convert.NativeBaseline{Source: v2part.Provenance{Incarnation: v2part.PartID{6}, Stream: "fenced-classic"}, Part: v2part.PartID{7}, Group: "local", Order: 100}}
	entries, err := os.ReadDir(app.Config.Whisper.StoreDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() == "LOCK" || entry.IsDir() {
			continue
		}
		sum, size, err := v2convert.FileSum(filepath.Join(app.Config.Whisper.StoreDir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		plan.Files = append(plan.Files, v2convert.File{Name: entry.Name(), SHA256: sum, Bytes: size})
	}
	plan.Baseline.Snapshot, err = plan.InventoryDigest()
	if err != nil {
		t.Fatal(err)
	}
	converted := filepath.Join(root, "converted")
	if _, err := v2convert.ConvertNativeCheckpoint(t.Context(), frozenV2Classic{snapshot}, plan, converted, 1<<20, 1); err != nil {
		t.Fatal(err)
	}
	receipts, err := filepath.Glob(filepath.Join(converted, "*", "receipt.json"))
	if err != nil || len(receipts) != 1 {
		t.Fatal(receipts, err)
	}
	baselinePath = filepath.Dir(receipts[0])
	app.Config.V2.BaselinePath = baselinePath
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if app != nil {
			app.Stop()
		}
	}()
	policy := v2read.Metric{AggregationMethod: "average", Archives: []v2read.Archive{{Step: 1, Points: 900}, {Step: 60, Points: 120}}}
	if err := app.Carbonserver.ValidateV2UnionPolicy("union.unseeded", policy); err == nil {
		t.Fatal("unseeded classic multiarchive admitted")
	}
	if err := app.Carbonserver.ValidateV2UnionPolicy("union.multi", policy); err != nil {
		t.Fatal("installed baseline rejected", err)
	}
	mismatch := policy
	mismatch.XFilesFactor = .5
	if err := app.Carbonserver.ValidateV2UnionPolicy("union.multi", mismatch); err == nil {
		t.Fatal("mismatched baseline policy admitted")
	}
	conn, err := net.Dial("tcp", app.v2.server.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintf(conn, "union.multi 12 %d\n", now-57); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	check := func() {
		t.Helper()
		deadline := time.Now().Add(8 * time.Second)
		for {
			series, masked, err := app.v2.reader.FetchWithMask(t.Context(), "union.multi", int(now-901), int(now))
			if err == nil && masked && series != nil && series.Step() == 60 {
				i := int((now - 60 - int64(series.FromTime())) / 60)
				if i >= 0 && i < len(series.Values()) && series.Values()[i] == 6 {
					return
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("converted baseline + TCP did not produce classic 6: %+v mask=%t err=%v", series, masked, err)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	check()
	app.Stop()
	app = nil
	baselinePath = filepath.Join(root, "conversion-source-removed")
	app = setup()
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	check()
}
