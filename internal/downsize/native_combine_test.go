package downsize

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestNativeCombineControllerUsesSameJournalWithoutMetricRPCs(t *testing.T) {
	ctx := context.Background()
	source, target, oracle := nativeControllerStore(t), nativeControllerStore(t), nativeControllerStore(t)
	var names []string
	for i := range 33 {
		name := fmt.Sprintf("combine.controller.%02d", i)
		names = append(names, name)
		addPoints(t, source, name, float64(i))
		addPoints(t, target, name, 42)
		addPoints(t, oracle, name, 42)
	}
	records, err := source.ExportTransfer(ctx, names)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := oracle.ImportTransfer(ctx, records); err != nil {
		t.Fatal(err)
	}
	var metricRPCs atomic.Int64
	wrap := func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/v1/transfer/export" || r.URL.Path == "/v1/transfer/import" || r.URL.Path == "/v1/transfer/pull" {
				metricRPCs.Add(1)
			}
			handler.ServeHTTP(w, r)
		})
	}
	donor := httptest.NewServer(wrap(nativeControllerService(t, source)))
	defer donor.Close()
	survivor := httptest.NewServer(wrap(nativeControllerService(t, target)))
	defer survivor.Close()
	plan := fixturePlan(t, donor.URL, survivor.URL, names)
	o := testOptions(plan)
	o.delete, o.bulkOnly, o.bulkCombine = false, true, true
	o.deleteMode, o.engine = "auto", "bulk"
	if err := run(ctx, o); err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		want, err := oracle.Snapshot(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		got, err := target.Snapshot(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.EqualSnapshots(want, got); err != nil {
			t.Fatal(name, err)
		}
	}
	before, _ := target.Metadata(ctx, names[0])
	// The reviewed engine skips the very same completed ordinals.
	o.engine, o.bulkCombine, o.bulkOnly = "metric", false, false
	if err := run(ctx, o); err != nil {
		t.Fatal(err)
	}
	after, _ := target.Metadata(ctx, names[0])
	if before.ID != after.ID || metricRPCs.Load() != 0 {
		t.Fatal("range was imported again through another engine", before, after, metricRPCs.Load())
	}
}
