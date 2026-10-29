package chunkstore

import (
	"context"
	"testing"
)

func TestCatalogHighWatermarkExcludesRecreatedMetric(t *testing.T) {
	t.Parallel()
	db, err := Open(t.TempDir(), Options{ExactBlocks: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	config := MetricConfig{Name: "metric", Retentions: []Retention{{Step: 1, Count: 60}}, AggregationMethod: Average}
	first, err := db.Create(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	limit, err := db.CatalogHighWatermark(ctx)
	if err != nil || limit != first.ID {
		t.Fatalf("catalog bound=%d row=%d err=%v", limit, first.ID, err)
	}
	if err = db.Delete(ctx, config.Name); err != nil {
		t.Fatal(err)
	}
	next, err := db.Create(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	if next.ID <= limit {
		t.Fatal("recreated metric inherited baseline eligibility")
	}
}
