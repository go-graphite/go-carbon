package whisperio

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// This file alone is copied into the exact release archive for the rollback
// binary. It uses only release APIs and contains no new maintenance option.
type tailReleasePlan struct {
	Now           int64
	Before, After store.Snapshot
	Input         []store.Point
}

func TestColdPackTailsReleaseHelper(t *testing.T) {
	path := os.Getenv("COLD_TAILS_RELEASE_STORE")
	if path == "" {
		t.Skip("cross-binary helper")
	}
	data, err := os.ReadFile(filepath.Join(path, "rollback.json"))
	if err != nil {
		t.Fatal(err)
	}
	var plan tailReleasePlan
	if err := json.Unmarshal(data, &plan); err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(path, store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: &ContinuationCodec{DirectLateWrites: true, ReuseBlockKeys: true}, Now: func() time.Time { return time.Unix(plan.Now, 0) }, CacheSize: 2 << 20, MemTableSize: 1 << 20, MetadataCacheSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	db.StopBackground()
	defer db.Close()
	if db.TransferFormat() != 9 {
		t.Fatal("release did not open v9")
	}
	name := plan.Before.Metadata.Name
	records, err := db.ExportTransfer(context.Background(), []string{name})
	if err != nil {
		t.Fatal(err)
	}
	packed := 0
	for _, kv := range records[0].Keys {
		if len(kv.Key) == 16 && kv.Key[9] == 'b' && len(kv.Value) == 12 && binary.BigEndian.Uint32(kv.Value[8:]) != 0 {
			packed++
		}
	}
	if packed == 0 {
		t.Fatal("release did not receive packed descriptors")
	}
	got, err := db.Snapshot(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EqualSnapshots(plan.Before, got); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateMany(context.Background(), name, plan.Input); err != nil {
		t.Fatal(err)
	}
	got, err = db.Snapshot(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EqualSnapshots(plan.After, got); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Create(context.Background(), store.MetricConfig{Name: "release.new.metric", Retentions: []store.Retention{{Step: 60, Count: 1440}}, AggregationMethod: store.Average}); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateMany(context.Background(), "release.new.metric", []store.Point{{Timestamp: plan.Now - 60, Value: 42}}); err != nil {
		t.Fatal(err)
	}
	t.Logf("release v9 read, corrected/resumed packed tails, and created/wrote a metric; packed descriptors=%d", packed)
}
