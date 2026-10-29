package chunkstore

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

// Legacy fixtures have a sidecar but predate inventory records.
type legacySidecarCodec struct{ ContinuationCodec }

func (legacySidecarCodec) Create(_ context.Context, io ContinuationIO, _ Metadata, _ func() time.Time) error {
	return io.Set([]byte("legacy-sidecar"), SidecarSummary{Present: true, Points: 3, Oldest: 100}.Encode())
}
func (legacySidecarCodec) InspectSidecar(io ContinuationIO) (SidecarSummary, error) {
	value, err := io.Get([]byte("legacy-sidecar"))
	if err != nil {
		return SidecarSummary{}, err
	}
	summary, err := DecodeSidecarSummary(value)
	if err != nil {
		return summary, err
	}
	return summary, io.Set(SidecarSummaryKey, summary.Encode())
}
func TestLegacySidecarInventoryReadiness(t *testing.T) {
	ctx := context.Background()
	fs := vfs.NewMem()
	options := Options{ExactBlocks: true, Continuation: legacySidecarCodec{}, fs: fs, CacheSize: 1 << 20, MemTableSize: 1 << 20}
	db, err := Open("store", options)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a", "b"} {
		if _, err := db.Create(ctx, MetricConfig{Name: name, Retentions: []Retention{{Step: 1, Count: 60}}, AggregationMethod: Average}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.db.Delete(sidecarReadyKey, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open("store", options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { db.Close() }()
	if db.Sidecars().Ready || db.CheckNoSidecars() == nil {
		t.Fatal("legacy inventory incorrectly reported complete")
	}
	dry, err := db.ScanSidecars(ctx, "", 10, false)
	if err != nil || len(dry.Sidecars) != 2 || db.Sidecars().Count != 0 || db.Sidecars().Ready {
		t.Fatalf("dry scan: %+v %v", dry, err)
	}
	first, err := db.ScanSidecars(ctx, "", 1, true)
	if err != nil || first.Done || db.Sidecars().Ready || db.Sidecars().Count != 1 {
		t.Fatalf("first page: %+v %v", first, err)
	}
	last, err := db.ScanSidecars(ctx, first.After, 10, true)
	if err != nil || !last.Done || !db.Sidecars().Ready || db.Sidecars().Count != 2 || db.Sidecars().Points != 6 || db.Sidecars().Oldest != 100 {
		t.Fatalf("final page: %+v %v", last, err)
	}
	// A completed inventory survives reopening without scanning metric values.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open("store", options)
	if err != nil {
		t.Fatal(err)
	}
	if !db.Sidecars().Ready || db.Sidecars().Count != 2 {
		t.Fatal(db.Sidecars())
	}
}
