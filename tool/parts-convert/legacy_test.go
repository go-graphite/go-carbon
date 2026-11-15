//go:build linux || darwin

package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/partsconvert"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

func TestConvertOfflineCopyDeletesActualLegacyAndResumes(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	fixture, err := compare(ctx, config{dir: filepath.Join(root, "fixture"), shards: 1, metrics: 3, points: 512, archives: 4, partitionSeconds: 86400, blockPoints: 4096})
	if err != nil {
		t.Fatal(err)
	}
	legacy := filepath.Join(root, "fixture", "legacy")
	output := filepath.Join(root, "conversion")
	source, err := chunkstore.OpenTransferSource(legacy, chunkstore.TransferSourceOptions{Continuation: &whisperio.ContinuationCodec{}, CacheBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := convertLegacyCopy(ctx, legacy, output, 1, 8<<20); err == nil {
		t.Fatal("accepted a legacy DB held by another reader")
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	// An admission failure leaves the legacy copy intact and a resumable plan.
	if _, err := convertLegacyCopy(ctx, legacy, output, 1, 1); err == nil {
		t.Fatal("ignored aggregate byte budget")
	}
	r, err := convertLegacyCopy(ctx, legacy, output, 1, 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	if r.Batches != 3 || r.Metrics != 3 || r.Points != fixture.Points || !r.StoredSlotsExact || r.MixedRenderQualified || r.DiskCapQualified {
		t.Fatalf("unexpected qualification result %+v; fixture %+v", r, fixture)
	}
	cursor, pending, err := partsconvert.NameState(filepath.Join(output, "names"))
	if err != nil || pending != nil || !cursor.Done {
		t.Fatalf("unfinished cursor %+v %+v %v", cursor, pending, err)
	}
	source, err = chunkstore.OpenTransferSource(legacy, chunkstore.TransferSourceOptions{Continuation: &whisperio.ContinuationCodec{}, CacheBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	page, err := source.ListCatalogPage(ctx, "", "", 100)
	if err != nil || len(page) != 0 {
		t.Fatalf("converted catalog remains: %v %v", page, err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	resumed, err := convertLegacyCopy(ctx, legacy, output, 1, 8<<20)
	if err != nil || resumed.Batches != 0 || resumed.Points != 0 || !resumed.StoredSlotsExact {
		t.Fatalf("completed resume recopied data: %+v %v", resumed, err)
	}
}
