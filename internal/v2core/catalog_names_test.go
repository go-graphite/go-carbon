package v2core

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

func TestScanCatalogNames(t *testing.T) {
	db, err := pebble.Open(t.TempDir(), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	name := partsformat.ID("service.cpu")
	policy, _ := json.Marshal(RollupPolicy{Name: name, Group: "test"})
	meta, _ := json.Marshal(RollupMetadata{Version: 1, ID: 1, Policy: policy, Frontier: sealrollup.Frontier{Order: 1}})
	key := partsformat.Key{Group: "test", VShard: partsformat.Route(name), Metric: name, Archive: rollupMetadataArchive}
	encoded, err := partsformat.Encode(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Set(encoded, meta, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	// Deliberately undecodable archive data: discovery must never decode it.
	key.Archive = 0
	raw, _ := partsformat.Encode(key)
	if err := db.Set(raw, []byte("not an archive record"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	snapshot := db.NewSnapshot()
	defer snapshot.Close()
	var names []string
	if err := ScanCatalogNames(context.Background(), snapshot, func(name string) error { names = append(names, name); return nil }); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(names, []string{"service.cpu"}) {
		t.Fatal(names)
	}
	sentinel := errors.New("visitor stopped")
	if err := ScanCatalogNames(context.Background(), snapshot, func(string) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := ScanCatalogNames(ctx, snapshot, func(string) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := db.Set(encoded, []byte("invalid metadata"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	corrupt := db.NewSnapshot()
	defer corrupt.Close()
	if err := ScanCatalogNames(context.Background(), corrupt, func(string) error { return nil }); err == nil {
		t.Fatal("corrupt metadata accepted")
	}
}
