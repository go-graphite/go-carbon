package chunkstore

import (
	"context"
	"testing"
)

func TestCatalogCheckpointTokenFencesRestartAndReplacement(t *testing.T) {
	dir := t.TempDir()
	options := Options{MetadataCacheSize: 1 << 20, MetadataCacheCheckpoint: true, CacheSize: 1 << 20, MemTableSize: 1 << 20}
	db, err := Open(dir, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.PrepareMetadataCheckpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	db.metricMu[123].Lock()
	if db.CatalogCheckpointToken() != "" {
		t.Fatal("certified an in-flight mutation")
	}
	db.metricMu[123].Unlock()
	release, err := db.admitNative(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if db.CatalogCheckpointToken() != "" {
		t.Fatal("certified an active native publication")
	}
	release()
	db.importPublishing[123] = map[string]pendingMutation{"reserved": {done: make(chan struct{})}}
	if db.CatalogCheckpointToken() != "" {
		t.Fatal("certified a reserved mutation")
	}
	db.importPublishing[123] = nil
	token := db.CatalogCheckpointToken()
	if token == "" {
		t.Fatal("missing persistent identity")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(dir, options)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if got := db.OpenCatalogCheckpointToken(); got != token {
		t.Fatalf("open token=%s want %s", got, token)
	}
	if db.CatalogCheckpointToken() == token {
		t.Fatal("startup stamp consumption did not change current state")
	}
	other, err := Open(t.TempDir(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if other.OpenCatalogCheckpointToken() == token {
		t.Fatal("foreign store shares restart identity")
	}
	_, cursor, _, err := db.CatalogChanges(context.Background(), 1)
	if err != nil || cursor != 1 {
		t.Fatalf("initial catalog journal: %d %v", cursor, err)
	}
}
