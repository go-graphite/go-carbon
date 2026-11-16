//go:build linux || darwin

package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestPruneRetainsCompleteAuthorityAndResume(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	fixturePath := filepath.Join(root, "fixture")
	if _, err := compare(ctx, config{dir: fixturePath, shards: 1, metrics: 2, points: 128, archives: 1, partitionSeconds: 86400, blockPoints: 4096}); err != nil {
		t.Fatal(err)
	}
	legacy, target := filepath.Join(fixturePath, "legacy"), filepath.Join(root, "target")
	if _, err := convertLegacyCopy(ctx, legacy, target, 1, 4<<20); err != nil {
		t.Fatal(err)
	}
	dry, err := pruneReclaimedReceipts(target, false)
	if err != nil || dry.Files != 2 {
		t.Fatal(dry, err)
	}
	got, err := pruneReclaimedReceipts(target, true)
	if err != nil || got != dry {
		t.Fatal(got, dry, err)
	}
	if r, err := convertLegacyCopy(ctx, legacy, target, 1, 4<<20); err != nil || r.Batches != 0 {
		t.Fatal(r, err)
	}
	if r, err := pruneReclaimedReceipts(target, true); err != nil || r.Files != 0 {
		t.Fatal(r, err)
	}
}

func TestPruneRejectsChangedDuplicateBeforeDeleting(t *testing.T) {
	root := t.TempDir()
	fixturePath := filepath.Join(root, "fixture")
	ctx := context.Background()
	if _, err := compare(ctx, config{dir: fixturePath, shards: 1, metrics: 2, points: 128, archives: 1, partitionSeconds: 86400, blockPoints: 4096}); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(root, "target")
	if _, err := convertLegacyCopy(ctx, filepath.Join(fixturePath, "legacy"), target, 1, 4<<20); err != nil {
		t.Fatal(err)
	}
	paths, _ := filepath.Glob(filepath.Join(target, "units", "*", "cursor.json"))
	if len(paths) != 2 {
		t.Fatal(paths)
	}
	if err := os.WriteFile(paths[1], []byte(`{"Version":1}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := pruneReclaimedReceipts(target, true); err == nil {
		t.Fatal("pruned mismatched authority")
	}
	for _, path := range paths {
		if _, err := os.Stat(path); err != nil {
			t.Fatal("deleted before checking all receipts", err)
		}
	}
}
