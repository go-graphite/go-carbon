package chunkstore

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

func TestExpirationReceiptAtomicConflictAndRecreation(t *testing.T) {
	ctx := context.Background()
	s, now := openTestStore(t, "/store", Options{fs: vfs.NewMem(), ExactBlocks: true, SyncInterval: time.Hour})
	defer closeTestStore(t, s)
	m := createTestMetric(t, s, "old")
	receipt := ExpirationReceipt{Policy: "exact-policy", DeletedAt: int64(now), LastUpdate: int64(now) - 100000}
	stale := m
	stale.Revision++
	results, err := s.DeleteBatchWithExpirationReceipts(ctx, []Metadata{stale}, map[string]ExpirationReceipt{"old": receipt})
	if err != nil || !errors.Is(results[0], ErrConflict) {
		t.Fatal(results, err)
	}
	if _, err = s.ExpirationReceiptFor(ctx, "old"); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	results, err = s.DeleteBatchWithExpirationReceipts(ctx, []Metadata{m}, map[string]ExpirationReceipt{"old": receipt})
	if err != nil || results[0] != nil {
		t.Fatal(results, err)
	}
	got, err := s.ExpirationReceiptFor(ctx, "old")
	if err != nil || got != receipt {
		t.Fatal(got, err)
	}
	createTestMetric(t, s, "old")
	if _, err = s.ExpirationReceiptFor(ctx, "old"); !errors.Is(err, ErrConflict) {
		t.Fatal("recreated name exempt", err)
	}
	if err = s.Delete(ctx, "old"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ExpirationReceiptFor(ctx, "old"); !errors.Is(err, ErrNotFound) {
		t.Fatal("stale receipt survived admin delete", err)
	}
}

func TestExpirationReceiptSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store")
	options := Options{ExactBlocks: true}
	s, err := Open(path, options)
	if err != nil {
		t.Fatal(err)
	}
	m := createTestMetric(t, s, "old")
	want := ExpirationReceipt{Policy: "policy", DeletedAt: 1790000000, LastUpdate: 1789000000}
	result, err := s.DeleteBatchWithExpirationReceipts(ctx, []Metadata{m}, map[string]ExpirationReceipt{"old": want})
	if err != nil || result[0] != nil {
		t.Fatal(result, err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := s.ExpirationReceiptFor(ctx, "old")
	if err != nil || got != want {
		t.Fatal(got, err)
	}
}
