package v2convert

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
)

func TestScanFilesKeepsIndependentDigestsAndErrors(t *testing.T) {
	a, _ := fixture(t)
	b, _ := fixture(t)
	paths := []string{a, filepath.Join(t.TempDir(), "missing.sst"), b, a}
	serial, err := ScanFiles(context.Background(), paths, 1)
	if err != nil {
		t.Fatal(err)
	}
	parallel, err := ScanFiles(context.Background(), paths, 3)
	if err != nil {
		t.Fatal(err)
	}
	for i := range paths {
		if !reflect.DeepEqual(serial[i].Stats, parallel[i].Stats) || (serial[i].Err == nil) != (parallel[i].Err == nil) {
			t.Fatalf("inventory or error moved at index %d", i)
		}
	}
	if parallel[1].Err == nil || parallel[0].Stats.Digest == "" || parallel[3].Stats.Digest != parallel[0].Stats.Digest {
		t.Fatal("lost per-table error or digest")
	}
}

func TestScanFilesCancellationAndBounds(t *testing.T) {
	if _, err := ScanFiles(context.Background(), nil, 0); err == nil {
		t.Fatal("zero workers accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ScanFiles(ctx, []string{"missing.sst"}, 4); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if rows, err := ScanFiles(context.Background(), nil, 4); err != nil || len(rows) != 0 {
		t.Fatal(rows, err)
	}
}
