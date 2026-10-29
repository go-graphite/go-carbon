package whisperio

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
)

func writeWalkTree(t *testing.T, root string) {
	t.Helper()
	for i := range 40 {
		dir := filepath.Join(root, fmt.Sprintf("a%d", i%7), fmt.Sprintf("b%d", i%5), fmt.Sprintf("c%d", i))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		for j := range 3 {
			if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("m%d.wsp", j)), nil, 0o644); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(filepath.Join(dir, "notes.txt"), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(filepath.Join(root, "empty", "deeper"), 0o755); err != nil {
		t.Fatal(err)
	}
}

func collectWalk(t *testing.T, walk func(func(MetricPath) error) error) []MetricPath {
	t.Helper()
	var mu sync.Mutex
	var got []MetricPath
	if err := walk(func(m MetricPath) error {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, m)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(got, func(a, b MetricPath) int {
		if a.Metric < b.Metric {
			return -1
		}
		if a.Metric > b.Metric {
			return 1
		}
		return 0
	})
	return got
}

// The parallel walk visits exactly the metrics the serial walk does.
func TestWalkMetricsParallelMatchesSerial(t *testing.T) {
	root := t.TempDir()
	writeWalkTree(t, root)
	ctx := context.Background()
	want := collectWalk(t, func(fn func(MetricPath) error) error { return WalkMetrics(ctx, root, true, fn) })
	if len(want) != 120 {
		t.Fatalf("serial walk found %d metrics, want 120", len(want))
	}
	for _, workers := range []int{1, 2, 16} {
		got := collectWalk(t, func(fn func(MetricPath) error) error { return WalkMetricsParallel(ctx, root, true, workers, fn) })
		if !slices.Equal(got, want) {
			t.Fatalf("workers %d: %d metrics differ from the serial walk's %d", workers, len(got), len(want))
		}
	}
}

func TestWalkMetricsParallelErrors(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	writeWalkTree(t, root)
	boom := errors.New("boom")
	if err := WalkMetricsParallel(ctx, root, true, 8, func(MetricPath) error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("callback error %v, want %v", err, boom)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := WalkMetricsParallel(cancelled, root, true, 8, func(MetricPath) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled walk returned %v", err)
	}
	if err := WalkMetricsParallel(ctx, filepath.Join(root, "missing"), true, 8, func(MetricPath) error { return nil }); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing root returned %v", err)
	}

	link := t.TempDir()
	writeWalkTree(t, link)
	if err := os.Symlink(filepath.Join(link, "a0"), filepath.Join(link, "a1", "link")); err != nil {
		t.Fatal(err)
	}
	if err := WalkMetricsParallel(ctx, link, true, 8, func(MetricPath) error { return nil }); err == nil {
		t.Fatal("symlink in the tree was accepted")
	}

	tagged := t.TempDir()
	if err := os.MkdirAll(filepath.Join(tagged, "_tagged", "x"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tagged, "_tagged", "x", "m.wsp"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := WalkMetricsParallel(ctx, tagged, true, 8, func(MetricPath) error { return nil }); err == nil {
		t.Fatal("tagged file was accepted")
	}
}

func TestWalkMetricsSkipsQuotaQuarantine(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, ".ns-quota-trim", "nested"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".ns-quota-trim", "nested", "keep.wsp"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "live.wsp"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	for _, workers := range []int{1, 2} {
		got := collectWalk(t, func(fn func(MetricPath) error) error {
			return WalkMetricsParallel(context.Background(), root, true, workers, fn)
		})
		if len(got) != 1 || got[0].Metric != "live" {
			t.Fatalf("quarantine discovered: %v", got)
		}
	}
}
