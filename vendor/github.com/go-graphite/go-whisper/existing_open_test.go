package whisper

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestExistingOpenDoesNotCreateMissingMetricLocks(t *testing.T) {
	for _, relative := range []string{"metric.wsp", "absent/metric.wsp"} {
		t.Run(relative, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), relative)
			w, err := OpenWithOptions(path, &Options{FLock: true, ExistingOnly: true})
			if w != nil || !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("missing original: %v, %v", w, err)
			}
			for _, name := range []string{path, PathLockPath(path)} {
				if _, err := os.Stat(name); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("opening a new shared metric created %s: %v", name, err)
				}
			}
		})
	}
}

func TestExistingOpenSupportsOriginalWithoutPathLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metric.wsp")
	retentions, err := ParseRetentionDefs("1s:1h")
	if err != nil {
		t.Fatal(err)
	}
	w, err := CreateWithOptions(path, retentions, Sum, .25, &Options{Compressed: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(PathLockPath(path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("fixture unexpectedly has a path lock: %v", err)
	}
	w, err = OpenWithOptions(path, &Options{FLock: true, ExistingOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if w.AggregationMethod() != Sum || w.XFilesFactor() != .25 || w.MaxRetention() != 3600 {
		t.Fatal("opening the original changed its archive policy")
	}
	if _, err := os.Stat(PathLockPath(path)); err != nil {
		t.Fatalf("existing original lacks its durable path fence: %v", err)
	}
}

func TestExistingOpenPreservesPreexistingOrphanLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metric.wsp")
	lock := PathLockPath(path)
	if err := os.WriteFile(lock, nil, 0600); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(lock)
	if err != nil {
		t.Fatal(err)
	}
	if w, err := OpenWithOptions(path, &Options{FLock: true, ExistingOnly: true}); w != nil || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing original with a preexisting fence: %v, %v", w, err)
	}
	after, err := os.Stat(lock)
	if err != nil || !os.SameFile(before, after) {
		t.Fatalf("opening a missing original replaced another owner's fence: %v", err)
	}
}

func BenchmarkIncrementalOriginalOpen(b *testing.B) {
	path := filepath.Join(b.TempDir(), "metric.wsp")
	retentions, err := ParseRetentionDefs("1s:2d,60s:28d,3600s:2y")
	if err != nil {
		b.Fatal(err)
	}
	w, err := CreateWithOptions(path, retentions, Sum, .5, &Options{Compressed: true, FLock: true})
	if err != nil {
		b.Fatal(err)
	}
	if err := w.Close(); err != nil {
		b.Fatal(err)
	}
	for _, existingOnly := range []bool{false, true} {
		name := "stat-then-open"
		if existingOnly {
			name = "reuse-existing-open"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if !existingOnly {
					if _, err := os.Lstat(path); err != nil {
						b.Fatal(err)
					}
				}
				w, err := OpenWithOptions(path, &Options{Compressed: true, FLock: true, ExistingOnly: existingOnly})
				if err != nil {
					b.Fatal(err)
				}
				if err := w.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
