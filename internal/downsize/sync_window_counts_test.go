package downsize

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWindowPointCountsResumeAndUncertainty(t *testing.T) {
	state := t.TempDir()
	path := filepath.Join(state, "copy.window-points.json")
	before, after := int64(10), int64(17)
	c, err := readWindowPointCount(path, windowPointCount{Before: &before})
	if err != nil || c.Before == nil || *c.Before != 10 {
		t.Fatal(c, err)
	}
	c.After = &after
	if err := atomicStateJSON(path, c); err != nil {
		t.Fatal(err)
	}
	// A successful transfer's later retry must retain the original delta.
	c, err = readWindowPointCount(path, windowPointCount{Before: &after})
	if err != nil || *c.Before != 10 || *c.After != 17 {
		t.Fatal(c, err)
	}
	counts, err := collectWindowPointCounts(state)
	if err != nil || counts.Added == nil || *counts.Added != 7 {
		t.Fatal(counts, err)
	}
	interrupted := filepath.Join(state, "interrupted.window-points.json")
	if _, err := readWindowPointCount(interrupted, windowPointCount{Before: &before}); err != nil {
		t.Fatal(err)
	}
	c, err = readWindowPointCount(interrupted, windowPointCount{Before: &after})
	if err != nil || c.Before != nil {
		t.Fatal("ambiguous interrupted baseline accepted", c, err)
	}
	counts, err = collectWindowPointCounts(state)
	if err != nil || counts.Added != nil || counts.Unknown != 1 {
		t.Fatal(counts, err)
	}
	if err := os.WriteFile(interrupted, []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := collectWindowPointCounts(state); err == nil {
		t.Fatal("corrupt count accepted")
	}
}
