package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestSourcePathProofRetainsAliasAndMissingChecks(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.MkdirAll(filepath.Join(source, "a"), 0700); err != nil {
		t.Fatal(err)
	}
	source, err := physicalPath(source)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{filepath.Join(source, "a", "metric.wsp"), filepath.Join(root, "outside.wsp")} {
		if err := os.WriteFile(path, []byte("fixture"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	for name, target := range map[string]string{"inside.wsp": "a/metric.wsp", "outside.wsp": "../outside.wsp", "alias": "a", "escape": ".."} {
		if err := os.Symlink(target, filepath.Join(source, name)); err != nil {
			t.Fatal(err)
		}
	}
	for _, relative := range []string{"a/metric.wsp", "inside.wsp", "alias/metric.wsp"} {
		if err := validateSourcePath(source, relative); err != nil {
			t.Fatalf("valid source %s: %v", relative, err)
		}
	}
	for _, relative := range []string{"../outside.wsp", "outside.wsp", "escape/outside.wsp", filepath.Join(root, "outside.wsp")} {
		if err := validateSourcePath(source, relative); err == nil {
			t.Fatalf("source escape accepted: %s", relative)
		}
	}
	if err := validateSourcePath(source, "missing.wsp"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing-file semantics changed: %v", err)
	}
}
