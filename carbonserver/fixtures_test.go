package carbonserver

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

var fixtureDirectory string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "carbonserver-fixtures-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fixtureDirectory = dir
	code := m.Run()
	if err := os.RemoveAll(dir); err != nil {
		fmt.Fprintln(os.Stderr, err)
		code = 1
	}
	os.Exit(code)
}

var snapshotFixtureTemplate struct {
	sync.Once
	cache   string
	entries []FLCEntry
}

func snapshotFixture(t *testing.T) (string, string, []FLCEntry) {
	t.Helper()
	snapshotFixtureTemplate.Do(func() {
		root := filepath.Join(fixtureDirectory, "snapshot-root")
		cache := filepath.Join(fixtureDirectory, "snapshot-cache", "files.gzip")
		for _, dir := range []string{root, filepath.Dir(cache)} {
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
		}
		entries := buildSnapshotFixture(t, root, cache)
		snapshotFixtureTemplate.cache = cache
		snapshotFixtureTemplate.entries = entries
	})
	if snapshotFixtureTemplate.cache == "" {
		t.Fatal("snapshot fixture initialization failed")
	}

	root, cache := t.TempDir(), filepath.Join(t.TempDir(), "files.gzip")
	entries := append([]FLCEntry(nil), snapshotFixtureTemplate.entries...)
	for _, entry := range entries {
		path := filepath.Join(root, entry.Path)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	copySnapshotFixture(t, snapshotFixtureTemplate.cache, cache, root)
	return cache, root, entries
}

func copySnapshotFixture(t *testing.T, source, cache, root string) {
	t.Helper()
	files, err := os.ReadDir(filepath.Dir(source))
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		info, err := file.Info()
		if err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(filepath.Join(filepath.Dir(source), file.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(filepath.Dir(cache), file.Name()), data, info.Mode().Perm()); err != nil {
			t.Fatal(err)
		}
	}
	manifest, err := readIndexSnapshotManifest(snapshotManifestPath(cache))
	if err != nil {
		t.Fatal(err)
	}
	manifest.Root, manifest.Device, manifest.Inode, err = snapshotRootIdentity(root)
	if err != nil {
		t.Fatal(err)
	}
	manifest.Source, err = snapshotSourceIdentity(cache)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(snapshotManifestPath(cache), data, 0600); err != nil {
		t.Fatal(err)
	}
}
