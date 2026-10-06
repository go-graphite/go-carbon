package carbonserver

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func snapshotFixture(t *testing.T) (string, string, []FLCEntry) {
	t.Helper()
	root, cache := t.TempDir(), filepath.Join(t.TempDir(), "files.gzip")
	names := []string{"a/value.wsp", "a-sibling/value.wsp", "a.wsp", "a0/value.wsp", "b/child/deep.wsp", "b/child.wsp", "空间/值.wsp"}
	for _, name := range names {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	w, err := newIndexSnapshotWriter(cache, root)
	if err != nil {
		t.Fatal(err)
	}
	flc, err := NewFileListCache(cache, FLCVersion2, 'w')
	if err != nil {
		t.Fatal(err)
	}
	var entries []FLCEntry
	err = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		n := int64(len(entries) + 1)
		entry := FLCEntry{Path: strings.TrimPrefix(path, root), LogicalSize: n, PhysicalSize: n * 4096, DataPoints: n * 60, FirstSeenAt: 1700000000 + n}
		entries = append(entries, entry)
		if err := flc.Write(&entry); err != nil {
			return err
		}
		return w.append(&entry)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := flc.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.finish(); err != nil {
		t.Fatal(err)
	}
	return cache, root, entries
}

func TestIndexSnapshotRoundTrip(t *testing.T) {
	cache, root, entries := snapshotFixture(t)
	s, err := openIndexSnapshot(cache, root)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	for _, want := range entries {
		got, ok, err := s.lookup(want.Path)
		if err != nil || !ok || !reflect.DeepEqual(got, &want) {
			t.Fatalf("lookup %q: got %v/%v/%v, want %v", want.Path, got, ok, err, want)
		}
	}
	if _, ok, err := s.lookup("/absent.wsp"); err != nil || ok {
		t.Fatalf("missing lookup: %v/%v", ok, err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 1000; n++ {
				want := entries[n%len(entries)]
				got, ok, err := s.lookup(want.Path)
				if err != nil || !ok || *got != want {
					t.Errorf("concurrent lookup differs: %v/%v/%v", got, ok, err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestIndexSnapshotWrittenByCompleteReconciliation(t *testing.T) {
	l := savedIndex(t, "a.value", "a-sibling.value", "a", "b.child.deep", "b.child")
	if !l.updateFileList(l.whisperData, nil, nil) {
		t.Fatal("expected the legacy cache to warm the index")
	}
	if _, err := os.Stat(snapshotManifestPath(l.fileListCache)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("cache warmup must not rebuild a snapshot", err)
	}
	if l.updateFileList(l.whisperData, nil, nil) {
		t.Fatal("expected filesystem reconciliation")
	}
	if l.CurrentFileIndex().trieIdx.snapshot == nil {
		t.Fatal("completed background scan retained the full heap trie")
	}
	s, err := openIndexSnapshot(l.fileListCache, l.whisperData)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	for _, metric := range []string{"a.value", "a-sibling.value", "a", "b.child.deep", "b.child"} {
		if _, ok, err := s.lookup("/" + strings.ReplaceAll(metric, ".", "/") + ".wsp"); err != nil || !ok {
			t.Fatalf("snapshot missed %q: %v", metric, err)
		}
	}
	before, err := os.ReadFile(snapshotManifestPath(l.fileListCache))
	if err != nil {
		t.Fatal(err)
	}
	update := newFileListUpdate(l, nil)
	update.fileListCache = update.newFileListCacheWriter()
	if update.snapshotWriter == nil {
		t.Fatal("snapshot writer not enabled for normal scan")
	}
	update.cacheFile("/partial.wsp", true, 1, 1, 1, 1)
	update.scanFailed = true
	update.closeFileListCaches()
	after, err := os.ReadFile(snapshotManifestPath(l.fileListCache))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("incomplete scan replaced checkpoint", err)
	}
	unchanged, err := openIndexSnapshot(l.fileListCache, l.whisperData)
	if err != nil {
		t.Fatal("incomplete scan replaced the legacy cache", err)
	}
	unchanged.close()
}

func TestIndexSnapshotFailedBuildPreservesGeneration(t *testing.T) {
	cache, root, entries := snapshotFixture(t)
	path := snapshotManifestPath(cache)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	w, err := newIndexSnapshotWriter(cache, root)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.append(&entries[1]); err != nil {
		t.Fatal(err)
	}
	if err := w.append(&entries[0]); err == nil {
		t.Fatal("unordered scan accepted")
	}
	if err := w.finish(); err == nil {
		t.Fatal("errored snapshot was published")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("failed build replaced the previous manifest", err)
	}
	if _, err := os.Stat(w.indexFile.Name()); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("failed build left an index file", err)
	}
	w, err = newIndexSnapshotWriter(cache, root)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.append(&entries[0]); err != nil {
		t.Fatal(err)
	}
	w.metadata.w = snapshotFailWriter{errors.New("disk full")}
	if err := w.finish(); err == nil {
		t.Fatal("write failure was ignored")
	}
	after, err = os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("write failure replaced manifest", err)
	}
	s, err := openIndexSnapshot(cache, root)
	if err != nil {
		t.Fatal(err)
	}
	s.close()
}

func TestIndexSnapshotReplacementKeepsActiveMappings(t *testing.T) {
	cache, root, entries := snapshotFixture(t)
	old, err := openIndexSnapshot(cache, root)
	if err != nil {
		t.Fatal(err)
	}
	defer old.close()
	w, err := newIndexSnapshotWriter(cache, root)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if err := w.append(&entry); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.finish(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(cache), old.manifest.Index.Name)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("old generation was not unlinked", err)
	}
	got, ok, err := old.lookup(entries[0].Path)
	if err != nil || !ok || *got != entries[0] {
		t.Fatal("active mapping changed after replacement", err)
	}
	fresh, err := openIndexSnapshot(cache, root)
	if err != nil {
		t.Fatal(err)
	}
	fresh.close()
}

func TestIndexSnapshotRejectsUnvalidatedGeneration(t *testing.T) {
	for _, mutation := range []string{"checksum", "truncate", "source", "root", "version", "count", "path"} {
		t.Run(mutation, func(t *testing.T) {
			cache, root, _ := snapshotFixture(t)
			path := snapshotManifestPath(cache)
			manifest, err := readIndexSnapshotManifest(path)
			if err != nil {
				t.Fatal(err)
			}
			switch mutation {
			case "checksum":
				f, err := os.OpenFile(filepath.Join(filepath.Dir(cache), manifest.Metadata.Name), os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = f.WriteAt([]byte{255}, 0); err != nil {
					t.Fatal(err)
				}
				f.Close()
			case "truncate":
				if err := os.Truncate(filepath.Join(filepath.Dir(cache), manifest.Index.Name), 1); err != nil {
					t.Fatal(err)
				}
			case "source":
				if err := os.WriteFile(cache, []byte("new generation"), 0600); err != nil {
					t.Fatal(err)
				}
			case "root":
				root = t.TempDir()
			case "version":
				manifest.Version++
			case "count":
				manifest.Records++
			case "path":
				manifest.Index.Name = "../" + manifest.Index.Name
			}
			data, err := json.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			if s, err := openIndexSnapshot(cache, root); err == nil {
				s.close()
				t.Fatal("invalid snapshot accepted")
			}
		})
	}
}
