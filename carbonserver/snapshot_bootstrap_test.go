package carbonserver

import (
	"errors"
	"io"
	"os"
	"reflect"
	"testing"
)

func rewriteCacheVersion(t *testing.T, path string, version FLCVersion) {
	t.Helper()
	reader, err := NewFileListCache(path, FLCVersionUnspecified, 'r')
	if err != nil {
		t.Fatal(err)
	}
	var entries []*FLCEntry
	for {
		entry, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, entry)
	}
	if err = reader.Close(); err != nil {
		t.Fatal(err)
	}
	writer, err := NewFileListCache(path, version, 'w')
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if err = writer.Write(entry); err != nil {
			t.Fatal(err)
		}
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotBootstrapFromSavedCatalogue(t *testing.T) {
	cache, root, entries := snapshotFixture(t)
	old, err := os.ReadFile(cache)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(snapshotManifestPath(cache)); err != nil {
		t.Fatal(err)
	}
	snapshot, err := buildSnapshotFromCache(cache, root, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.close()
	for _, want := range entries {
		got, ok, err := snapshot.lookup(want.Path)
		if err != nil || !ok || *got != want {
			t.Fatalf("bootstrap differs %v %t %v", got, ok, err)
		}
	}
	after, err := os.ReadFile(cache)
	if err != nil || !reflect.DeepEqual(old, after) {
		t.Fatal("bootstrap rewrote the legacy authority", err)
	}
}

func TestSnapshotBootstrapFallback(t *testing.T) {
	for _, kind := range []string{"old-format", "unordered", "cancelled", "truncated"} {
		t.Run(kind, func(t *testing.T) {
			cache, root, _ := snapshotFixture(t)
			before, err := os.ReadFile(snapshotManifestPath(cache))
			if err != nil {
				t.Fatal(err)
			}
			var stop chan struct{}
			switch kind {
			case "old-format":
				rewriteCacheVersion(t, cache, FLCVersion1)
			case "unordered":
				writer, err := NewFileListCache(cache, FLCVersion2, 'w')
				if err != nil {
					t.Fatal(err)
				}
				for _, name := range []string{"/z.wsp", "/a.wsp"} {
					if err = writer.Write(&FLCEntry{Path: name}); err != nil {
						t.Fatal(err)
					}
				}
				if err = writer.Close(); err != nil {
					t.Fatal(err)
				}
			case "cancelled":
				stop = make(chan struct{})
				close(stop)
			case "truncated":
				info, err := os.Stat(cache)
				if err != nil {
					t.Fatal(err)
				}
				if err = os.Truncate(cache, info.Size()/2); err != nil {
					t.Fatal(err)
				}
			}
			if snapshot, err := buildSnapshotFromCache(cache, root, stop); err == nil {
				snapshot.close()
				t.Fatal("invalid source accepted")
			}
			after, err := os.ReadFile(snapshotManifestPath(cache))
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("failed bootstrap replaced prior generation", err)
			}
		})
	}
}

func TestSnapshotBootstrapRejectsSourceReplacement(t *testing.T) {
	cache, root, entries := snapshotFixture(t)
	source, err := snapshotSourceIdentity(cache)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := newIndexSnapshotWriter(cache, root)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.abort()
	writer.expectedSource = &source
	for _, entry := range entries {
		if err = writer.append(&entry); err != nil {
			t.Fatal(err)
		}
	}
	rewriteCacheVersion(t, cache, FLCVersion2)
	if err = writer.finish(); err == nil {
		t.Fatal("bootstrap adopted another source generation")
	}
}
