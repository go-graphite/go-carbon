package v2core

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

type fixturePartReader []SnapshotPart

func (r fixturePartReader) ReadPartFiles(context.Context) ([]SnapshotPart, error) { return r, nil }

func TestLinkPartSnapshotPinsAndValidates(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "sealed.sst")
	if e := os.WriteFile(source, []byte("opaque sealed bytes"), 0600); e != nil {
		t.Fatal(e)
	}
	dir := filepath.Join(root, "snapshot")
	s, e := LinkPartSnapshot(context.Background(), dir, fixturePartReader{{Path: source}})
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Validate(); e != nil || len(s.Parts) != 1 {
		t.Fatalf("%+v %v", s, e)
	}
	a, _ := os.Stat(source)
	b, _ := os.Stat(filepath.Join(dir, s.Parts[0].ID+".part"))
	if !os.SameFile(a, b) {
		t.Fatal("snapshot rewrote file instead of hardlinking")
	}
	if e = os.Remove(source); e != nil {
		t.Fatal(e)
	}
	if _, e = os.ReadFile(filepath.Join(dir, s.Parts[0].ID+".part")); e != nil {
		t.Fatal(e)
	}
	s.Parts = append(s.Parts, s.Parts[0])
	if s.Validate() == nil {
		t.Fatal("duplicate accepted")
	}
	bad := filepath.Join(root, "bad")
	if _, e = LinkPartSnapshot(context.Background(), bad, fixturePartReader{{Path: filepath.Join(dir, s.Parts[0].ID+".part"), SHA256: "wrong"}}); e == nil {
		t.Fatal("checksum mismatch accepted")
	}
	if _, e = os.Stat(bad); !os.IsNotExist(e) {
		t.Fatal("failed snapshot leaked")
	}
}
