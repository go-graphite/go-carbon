package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"

	"github.com/go-graphite/go-carbon/internal/v2core"
)

func partFixture(t *testing.T, count int) (string, v2core.PartSnapshot) {
	t.Helper()
	root := t.TempDir()
	var files []v2core.SnapshotPart
	for i := range count {
		path := filepath.Join(root, string(rune('a'+i)))
		if e := os.WriteFile(path, bytes.Repeat([]byte{byte(i + 1)}, 1<<20), 0600); e != nil {
			t.Fatal(e)
		}
		files = append(files, v2core.SnapshotPart{Path: path})
	}
	dir := filepath.Join(root, "snap")
	s, e := v2core.LinkPartSnapshot(context.Background(), dir, partManifestReader{files})
	if e != nil {
		t.Fatal(e)
	}
	return dir, s
}

func TestPartSyncIncrementalSnapshot(t *testing.T) {
	dir, s := partFixture(t, 3)
	var mu sync.Mutex
	requests := map[string]int{}
	h := partSnapshotHandler(dir, s, "")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests[r.URL.Path]++
		mu.Unlock()
		h.ServeHTTP(w, r)
	}))
	defer server.Close()
	target := t.TempDir()
	first := s
	first.Parts = s.Parts[:2]
	seed := httptest.NewServer(partSnapshotHandler(dir, first, ""))
	defer seed.Close()
	r, e := receivePartSnapshot(context.Background(), seed.Client(), seed.URL, "", target, 2)
	if e != nil || r.Parts != 2 {
		t.Fatalf("%+v %v", r, e)
	}
	r, e = receivePartSnapshot(context.Background(), server.Client(), server.URL, "", target, 2)
	if e != nil || r.Parts != 1 || r.Skipped != 2 || r.Bytes != 1<<20 {
		t.Fatalf("%+v %v", r, e)
	}
	for _, p := range s.Parts[:2] {
		if requests["/parts/"+p.ID] != 0 {
			t.Fatal("verified part resent")
		}
	}
	r, e = receivePartSnapshot(context.Background(), server.Client(), server.URL, "", target, 8)
	if e != nil || r.Bytes != 0 || r.Skipped != 3 {
		t.Fatalf("%+v %v", r, e)
	}
	// Completed objects are locally reconciled, including the crash edge before
	// manifest publication. Corrupt finals fail loudly, never count as skipped.
	if e = os.WriteFile(filepath.Join(target, s.Parts[0].ID+".part"), []byte("corrupt"), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e = receivePartSnapshot(context.Background(), server.Client(), server.URL, "", target, 1); e == nil {
		t.Fatal("corruption accepted")
	}
}

func TestPartSyncRejectsCorruptionAndAuth(t *testing.T) {
	dir, s := partFixture(t, 1)
	server := httptest.NewServer(partSnapshotHandler(dir, s, "secret"))
	defer server.Close()
	if _, e := receivePartSnapshot(context.Background(), server.Client(), server.URL, "wrong", t.TempDir(), 1); e == nil {
		t.Fatal("unauthorized accepted")
	}
	if e := os.WriteFile(filepath.Join(dir, s.Parts[0].ID+".part"), bytes.Repeat([]byte{99}, 1<<20), 0600); e != nil {
		t.Fatal(e)
	}
	target := t.TempDir()
	if _, e := receivePartSnapshot(context.Background(), server.Client(), server.URL, "secret", target, 1); e == nil {
		t.Fatal("bad transfer accepted")
	}
	if _, e := os.Stat(filepath.Join(target, "snapshot.json")); !os.IsNotExist(e) {
		t.Fatal("bad snapshot published")
	}
}

// Run the real receiver in a separate process so SIGKILL bypasses deferred
// cleanup and releases the OS target lock without an orderly Close.
func TestPartReceiverProcess(t *testing.T) {
	if os.Getenv("PART_COPY_CHILD") != "1" {
		return
	}
	_, e := receivePartSnapshot(context.Background(), http.DefaultClient, os.Getenv("PART_COPY_URL"), "", os.Getenv("PART_COPY_DIR"), 1)
	if e != nil {
		os.Exit(2)
	}
	os.Exit(0)
}

func TestPartSyncSIGKILLResume(t *testing.T) {
	dir, s := partFixture(t, 3)
	target := t.TempDir()
	blocked := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)
	var mu sync.Mutex
	counts := map[string]int{}
	block := true
	h := partSnapshotHandler(dir, s, "")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		counts[r.URL.Path]++
		wait := block && r.URL.Path == "/parts/"+s.Parts[1].ID
		mu.Unlock()
		if wait {
			w.Header().Set("Content-Length", "1048576")
			_, _ = w.Write([]byte{2})
			w.(http.Flusher).Flush()
			select {
			case blocked <- struct{}{}:
			default:
			}
			select {
			case <-release:
			case <-r.Context().Done():
			}
			return
		}
		h.ServeHTTP(w, r)
	}))
	defer server.Close()
	cmd := exec.Command(os.Args[0], "-test.run=^TestPartReceiverProcess$")
	cmd.Env = append(os.Environ(), "PART_COPY_CHILD=1", "PART_COPY_URL="+server.URL, "PART_COPY_DIR="+target)
	if e := cmd.Start(); e != nil {
		t.Fatal(e)
	}
	defer func() { _ = cmd.Process.Kill() }()
	select {
	case <-blocked:
	case <-time.After(10 * time.Second):
		t.Fatal("receiver did not reach interrupted part")
	}
	if _, e := os.Stat(filepath.Join(target, s.Parts[0].ID+".part")); e != nil {
		t.Fatal("first part was not durable", e)
	}
	if e := cmd.Process.Kill(); e != nil {
		t.Fatal(e)
	}
	_ = cmd.Wait()
	mu.Lock()
	block = false
	mu.Unlock()
	r, e := receivePartSnapshot(context.Background(), server.Client(), server.URL, "", target, 1)
	if e != nil || r.Parts != 2 || r.Skipped != 1 {
		t.Fatalf("%+v %v", r, e)
	}
	mu.Lock()
	defer mu.Unlock()
	if counts["/parts/"+s.Parts[0].ID] != 1 || counts["/parts/"+s.Parts[1].ID] != 2 {
		t.Fatalf("request audit %+v", counts)
	}
	b, e := os.ReadFile(filepath.Join(target, "snapshot.json"))
	if e != nil {
		t.Fatal(e)
	}
	var got v2core.PartSnapshot
	if e = json.Unmarshal(b, &got); e != nil || len(got.Parts) != 3 {
		t.Fatal("final inventory", e)
	}
}

func TestPartSyncPortableDenseFormat(t *testing.T) {
	ctx := context.Background()
	source := t.TempDir()
	path := filepath.Join(source, "building.sst")
	definition := []byte(`{"step":86400,"count":3650}`)
	sourceID := v2part.PartID{9}
	spec := v2part.Spec{Scope: v2part.Scope{Group: "fixture", VShard: v2part.GroupVShard, Part: v2part.PartID{1}}, Source: v2part.Provenance{Incarnation: sourceID, Stream: "fixture", MinOrder: 1, MaxOrder: 1}, MinTime: 1700000000, MaxTime: 1700000000 + 255*86400, Policies: []v2part.ArchivePolicy{{Archive: 0, Definition: definition, Digest: sha256.Sum256(definition)}}}
	w, e := v2part.NewDenseWriter(path, spec)
	if e != nil {
		t.Fatal(e)
	}
	defer w.Abort()
	for i := int64(0); i < 256; i++ {
		k := v2part.Key{Group: "fixture", VShard: v2part.Route("fixture.metric"), Part: spec.Scope.Part, Metric: "fixture.metric", BlockStart: i}
		key, _ := v2part.Encode(k)
		r := sealrollup.Record{Delta: sealrollup.Delta{Archive: 0, Slot: i, Order: 1, Sample: sealrollup.Sample{Time: 1700000000 + i*86400, Bits: uint64(i)}}}
		if e = w.SetEncoded(key, sealrollup.EncodeRecord(r)); e != nil {
			t.Fatal(e)
		}
	}
	d, e := w.Close()
	if e != nil {
		t.Fatal(e)
	}
	if e = os.Rename(path, filepath.Join(source, fmt.Sprintf("%x.sst", d.SHA256))); e != nil {
		t.Fatal(e)
	}
	root := sha256.Sum256([]byte("empty-fixture-authority"))
	m := v2part.Manifest{Version: 1, Group: "fixture", Generation: 1, CatalogRoot: root, PolicyRoot: root, TombstoneRoot: root, Frontiers: []v2part.StreamFrontier{{Incarnation: sourceID, Stream: "fixture", Cursor: 1}}, Parts: []v2part.Descriptor{d}}
	b, e := m.MarshalBinary()
	if e != nil {
		t.Fatal(e)
	}
	manifest := filepath.Join(source, "manifest.json")
	if e = os.WriteFile(manifest, b, 0600); e != nil {
		t.Fatal(e)
	}
	snapDir := filepath.Join(t.TempDir(), "snapshot")
	if e = partSyncMain([]string{"--parts", "--portable-manifest", manifest, "--snapshot-dir", snapDir}); e != nil {
		t.Fatal(e)
	}
	snapshot, e := readPartSnapshot(filepath.Join(snapDir, "snapshot.json"))
	if e != nil || len(snapshot.Parts) != 2 {
		t.Fatalf("snapshot: %+v %v", snapshot, e)
	}
	server := httptest.NewServer(partSnapshotHandler(snapDir, snapshot, ""))
	defer server.Close()
	target := t.TempDir()
	result, e := receivePartSnapshot(ctx, server.Client(), server.URL, "", target, 2)
	if e != nil || result.Parts != 2 {
		t.Fatalf("copy %+v %v", result, e)
	}
	result, e = receivePartSnapshot(ctx, server.Client(), server.URL, "", target, 2)
	if e != nil || result.Bytes != 0 || result.Skipped != 2 {
		t.Fatalf("resume %+v %v", result, e)
	}
	copied := filepath.Join(target, fmt.Sprintf("%x.part", d.SHA256))
	n := 0
	e = v2part.ReadExpanded(ctx, copied, &d, func(k v2part.Key, b []byte) error {
		r, e := sealrollup.DecodeRecord(0, k.BlockStart, b)
		if e != nil {
			return e
		}
		if r.Sample.Bits != uint64(n) {
			t.Fatal("value changed in transfer")
		}
		n++
		return nil
	})
	if e != nil || n != 256 {
		t.Fatalf("readback %d: %v", n, e)
	}
	// The physical checksum binds the descriptor supplied by the format reader.
	f, e := os.OpenFile(filepath.Join(source, fmt.Sprintf("%x.sst", d.SHA256)), os.O_WRONLY, 0)
	if e != nil {
		t.Fatal(e)
	}
	_, e = f.WriteAt([]byte("corrupt"), 0)
	f.Close()
	if e != nil {
		t.Fatal(e)
	}
	if _, e = v2core.LinkPartSnapshot(ctx, filepath.Join(t.TempDir(), "bad"), v2core.PortableSnapshotReader{ManifestPath: manifest, Directory: source}); e == nil {
		t.Fatal("descriptor checksum mismatch accepted")
	}
}
