package v2core

import (
	"bytes"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// syncCountFS counts durable syncs per path class on ownerFS.
type syncCountFS struct {
	vfs.FS
	mu     sync.Mutex
	counts map[string]int
}

type syncCountFile struct {
	vfs.File
	fs    *syncCountFS
	class string
}

func (fs *syncCountFS) wrap(name string, f vfs.File, err error) (vfs.File, error) {
	if err != nil {
		return nil, err
	}
	class := filepath.Base(name)
	switch {
	case strings.HasSuffix(class, ".log"):
		class = "WAL"
	case strings.HasPrefix(class, "MANIFEST"):
		class = "MANIFEST"
	case strings.HasPrefix(class, "stage-"):
		class = "staged SST"
	case strings.HasPrefix(class, "owner-"):
		class = "owner dir"
	}
	return &syncCountFile{File: f, fs: fs, class: class}, nil
}
func (fs *syncCountFS) Create(name string) (vfs.File, error) {
	f, err := fs.FS.Create(name)
	return fs.wrap(name, f, err)
}
func (fs *syncCountFS) OpenReadWrite(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	f, err := fs.FS.OpenReadWrite(name, opts...)
	return fs.wrap(name, f, err)
}
func (fs *syncCountFS) ReuseForWrite(oldname, newname string) (vfs.File, error) {
	f, err := fs.FS.ReuseForWrite(oldname, newname)
	return fs.wrap(newname, f, err)
}
func (fs *syncCountFS) OpenDir(name string) (vfs.File, error) {
	f, err := fs.FS.OpenDir(name)
	return fs.wrap(name, f, err)
}
func (fs *syncCountFS) add(class string) {
	fs.mu.Lock()
	fs.counts[class]++
	fs.mu.Unlock()
}
func (fs *syncCountFS) take() map[string]int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	out := fs.counts
	fs.counts = map[string]int{}
	return out
}
func (f *syncCountFile) Sync() error     { f.fs.add(f.class); return f.File.Sync() }
func (f *syncCountFile) SyncData() error { f.fs.add(f.class); return f.File.SyncData() }
func (f *syncCountFile) SyncTo(n int64) (bool, error) {
	full, err := f.File.SyncTo(n)
	if full {
		f.fs.add(f.class)
	}
	return full, err
}

// A rollup seal makes three syncs: the part (its writer's one sync), one
// parts/ directory sync, and one WAL sync publishing descriptor and frontier
// together. No sidecar, staged SST, owner-directory or manifest sync.
func TestSealFsyncBudget(t *testing.T) {
	counter := &syncCountFS{FS: vfs.Default, counts: map[string]int{}}
	ownerFS = counter
	defer func() { ownerFS = vfs.Default }()
	cfg := coreTestConfig()
	cfg.Shards, cfg.Producers, cfg.Rollups = 1, 1, true
	_, policy := rollupTestPolicy(t, "fsync.budget", "1s:1d", sealrollup.Last, .5)
	e, err := openCore(t.TempDir(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e.close()
	o := e.owners[0]
	for seal := 1; seal <= 3; seal++ {
		ts := int64(1700000000 + 10*seal)
		batch := []coreBatch{{ID: 7, Order: uint64(seal), Clock: ts + 2, Policy: policy,
			Points: []row{{ID: 7, Time: ts, Bits: 0x7ff8000000000042, Seq: uint64(seal)}}}}
		counter.take()
		parts, ingests := o.syncCalls.Load(), o.db.Metrics().Ingest.Count
		if err = e.submitBatches(0, batch, uint64(seal), true); err != nil {
			t.Fatal(err)
		}
		got := counter.take()
		total := 0
		for _, n := range got {
			total += n
		}
		if got["parts"] != 1 || got["WAL"] != 1 || total != 2 {
			classes := make([]string, 0, len(got))
			for c, n := range got {
				classes = append(classes, c+"="+string(rune('0'+n)))
			}
			sort.Strings(classes)
			t.Fatalf("seal %d: owner syncs %v, want parts=1 WAL=1 only", seal, classes)
		}
		if n := o.syncCalls.Load() - parts; n != 1 {
			t.Fatalf("seal %d: %d part syncs, want 1", seal, n)
		}
		if n := o.db.Metrics().Ingest.Count - ingests; n != 0 {
			t.Fatalf("seal %d: %d ingests, want 0", seal, n)
		}
	}
}

// Seals do not sync sidecars, so a power loss can leave a published part's
// sidecar empty or missing. The owner open restores both from the binding.
func TestSealSidecarRepairedAtOpen(t *testing.T) {
	cfg := coreTestConfig()
	cfg.Shards, cfg.Producers, cfg.Rollups = 1, 1, true
	_, policy := rollupTestPolicy(t, "fsync.repair", "1s:1d", sealrollup.Last, .5)
	root := t.TempDir()
	e, err := openCore(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for seal := 1; seal <= 2; seal++ {
		ts := int64(1700000000 + 10*seal)
		batch := []coreBatch{{ID: 7, Order: uint64(seal), Clock: ts + 2, Policy: policy,
			Points: []row{{ID: 7, Time: ts, Bits: 0x7ff8000000000042, Seq: uint64(seal)}}}}
		if err = e.submitBatches(0, batch, uint64(seal), true); err != nil {
			t.Fatal(err)
		}
	}
	if err = e.close(); err != nil {
		t.Fatal(err)
	}
	sidecars, err := filepath.Glob(filepath.Join(root, "owner-000", "parts", "*.descriptor"))
	if err != nil || len(sidecars) != 2 {
		t.Fatalf("sidecars %v %v", sidecars, err)
	}
	want := map[string][]byte{}
	for _, p := range sidecars {
		if want[p], err = os.ReadFile(p); err != nil {
			t.Fatal(err)
		}
	}
	if err = os.Truncate(sidecars[0], 0); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(sidecars[1]); err != nil {
		t.Fatal(err)
	}
	e, err = openCore(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e.close()
	for p, b := range want {
		got, err := os.ReadFile(p)
		if err != nil || !bytes.Equal(got, b) {
			t.Fatalf("sidecar %s not repaired: %d bytes, want %d: %v", filepath.Base(p), len(got), len(b), err)
		}
	}
}
