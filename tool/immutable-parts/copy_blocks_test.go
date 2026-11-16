package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/internal/partsformat"
)

func copySubsetFixture(t *testing.T, p copyPartition, dir string, chosen []int, part byte, corrupt int, blockSize int) copyPartition {
	t.Helper()
	f, err := os.Open(p.Files[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	rd, err := sstable.NewSimpleReadable(f)
	if err != nil {
		t.Fatal(err)
	}
	r, err := sstable.NewReader(rd, sstable.ReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	it, err := r.NewIter(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	path := filepath.Join(dir, "subset.sst")
	vf, err := vfs.Default.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(vf), sstable.WriterOptions{Compression: sstable.NoCompression, TableFormat: sstable.TableFormatPebblev4, BlockSize: blockSize})
	keep := make(map[int]bool)
	for _, i := range chosen {
		keep[i] = true
	}
	index := 0
	for key, lazy := it.First(); key != nil; key, lazy = it.Next() {
		if keep[index] {
			k, e := partsformat.Decode(key.UserKey)
			if e != nil {
				t.Fatal(e)
			}
			if part != 0 {
				k.Part = partsformat.PartID{part}
			}
			encoded, e := partsformat.Encode(k)
			if e != nil {
				t.Fatal(e)
			}
			v, _, e := lazy.Value(nil)
			if e != nil {
				t.Fatal(e)
			}
			if index == corrupt {
				v = bytes.Clone(v)
				v[len(v)-1] ^= 1
			}
			if e = w.Set(encoded, v); e != nil {
				t.Fatal(e)
			}
		}
		index++
	}
	if err = it.Error(); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	desc, err := copyDescribe(path, partsformat.PartID{part})
	if err != nil {
		t.Fatal(err)
	}
	m := p.Manifest
	m.Parts = []partsformat.Part{desc.Part}
	return copyPartition{Manifest: m, Files: []copyFile{desc}}
}

func copySealedFixture(t *testing.T, p copyPartition) copyPartition {
	t.Helper()
	m, err := copySealBlocks(p)
	if err != nil {
		t.Fatal(err)
	}
	// Round-trip the persisted manifest; compare/repair must not depend on a
	// process-local cache or an unpersisted logical digest.
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var persisted copyBlockManifest
	if err = json.Unmarshal(b, &persisted); err != nil {
		t.Fatal(err)
	}
	p.Blocks = &persisted
	return p
}

func TestCopyLogicalDigestDifferentSealHistories(t *testing.T) {
	full := copyFixture(t, t.TempDir(), 1, 6, 1024, nil)[0]
	source := copySealedFixture(t, full)
	a := copySubsetFixture(t, full, t.TempDir(), []int{0, 1, 2}, 21, -1, 4096)
	b := copySubsetFixture(t, full, t.TempDir(), []int{3, 4, 5}, 22, -1, 4096)
	target := a
	target.Files = append(target.Files, b.Files...)
	target.Manifest.Parts = append(target.Manifest.Parts, b.Manifest.Parts...)
	target = copySealedFixture(t, target)
	if source.Blocks.Digest != target.Blocks.Digest {
		t.Fatal("physical sealing history changed logical digest")
	}
	physicalA, _ := source.Manifest.Digest()
	physicalB, _ := target.Manifest.Digest()
	if physicalA == physicalB {
		t.Fatal("fixture physical histories must differ")
	}
	// Prove comparison/zero-copy is metadata only: remove every source SST.
	for _, p := range []copyPartition{source, target} {
		for _, f := range p.Files {
			if e := os.Remove(f.Path); e != nil {
				t.Fatal(e)
			}
		}
	}
	p, plan, err := copyRepairPartition(source, *target.Blocks, t.TempDir())
	if err != nil || len(plan.Missing) != 0 || len(p.Files) != 0 || len(p.Manifest.Parts) != 0 || plan.RawBytes != 0 {
		t.Fatalf("expected zero-byte logical repair: %+v %v", plan, err)
	}
	t.Logf("different histories: logical equal; raw payload=0 bytes, values decoded=0")
}

func TestCopyTwoSidedHolesRawBlocks(t *testing.T) {
	full := copyFixture(t, t.TempDir(), 1, 6, 1024, nil)[0]
	source := copySealedFixture(t, copySubsetFixture(t, full, t.TempDir(), []int{0, 1, 2, 4}, 0, -1, 4096))
	target := copySealedFixture(t, copySubsetFixture(t, full, t.TempDir(), []int{1, 3, 4, 5}, 0, -1, 4096))
	dir := t.TempDir()
	patch, plan, err := copyRepairPartition(source, *target.Blocks, dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Missing) != 2 || plan.Shared != 2 || plan.Conflicts != 0 {
		t.Fatalf("bad hole plan %+v", plan)
	}
	assembled := copySealedFixture(t, patch)
	if len(assembled.Blocks.Blocks) != 2 {
		t.Fatal("healthy blocks shipped")
	}
	for i, bi := range plan.Missing {
		before := source.Blocks.Blocks[bi]
		after := assembled.Blocks.Blocks[i]
		if before.SHA256 != after.SHA256 || before.Length != after.Length {
			t.Fatal("physical data block changed")
		}
	}
	db := copyTestDB(t, filepath.Join(dir, "target"))
	defer db.Close()
	expected := copyTestDB(t, filepath.Join(dir, "expected"))
	defer expected.Close()
	for i, p := range []copyPartition{target, full} {
		link := filepath.Join(dir, []string{"target.sst", "expected.sst"}[i])
		if err = os.Link(p.Files[0].Path, link); err != nil {
			t.Fatal(err)
		}
		if err = []*pebble.DB{db, expected}[i].Ingest([]string{link}); err != nil {
			t.Fatal(err)
		}
	}
	address, stop := copyTestServer(t, []copyPartition{patch})
	defer stop()
	stats, err := copyTransfer(context.Background(), address, []copyPartition{patch}, []int{0}, 4, []*pebble.DB{db}, dir)
	if err != nil {
		t.Fatal(err)
	}
	if copyDBDigest(t, db) != copyDBDigest(t, expected) {
		t.Fatal("two-sided union differs byte-for-byte")
	}
	if stats.Bytes != patch.Files[0].Part.Bytes || stats.Bytes < plan.RawBytes {
		t.Fatal("payload accounting mismatch")
	}
	t.Logf("union exact: missing raw blocks=%d bytes=%d; SST metadata=%d; wire SST=%d; sample decode=0", len(plan.Missing), plan.RawBytes, stats.Bytes-plan.RawBytes, stats.Bytes)
}

func TestCopyBlockConflictsAndInvalidMetadata(t *testing.T) {
	full := copyFixture(t, t.TempDir(), 1, 6, 1024, nil)[0]
	source := copySealedFixture(t, copySubsetFixture(t, full, t.TempDir(), []int{0, 1, 2, 4}, 0, -1, 4096))
	target := copySealedFixture(t, copySubsetFixture(t, full, t.TempDir(), []int{1, 3, 4, 5}, 0, 4, 4096))
	source.Manifest.Policy[0] ^= 1
	if _, _, err := copyRepairPartition(source, *target.Blocks, t.TempDir()); err == nil {
		t.Fatal("accepted source seal metadata mismatch")
	}
	source.Manifest.Policy[0] ^= 1
	plan, err := copyPlanBlocks(*source.Blocks, *target.Blocks)
	if err != nil || plan.Conflicts != 1 || len(plan.Missing) != 2 || plan.Shared != 1 {
		t.Fatalf("conflicts %+v %v", plan, err)
	}
	target.Blocks.Policy[0] ^= 1
	if _, err = copyPlanBlocks(*source.Blocks, *target.Blocks); err == nil {
		t.Fatal("accepted policy mismatch")
	}
	target.Blocks.Policy[0] ^= 1
	target.Blocks.Blocks[0].ValueSHA256[0] ^= 1
	if _, err = copyPlanBlocks(*source.Blocks, *target.Blocks); err == nil {
		t.Fatal("accepted stale logical digest")
	}
	packed := copySubsetFixture(t, full, t.TempDir(), []int{0, 1, 2, 3, 4, 5}, 0, -1, 1<<20)
	if _, err = copySealBlocks(packed); err == nil {
		t.Fatal("accepted unsupported multiple-record physical blocks")
	}
	bad := *source.Blocks
	bad.Blocks = append([]copyBlock(nil), bad.Blocks...)
	bad.Blocks[0].SHA256[0] ^= 1
	source.Blocks = &bad
	if _, _, err = copyAssembleBlocks(source, []int{0}, t.TempDir()); err == nil {
		t.Fatal("accepted corrupt raw block")
	}
}
