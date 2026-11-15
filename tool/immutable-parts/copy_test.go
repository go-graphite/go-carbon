package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/internal/partsformat"
)

func copyTestDB(t *testing.T, dir string) *pebble.DB {
	t.Helper()
	db, e := pebble.Open(dir, &pebble.Options{FormatMajorVersion: pebble.FormatNewest, DisableAutomaticCompactions: true, MemTableSize: 8 << 20, Logger: copyQuietLogger{}})
	if e != nil {
		t.Fatal(e)
	}
	return db
}

type copyQuietLogger struct{}

func (copyQuietLogger) Infof(string, ...interface{})      {}
func (copyQuietLogger) Fatalf(f string, a ...interface{}) { panic("pebble fatal: " + f) }

// One named metric per partition with exact special float payloads and ordered
// sample bits. Random finite payloads make transfer-byte measurements honest.
func copyFixture(t *testing.T, root string, partitions, blocks, points int, donors []*pebble.DB) []copyPartition {
	return copyFixtureGrouped(t, root, partitions, blocks, points, donors, 0)
}
func copyFixtureGrouped(t *testing.T, root string, partitions, blocks, points int, donors []*pebble.DB, groups int) []copyPartition {
	t.Helper()
	var out []copyPartition
	for pi := 0; pi < partitions; pi++ {
		name := partsformat.ID(fmt.Sprintf("copy.fixture.metric.%04d", pi%64))
		vs := partsformat.Route(name)
		var id partsformat.PartID
		binary.BigEndian.PutUint64(id[:8], uint64(pi+1))
		id[15] = 1
		var group partsformat.Group
		if groups > 0 {
			group = partsformat.Group(fmt.Sprintf("namespace-%d", pi%groups))
		}
		m := partsformat.Manifest{Group: group, VShard: vs, Partition: int64(pi), Policy: sha256.Sum256([]byte("raw-ieee-v1"))}
		path := filepath.Join(root, "part-"+stringID(pi)+".sst")
		f, e := vfs.Default.Create(path)
		if e != nil {
			t.Fatal(e)
		}
		w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{Compression: sstable.NoCompression, TableFormat: sstable.TableFormatPebblev4})
		state := uint64(pi + 1)
		for bi := 0; bi < blocks; bi++ {
			k, e := partsformat.Encode(partsformat.Key{Group: group, VShard: vs, Partition: int64(pi), Metric: name, BlockStart: int64(bi * points), Part: id, Ordinal: uint32(bi)})
			if e != nil {
				t.Fatal(e)
			}
			v := make([]byte, 24*points)
			for p := 0; p < points; p++ {
				state ^= state << 13
				state ^= state >> 7
				state ^= state << 17
				bits := state
				if p == 0 {
					bits = 0x8000000000000000
				}
				if p == 1 {
					bits = 0x7ff80000000000ab
				}
				if p == 2 {
					bits = 1
				}
				if p == 3 {
					bits = 0x7ff0000000000000
				}
				binary.LittleEndian.PutUint64(v[p*24:], uint64(pi*blocks*points+bi*points+p))
				binary.LittleEndian.PutUint64(v[p*24+8:], bits)
				binary.LittleEndian.PutUint64(v[p*24+16:], uint64(bi*points+p))
			}
			if e = w.Set(k, v); e != nil {
				t.Fatal(e)
			}
		}
		if e = w.Close(); e != nil {
			t.Fatal(e)
		}
		fd, e := copyDescribe(path, id)
		if e != nil {
			t.Fatal(e)
		}
		m.Parts = []partsformat.Part{fd.Part}
		if len(donors) > 0 {
			link := path + ".ingest"
			if e = os.Link(path, link); e != nil {
				t.Fatal(e)
			}
			if e = donors[int(vs)%len(donors)].Ingest([]string{link}); e != nil {
				t.Fatal(e)
			}
		}
		out = append(out, copyPartition{Manifest: m, Files: []copyFile{fd}})
	}
	return out
}
func stringID(i int) string {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(i))
	const hex = "0123456789abcdef"
	out := make([]byte, 16)
	for j, v := range b {
		out[j*2] = hex[v>>4]
		out[j*2+1] = hex[v&15]
	}
	return string(out)
}

func copyTestServer(t *testing.T, source []copyPartition) (string, func()) {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	done := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			c, e := l.Accept()
			if e != nil {
				return
			}
			wg.Add(1)
			go func() { defer wg.Done(); _ = copyServeConn(c, source) }()
		}
	}()
	return l.Addr().String(), func() {
		_ = l.Close()
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("server connections leaked")
		}
	}
}
func copyDBDigest(t *testing.T, db *pebble.DB) [32]byte {
	t.Helper()
	it, e := db.NewIter(nil)
	if e != nil {
		t.Fatal(e)
	}
	defer it.Close()
	h := sha256.New()
	for ok := it.First(); ok; ok = it.Next() {
		v, e := it.ValueAndErr()
		if e != nil {
			t.Fatal(e)
		}
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(it.Key())))
		h.Write(n[:])
		h.Write(it.Key())
		binary.BigEndian.PutUint64(n[:], uint64(len(v)))
		h.Write(n[:])
		h.Write(v)
	}
	if e = it.Error(); e != nil {
		t.Fatal(e)
	}
	var d [32]byte
	copy(d[:], h.Sum(nil))
	return d
}

func TestCopyRangeRepairResizeAndCatchup(t *testing.T) {
	root := t.TempDir()
	donor := copyTestDB(t, filepath.Join(root, "donor"))
	defer donor.Close()
	target := copyTestDB(t, filepath.Join(root, "target"))
	defer target.Close()
	source := copyFixture(t, root, 8, 3, 16, []*pebble.DB{donor})
	addr, stop := copyTestServer(t, source)
	defer stop()
	plan, e := copyPlan(source, nil)
	if e != nil {
		t.Fatal(e)
	}
	stats, e := copyTransfer(context.Background(), addr, source, plan, 4, []*pebble.DB{target}, root)
	if e != nil {
		t.Fatal(e)
	}
	if stats.Bytes == 0 {
		t.Fatal("empty transfer")
	}
	if copyDBDigest(t, donor) != copyDBDigest(t, target) {
		t.Fatal("resized target is not bit exact")
	}
	var manifests []partsformat.Manifest
	for _, p := range source {
		manifests = append(manifests, p.Manifest)
	}
	plan, e = copyPlan(source, manifests)
	if e != nil || len(plan) != 0 {
		t.Fatalf("equal inventories %v %v", plan, e)
	}
	wire, wireBytes, e := copyCompare(context.Background(), addr, manifests)
	if e != nil || len(wire) != 0 || wireBytes != uint64(20+54*len(manifests)) {
		t.Fatalf("wire equal plan %v bytes%d err%v", wire, wireBytes, e)
	}
	// Remove a partition and add target-only stale data inside the same range.
	lo, _ := partsformat.PartitionPrefix(source[2].Manifest.VShard, 2)
	if e = target.DeleteRange(lo, partsformat.PrefixEnd(lo), pebble.Sync); e != nil {
		t.Fatal(e)
	}
	extra := append(bytes.Clone(lo), []byte("stale-target-only-key")...)
	if e = target.Set(extra, []byte("stale"), pebble.Sync); e != nil {
		t.Fatal(e)
	}
	manifests[2].Parts = nil
	plan, e = copyPlan(source, manifests)
	if e != nil || len(plan) != 1 || plan[0] != 2 {
		t.Fatalf("hole plan %v %v", plan, e)
	}
	_, e = copyTransfer(context.Background(), addr, source, plan, 2, []*pebble.DB{target}, root)
	if e != nil {
		t.Fatal(e)
	}
	if copyDBDigest(t, donor) != copyDBDigest(t, target) {
		t.Fatal("hole repair retained stale data or changed value bits")
	}
	// New snapshot window: export donor snapshot including a late correction.
	k, e := partsformat.Encode(partsformat.Key{VShard: source[2].Manifest.VShard, Partition: 2, Metric: "copy.fixture.metric.0002", BlockStart: 99999, Part: partsformat.PartID{9}})
	if e != nil {
		t.Fatal(e)
	}
	if e = donor.Set(k, []byte("late-log-record-exact-bits"), pebble.Sync); e != nil {
		t.Fatal(e)
	}
	snap := donor.NewSnapshot()
	p, e := copyExport(snap, root, source[2].Manifest, partsformat.PartID{8})
	snap.Close()
	if e != nil {
		t.Fatal(e)
	}
	catchup := []copyPartition{p}
	a2, s2 := copyTestServer(t, catchup)
	defer s2()
	_, e = copyTransfer(context.Background(), a2, catchup, []int{0}, 1, []*pebble.DB{target}, root)
	if e != nil {
		t.Fatal(e)
	}
	if copyDBDigest(t, donor) != copyDBDigest(t, target) {
		t.Fatal("snapshot catch-up differs")
	}
}

func TestCopyRejectsCrossPartitionAndRepairsEmpty(t *testing.T) {
	root := t.TempDir()
	target := copyTestDB(t, filepath.Join(root, "target"))
	defer target.Close()
	source := copyFixture(t, root, 1, 1, 4, nil)
	wrong := source[0].Manifest
	wrong.Partition++
	if e := copyValidateFile(wrong, source[0].Files[0].Path); e == nil {
		t.Fatal("cross partition SST accepted")
	}
	lo, _ := partsformat.PartitionPrefix(wrong.VShard, wrong.Partition)
	if e := target.Set(append(bytes.Clone(lo), byte('x')), []byte("stale"), pebble.Sync); e != nil {
		t.Fatal(e)
	}
	empty := []copyPartition{{Manifest: partsformat.Manifest{VShard: wrong.VShard, Partition: wrong.Partition}}}
	addr, stop := copyTestServer(t, empty)
	defer stop()
	_, e := copyTransfer(context.Background(), addr, empty, []int{0}, 1, []*pebble.DB{target}, root)
	if e != nil {
		t.Fatal(e)
	}
	it, e := target.NewIter(nil)
	if e != nil {
		t.Fatal(e)
	}
	defer it.Close()
	if it.First() {
		t.Fatal("empty donor did not remove stale partition")
	}
}

func TestCopyMultipleDisjointParts(t *testing.T) {
	root := t.TempDir()
	target := copyTestDB(t, filepath.Join(root, "target"))
	defer target.Close()
	source := copyFixture(t, root, 1, 1, 4, nil)
	first := source[0]
	var id partsformat.PartID
	id[0] = 255
	path := filepath.Join(root, "second.sst")
	f, e := vfs.Default.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{TableFormat: sstable.TableFormatPebblev4})
	k, e := partsformat.Encode(partsformat.Key{VShard: first.Manifest.VShard, Partition: 0, Part: id, Metric: "copy.fixture.metric.0000"})
	if e != nil {
		t.Fatal(e)
	}
	if e = w.Set(k, []byte("second part")); e != nil {
		t.Fatal(e)
	}
	if e = w.Close(); e != nil {
		t.Fatal(e)
	}
	fd, e := copyDescribe(path, id)
	if e != nil {
		t.Fatal(e)
	}
	source[0].Files = append(source[0].Files, fd)
	source[0].Manifest.Parts = append(source[0].Manifest.Parts, fd.Part)
	addr, stop := copyTestServer(t, source)
	defer stop()
	_, e = copyTransfer(context.Background(), addr, source, []int{0}, 2, []*pebble.DB{target}, root)
	if e != nil {
		t.Fatal(e)
	}
	v, closer, e := target.Get(k)
	if e != nil {
		t.Fatal(e)
	}
	defer closer.Close()
	if string(v) != "second part" {
		t.Fatal("lost second part")
	}
}

func TestCopyCorruptionAndCancellation(t *testing.T) {
	root := t.TempDir()
	target := copyTestDB(t, filepath.Join(root, "target"))
	defer target.Close()
	source := copyFixture(t, root, 1, 1, 16, nil)
	before := copyDBDigest(t, target)
	f, e := os.OpenFile(source[0].Files[0].Path, os.O_WRONLY, 0)
	if e != nil {
		t.Fatal(e)
	}
	_, e = f.WriteAt([]byte{0xff}, 1)
	if e != nil {
		t.Fatal(e)
	}
	f.Close()
	addr, stop := copyTestServer(t, source)
	defer stop()
	_, e = copyTransfer(context.Background(), addr, source, []int{0}, 1, []*pebble.DB{target}, root)
	if e == nil {
		t.Fatal("corrupted SST accepted")
	}
	if before != copyDBDigest(t, target) {
		t.Fatal("failed transfer published")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, e = copyTransfer(ctx, addr, source, []int{0}, 1, []*pebble.DB{target}, root)
	if !errors.Is(e, context.Canceled) {
		t.Fatalf("cancel: %v", e)
	}
	files, _ := filepath.Glob(filepath.Join(root, "receive-*"))
	if len(files) != 0 {
		t.Fatal("staging files leaked")
	}
}

func TestCopySenderRejectsRemoteIndex(t *testing.T) {
	a, b := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- copyServeConn(a, nil) }()
	_, e := b.Write(make([]byte, 8))
	if e != nil {
		t.Fatal(e)
	}
	var v [1]byte
	_, e = io.ReadFull(b, v[:])
	if !errors.Is(e, io.EOF) {
		t.Fatal(e)
	}
	b.Close()
	if <-done == nil {
		t.Fatal("bad index accepted")
	}
}

func TestCopyNamespaceMoveAndSourceRetirement(t *testing.T) {
	root := t.TempDir()
	donor := copyTestDB(t, filepath.Join(root, "donor"))
	defer donor.Close()
	target := copyTestDB(t, filepath.Join(root, "target"))
	defer target.Close()
	source := copyFixtureGrouped(t, root, 8, 2, 16, []*pebble.DB{donor}, 2)
	plan, e := copyPlan(source, nil)
	if e != nil {
		t.Fatal(e)
	}
	plan = copySelectGroup(source, plan, "namespace-1")
	if len(plan) != 4 {
		t.Fatal("group selection", plan)
	}
	addr, stop := copyTestServer(t, source)
	defer stop()
	_, e = copyTransfer(context.Background(), addr, source, plan, 4, []*pebble.DB{target}, root)
	if e != nil {
		t.Fatal(e)
	}
	lo, hi, e := partsformat.GroupRange("namespace-1")
	if e != nil {
		t.Fatal(e)
	}
	if copyRangeDigest(t, donor, lo, hi) != copyRangeDigest(t, target, lo, hi) {
		t.Fatal("namespace target differs")
	}
	// Simulate the overlap window with an additional source-side log record;
	// the second pinned snapshot is compared and repaired before retirement.
	m := source[1].Manifest
	k, e := partsformat.Encode(partsformat.Key{Group: m.Group, VShard: m.VShard, Partition: m.Partition, Metric: "copy.fixture.metric.0001", Part: partsformat.PartID{254}, BlockStart: 999})
	if e != nil {
		t.Fatal(e)
	}
	if e = donor.Set(k, []byte("overlap-window-record"), pebble.Sync); e != nil {
		t.Fatal(e)
	}
	snap := donor.NewSnapshot()
	p, e := copyExport(snap, root, m, partsformat.PartID{253})
	snap.Close()
	if e != nil {
		t.Fatal(e)
	}
	tail := []copyPartition{p}
	tailAddr, tailStop := copyTestServer(t, tail)
	defer tailStop()
	old := []partsformat.Manifest{m}
	tailPlan, _, e := copyCompare(context.Background(), tailAddr, old)
	if e != nil || len(tailPlan) != 1 {
		t.Fatal("overlap digest", tailPlan, e)
	}
	if _, e = copyTransfer(context.Background(), tailAddr, tail, tailPlan, 2, []*pebble.DB{target}, root); e != nil {
		t.Fatal(e)
	}
	if copyRangeDigest(t, donor, lo, hi) != copyRangeDigest(t, target, lo, hi) {
		t.Fatal("overlap repair differs")
	}
	otherLo, otherHi, _ := partsformat.GroupRange("namespace-0")
	before := copyRangeDigest(t, donor, otherLo, otherHi)
	// This private-test range deletion models the final, explicitly fenced
	// retirement step. The transport itself never initiates source deletion.
	if e = donor.DeleteRange(lo, hi, pebble.Sync); e != nil {
		t.Fatal(e)
	}
	if before != copyRangeDigest(t, donor, otherLo, otherHi) {
		t.Fatal("retirement changed unrelated namespace")
	}
	it, e := donor.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if e != nil {
		t.Fatal(e)
	}
	defer it.Close()
	if it.First() {
		t.Fatal("retired namespace remains")
	}
}
func copyRangeDigest(t *testing.T, db *pebble.DB, lo, hi []byte) [32]byte {
	t.Helper()
	it, e := db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if e != nil {
		t.Fatal(e)
	}
	defer it.Close()
	h := sha256.New()
	for ok := it.First(); ok; ok = it.Next() {
		v, e := it.ValueAndErr()
		if e != nil {
			t.Fatal(e)
		}
		h.Write(it.Key())
		h.Write(v)
	}
	if e = it.Error(); e != nil {
		t.Fatal(e)
	}
	var d [32]byte
	copy(d[:], h.Sum(nil))
	return d
}
