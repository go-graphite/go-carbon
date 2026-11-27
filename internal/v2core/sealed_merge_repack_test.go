package v2core

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/sstable"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// forEachPartKV calls fn with every logical key and value of a part file in
// key order: a folded envelope follows its run at its own key.
func forEachPartKV(t *testing.T, path string, fn func(key, value []byte)) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	readable, err := sstable.NewSimpleReadable(f)
	if err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	reader, err := sstable.NewReader(readable, sstable.ReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var iterated uint64
	var buffers sstable.BufferPool
	buffers.Init(4)
	defer buffers.Release()
	it, err := reader.NewCompactionIter(&iterated, sstable.TrivialReaderProvider{Reader: reader}, &buffers)
	if err != nil {
		t.Fatal(err)
	}
	c := newHeaderCursor(&tableCursor{it: it}, nil)
	defer c.Close()
	for ok := c.First(); ok; ok = c.Next() {
		fn(c.Key(), c.Value())
	}
	if err = c.Error(); err != nil {
		t.Fatal(err)
	}
}

// writeDecodedPart writes a merged part's content independent of its run
// layout and envelope placement: every slot as its singleton key and record,
// every envelope at its own key, and the descriptor without file bytes, digest
// or KV count, with its logical last key (a folded part's physical last key is
// its last run, before that run's envelope).
func writeDecodedPart(t *testing.T, w io.Writer, path string, d v2part.Descriptor) {
	t.Helper()
	var last []byte
	forEachPartKV(t, path, func(key, value []byte) {
		last = append(last[:0], key...)
		k, err := v2part.Decode(key)
		if err != nil {
			t.Fatal(err)
		}
		if k.Archive == v2part.MetadataArchive {
			_, _ = fmt.Fprintf(w, "%x=%x\n", key, value)
			return
		}
		records, err := v2part.DecodeSlots(k.Archive, k.BlockStart, value)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range records {
			k.BlockStart = r.Slot
			enc, err := v2part.Encode(k)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = fmt.Fprintf(w, "%x=%x\n", enc, sealrollup.EncodeRecord(r))
		}
	})
	d.Bytes, d.SHA256, d.Coverage.Records, d.LastKey = 0, v2part.Digest{}, 0, last
	if d.DictGeneration != 0 {
		t.Fatal("decoded part digests cover name-keyed parts")
	}

	// Keep the original descriptor surface of the decoded-content golden;
	// admission coverage is checked separately by TestMergeAdmissionBound.
	c := d.Coverage
	_, _ = fmt.Fprint(w, withoutSeq(fmt.Sprintf("{Version:%d KeyVersion:%d Scope:%+v Encoding:%s Source:%+v Bytes:%d SHA256:%v FirstKey:%v LastKey:%v Coverage:{MinTime:%d MaxTime:%d Names:%d Records:%d Policies:%+v MinExpiry:%d MaxExpiry:%d} VShards:%v}\n",
		d.Version, d.KeyVersion, d.Scope, d.Encoding, d.Source, d.Bytes, d.SHA256, d.FirstKey, d.LastKey,
		c.MinTime, c.MaxTime, c.Names, c.Records, c.Policies, c.MinExpiry, c.MaxExpiry, d.VShards)))

}

// partRunLengths lists the slot count of every dense run in a part, per name
// and archive in key order.
func partRunLengths(t *testing.T, path string) map[string][]int {
	t.Helper()
	runs := map[string][]int{}
	forEachPartKV(t, path, func(key, value []byte) {
		k, err := v2part.Decode(key)
		if err != nil {
			t.Fatal(err)
		}
		if k.Archive == v2part.MetadataArchive {
			return
		}
		records, err := v2part.DecodeSlots(k.Archive, k.BlockStart, value)
		if err != nil {
			t.Fatal(err)
		}
		id := fmt.Sprintf("%s/%d", k.Metric, k.Archive)
		runs[id] = append(runs[id], len(records))
	})
	return runs
}

// Merges repack adjacent non-overlapping runs of one name and archive into
// runs of up to MaxDenseSlots, keep every slot exact, and copy runs that are
// already full. Before repacking, every 1200-slot input run stayed a run.
func TestSealedMergeRepacksRunsToCap(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	e := mergeFixture(t, t.TempDir(), nil)
	defer e.close()
	policy, raw := rollupTestPolicy(t, "merge.repack", "1s:4h,1m:1d", sealrollup.Last, .5)
	digest, err := sealrollup.PolicyDigest(policy.Config)
	if err != nil {
		t.Fatal(err)
	}
	const perInput = 400 // total must exceed one dense run (4096 slots) and fit the 4h archive
	for n := 1; n <= sealedMergeFanIn; n++ {
		var rs []sealrollup.Record
		for i := 0; i < perInput; i++ {
			slot := int64((n-1)*perInput + i)
			order := uint64(n)
			bits := uint64(n*10000 + i)
			if n == 3 {
				bits = 0x7ff8000000000042 // one float run inside integer runs
			}
			rs = append(rs, sealrollup.Record{Delta: sealrollup.Delta{Archive: 0, Slot: slot, Order: order, Sample: sealrollup.Sample{Time: 1000 + slot, Bits: bits, Sequence: order}}, Correction: i%7 == 0})
		}
		rs = append(rs, sealrollup.Record{Delta: sealrollup.Delta{Archive: 1, Slot: int64(n), Order: uint64(n), Sample: sealrollup.Sample{Time: 1000 + int64(n)*60, Bits: uint64(n), Sequence: uint64(n)}}})
		m := RollupMetadata{Version: 2, ID: 7, Policy: raw, Frontier: sealrollup.Frontier{Order: uint64(n), MaxTime: 1000 + int64(sealedMergeFanIn*perInput), Policy: digest}, Revision: uint64(n)}
		installMergeRecords(t, e.owners[0], byte(n), []RollupMetadata{m}, [][]sealrollup.Record{rs})
	}
	s := e.owners[0].db.NewSnapshot()
	want := mergeRead(t, s)
	_ = s.Close()
	if n := mergeDrain(t, e.owners[0]); n != 1 {
		t.Fatalf("merges=%d", n)
	}
	s = e.owners[0].db.NewSnapshot()
	defer s.Close()
	if got := mergeRead(t, s); !reflect.DeepEqual(got, want) {
		t.Fatal("repacking changed exact winners")
	}
	e.owners[0].gc.mu.Lock()
	inventory := e.owners[0].gc.inventory
	e.owners[0].gc.mu.Unlock()
	if len(inventory.bindings) != 1 {
		t.Fatalf("parts=%d", len(inventory.bindings))
	}
	for _, value := range inventory.bindings {
		d, err := v2part.ParseDescriptor(value)
		if err != nil {
			t.Fatal(err)
		}
		total := sealedMergeFanIn * perInput
		runs := partRunLengths(t, gcPath(e.owners[0].gc, d))
		if got, want := runs["merge.repack/0"], []int{v2part.MaxDenseSlots, total - v2part.MaxDenseSlots}; !slices.Equal(got, want) {
			t.Fatalf("archive 0 runs %v, want %v", got, want)
		}
		if got := runs["merge.repack/1"]; !slices.Equal(got, []int{sealedMergeFanIn}) {
			t.Fatalf("archive 1 runs %v", got)
		}
		// A second merge copies the full run and keeps the layout.
		path := gcPath(e.owners[0].gc, d) + ".again"
		again, err := mergePartFiles(t.Context(), e.owners[0].dir, path, []v2part.Descriptor{d}, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		if again.Coverage.Records != d.Coverage.Records || !reflect.DeepEqual(partRunLengths(t, path), runs) {
			t.Fatalf("remerge changed runs: %v", partRunLengths(t, path))
		}
	}
}

// A full run follows a partial pending run of its name and archive: it is
// re-encoded into the pending run instead of leaving a short run behind.
func TestDenseWriterAppendSlotsCoalesces(t *testing.T) {
	dir := t.TempDir()
	key := v2part.Key{Group: "g", VShard: v2part.Route("m"), Part: v2part.PartID{1}, Metric: "m"}
	spec := v2part.Spec{Scope: v2part.Scope{Group: "g", VShard: v2part.GroupVShard, Part: key.Part}, Source: v2part.Provenance{Stream: "t", Incarnation: v2part.PartID{2}, MinOrder: 1, MaxOrder: 1}, MinTime: 1, MaxTime: 1 << 20,
		Policies: []v2part.ArchivePolicy{{Archive: 0, Digest: sha256.Sum256([]byte{1}), Definition: []byte{1}}}}
	w, err := v2part.NewDenseWriter(dir+"/p.sst", spec)
	if err != nil {
		t.Fatal(err)
	}
	run := func(first, n int) ([]byte, []byte, []sealrollup.Record) {
		var rs []sealrollup.Record
		for i := 0; i < n; i++ {
			s := int64(first + i)
			rs = append(rs, sealrollup.Record{Delta: sealrollup.Delta{Slot: s, Order: 1, Sample: sealrollup.Sample{Time: 1 + s, Bits: uint64(s), Sequence: 1}}})
		}
		k := key
		k.BlockStart = int64(first)
		enc, err := v2part.Encode(k)
		if err != nil {
			t.Fatal(err)
		}
		value, err := v2part.EncodeSlots(rs)
		if err != nil {
			t.Fatal(err)
		}
		return enc, value, rs
	}
	for _, r := range [][2]int{{0, 100}, {100, v2part.MaxDenseSlots}, {100 + v2part.MaxDenseSlots + 10, v2part.MaxDenseSlots}} {
		if err = w.AppendSlots(run(r[0], r[1])); err != nil {
			t.Fatal(err)
		}
	}
	key.Archive, key.BlockStart = v2part.MetadataArchive, 0
	enc, _ := v2part.Encode(key)
	if err = w.SetEncoded(enc, []byte{1}); err != nil {
		t.Fatal(err)
	}
	d, err := w.Close()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := partRunLengths(t, dir+"/p.sst")["m/0"], []int{v2part.MaxDenseSlots, v2part.MaxDenseSlots, 100}; !slices.Equal(got, want) {
		t.Fatalf("runs %v, want %v (records %d)", got, want, d.Coverage.Records)
	}
}

// withoutSeq drops a zero Provenance.Seq from a %+v dump, so digests recorded
// before the field existed still cover every other field byte for byte.
func withoutSeq(s string) string { return strings.Replace(s, " Seq:0}", "}", 1) }
