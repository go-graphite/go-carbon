package v2core

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// A small seal suffix must not force the next normal publication to rebuild
// its codec workspace. Two small publications must still release burst memory.
func TestL0PublicationScratchReuse(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "l0"), 0700); err != nil {
		t.Fatal(err)
	}
	o := &coreOwner{dir: dir, l0Next: 1}
	points := make([]row, 32768)
	for i := range points {
		points[i] = row{ID: 7, Time: 1700000000, Bits: uint64(i), Seq: 1}
	}
	large := PendingBatches([]Batch{{ID: 7, Order: 1, Clock: 1700000000, Points: points}})
	small := PendingBatches([]Batch{{ID: 7, Order: 2, Clock: 1700000001, Points: points[:1]}})
	publish := func(v PendingView) {
		t.Helper()
		path, err := o.writeL0(v)
		if err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, encodeL0(nil, v)) {
			t.Fatal("scratch reuse changed published bytes")
		}
	}
	publish(large)
	workspace := &o.l0Buf[:cap(o.l0Buf)][0]
	for range 3 {
		publish(small)
		publish(large)
		if len(o.l0Buf) == 0 || &o.l0Buf[0] != workspace {
			t.Fatal("alternating publication sizes rebuilt L0 scratch")
		}
	}
	publish(small)
	publish(small)
	if cap(o.l0Buf) > 64<<10 {
		t.Fatal("two small publications retained burst scratch")
	}
	publish(large)
}
