package partsformat

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"slices"
	"testing"
)

func TestPortableIdentityAndVShardRanges(t *testing.T) {
	ids := []ID{"sample;a=1;b=2", "sample;a=1;b=3", ID("sample\x00binary"), ID("\xffname")}
	var keys [][]byte
	for _, id := range ids {
		for _, partition := range []int64{-2, -1, 0, 1, 1 << 40} {
			k := Key{VShard: Route(id), Partition: partition, Metric: id, Archive: 60, BlockStart: -123, Part: PartID{7}, Ordinal: 9}
			a, err := Encode(k)
			if err != nil {
				t.Fatal(err)
			}
			b, err := Encode(k)
			if err != nil || !bytes.Equal(a, b) {
				t.Fatal("replica encoding differs")
			}
			got, err := Decode(a)
			if err != nil || got != k {
				t.Fatalf("roundtrip %v %v", got, err)
			}
			lo, hi, err := VShardRange(k.VShard)
			if err != nil || bytes.Compare(a, lo) < 0 || bytes.Compare(a, hi) >= 0 {
				t.Fatal("outside virtual shard")
			}
			if err = ValidateFileRange(Manifest{VShard: k.VShard, Partition: partition}, a, a); err != nil {
				t.Fatal(err)
			}
			keys = append(keys, a)
		}
	}
	slices.SortFunc(keys, bytes.Compare)
	for _, id := range ids {
		var previous int64 = -1 << 63
		for _, b := range keys {
			k, _ := Decode(b)
			if k.Metric != id {
				continue
			}
			if k.Partition < previous {
				t.Fatal("partition order changed")
			}
			previous = k.Partition
		}
	}
}
func TestManifestInventoryDigest(t *testing.T) {
	a := Part{ID: PartID{1}, Bytes: 123, SHA256: sha256.Sum256([]byte("a"))}
	b := Part{ID: PartID{2}, Bytes: 456, SHA256: sha256.Sum256([]byte("b"))}
	m := Manifest{VShard: 5, Partition: -1, Parts: []Part{a, b}}
	one, err := m.Digest()
	if err != nil {
		t.Fatal(err)
	}
	m.Parts = []Part{b, a}
	two, err := m.Digest()
	if err != nil || one != two {
		t.Fatal("inventory ordering changed digest")
	}
	m.Parts[0].Bytes++
	three, _ := m.Digest()
	if three == one {
		t.Fatal("size omitted")
	}
	m.Parts = []Part{a, a}
	if _, err = m.Digest(); err == nil {
		t.Fatal("duplicate ID accepted")
	}
	m.Parts = []Part{a, b}
	m.Policy[0] = 1
	four, _ := m.Digest()
	if four == one {
		t.Fatal("policy omitted")
	}
}
func TestRejectCrossPartitionSSTAndInvalidKeys(t *testing.T) {
	id := ID("metric")
	k := Key{VShard: Route(id), Partition: 1, Metric: id}
	a, err := Encode(k)
	if err != nil {
		t.Fatal(err)
	}
	k.Partition = 2
	b, _ := Encode(k)
	if err = ValidateFileRange(Manifest{VShard: k.VShard, Partition: 1}, a, b); err == nil {
		t.Fatal("cross-partition SST accepted")
	}
	for n := 0; n < len(a); n++ {
		if _, err = Decode(a[:n]); err == nil {
			t.Fatalf("truncated%d accepted", n)
		}
	}
	k.VShard = (k.VShard + 1) % VirtualShards
	if _, err = Encode(k); err == nil {
		t.Fatal("wrong routing accepted")
	}
}

func TestImmutablePartRangesDoNotOverlap(t *testing.T) {
	// Find many metric IDs in the same vshard. Every key of the first part
	// must precede every key of the next, independent of metric sort order.
	var ids []ID
	for i := 0; len(ids) < 30; i++ {
		id := ID(fmt.Sprintf("sample.%d", i))
		if Route(id) == 0 {
			ids = append(ids, id)
		}
	}
	var previousLargest []byte
	for part := byte(1); part < 6; part++ {
		var keys [][]byte
		for _, id := range ids {
			k, err := Encode(Key{VShard: 0, Partition: 7, Metric: id, Part: PartID{part}})
			if err != nil {
				t.Fatal(err)
			}
			keys = append(keys, k)
		}
		slices.SortFunc(keys, bytes.Compare)
		if previousLargest != nil && bytes.Compare(previousLargest, keys[0]) >= 0 {
			t.Fatal("immutable SST ranges overlap")
		}
		previousLargest = keys[len(keys)-1]
	}
}
