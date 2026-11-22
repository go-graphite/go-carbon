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

func TestPlacementGroupsBoundNamespaceCopies(t *testing.T) {
	rules := []PlacementRule{{"infra", "platform"}, {"infra.redis", "databases"}}
	for _, tc := range []struct {
		name ID
		want Group
	}{
		{"containers.cpu;host=a", "containers"}, {"standalone;tag=with.dot", "standalone"},
		{"infra.redis.reads", "databases"}, {"infra.redis;host=a", "databases"},
		{"infra.rediscover.ops", "platform"},
	} {
		group, err := AssignGroup(tc.name, rules)
		if err != nil || group != tc.want {
			t.Fatalf("placement %q=%q,%v", tc.name, group, err)
		}
		k := Key{Group: group, VShard: Route(tc.name), Partition: -1, Metric: tc.name, Part: PartID{1}}
		encoded, err := Encode(k)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := Decode(encoded)
		if err != nil || decoded != k {
			t.Fatalf("placement roundtrip %v %v", decoded, err)
		}
		lo, hi, err := GroupRange(group)
		if err != nil || bytes.Compare(encoded, lo) < 0 || bytes.Compare(encoded, hi) >= 0 {
			t.Fatal("group copy range excludes member")
		}
		lo, hi, err = VShardRangeForGroup(group, k.VShard)
		if err != nil || bytes.Compare(encoded, lo) < 0 || bytes.Compare(encoded, hi) >= 0 {
			t.Fatal("resize range excludes member")
		}
		if err = ValidateFileRange(Manifest{Group: group, VShard: k.VShard, Partition: -1}, encoded, encoded); err != nil {
			t.Fatal(err)
		}
		other := k
		other.Group += "-other"
		foreign, _ := Encode(other)
		if bytes.Compare(foreign, lo) >= 0 && bytes.Compare(foreign, hi) < 0 {
			t.Fatal("placement group aliases another group")
		}
		if err = ValidateFileRange(Manifest{Group: other.Group, VShard: k.VShard, Partition: -1}, encoded, encoded); err == nil {
			t.Fatal("foreign group accepted")
		}
		m := Manifest{Group: group, VShard: k.VShard, Partition: -1}
		a, _ := m.Digest()
		m.Group = other.Group
		b, _ := m.Digest()
		if a == b {
			t.Fatal("placement omitted from inventory digest")
		}
		for n := 0; n < len(encoded); n++ {
			if _, err = Decode(encoded[:n]); err == nil {
				t.Fatalf("truncated placement key%d accepted", n)
			}
		}
	}
	if _, err := AssignGroup("infra.x", []PlacementRule{{"infra", "a"}, {"infra", "b"}}); err == nil {
		t.Fatal("ambiguous assignment accepted")
	}
	if _, err := AssignGroup(";x=y", nil); err == nil {
		t.Fatal("empty base accepted")
	}
}
