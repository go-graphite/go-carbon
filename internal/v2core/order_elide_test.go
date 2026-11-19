package v2core

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// TestMergeOrderElideLateWrite writes two seals, merges them with the order
// column elided, writes a late duplicate of two old slots in a newer part and
// merges again. The newest write wins and every record (slot, time, value
// bits, correction) and envelope matches the classic per-slot-order merge.
func TestMergeOrderElideLateWrite(t *testing.T) {
	type out struct {
		lines []string
		bytes uint64
		one   int // runs written with one header order
	}
	run := func(elide bool) out {
		dir := t.TempDir()
		if err := os.Mkdir(filepath.Join(dir, "parts"), 0700); err != nil {
			t.Fatal(err)
		}
		config := sealrollup.Config{Method: sealrollup.Last, XFilesFactor: .5, Archives: []sealrollup.Archive{{Step: 1, Count: 600}}}
		digest, err := sealrollup.PolicyDigest(config)
		if err != nil {
			t.Fatal(err)
		}
		names := []v2part.ID{"a.b.c", "a.b.d", "x.yy"}
		write := func(i int, order uint64, slots []int64, value float64) v2part.Descriptor {
			var rows []KV
			for n, name := range names {
				policy, e := json.Marshal(RollupPolicy{Name: name, Group: "g", Config: config})
				if e != nil {
					t.Fatal(e)
				}
				k := v2part.Key{Group: "g", VShard: v2part.Route(name), Part: v2part.PartID{byte(i + 1)}, Metric: name}
				for _, s := range slots {
					k.Archive, k.BlockStart = 0, s
					enc, e := v2part.Encode(k)
					if e != nil {
						t.Fatal(e)
					}
					r := sealrollup.Record{Delta: sealrollup.Delta{Slot: s, Order: order, Sample: sealrollup.Sample{Time: 100000 + s, Bits: math.Float64bits(value + float64(n))}}}
					rows = append(rows, KV{Key: enc, Value: sealrollup.EncodeRecord(r)})
				}
				md := RollupMetadata{Version: 2, ID: uint64(n + 1), Policy: policy, Frontier: sealrollup.Frontier{Order: order, MaxTime: 100000 + slices.Max(slots), Policy: digest}, LastUpdate: int64(order) * 1e9, Revision: order}
				k.Archive, k.BlockStart = v2part.MetadataArchive, 0
				enc, e := v2part.Encode(k)
				if e != nil {
					t.Fatal(e)
				}
				rows = append(rows, KV{Key: enc, Value: AppendRollupMetadata(nil, md)})
			}
			slices.SortFunc(rows, func(a, b KV) int { return bytes.Compare(a.Key, b.Key) })
			path := filepath.Join(dir, fmt.Sprintf("in-%d.sst", i))
			d, e := WriteDenseRollupPart(path, rows, v2part.Provenance{Stream: "elide", Incarnation: v2part.PartID{9}, MinOrder: order, MaxOrder: order})
			if e != nil {
				t.Fatal(e)
			}
			if e = os.Rename(path, filepath.Join(dir, "parts", hex.EncodeToString(d.SHA256[:])+".sst")); e != nil {
				t.Fatal(e)
			}
			return d
		}
		var live []v2part.Descriptor
		merge := func(name string, inputs []v2part.Descriptor) v2part.Descriptor {
			if !orderRangeAlone(live, inputs) {
				t.Fatalf("%s: inputs not alone in their order range", name)
			}
			path := filepath.Join(dir, name+".sst")
			d, e := mergePartFiles(context.Background(), dir, path, inputs, 0, elide, nil)
			if e != nil {
				t.Fatalf("%s: %v", name, e)
			}
			if e = os.Rename(path, filepath.Join(dir, "parts", hex.EncodeToString(d.SHA256[:])+".sst")); e != nil {
				t.Fatal(e)
			}
			live = slices.DeleteFunc(live, func(x v2part.Descriptor) bool {
				return slices.ContainsFunc(inputs, func(in v2part.Descriptor) bool { return in.SHA256 == x.SHA256 })
			})
			return d
		}
		var seq []int64
		for s := int64(0); s < 40; s++ {
			seq = append(seq, s)
		}
		s0 := write(0, 1, seq[:20], 1)
		s1 := write(1, 2, seq[20:], 2)
		live = []v2part.Descriptor{s0, s1}
		m1 := merge("m1", []v2part.Descriptor{s0, s1})
		// Late duplicates of old slots 5 and 30: same timestamps, newer order.
		late := write(2, 3, []int64{5, 30}, 9)
		live = []v2part.Descriptor{m1, late}
		m2 := merge("m2", []v2part.Descriptor{m1, late})
		var o out
		o.bytes = m1.Bytes + m2.Bytes
		for _, d := range []v2part.Descriptor{m1, m2} {
			last := d.Scope.Part == m2.Scope.Part
			err = v2part.Read(context.Background(), filepath.Join(dir, "parts", hex.EncodeToString(d.SHA256[:])+".sst"), &d, func(k v2part.Key, b []byte) error {
				if k.Archive == v2part.MetadataArchive {
					if last {
						o.lines = append(o.lines, fmt.Sprintf("%s meta %x", k.Metric, b))
					}
					return nil
				}
				if b[4]&2 != 0 {
					o.one++
				}
				rs, e := v2part.DecodeSlots(k.Archive, k.BlockStart, b)
				for _, r := range rs {
					if r.Order > d.Source.MaxOrder || r.Order < d.Source.MinOrder {
						t.Fatalf("order %d outside part range %+v", r.Order, d.Source)
					}
					if last {
						o.lines = append(o.lines, fmt.Sprintf("%s %d %d %d %x %v", k.Metric, r.Archive, r.Slot, r.Sample.Time, r.Sample.Bits, r.Correction))
					}
				}
				return e
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		return o
	}
	classic, elided := run(false), run(true)
	if !slices.Equal(classic.lines, elided.lines) {
		t.Fatalf("elided merge differs from classic:\nclassic %q\nelided  %q", classic.lines, elided.lines)
	}
	for _, want := range []string{"a.b.c 0 5 100005 4022000000000000 false", "x.yy 0 30 100030 4026000000000000 false"} {
		if !slices.Contains(elided.lines, want) {
			t.Fatalf("late write lost: no %q in %q", want, elided.lines)
		}
	}
	if classic.one != 0 || elided.one == 0 || elided.bytes >= classic.bytes {
		t.Fatalf("order column not elided: classic %d one-order runs %d B, elided %d runs %d B", classic.one, classic.bytes, elided.one, elided.bytes)
	}
}

// TestOrderRangeAlone pins when a merge may elide orders: no other live part
// of the same source overlaps the inputs' joint order range, in any partition
// of the group (a reset masks records in every partition).
func TestOrderRangeAlone(t *testing.T) {
	part := func(id byte, group v2part.Group, partition int64, stream string, lo, hi uint64) v2part.Descriptor {
		var d v2part.Descriptor
		d.Scope.Group, d.Scope.Partition = group, partition
		d.Source = v2part.Provenance{Incarnation: v2part.PartID{1}, Stream: stream, MinOrder: lo, MaxOrder: hi}
		d.SHA256 = v2part.Digest{id}
		return d
	}
	inputs := []v2part.Descriptor{part(1, "g", 0, "s", 10, 20), part(2, "g", 0, "s", 21, 30)}
	for _, c := range []struct {
		name  string
		other v2part.Descriptor
		want  bool
	}{
		{"disjoint above", part(3, "g", 0, "s", 31, 40), true},
		{"other source", part(3, "g", 0, "t", 15, 25), true},
		{"other group", part(3, "h", 0, "s", 15, 25), true},
		{"inside", part(3, "g", 0, "s", 15, 16), false},
		{"touches max", part(3, "g", 0, "s", 30, 31), false},
		{"touches min", part(3, "g", 0, "s", 1, 10), false},
		{"other partition", part(3, "g", 1, "s", 25, 35), false},
	} {
		if got := orderRangeAlone(append(slices.Clone(inputs), c.other), inputs); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}
