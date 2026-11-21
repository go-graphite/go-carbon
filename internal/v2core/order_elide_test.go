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

// TestMergeOrderCodeExact writes two seals with regular large orders, merges
// them (copied runs take the predicted order code), writes late duplicates
// of two old slots with newer orders and merges again. Every merged record,
// including Order and Sequence, equals the last-write-wins winner of the inputs.
func TestMergeOrderCodeExact(t *testing.T) {
	{
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
					r := sealrollup.Record{Delta: sealrollup.Delta{Slot: s, Order: slotOrder(order, s), Sample: sealrollup.Sample{Time: 100000 + s, Bits: math.Float64bits(value + float64(n))}}}
					rows = append(rows, KV{Key: enc, Value: sealrollup.EncodeRecord(r)})
				}
				md := RollupMetadata{Version: 2, ID: uint64(n + 1), Policy: policy, Frontier: sealrollup.Frontier{Order: slotOrder(order+1, 0) - 1, MaxTime: 100000 + slices.Max(slots), Policy: digest}, LastUpdate: int64(order) * 1e9, Revision: order}
				k.Archive, k.BlockStart = v2part.MetadataArchive, 0
				enc, e := v2part.Encode(k)
				if e != nil {
					t.Fatal(e)
				}
				rows = append(rows, KV{Key: enc, Value: AppendRollupMetadata(nil, md)})
			}
			slices.SortFunc(rows, func(a, b KV) int { return bytes.Compare(a.Key, b.Key) })
			path := filepath.Join(dir, fmt.Sprintf("in-%d.sst", i))
			d, e := WriteDenseRollupPart(path, rows, v2part.Provenance{Stream: "elide", Incarnation: v2part.PartID{9}, MinOrder: slotOrder(order, 0), MaxOrder: slotOrder(order+1, 0) - 1})
			if e != nil {
				t.Fatal(e)
			}
			if e = os.Rename(path, filepath.Join(dir, "parts", hex.EncodeToString(d.SHA256[:])+".sst")); e != nil {
				t.Fatal(e)
			}
			return d
		}
		merge := func(name string, inputs []v2part.Descriptor) v2part.Descriptor {
			path := filepath.Join(dir, name+".sst")
			d, e := mergePartFiles(context.Background(), dir, path, inputs, 0, nil)
			if e != nil {
				t.Fatalf("%s: %v", name, e)
			}
			if e = os.Rename(path, filepath.Join(dir, "parts", hex.EncodeToString(d.SHA256[:])+".sst")); e != nil {
				t.Fatal(e)
			}
			return d
		}
		var seq []int64
		for s := int64(0); s < 40; s++ {
			seq = append(seq, s)
		}
		s0 := write(0, 1, seq[:20], 1)
		s1 := write(1, 2, seq[20:], 2)
		m1 := merge("m1", []v2part.Descriptor{s0, s1})
		// Late duplicates of old slots 5 and 30: same timestamps, newer order.
		late := write(2, 3, []int64{5, 30}, 9)
		m2 := merge("m2", []v2part.Descriptor{m1, late})
		read := func(d v2part.Descriptor) (lines []string, one int) {
			err := v2part.Read(context.Background(), filepath.Join(dir, "parts", hex.EncodeToString(d.SHA256[:])+".sst"), &d, func(k v2part.Key, b []byte) error {
				if k.Archive == v2part.MetadataArchive {
					return nil
				}
				if b[4]&0x10 != 0 {
					one++
				}
				rs, e := v2part.DecodeSlots(k.Archive, k.BlockStart, b)
				for _, r := range rs {
					lines = append(lines, fmt.Sprintf("%s %+v", k.Metric, r))
				}
				return e
			})
			if err != nil {
				t.Fatal(err)
			}
			slices.Sort(lines)
			return lines, one
		}
		want := func(late bool) (lines []string) {
			for n, name := range names {
				for s := int64(0); s < 40; s++ {
					order, value := slotOrder(uint64(1+s/20), s), float64(1+s/20)
					if late && (s == 5 || s == 30) {
						order, value = slotOrder(3, s), 9
					}
					r := sealrollup.Record{Delta: sealrollup.Delta{Slot: s, Order: order, Sample: sealrollup.Sample{Time: 100000 + s, Bits: math.Float64bits(value + float64(n)), Sequence: order}}}
					lines = append(lines, fmt.Sprintf("%s %+v", name, r))
				}
			}
			slices.Sort(lines)
			return lines
		}
		got1, one1 := read(m1)
		got2, one2 := read(m2)
		if !slices.Equal(got1, want(false)) {
			t.Fatalf("m1 records differ:\ngot  %q\nwant %q", got1, want(false))
		}
		if !slices.Equal(got2, want(true)) {
			t.Fatalf("m2 records differ:\ngot  %q\nwant %q", got2, want(true))
		}
		if one1 == 0 || one2 == 0 {
			t.Fatalf("predicted order runs: m1 %d, m2 %d, want both >0", one1, one2)
		}
	}
}

// slotOrder is a regular arrival order: seal o writes slot s at o*1e5 + 997s.
func slotOrder(o uint64, s int64) uint64 { return o*100000 + uint64(s)*997 }
