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

// TestMergeOrderDropLateWrite writes two seals, merges them, writes a late
// duplicate of two old slots in a newer part and merges again. Merged runs
// store no order column, yet the newest write wins every slot: the final part
// holds exactly what last-write-wins over the write log holds (whisper's rule).
func TestMergeOrderDropLateWrite(t *testing.T) {
	type out struct {
		lines []string
		one   int // runs written without an order column
	}
	want := map[string]string{} // metric slot -> newest "time bits"
	run := func() out {
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
					want[fmt.Sprintf("%s 0 %d", name, s)] = fmt.Sprintf("%d %x false", 100000+s, math.Float64bits(value+float64(n)))
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
		var o out
		for _, d := range []v2part.Descriptor{m1, m2} {
			last := d.Scope.Part == m2.Scope.Part
			err = v2part.Read(context.Background(), filepath.Join(dir, "parts", hex.EncodeToString(d.SHA256[:])+".sst"), &d, func(k v2part.Key, b []byte) error {
				if k.Archive == v2part.MetadataArchive {
					return nil
				}
				if b[4]&4 != 0 { // dense mode: no order column
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
	got := run()
	slices.Sort(got.lines)
	var lines []string
	for k, v := range want {
		lines = append(lines, k+" "+v)
	}
	slices.Sort(lines)
	if !slices.Equal(got.lines, lines) {
		t.Fatalf("merged records differ from last-write-wins:\ngot  %q\nwant %q", got.lines, lines)
	}
	for _, w := range []string{"a.b.c 0 5 100005 4022000000000000 false", "x.yy 0 30 100030 4026000000000000 false"} {
		if !slices.Contains(got.lines, w) {
			t.Fatalf("late write lost: no %q in %q", w, got.lines)
		}
	}
	if got.one == 0 {
		t.Fatal("merged runs kept their order column")
	}
}
