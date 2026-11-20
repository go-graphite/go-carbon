package inputlog

import (
	"errors"
	"io"
	"math"
	"os"
	"testing"

	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/tags"
)

func TestSegmentTaggedBatchOrder(t *testing.T) {
	for _, split := range []bool{false, true} {
		t.Run(map[bool]string{false: "batch", true: "row-limit"}[split], func(t *testing.T) {
			f, err := os.CreateTemp(t.TempDir(), "segment")
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			w, err := NewSegmentWriter(f, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			bits := []uint64{0, 1 << 63, 1, 0x7ff8000000000042, 0x7ff0000000000000, 0xfff0000000000000}
			names := []string{"a;b=2;a=1", "other;x=1", "a;a=1;b=2", "other;x=1", "a;b=2;a=1", "a;a=old;a=1;b=2"}
			var records []*points.Points
			want := map[string][]points.Point{}
			rawWant := map[string][]points.Point{}
			for i, name := range names {
				p := points.OnePoint(name, math.Float64frombits(bits[i]), int64(i%2))
				if split && i == 0 {
					for range maxSortRows {
						p.Add(math.Float64frombits(bits[3]), 0)
					}
				}
				records = append(records, p)
				canonical, err := tags.Normalize(name)
				if err != nil {
					t.Fatal(err)
				}
				want[canonical] = append(want[canonical], p.Data...)
				rawWant[name] = append(rawWant[name], p.Data...)
			}
			if _, err = w.Append(records, nil); err != nil {
				t.Fatal(err)
			}
			info, _ := f.Stat()
			r, err := NewSegmentReplay(f)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			got, rawGot := map[string][]points.Point{}, map[string][]points.Point{}
			for {
				records, _, _, err := r.Next(info.Size())
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				for _, p := range records {
					canonical, _ := tags.Normalize(p.Metric)
					got[canonical] = append(got[canonical], p.Data...)
					rawGot[p.Metric] = append(rawGot[p.Metric], p.Data...)
				}
			}
			if len(got) != len(want) || len(rawGot) != len(rawWant) {
				t.Fatal("replayed metric set differs")
			}
			for k, ws := range want {
				assertSegmentPointBits(t, k, got[k], ws)
			}
			for k, ws := range rawWant {
				assertSegmentPointBits(t, k, rawGot[k], ws)
			}
		})
	}
}
func assertSegmentPointBits(t *testing.T, name string, got, want []points.Point) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s count %d want %d", name, len(got), len(want))
	}
	for i, p := range got {
		if p.Timestamp != want[i].Timestamp || math.Float64bits(p.Value) != math.Float64bits(want[i].Value) {
			t.Fatalf("%s point %d differs", name, i)
		}
	}
}
func TestSegmentIndependentTagsShareFrame(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "segment")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	w, err := NewSegmentWriter(f, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	var records []*points.Points
	for range 100 {
		records = append(records, points.OnePoint("a;x=1", 1, 1), points.OnePoint("b;x=2", 2, 1))
	}
	if _, err = w.Append(records, nil); err != nil {
		t.Fatal(err)
	}
	info, _ := f.Stat()
	r, err := NewSegmentReplay(f)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	_, end, n, err := r.Next(info.Size())
	if err != nil || n != 200 || end != info.Size() {
		t.Fatalf("independent tags fragmented: n=%d end=%d size=%d err=%v", n, end, info.Size(), err)
	}
}
