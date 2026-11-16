package main

import (
	"fmt"
	"github.com/go-graphite/go-carbon/internal/partlog"
	"github.com/go-graphite/go-carbon/points"
	"math"
	"os"
	"path/filepath"
	"testing"
)

func TestCatalogWireExact(t *testing.T) {
	c, e := partlog.OpenNameCatalog(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	fs := []partlog.DictionaryFrame{{Key: "binary\x00\xff", Definition: []byte{0, 255}, Clock: math.MinInt64, Points: []points.Point{{Timestamp: math.MaxInt64, Value: math.Float64frombits(0x7ff0000000000001)}, {Timestamp: -1, Value: math.Float64frombits(1 << 63)}}}, {Key: "next", Clock: math.MaxInt64, Points: []points.Point{{Timestamp: math.MinInt64, Value: math.SmallestNonzeroFloat64}}}}
	wire := catalogWire(fs, c)
	if len(wire) != 1 {
		t.Fatal(len(wire))
	}
	got, n := catalogUnwire(wire[0], c)
	if n != 3 {
		t.Fatal(n)
	}
	catalogExact(fs, got)
}

func TestCatalogParallelCaptureVerification(t *testing.T) {
	root := t.TempDir()
	fs := []partlog.DictionaryFrame{{Key: "one\x00", Definition: []byte{1, 255}, Clock: math.MinInt64, Points: []points.Point{{Timestamp: math.MaxInt64, Value: math.Float64frombits(0x7ff0000000000001)}}}, {Key: "two", Clock: math.MaxInt64, Points: []points.Point{{Timestamp: math.MinInt64, Value: math.Float64frombits(1 << 63)}}}}
	var wire [][]byte
	for i := range 3 {
		c, e := partlog.OpenNameCatalog(filepath.Join(root, fmt.Sprint(i)))
		if e != nil {
			t.Fatal(e)
		}
		wire = catalogWire(fs, c)
		if e = c.Checkpoint(); e != nil {
			t.Fatal(e)
		}
		enc := partlog.CatalogEncoder{Catalog: c, SeriesClocks: true, SparseClocks: true}
		f, e := os.Create(filepath.Join(root, fmt.Sprint(i), "segment"))
		if e != nil {
			t.Fatal(e)
		}
		for range 2 {
			b, e := enc.EncodeDurable(fs)
			if e != nil {
				t.Fatal(e)
			}
			writePacket(f, b)
		}
		if e = f.Close(); e != nil {
			t.Fatal(e)
		}
		enc.Close()
		if e = c.Close(); e != nil {
			t.Fatal(e)
		}
	}
	fixture, e := os.Create(filepath.Join(root, "sender.fixture"))
	if e != nil {
		t.Fatal(e)
	}
	for _, b := range wire {
		writePacket(fixture, b)
	}
	if e = fixture.Close(); e != nil {
		t.Fatal(e)
	}
	if got := catalogVerify(root, 3); got != 12 {
		t.Fatal(got)
	}
}

func TestUnseenNamesAcrossPackets(t *testing.T) {
	fs := []partlog.DictionaryFrame{{Key: "binary\x00\xff", Definition: []byte{0, 255}, Clock: math.MinInt64}}
	for i := 0; i < 37; i++ {
		fs[0].Points = append(fs[0].Points, points.Point{Timestamp: int64(i) - 17, Value: math.Float64frombits(uint64(i))})
	}
	var ordinal uint64
	var got []partlog.DictionaryFrame
	for _, span := range [][2]int{{0, 7}, {7, 11}, {11, 37}} {
		f := fs[0]
		f.Points = f.Points[span[0]:span[1]]
		got = append(got, unseenFrames([]partlog.DictionaryFrame{f}, &ordinal)...)
	}
	unseen := 0
	position := 0
	for _, f := range got {
		for _, p := range f.Points {
			if position%10 == 9 {
				unseen++
				if f.Key != fmt.Sprintf("\x00unseen/%d/%s", position, fs[0].Key) {
					t.Fatal("unseen name", position, f.Key)
				}
			} else if f.Key != fs[0].Key {
				t.Fatal("changed mature name")
			}
			if p.Timestamp != fs[0].Points[position].Timestamp || math.Float64bits(p.Value) != math.Float64bits(fs[0].Points[position].Value) || f.Clock != fs[0].Clock {
				t.Fatal("tuple changed")
			}
			position++
		}
	}
	if unseen != 3 || ordinal != 37 || position != 37 {
		t.Fatal(unseen, ordinal, position)
	}
}

func TestCatalogUnseenCaptureVerification(t *testing.T) {
	root := t.TempDir()
	c, e := partlog.OpenNameCatalog(filepath.Join(root, "0"))
	if e != nil {
		t.Fatal(e)
	}
	fs := []partlog.DictionaryFrame{{Key: "real\x00name", Definition: []byte{255, 0}, Clock: math.MinInt64}}
	for i := 0; i < 23; i++ {
		fs[0].Points = append(fs[0].Points, points.Point{Timestamp: int64(i), Value: math.Float64frombits(0x7ff0000000000001 + uint64(i))})
	}
	wire := catalogWire(fs, c)
	if e = c.Checkpoint(); e != nil {
		t.Fatal(e)
	}
	fixture, e := os.Create(filepath.Join(root, "sender.fixture"))
	if e != nil {
		t.Fatal(e)
	}
	for _, b := range wire {
		writePacket(fixture, b)
	}
	if e = fixture.Close(); e != nil {
		t.Fatal(e)
	}
	enc := partlog.CatalogEncoder{Catalog: c, SeriesClocks: true, SparseClocks: true}
	defer enc.Close()
	f, e := os.Create(filepath.Join(root, "0", "segment"))
	if e != nil {
		t.Fatal(e)
	}
	var ordinal uint64
	for range 3 {
		b, e := enc.EncodeDurable(unseenFrames(fs, &ordinal))
		if e != nil {
			t.Fatal(e)
		}
		writePacket(f, b)
	}
	if e = f.Sync(); e != nil {
		t.Fatal(e)
	}
	if e = f.Close(); e != nil {
		t.Fatal(e)
	}
	if e = c.Close(); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(root, "unseen"), nil, 0600); e != nil {
		t.Fatal(e)
	}
	if got := catalogVerify(root, 1); got != 69 {
		t.Fatal(got)
	}
}

func TestChurnRateAcrossPackets(t *testing.T) {
	for _, every := range []uint64{1, 7, 1003} {
		var ordinal uint64
		count := 0
		for range 17 {
			fs := []partlog.DictionaryFrame{{Key: "real\x00metric", Clock: -1, Definition: []byte{0, 255}, Points: make([]points.Point, 87)}}
			got := churnFrames(fs, &ordinal, every)
			for _, f := range got {
				if f.Key != fs[0].Key {
					count++
					if len(f.Points) != 1 {
						t.Fatal("churn frame")
					}
				}
			}
		}
		if uint64(count) != ordinal/every || ordinal != 17*87 {
			t.Fatal(every, count, ordinal)
		}
	}
}
