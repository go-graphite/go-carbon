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
