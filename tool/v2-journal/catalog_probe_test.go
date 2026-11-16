package main

import (
	"github.com/go-graphite/go-carbon/internal/partlog"
	"github.com/go-graphite/go-carbon/points"
	"math"
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
