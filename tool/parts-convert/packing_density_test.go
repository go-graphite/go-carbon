//go:build linux || darwin

package main

import (
	"context"
	"testing"
)

func TestPackingDensityAccountsForEverySSTByte(t *testing.T) {
	dir := packingFixture(t)
	r, err := inspectPackingDensity(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if r.SSTs != 48 || r.Records != 48 || r.Points != 96 || r.PointsPerRecord["2"] != 48 {
		t.Fatalf("wrong corpus accounting: %+v", r)
	}
	if r.SSTBytes != r.DataBytes+r.IndexBytes+r.FilterBytes+r.PropertiesBytes+r.MetaIndexBytes+r.FooterBytes+r.OtherBytes {
		t.Fatalf("physical categories do not reconcile: %+v", r)
	}
	if r.RawValueBytes != r.SchemaBytes+r.PayloadBytes || r.MinTimestamp != 0 || r.MaxTimestamp != 2*86400+60 {
		t.Fatalf("raw categories or timestamps differ: %+v", r)
	}
}
