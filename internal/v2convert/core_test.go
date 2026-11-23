package v2convert

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-graphite/go-carbon/internal/v2part"
)

func TestConvertOrderSummaryReceipt(t *testing.T) {
	_, rows := fixture(t)
	// Use the real dense writer with its explicit owner-only selector to model
	// an eligible merged source; the converter itself must remain lossless.
	source := filepath.Join(t.TempDir(), "summary.sst")
	cw := testWriter()
	cw.Source.MinOrder = 1
	cw.Source.MaxOrder = 1
	// Get the authoritative policy table from an ordinary conversion first.
	dir := t.TempDir()
	files, err := cw.Write(context.Background(), dir, rows)
	if err != nil {
		t.Fatal(err)
	}
	var d v2part.Descriptor
	if err = v2part.Read(context.Background(), filepath.Join(dir, files[0]), nil, func(v2part.Key, []byte) error { return nil }); err != nil {
		t.Fatal(err)
	}
	// The descriptor is also the converter's source-bound receipt material.
	b, err := os.ReadFile(filepath.Join(dir, files[1]))
	if err != nil {
		t.Fatal(err)
	}
	d, err = v2part.ParseDescriptor(b)
	if err != nil {
		t.Fatal(err)
	}
	spec := v2part.Spec{Scope: d.Scope, Source: cw.Source, Policies: d.Coverage.Policies, MinTime: d.Coverage.MinTime, MaxTime: d.Coverage.MaxTime}
	w, err := v2part.NewDenseWriter(source, spec)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	if err = w.SetOrderEncoding(v2part.SummaryOrders); err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if err = w.SetEncoded(r.Key, r.Value); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = w.Close(); err != nil {
		t.Fatal(err)
	}
	opts := Options{Directory: t.TempDir(), PartBytes: 1 << 20, Adapter: CoreRecords{}, Writer: cw}
	first, err := ConvertSST(context.Background(), source, opts)
	if err != nil {
		t.Fatal(err)
	}
	second, err := ConvertSST(context.Background(), source, opts)
	if err != nil {
		t.Fatal(err)
	}
	if first.LogicalDigest != second.LogicalDigest || first.Entries != uint64(len(rows)) {
		t.Fatal("summary receipt/resume changed logical stream")
	}
}
