package main

import (
	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
	"os"
	"path/filepath"
	"testing"
)

func TestScanCountsLogicalBytesAndRejectsCorruptHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.log")
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	w, e := inputlog.NewSegmentWriter(f, 1)
	if e != nil {
		t.Fatal(e)
	}
	_, e = w.Append([]*points.Points{{Metric: "a.b", Data: []points.Point{{Timestamp: 123, Value: 1}, {Timestamp: 124, Value: 2}}}}, nil)
	if e != nil {
		t.Fatal(e)
	}
	w.Close()
	f.Close()
	s, e := scan(path)
	if e != nil {
		t.Fatal(e)
	}
	s.f.Close()
	if s.Points != 2 || s.Frames == 0 || s.Bytes <= 64 {
		t.Fatalf("%+v", s)
	}
	f, e = os.OpenFile(path, os.O_WRONLY, 0)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = f.WriteAt([]byte{0}, 0); e != nil {
		t.Fatal(e)
	}
	f.Close()
	bad, e := scan(path)
	if bad.f != nil {
		bad.f.Close()
	}
	if e == nil {
		t.Fatal("accepted corrupted header")
	}
}
