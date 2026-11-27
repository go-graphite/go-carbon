package main

import (
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
)

func TestShadowTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.log")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	writer, err := inputlog.NewSegmentWriter(f, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	bits := uint64(0x7ff8000000000042)
	if _, err = writer.Append([]*points.Points{points.OnePoint("a", math.Float64frombits(bits), 123)}, nil); err != nil {
		t.Fatal(err)
	}
	tail, err := openShadowTail(path)
	if err != nil {
		t.Fatal(err)
	}
	defer tail.close()
	// A header-visible partial payload must not advance the dictionary or cursor.
	info, _ := f.Stat()
	last := make([]byte, 1)
	_, _ = f.ReadAt(last, info.Size()-1)
	if err = f.Truncate(info.Size() - 1); err != nil {
		t.Fatal(err)
	}
	if _, _, err = tail.next(); err == nil {
		t.Fatal("accepted partial payload")
	}
	if tail.offset != 64 {
		t.Fatal("advanced partial frame")
	}
	if _, err = f.WriteAt(last, info.Size()-1); err != nil {
		t.Fatal(err)
	}
	groups, _, err := tail.next()
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 1 || groups[0].Metric != "a" || len(groups[0].Data) != 1 || math.Float64bits(groups[0].Data[0].Value) != bits {
		t.Fatalf("raw bits changed: %+v", groups)
	}
	if _, err = writer.Append([]*points.Points{points.OnePoint("a", math.Copysign(0, -1), 124)}, nil); err != nil {
		t.Fatal(err)
	}
	groups, _, err = tail.next()
	if err != nil {
		t.Fatal(err)
	}
	if math.Float64bits(groups[0].Data[0].Value) != 1<<63 {
		t.Fatal("lost signed zero across growing frontier")
	}
	if _, err = f.WriteAt([]byte{0}, 16); err != nil {
		t.Fatal(err)
	}
	if _, _, err = tail.next(); err == nil {
		t.Fatal("accepted recycled segment")
	}
}
