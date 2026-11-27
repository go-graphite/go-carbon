package main

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
)

// Independent dictionary prefixes are replayed through one worker's synthetic
// offset space. Interleaving and segment replacement must preserve both names.
func TestPartitionFrameView(t *testing.T) {
	var view frameView
	table := crc32.MakeTable(crc32.Castagnoli)
	frames := make([][]byte, 2)
	for i, name := range []string{"alpha", "beta"} {
		f, e := os.Create(filepath.Join(t.TempDir(), "segment"))
		if e != nil {
			t.Fatal(e)
		}
		w, e := inputlog.NewSegmentWriter(f, 1)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = w.Append([]*points.Points{points.OnePoint(name, float64(i+1), 123)}, nil); e != nil {
			t.Fatal(e)
		}
		w.Close()
		b, e := os.ReadFile(f.Name())
		_ = f.Close()
		if e != nil {
			t.Fatal(e)
		}
		if i == 0 {
			copy(view.header[:], b[:64])
		}
		frames[i] = append([]byte(nil), b[64:]...)
		copy(frames[i][8:16], view.header[16:24])
		binary.LittleEndian.PutUint16(frames[i][6:8], uint16(i))
		binary.LittleEndian.PutUint32(frames[i][48:52], crc32.Checksum(frames[i][:48], table))
	}
	view.offset = 64
	replay, e := inputlog.NewSegmentReplay(&view)
	if e != nil {
		t.Fatal(e)
	}
	defer replay.Close()
	for i, name := range []string{"alpha", "beta"} {
		view.frame = frames[i]
		groups, end, _, e := replay.Next(view.offset + int64(len(view.frame)))
		if e != nil {
			t.Fatal(e)
		}
		if len(groups) != 1 || groups[0].Metric != name || groups[0].Data[0].Value != float64(i+1) {
			t.Fatalf("wrong partition: %+v", groups)
		}
		view.offset = end
	}
	// Corrupt payloads must still be rejected by the real replay decoder.
	view.frame = append([]byte(nil), frames[0]...)
	view.frame[len(view.frame)-1] ^= 1
	if _, _, _, e = replay.Next(view.offset + int64(len(view.frame))); e == nil {
		t.Fatal("accepted corrupted payload")
	}
}

func TestReadyTailWaitsForRecycledSuccessor(t *testing.T) {
	path := filepath.Join(t.TempDir(), "0000000000000002.log")
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	w, e := inputlog.NewSegmentWriter(f, 1)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = w.Append([]*points.Points{points.OnePoint("old", 1, 1)}, nil); e != nil {
		t.Fatal(e)
	}
	w.Close()
	var header [64]byte
	if _, e = f.ReadAt(header[:], 0); e != nil {
		t.Fatal(e)
	}
	binary.LittleEndian.PutUint64(header[8:16], 2)
	binary.LittleEndian.PutUint32(header[32:36], crc32.Checksum(header[:32], crc32.MakeTable(crc32.Castagnoli)))
	changed := make(chan error, 1)
	go func() { time.Sleep(50 * time.Millisecond); _, e := f.WriteAt(header[:], 0); changed <- e }()
	tail, e := readyShadowTail(path)
	if e != nil {
		t.Fatal(e)
	}
	defer tail.close()
	if e := <-changed; e != nil {
		t.Fatal(e)
	}
	if binary.LittleEndian.Uint64(tail.header[8:16]) != 2 {
		t.Fatal("accepted predecessor header at successor path")
	}
}
