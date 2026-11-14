//go:build unix

package carbon

import (
	"bytes"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
)

func inputlogCaptureFixture(t testing.TB, frames ...[]byte) []byte {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "segment-*.log")
	if err != nil {
		t.Fatal(err)
	}
	writer, err := inputlog.NewSegmentWriter(f, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	for _, raw := range frames {
		var records []*points.Points
		if err := points.ReadBinary(bytes.NewReader(raw), func(p *points.Points) { records = append(records, p) }); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Append(records, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	// The ordinary input-log recovery path must understand every capture segment,
	// including replay-from and dictionary references from later appends.
	log, err := inputlog.Open(filepath.Dir(f.Name()), inputlog.Options{})
	if err != nil {
		t.Fatal(err)
	}
	want := 0
	for _, raw := range frames {
		_ = points.ReadBinary(bytes.NewReader(raw), func(p *points.Points) { want += len(p.Data) })
	}
	stats, err := log.Replay(func(*points.Points) {})
	if err != nil || stats.Points != int64(want) || stats.Corrupt != 0 {
		t.Fatalf("ordinary journal replay: %+v %v want %d", stats, err, want)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	wire, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	return wire
}

func TestInputlogCaptureCursorAndAliases(t *testing.T) {
	core := cache.New()
	core.SetMaxSize(0)
	core.SetTagsEnabled(true)
	names := []string{"metric;b=2;a=1", "other", "metric;a=1;b=2", "metric;b=2;a=1"}
	bits := []uint64{0x7ff8000000000042, 1 << 63, 1, 0xfff0000000000000}
	var raw []byte
	for i, name := range names {
		raw = points.OnePoint(name, math.Float64frombits(bits[i]), -7).AppendBinary(raw)
	}
	wire := inputlogCaptureFixture(t, raw, raw)
	file, err := os.CreateTemp(t.TempDir(), "capture")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err = file.Write(wire); err != nil {
		t.Fatal(err)
	}
	reader, err := inputlog.NewSegmentReplay(file)
	if err != nil {
		t.Fatal(err)
	}
	_, cursor, _, err := reader.Next(int64(len(wire)))
	if err != nil {
		t.Fatal(err)
	}
	reader.Close()
	// Every possible truncation must replay only whole verified frames.
	for cut := 64; cut < len(wire); cut++ {
		r, err := inputlog.NewSegmentReplay(bytes.NewReader(wire[:cut]))
		if err != nil {
			t.Fatal(err)
		}
		for {
			_, end, _, e := r.Next(int64(cut))
			if e != nil {
				break
			}
			if end > int64(cut) {
				t.Fatal("torn frame applied")
			}
		}
		r.Close()
	}
	// Decode a resumed dictionary prefix but apply only complete frames after it.
	var stream cache.RecoveryStream
	seg := &startupSegment{file: file, source: stream.Next()}
	seg.size.Store(int64(len(wire)))
	sink := &startupSink{admission: core, add: core.AddRecoveredFrom, groupedAdd: core.AddRecoveryMetric}
	_, err = batchReplayOwner().replayInputSegment(seg, sink, cursor, func(int64, int) {})
	if err != nil {
		t.Fatal(err)
	}
	// First append has several tag-barrier frames; resume skipped its first one.
	got := core.Get("metric;a=1;b=2")
	want := []uint64{bits[2], bits[3], bits[0], bits[2], bits[3]}
	if len(got) != len(want) {
		t.Fatalf("alias cursor result: %+v", got)
	}
	for i, p := range got {
		if p.Timestamp != -7 || math.Float64bits(p.Value) != want[i] {
			t.Fatalf("alias order/bits: %+v", got)
		}
	}
}
