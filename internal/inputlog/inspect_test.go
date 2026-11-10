package inputlog

import (
	"bytes"
	"context"
	"errors"
	"hash/crc32"
	"math"
	"testing"

	"github.com/klauspost/compress/zstd"

	"github.com/go-graphite/go-carbon/points"
)

func inspectionFixture(t *testing.T) ([]byte, map[string][]points.Point) {
	t.Helper()
	batches := [][]Entry{
		{NewStreamRef("earlier").Entry(math.Inf(1), 100), NewStreamRef("same").Entry(math.Copysign(0, -1), 120)},
		{NewStreamRef("same").Entry(math.Float64frombits(0x7ff8000000000001), 115), NewStreamRef("later").Entry(math.Inf(-1), 130)},
	}
	var encoder streamEncoder
	raw := encodeStream(&encoder, 7, batches...)
	compressor, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer compressor.Close()
	data := (segmentHeader{seq: 7, salt: 39, created: 123}).append(nil)
	for i, frame := range raw {
		payload := compressor.EncodeAll(frame, nil)
		// Recovery would skip the first frame; inventory must retain its names
		// and points as well as decode the second frame's dictionary references.
		h := frameHeader{codec: codecStream, salt: 39, partition: 3, length: uint32(len(payload)), rawLength: uint32(len(frame)), points: uint32(len(batches[i])), replayFrom: position{seq: 99, off: 64}, crc: crc32.Checksum(payload, castagnoli)}
		data = append(h.append(data), payload...)
	}
	return data, wantPoints(batches...)
}

func inspectionLimits() InspectLimits {
	return InspectLimits{Bytes: 1 << 20, Points: 100, Frames: 10, Names: 10, DictionaryBytes: 1000}
}

func TestInspectRetainsPersistedHistoryAndExactBits(t *testing.T) {
	data, want := inspectionFixture(t)
	got := map[string][]points.Point{}
	stats, err := Inspect(context.Background(), bytes.NewReader(data), inspectionLimits(), func(p *points.Points) error {
		got[p.Metric] = append(got[p.Metric], p.Data...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	equalPoints(t, got, want)
	if stats.Bytes != int64(len(data)) || stats.Points != 4 || stats.Frames != 2 || stats.Names != 3 || stats.Segment != 7 {
		t.Fatal(stats)
	}
}

func TestInspectRejectsDamageAndIncompletePrefixes(t *testing.T) {
	data, _ := inspectionFixture(t)
	for _, offset := range []int{0, 16, segmentHeaderSize, segmentHeaderSize + 48, len(data) - 1} {
		corrupt := bytes.Clone(data)
		corrupt[offset] ^= 1
		if _, err := Inspect(context.Background(), bytes.NewReader(corrupt), inspectionLimits(), nil); err == nil {
			t.Fatalf("corruption at %d accepted", offset)
		}
	}
	for _, length := range []int{0, 63, 64, 70, len(data) - 1} {
		if _, err := Inspect(context.Background(), bytes.NewReader(data[:length]), inspectionLimits(), nil); err == nil {
			t.Fatalf("incomplete length %d accepted", length)
		}
	}
	if _, err := Inspect(context.Background(), bytes.NewReader(append(bytes.Clone(data), make([]byte, 64)...)), inspectionLimits(), nil); err == nil {
		t.Fatal("active preallocated tail accepted")
	}
}

func TestInspectBoundsAndCancellation(t *testing.T) {
	data, _ := inspectionFixture(t)
	for _, limit := range []InspectLimits{
		{Bytes: 64, Points: 100, Frames: 10, Names: 10, DictionaryBytes: 1000},
		{Bytes: 1 << 20, Points: 1, Frames: 10, Names: 10, DictionaryBytes: 1000},
		{Bytes: 1 << 20, Points: 100, Frames: 1, Names: 10, DictionaryBytes: 1000},
		{Bytes: 1 << 20, Points: 100, Frames: 10, Names: 1, DictionaryBytes: 1000},
		{Bytes: 1 << 20, Points: 100, Frames: 10, Names: 10, DictionaryBytes: 1},
	} {
		if _, err := Inspect(context.Background(), bytes.NewReader(data), limit, nil); err == nil {
			t.Fatalf("limit accepted: %+v", limit)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Inspect(ctx, bytes.NewReader(data), inspectionLimits(), nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	stop := errors.New("inventory consumer stopped")
	if _, err := Inspect(context.Background(), bytes.NewReader(data), inspectionLimits(), func(*points.Points) error { return stop }); !errors.Is(err, stop) {
		t.Fatal(err)
	}
}
