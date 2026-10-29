package inputlog

import (
	"slices"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

// maxRecordPoints splits a long run of one metric into several records, which
// replay appends in order like one.
const maxRecordPoints = 1 << 14

// appendRecords encodes entries as codecRecords frames did: legacy binary
// records, one per run of consecutive entries of the same metric, until dst
// holds limit bytes. It returns the entries left; scratch is reused.
func appendRecords(dst []byte, entries []Entry, scratch []points.Point, limit int) ([]byte, []Entry, []points.Point) {
	for len(entries) > 0 && len(dst) < limit {
		metric := entries[0].Metric()
		scratch = scratch[:0]
		i := 0
		for ; i < len(entries) && entries[i].Metric() == metric && i < maxRecordPoints; i++ {
			scratch = append(scratch, points.Point{Value: entries[i].Value, Timestamp: entries[i].Timestamp})
		}
		p := points.Points{Metric: metric, Data: scratch}
		dst = p.AppendBinary(dst)
		entries = entries[i:]
	}
	return dst, entries, scratch
}

func TestRecordsRoundTrip(t *testing.T) {
	var entries []Entry
	for i := range 3*maxRecordPoints + 5 {
		entries = append(entries, NewStreamRef("long.run").Entry(float64(i), int64(i)))
	}
	entries = append(entries, NewStreamRef("a").Entry(1, 2), NewStreamRef("b").Entry(-3.5, 4), NewStreamRef("a").Entry(5, 6))

	// A small limit cuts the encoding into several frames between records.
	var frames [][]byte
	var scratch []points.Point
	for rest := entries; len(rest) > 0; {
		var raw []byte
		raw, rest, scratch = appendRecords(nil, rest, scratch, 1024)
		frames = append(frames, raw)
	}
	if len(frames) < 4 {
		t.Fatalf("%d frames, want a long run split into several", len(frames))
	}
	got := map[string][]points.Point{}
	records := 0
	for _, raw := range frames {
		if _, err := decodeRecords(raw, func(p *points.Points) {
			records++
			got[p.Metric] = append(got[p.Metric], p.Data...)
		}); err != nil {
			t.Fatal(err)
		}
	}
	want := map[string][]points.Point{}
	for _, e := range entries {
		want[e.Metric()] = append(want[e.Metric()], points.Point{Value: e.Value, Timestamp: e.Timestamp})
	}
	for metric, w := range want {
		if !slices.Equal(got[metric], w) {
			t.Fatalf("%s decoded %d points, want %d", metric, len(got[metric]), len(w))
		}
	}
	if records != 4+3 {
		t.Fatalf("%d records, want 4 for the long run and one per other run", records)
	}
}

func TestDecodeRejectsTruncatedRecords(t *testing.T) {
	raw, _, _ := appendRecords(nil, []Entry{NewStreamRef("metric").Entry(1, 2)}, nil, maxFrameRaw)
	for n := 1; n < len(raw); n++ {
		if _, err := decodeRecords(raw[:n], func(*points.Points) {}); err == nil {
			t.Fatalf("truncated record of %d bytes accepted", n)
		}
	}
}

func TestHeadersRoundTrip(t *testing.T) {
	sh := segmentHeader{seq: 7, salt: 0xdeadbeef, created: 123}
	raw := sh.append(nil)
	if len(raw) != segmentHeaderSize {
		t.Fatalf("segment header of %d bytes", len(raw))
	}
	if got, ok := parseSegmentHeader(raw); !ok || got != sh {
		t.Fatalf("segment header %+v %v", got, ok)
	}
	fh := frameHeader{codec: codecStream, partition: 3, salt: 9, length: 10, rawLength: 20, points: 30, replayFrom: position{4, 5}, crc: 6}
	raw = fh.append(nil)
	if len(raw) != frameHeaderSize {
		t.Fatalf("frame header of %d bytes", len(raw))
	}
	if got, ok := parseFrameHeader(raw); !ok || got != fh {
		t.Fatalf("frame header %+v %v", got, ok)
	}
	for i := range raw {
		corrupt := slices.Clone(raw)
		corrupt[i] ^= 1
		if _, ok := parseFrameHeader(corrupt); ok {
			t.Fatalf("frame header with byte %d flipped accepted", i)
		}
	}
}
