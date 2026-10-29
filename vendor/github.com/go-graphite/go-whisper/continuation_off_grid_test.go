package whisper

import (
	"math"
	"strings"
	"testing"
)

// A block may hold points off its archive's grid: converted CWhisper files
// carry corrupt blocks, and continuation storage keeps their points exactly.
// The interval encoder stored an off-grid change of interval truncated to
// whole steps, so the block read back with that point, and every later one in
// the block, moved; EncodeContinuationBlock refused such blocks outright, which
// failed every rewrite reading them. Both must round-trip exactly.
func TestContinuationBlockOffGridRoundTrip(t *testing.T) {
	const step = 60
	base := 1792400820
	shapes := map[string][]TimeSeriesPoint{}
	var run, mixed, tail []TimeSeriesPoint
	for i := 0; i < 500; i++ {
		v := 0.0
		if i%37 == 0 {
			v = 58
		}
		// A corrupt raw first point moves every delta-coded point after it.
		run = append(run, TimeSeriesPoint{Time: base + 17 + step*i, Value: v})
		// Aligned writes appended after an off-grid run, with gaps.
		ts := base + step*i
		if i < 200 {
			ts += 17
		} else if i%11 == 0 {
			ts += step * 3
		}
		if len(mixed) == 0 || ts > mixed[len(mixed)-1].Time {
			mixed = append(mixed, TimeSeriesPoint{Time: ts, Value: float64(i % 5)})
		}
		tail = append(tail, TimeSeriesPoint{Time: base + step*i, Value: v})
	}
	// A corrupt far-future tail, as some production files end a block.
	tail = append(tail, TimeSeriesPoint{Time: 0xF0000000 + 7, Value: math.NaN()})
	shapes["residue run"], shapes["aligned after off-grid"], shapes["far-future tail"] = run, mixed, tail
	for name, points := range shapes {
		block := ContinuationBlock{Archive: 0, Index: 0, Offset: 4096, Size: 4096, Step: step,
			First: points[0].Time, Last: points[len(points)-1].Time, Count: len(points)}
		data, err := EncodeContinuationBlock(block, points)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		got, err := DecodeContinuationBlock(block, data)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		for i := range points {
			if got[i].Time != points[i].Time || math.Float64bits(got[i].Value) != math.Float64bits(points[i].Value) {
				t.Fatalf("%s: point %d reads back as %+v, want %+v", name, i, got[i], points[i])
			}
		}
	}

	// Aligned blocks keep their exact bytes: the full-interval form is taken
	// only for a remainder the delta forms cannot hold.
	aligned := shapes["far-future tail"][:500]
	block := ContinuationBlock{Offset: 4096, Size: 4096, Step: step, First: aligned[0].Time, Last: aligned[499].Time, Count: 500}
	data, err := EncodeContinuationBlock(block, aligned)
	if err != nil {
		t.Fatal(err)
	}
	archive := archiveInfo{Retention: NewRetention(step, 500), offset: 4096, blockSize: 4096, blockRanges: make([]blockRange, 1),
		cblock: blockInfo{lastByteOffset: 4096, lastByteBitPos: 7}}
	input := make([]dataPoint, len(aligned))
	for i, p := range aligned {
		input[i] = dataPoint{interval: p.Time, value: p.Value}
	}
	archive.AppendPointsToBlock(make([]byte, 8192), input)
	if archive.stats.interval.len36 != 0 {
		t.Fatalf("aligned points took the full-interval form %d times", archive.stats.interval.len36)
	}
	if len(data) != 4096 {
		t.Fatal("encoded block size changed")
	}
}

// A rewrite stores off-grid points at their interval's start, as a fetch
// serves them. Of several points in one interval an on-grid one is a later
// write and wins; otherwise the latest does.
func TestAlignBlockPoints(t *testing.T) {
	for _, c := range []struct {
		name      string
		in, want  []dataPoint
		wantMoved bool
	}{
		{"on grid", []dataPoint{{60, 1}, {120, 2}}, []dataPoint{{60, 1}, {120, 2}}, false},
		{"residue run", []dataPoint{{77, 1}, {137, 2}, {197, 3}}, []dataPoint{{60, 1}, {120, 2}, {180, 3}}, true},
		{"latest off-grid wins", []dataPoint{{61, 1}, {100, 2}, {130, 3}}, []dataPoint{{60, 2}, {120, 3}}, true},
		{"on-grid wins", []dataPoint{{60, 1}, {77, 2}, {119, 3}, {137, 4}}, []dataPoint{{60, 1}, {120, 4}}, true},
		{"far-future tail", []dataPoint{{60, 1}, {0xF0000000 + 7, 2}}, []dataPoint{{60, 1}, {0xF0000000, 2}}, true},
	} {
		got, moved := alignBlockPoints(append([]dataPoint(nil), c.in...), 60)
		if moved != c.wantMoved || len(got) != len(c.want) {
			t.Fatalf("%s: got %v moved %v, want %v moved %v", c.name, got, moved, c.want, c.wantMoved)
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Fatalf("%s: got %v, want %v", c.name, got, c.want)
			}
		}
	}
}

// Rejections name the block and the first offending point, so a production
// log identifies the cause.
func TestContinuationBlockEncodeErrorsNameTheBlock(t *testing.T) {
	points := []TimeSeriesPoint{{Time: 600, Value: 1}, {Time: 660, Value: 2}, {Time: 660, Value: 3}, {Time: 720, Value: 4}}
	block := ContinuationBlock{Archive: 1, Index: 3, Offset: 4096, Size: 4096, Step: 60, First: 600, Last: 720, Count: 4}
	_, err := EncodeContinuationBlock(block, points)
	if err == nil {
		t.Fatal("non-increasing points accepted")
	}
	for _, want := range []string{"archive 1 block 3", "first 600", "last 720", "count 4", "step 60", "point 2 at 660 follows 660"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q lacks %q", err, want)
		}
	}
	block.Size = 16
	if _, err := EncodeContinuationBlock(block, points[:2]); err == nil || !strings.Contains(err.Error(), "archive 1 block 3") {
		t.Fatalf("undersized block error %v", err)
	}
}
