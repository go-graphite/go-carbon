package whisper

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

type ownedPlanReader struct{ points []TimeSeriesPoint }

func (r ownedPlanReader) ReadStorageBlock(ContinuationBlock) ([]TimeSeriesPoint, error) {
	return append([]TimeSeriesPoint(nil), r.points...), nil
}

type bufferedPlanReader struct {
	ownedPlanReader
	reads int
	err   error
}

func (r *bufferedPlanReader) ReadStorageBlockInto(_ ContinuationBlock, dst []TimeSeriesPoint) ([]TimeSeriesPoint, error) {
	r.reads++
	if r.err != nil {
		return nil, r.err
	}
	return append(dst[:0], r.points...), nil
}

func planScratchFixture(n int) (*archiveInfo, blockRange, []TimeSeriesPoint) {
	points := make([]TimeSeriesPoint, n)
	for i := range points {
		points[i] = TimeSeriesPoint{Time: 1700000040 + i*60, Value: float64(i % 97)}
	}
	block := blockRange{index: 0, start: points[0].Time, end: points[n-1].Time, count: n - 1}
	archive := &archiveInfo{Retention: Retention{secondsPerPoint: 60, numberOfPoints: 100000, blockCount: 2},
		blockSize: 128 << 10, blockRanges: []blockRange{block, {}}, cblock: blockInfo{index: 1}}
	return archive, block, points
}

func TestPlanBlockScratchMatchesOwned(t *testing.T) {
	archive, block, points := planScratchFixture(7200)
	owned := ownedPlanReader{points}
	buffered := &bufferedPlanReader{ownedPlanReader: owned}
	w := new(Whisper)
	for _, extras := range [][]extraPoint{
		nil,
		{{dataPoint: dataPoint{interval: points[15].Time, value: -1}, replace: true}},
		{{dataPoint: dataPoint{interval: points[15].Time, value: -1}}},
		{{dataPoint: dataPoint{interval: points[15].Time + 60, value: -1}, replace: true}, {dataPoint: dataPoint{interval: points[7000].Time, value: -2}, replace: true}},
	} {
		want, carry, result, err := w.planBlock(owned, 0, archive, block, nil, nil, extras)
		got, left, kind, e := w.planBlock(buffered, 0, archive, block, nil, nil, extras)
		if err != nil || e != nil || kind != result || !reflect.DeepEqual(got, want) || !reflect.DeepEqual(left, carry) {
			t.Fatalf("buffered plan differs: %v %v %v %v", result, kind, err, e)
		}
		if result != InPlaced || (len(extras) > 0 && extras[0].replace && want == nil) {
			t.Fatalf("aligned correction was not planned: %v %+v", result, want)
		}
		// Reuse scratch for a different correction before consuming the first plan.
		if _, _, _, e := w.planBlock(buffered, 0, archive, block, nil, nil,
			[]extraPoint{{dataPoint: dataPoint{interval: points[30].Time, value: -7}, replace: true}}); e != nil {
			t.Fatal(e)
		}
		if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(left, carry) {
			t.Fatal("retained plan changed after scratch reuse")
		}
	}
	if buffered.reads == 0 {
		t.Fatal("borrowed reader was not used")
	}
	sentinel := errors.New("read failed")
	buffered.err = sentinel
	if _, _, _, err := w.planBlock(buffered, 0, archive, block, nil, nil, nil); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
}

func TestPlanBlockScratchRejectsOffGrid(t *testing.T) {
	archive, block, points := planScratchFixture(256)
	points[15].Time++
	owned := ownedPlanReader{points}
	buffered := &bufferedPlanReader{ownedPlanReader: owned}
	w := new(Whisper)
	for _, reader := range []StorageBlockReader{owned, buffered} {
		change, carry, result, err := w.planBlock(reader, 0, archive, block, nil, nil, nil)
		if err != nil || result != InPlaceUnsupported || change != nil || carry != nil {
			t.Fatalf("off-grid block was planned: %v %v", result, err)
		}
	}
	points[15].Time--
	change, _, result, err := w.planBlock(buffered, 0, archive, block, nil, nil,
		[]extraPoint{{dataPoint: dataPoint{interval: points[15].Time, value: -1}, replace: true}})
	if err != nil || result != InPlaced || change == nil {
		t.Fatalf("aligned block after scratch reuse was not planned: %v %v", result, err)
	}
}

func TestPlanBlockScratchCarry(t *testing.T) {
	for worker := 0; worker < 8; worker++ {
		t.Run(fmt.Sprint(worker), func(t *testing.T) {
			t.Parallel()
			archive, block, points := planScratchFixture(256)
			archive.blockSize = 64
			owned := ownedPlanReader{points}
			buffered := &bufferedPlanReader{ownedPlanReader: owned}
			w := new(Whisper)
			carry := []TimeSeriesPoint{{Time: block.start - 120, Value: -1}}
			before := []extraPoint{{dataPoint: dataPoint{interval: block.start - 60, value: -2}}}
			want, overflow, kind, err := w.planBlock(owned, 0, archive, block, carry, before, nil)
			got, left, result, e := w.planBlock(buffered, 0, archive, block, carry, before, nil)
			if err != nil || e != nil || kind != InPlaced || result != kind || len(overflow) == 0 || !reflect.DeepEqual(got, want) || !reflect.DeepEqual(left, overflow) {
				t.Fatalf("carry differs: %v %v %v %v", kind, result, err, e)
			}
			if _, _, _, err := w.planBlock(buffered, 0, archive, block, nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(left, overflow) {
				t.Fatal("retained carry changed after scratch reuse")
			}
		})
	}
}

func BenchmarkPlanBlockScratch(b *testing.B) {
	archive, block, points := planScratchFixture(7200)
	owned := ownedPlanReader{points}
	buffered := &bufferedPlanReader{ownedPlanReader: owned}
	extras := []extraPoint{{dataPoint: dataPoint{interval: points[15].Time, value: -1}, replace: true}}
	for _, tc := range []struct {
		name   string
		reader StorageBlockReader
	}{{"owned", owned}, {"buffered", buffered}} {
		b.Run(tc.name, func(b *testing.B) {
			w := new(Whisper)
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				change, _, result, err := w.planBlock(tc.reader, 0, archive, block, nil, nil, extras)
				if err != nil || result != InPlaced || change == nil {
					b.Fatalf("plan: %v %v", result, err)
				}
			}
		})
	}
}
