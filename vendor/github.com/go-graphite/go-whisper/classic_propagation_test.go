package whisper

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Preserve the previous propagation loop as a byte oracle. The higher archive
// is completely written before either loop starts; revisiting a coarse interval
// cannot see any additional higher points.
func updateClassicRepeated(w *Whisper, points []dataPoint) error {
	archive := w.archives[0]
	if err := w.archiveUpdateManyDataPoints(archive, points, false); err != nil {
		return err
	}
	higher := archive
	for _, lower := range w.lowerArchives(archive) {
		further := false
		for _, point := range points {
			ok, err := w.propagate(point.interval-mod(point.interval, lower.secondsPerPoint), higher, lower)
			if err != nil {
				return err
			}
			further = further || ok
		}
		if !further {
			break
		}
		higher = lower
	}
	return nil
}

func TestClassicPropagationDedupeBytes(t *testing.T) {
	for _, method := range []AggregationMethod{Average, Sum, Min, Max, Last, First} {
		for _, xff := range []float32{0, .01, .5, .99, 1} {
			t.Run(fmt.Sprintf("%s/%g", method, xff), func(t *testing.T) {
				dir := t.TempDir()
				paths := []string{filepath.Join(dir, "reference.wsp"), filepath.Join(dir, "deduped.wsp")}
				var writers []*Whisper
				for _, path := range paths {
					w, err := CreateWithOptions(path, MustParseRetentionDefs("1s:2h,1m:2d,30m:2y"), method, xff, &Options{Now: func() time.Time { return time.Unix(1794000000, 0) }})
					if err != nil {
						t.Fatal(err)
					}
					defer w.Close()
					writers = append(writers, w)
				}
				bits := []uint64{0, 1 << 63, 1, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000012, 0x3ff0000000000001}
				for round := 0; round < 8; round++ {
					var input []*TimeSeriesPoint
					for j := 0; j < 1980; j++ {
						if round%2 == 1 && j%3 == 0 {
							continue
						}
						input = append(input, &TimeSeriesPoint{Time: 1794000000 + round*1980 + j, Value: math.Float64frombits(bits[(round+j)%len(bits)])})
					}
					// Duplicate, late, and out-of-order corrections retain their existing
					// aligned ordering and the exact same call boundary on both writers.
					input = append(input, &TimeSeriesPoint{Time: 1794000000 + round*1980 - 77, Value: .125}, input[0], input[17])
					aligned := alignPoints(writers[0].archives[0], input)
					if err := updateClassicRepeated(writers[0], aligned); err != nil {
						t.Fatal(err)
					}
					if err := writers[1].archiveUpdateManyDataPoints(writers[1].archives[0], aligned, true); err != nil {
						t.Fatal(err)
					}
					before, err := os.ReadFile(paths[0])
					if err != nil {
						t.Fatal(err)
					}
					after, err := os.ReadFile(paths[1])
					if err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(before, after) {
						t.Fatalf("round %d changed persisted bytes", round)
					}
				}
			})
		}
	}
}

type propagationWriteCounter struct {
	StorageFile
	lowerStart, lowerEnd int64
	writes               int
}

func (f *propagationWriteCounter) WriteAt(p []byte, off int64) (int, error) {
	if off >= f.lowerStart && off < f.lowerEnd {
		f.writes++
	}
	return f.StorageFile.WriteAt(p, off)
}
func TestClassicPropagationVisitsEachIntervalOnce(t *testing.T) {
	w, err := Create(filepath.Join(t.TempDir(), "count.wsp"), MustParseRetentionDefs("1s:2h,1m:2d"), Average, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	counter := &propagationWriteCounter{StorageFile: w.file, lowerStart: w.archives[1].Offset(), lowerEnd: w.archives[1].End()}
	w.file = counter
	points := make([]dataPoint, 120)
	for i := range points {
		points[i] = dataPoint{interval: 1794000000 + i, value: float64(i)}
	}
	if err := w.archiveUpdateManyDataPoints(w.archives[0], points, true); err != nil {
		t.Fatal(err)
	}
	if counter.writes != 2 {
		t.Fatalf("two coarse intervals issued %d writes", counter.writes)
	}
}
