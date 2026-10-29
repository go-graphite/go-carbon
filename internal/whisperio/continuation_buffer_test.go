package whisperio

import (
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

func TestContinuationScratchPreservesPhysicalBitsAndOwnedResults(t *testing.T) {
	for _, config := range []struct {
		name, retentions string
		block, seed      int
	}{
		{"multiple-archives", "1s:1h,10s:1d", 16, 500},
		{"circular-old-owners", "1s:20s,5s:2m", 16, 200},
		{"production-layout", "1s:2d,60s:28d,3600s:2y", 7200, 8000},
	} {
		t.Run(config.name, func(t *testing.T) {
			const base = 1792432631
			clock := func() time.Time { return time.Unix(int64(base+config.seed+2), 0) }
			path := filepath.Join(t.TempDir(), "source.wsp")
			w, err := whisper.CreateWithOptions(path, whisper.MustParseRetentionDefs(config.retentions), whisper.Average, .5, &whisper.Options{Compressed: true, PointsPerBlock: config.block, Now: clock})
			if err != nil {
				t.Fatal(err)
			}
			values := []float64{.1, math.Nextafter(1, 2), math.Copysign(0, -1), 1e-300, 1e100, 9007199254740991, math.Float64frombits(0x7ff8000000000011), math.Float64frombits(0x7ff8000000000022)}
			input := make([]*whisper.TimeSeriesPoint, config.seed)
			for i := range input {
				input[i] = &whisper.TimeSeriesPoint{Time: base + i, Value: values[i%len(values)]}
			}
			if err := w.UpdateMany(input); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			w, err = whisper.OpenWithOptions(path, &whisper.Options{Now: clock})
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			layout, err := w.ContinuationLayout()
			if err != nil {
				t.Fatal(err)
			}
			if len(layout.Blocks) < 2 {
				t.Fatalf("fixture needs multiple blocks: %d", len(layout.Blocks))
			}
			raw, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer raw.Close()
			var scratch whisper.ContinuationBlockBuffer
			var retained [][]whisper.TimeSeriesPoint
			var expected [][]whisper.TimeSeriesPoint
			for _, block := range layout.Blocks {
				body := make([]byte, block.Size)
				if _, err := raw.ReadAt(body, int64(block.Offset)); err != nil {
					t.Fatal(err)
				}
				// This decoder is unchanged by the scratch implementation and reads the
				// original Gorilla bytes rather than the virtual storage accessor.
				want, err := whisper.DecodeContinuationBlock(block, body)
				if err != nil {
					t.Fatal(err)
				}
				got, err := w.ContinuationBlockPointsBuffered(block, &scratch)
				if err != nil {
					t.Fatal(err)
				}
				if err := sameContinuationPoints(want, got); err != nil {
					t.Fatal(err)
				}
				owned, err := w.ContinuationBlockPoints(block)
				if err != nil {
					t.Fatal(err)
				}
				retained = append(retained, owned)
				expected = append(expected, want)
			}
			// Reuse from largest/smallest archives in both orders. The old API's
			// previously returned owned slices must remain valid throughout.
			for i := len(layout.Blocks) - 1; i >= 0; i-- {
				got, err := w.ContinuationBlockPointsBuffered(layout.Blocks[i], &scratch)
				if err != nil {
					t.Fatal(err)
				}
				if err := sameContinuationPoints(expected[i], got); err != nil {
					t.Fatal(err)
				}
			}
			for i := range retained {
				if err := sameContinuationPoints(expected[i], retained[i]); err != nil {
					t.Fatalf("owned result %d changed: %v", i, err)
				}
			}
			bad := layout.Blocks[0]
			bad.Count++
			if _, err := w.ContinuationBlockPointsBuffered(bad, &scratch); err == nil {
				t.Fatal("accepted changed source count")
			}
			if _, err := w.ContinuationBlockPointsBuffered(layout.Blocks[0], nil); err == nil {
				t.Fatal("accepted nil scratch")
			}
		})
	}
}
