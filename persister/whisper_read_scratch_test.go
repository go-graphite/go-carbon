package persister

import (
	"fmt"
	"math"
	"path/filepath"
	"sync"
	"testing"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

// Independent readers contend for the package-wide scratch pools. Their
// returned values must remain detached after another file reuses the scratch,
// including when a sidecar correction replaces a compressed main-file value.
func TestWhisperArchiveReadScratchOwnership(t *testing.T) {
	now := int(time.Now().Unix())
	for _, tc := range []struct {
		name, retention string
		pointsPerBlock  int
		pointSize       float32
	}{
		{"small", "1s:4h", 1200, 2},
		{"normal", "1s:4h", 1200, 14},
		{"unpooled-large", "1s:5d", 100000, 14},
	} {
		t.Run(tc.name, func(t *testing.T) {
			paths := make([]string, 2)
			from, until := now-2600, now-2200
			value := func(file, stamp int) float64 {
				if stamp == from+17 {
					return float64(file+1) + .1 // Deliberate historical correction.
				}
				return float64((stamp-(now-10000))*(file+1)) + .125
			}
			for file := range paths {
				paths[file] = filepath.Join(t.TempDir(), "source.wsp")
				w, err := whisper.CreateWithOptions(paths[file], whisper.MustParseRetentionDefs(tc.retention), whisper.Average, 0, &whisper.Options{Compressed: true, OutOfOrder: true, PointsPerBlock: tc.pointsPerBlock, PointSize: tc.pointSize})
				if err != nil {
					t.Fatal(err)
				}
				input := make([]*whisper.TimeSeriesPoint, 8000)
				for i := range input {
					input[i] = &whisper.TimeSeriesPoint{Time: now - 10000 + i, Value: float64(i*(file+1)) + .125}
				}
				if err = w.UpdateMany(input); err != nil {
					t.Fatal(err)
				}
				if err = w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: from + 17, Value: value(file, from+17)}}); err != nil {
					t.Fatal(err)
				}
				if err = w.Close(); err != nil {
					t.Fatal(err)
				}
			}
			check := func(file int, series *whisper.TimeSeries) error {
				values := series.Values()
				if len(values) != until-from {
					return fmt.Errorf("got %d slots, want %d", len(values), until-from)
				}
				for i, got := range values {
					want := value(file, from+i)
					if math.Float64bits(got) != math.Float64bits(want) {
						return fmt.Errorf("file %d slot %d: got %x, want %x", file, i, math.Float64bits(got), math.Float64bits(want))
					}
				}
				return nil
			}
			var wg sync.WaitGroup
			for reader := range 8 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					file := reader % len(paths)
					w, err := whisper.OpenWithOptions(paths[file], &whisper.Options{OutOfOrder: true})
					if err != nil {
						t.Error(err)
						return
					}
					defer w.Close()
					var retained []*whisper.TimeSeries
					for range 24 {
						series, err := w.FetchArchiveWindow(whisper.ArchiveWindow{Archive: 0, From: from, Until: until})
						if err == nil {
							err = check(file, series)
						}
						if err != nil {
							t.Error(err)
							return
						}
						retained = append(retained, series)
					}
					for _, series := range retained {
						if err = check(file, series); err != nil {
							t.Error(err)
						}
					}
				}()
			}
			wg.Wait()
		})
	}
}
