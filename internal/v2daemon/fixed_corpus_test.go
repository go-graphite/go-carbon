package v2daemon

import (
	"errors"
	"fmt"
	"io"
	"runtime"
	"testing"
)

func TestFixedCorpusAllLayers(t *testing.T) {
	t.Parallel()
	for _, f := range []Fixture{
		{Names: 29, Rounds: 8, Producers: 3, Shards: 2, BatchSize: 7, SampleNames: 29, coreMaxRows: 256},
		{Names: 240, Rounds: 8, Producers: 48, Shards: 12, BatchSize: 7, SampleNames: 240, coreMaxRows: 256},
	} {
		for _, layer := range []int{0, 1, 3} {
			t.Run(fmt.Sprintf("names%d/producers%d/shards%d/layer%d", f.Names, f.Producers, f.Shards, layer), func(t *testing.T) {
				t.Parallel()
				// Only process I/O accounting is Linux-specific; correctness checks
				// still run every layer and geometry on every supported platform.
				written := procWritten
				if runtime.GOOS != "linux" {
					written = func() (uint64, error) { return 0, nil }
				}
				r, err := runFixed(t.TempDir(), layer, f, io.Discard, written)
				if err != nil {
					t.Fatal(layer, err)
				}
				if r["exact_sample_points"] != f.Names*f.Rounds {
					t.Fatal(r)
				}
				if r["exact_core_points"] != uint64(f.Names*f.Rounds) {
					t.Fatal(r)
				}
				if layer > 0 && r["exact_catalog_names"] != f.Names {
					t.Fatal(r)
				}
			})
		}
	}
}

// Accounting failures must remain fatal for the public benchmark entry point.
func TestFixedCorpusWriteAccountingFailure(t *testing.T) {
	t.Parallel()
	failure := errors.New("counter unavailable")
	for _, failAt := range []int{1, 2} {
		t.Run(fmt.Sprint(failAt), func(t *testing.T) {
			t.Parallel()
			calls := 0
			_, err := runFixed(t.TempDir(), 0, Fixture{Names: 1, Rounds: 4, Producers: 1, Shards: 1, BatchSize: 1, SampleNames: 1, coreMaxRows: 256}, io.Discard, func() (uint64, error) {
				calls++
				if calls == failAt {
					return 0, failure
				}
				return 0, nil
			})
			if !errors.Is(err, failure) || calls != failAt {
				t.Fatalf("error=%v calls=%d", err, calls)
			}
		})
	}
}
