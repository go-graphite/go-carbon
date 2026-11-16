package v2daemon

import (
	"io"
	"testing"
)

func TestFixedCorpusAllLayers(t *testing.T) {
	for _, f := range []Fixture{
		{Names: 29, Rounds: 8, Producers: 3, Shards: 2, BatchSize: 7, SampleNames: 29},
		{Names: 240, Rounds: 8, Producers: 48, Shards: 12, BatchSize: 7, SampleNames: 240},
	} {
		for layer := 0; layer <= 2; layer++ {
			r, err := RunFixed(t.TempDir(), layer, f, io.Discard)
			if err != nil {
				t.Fatal(layer, err)
			}
			if r["exact_sample_points"] != f.Names*f.Rounds {
				t.Fatal(r)
			}
			if layer == 2 && r["exact_journal_points"] != f.Names*f.Rounds {
				t.Fatal(r)
			}
		}
	}
}
