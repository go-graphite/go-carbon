package v2core

import (
	"bytes"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble"
)

func TestCoreRadixPreservesSSTBytes(t *testing.T) {
	for _, ambiguous := range []bool{false, true} {
		name := "unique-sequence"
		if ambiguous {
			name = "equal-key-distinct-bits"
		}
		t.Run(name, func(t *testing.T) { testCoreRadixSSTBytes(t, ambiguous) })
	}
}
func testCoreRadixSSTBytes(t *testing.T, ambiguous bool) {
	rng := rand.New(rand.NewPCG(120, 192))
	input := make([]row, 8192)
	for i := range input {
		input[i] = row{rng.Uint64N(200), 1800000000 + int64(i%30), rng.Uint64(), uint64(i)}
	}
	if ambiguous {
		for i := range 1000 {
			input[i] = row{17, 1800000000, []uint64{0, 1 << 63, 0x7ff8000000000001, 0xfff0000000000000}[i%4], 1}
		}
	}
	var want [][]byte
	for _, radix := range []bool{false, true} {
		root := t.TempDir()
		db, err := pebble.Open(root, &pebble.Options{})
		if err != nil {
			t.Fatal(err)
		}
		cfg := coreConfig{Shards: 1, Producers: 1, MaxRows: 16384, PartitionSeconds: 3600, AppendParts: true}
		o := &coreOwner{db: db, dir: root, cfg: cfg, meta: coreFrontier{Version: 1, Config: cfg, Cursors: []uint64{0}, ExpiredBefore: -1 << 63}, radixSort: radix}
		next := o.meta
		next.Cursors = []uint64{8192}
		next.NextPart = 1
		if _, err = o.seal(slices.Clone(input), next); err != nil {
			t.Fatal(err)
		}
		if err = db.Close(); err != nil {
			t.Fatal(err)
		}
		paths, err := filepath.Glob(filepath.Join(root, "*.sst"))
		if err != nil {
			t.Fatal(err)
		}
		var got [][]byte
		for _, path := range paths {
			b, e := os.ReadFile(path)
			if e != nil {
				t.Fatal(e)
			}
			got = append(got, b)
		}
		if len(got) != 2 {
			t.Fatalf("expected raw+frontier SSTs, got %d", len(got))
		}
		if !radix {
			want = got
			continue
		}
		for i := range want {
			if !bytes.Equal(want[i], got[i]) {
				t.Fatalf("SST %d differs", i)
			}
		}
	}
}
