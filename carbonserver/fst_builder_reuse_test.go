package carbonserver

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/blevesearch/vellum"
)

func fstBuilderReuseKeys() [][]byte {
	keys := [][]byte{nil, {0}, {0, 0}, []byte("prefix"), []byte("prefix\x00child"), {0xff}}
	for i := range 2048 {
		keys = append(keys, []byte(fmt.Sprintf("\x00%s\x00service%04d\x00host%06d\x00operation%d\x00latency\x00count", strings.Repeat("namespace\x00", 8), i%97, i/97, i%31)))
	}
	slices.SortFunc(keys, bytes.Compare)
	return keys
}

func buildReuseFST(w io.Writer, keys [][]byte, opts *vellum.BuilderOpts) error {
	b, err := vellum.New(w, opts)
	if err != nil {
		return err
	}
	return insertReuseFST(b, keys)
}

func insertReuseFST(b *vellum.Builder, keys [][]byte) error {
	for i, key := range keys {
		// Exercise the shared zero-output final node and nonzero outputs.
		value := uint64(i) * uint64(i%7)
		if err := b.Insert(key, value); err != nil {
			return err
		}
	}
	return b.Close()
}

func TestFSTBuilderReuseCompatibility(t *testing.T) {
	t.Parallel()
	keys := fstBuilderReuseKeys()
	// SHA-256 of the complete FST emitted by unpatched Vellum v1.2.0.
	stock := map[int]string{
		1: "0bdeea29d3edeb01a64af71219bb85a75fd0b4398080ea3a5648cf9cf0ab0b73",
		2: "795420e3f5b502b7e3d088ca92e11133e1bb08e1dee77e9507e51248e6a69142",
	}
	for _, mru := range []int{1, 2} {
		t.Run(fmt.Sprintf("mru%d", mru), func(t *testing.T) {
			var buf bytes.Buffer
			opts := &vellum.BuilderOpts{Encoder: 1, RegistryTableSize: 10000, RegistryMRUSize: mru}
			builder, err := vellum.New(&buf, opts)
			if err != nil {
				t.Fatal(err)
			}
			// Reset through a short tree and back to long keys to exercise the
			// overflow cache as well as registered and pooled nodes.
			for round, current := range [][][]byte{keys, {nil, []byte("a"), []byte("ab")}, keys} {
				if round > 0 {
					buf.Reset()
					if err := builder.Reset(&buf); err != nil {
						t.Fatal(err)
					}
				}
				if err := insertReuseFST(builder, current); err != nil {
					t.Fatal(err)
				}
				if round != 1 {
					if got := fmt.Sprintf("%x", sha256.Sum256(buf.Bytes())); got != stock[mru] {
						t.Fatalf("encoding differs from stock Vellum: got %s, want %s", got, stock[mru])
					}
				}
				fst, err := vellum.Load(buf.Bytes())
				if err != nil {
					t.Fatal(err)
				}
				if fst.Len() != len(current) {
					t.Fatalf("got %d keys, want %d", fst.Len(), len(current))
				}
				for i, key := range current {
					got, exists, err := fst.Get(key)
					want := uint64(i) * uint64(i%7)
					if err != nil || !exists || got != want {
						t.Fatalf("row %d: got value=%d, exists=%v, err=%v; want %d", i, got, exists, err, want)
					}
				}
				if err := fst.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestFSTBuilderReuseAllocations(t *testing.T) {
	keys := fstBuilderReuseKeys()
	allocs := testing.AllocsPerRun(3, func() {
		if err := buildReuseFST(io.Discard, keys, nil); err != nil {
			t.Fatal(err)
		}
	})
	// Long paths used to allocate a node for nearly every suffix byte.
	// Leave room for the registry and initial pool while detecting lost reuse.
	if perKey := allocs / float64(len(keys)); perKey > 32 {
		t.Fatalf("%.1f allocations/key, want at most 32", perKey)
	}
}

func BenchmarkFSTBuilderReuse(b *testing.B) {
	keys := fstBuilderReuseKeys()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := buildReuseFST(io.Discard, keys, nil); err != nil {
			b.Fatal(err)
		}
	}
}
