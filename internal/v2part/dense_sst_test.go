package v2part

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
)

// Exercise the public reader, forward/backward block boundaries and an actual
// Pebble ingest independently of the SET-only implementation. Pooled encoder
// state must not change a subsequent part's bytes.
func TestDenseSSTMultiBlockCompatibilityAndReuse(t *testing.T) {
	spec := fixtureSpec()
	spec.Encoding = DenseEncoding
	const count = 3000
	keys := make([][]byte, count)
	values := make([][]byte, count)
	random := rand.New(rand.NewPCG(11, 83))
	for i := range keys {
		key := fixtureKey(spec, uint32(i))
		key.Ordinal = 0
		key.BlockStart = int64(i)
		var err error
		keys[i], err = Encode(key)
		if err != nil {
			t.Fatal(err)
		}
		value := make([]byte, 1024)
		for j := 0; j < len(value); j += 8 {
			// Incompressible and repeated ranges exercise both block types.
			if i%3 == 0 {
				value[j] = byte(random.Uint64())
			} else {
				copy(value[j:], []byte("archived"))
			}
		}
		values[i] = value
	}
	root := t.TempDir()
	var previous []byte
	for pass := 0; pass < 2; pass++ {
		path := filepath.Join(root, fmt.Sprintf("part-%d.sst", pass))
		writer, err := NewWriter(path, spec)
		if err != nil {
			t.Fatal(err)
		}
		for i := range keys {
			if err = writer.SetEncoded(keys[i], values[i]); err != nil {
				t.Fatal(err)
			}
		}
		d, err := writer.Close()
		if err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if pass > 0 && !bytes.Equal(data, previous) {
			t.Fatal("pooled encoder state changed output")
		}
		previous = data
		seen := 0
		if err = Read(context.Background(), path, &d, func(k Key, value []byte) error {
			encoded, err := Encode(k)
			if err != nil || !bytes.Equal(encoded, keys[seen]) || !bytes.Equal(value, values[seen]) {
				return fmt.Errorf("record %d changed: %w", seen, err)
			}
			seen++
			return nil
		}); err != nil || seen != count {
			t.Fatalf("standard sequential reader: %d records, %v", seen, err)
		}
		file, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		it, err := pebble.NewExternalIter((&pebble.Options{}).EnsureDefaults(), nil, [][]sstable.ReadableFile{{file}})
		if err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
		for sample := 0; sample < 100; sample++ {
			i := random.IntN(count)
			if !it.SeekGE(keys[i]) || !bytes.Equal(it.Key(), keys[i]) || !bytes.Equal(it.Value(), values[i]) {
				t.Fatalf("forward seek %d: %v", i, it.Error())
			}
			if i > 0 && (!it.SeekLT(keys[i]) || !bytes.Equal(it.Key(), keys[i-1]) || !bytes.Equal(it.Value(), values[i-1])) {
				t.Fatalf("backward seek %d: %v", i, it.Error())
			}
		}
		if err := it.Close(); err != nil {
			t.Fatal(err)
		}
		if pass == 1 {
			db, err := pebble.Open(filepath.Join(root, "db"), &pebble.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if err := db.Ingest([]string{path}); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < count; i += 113 {
				value, closer, err := db.Get(keys[i])
				if err != nil {
					t.Fatal(err)
				}
				ok := bytes.Equal(value, values[i])
				_ = closer.Close()
				if !ok {
					t.Fatalf("ingest changed record %d", i)
				}
			}
		}
	}
}

func BenchmarkDenseReusableZstdBlock(b *testing.B) {
	encoder, err := newDenseEncoder()
	if err != nil {
		b.Fatal(err)
	}
	input := bytes.Repeat([]byte("archive-metric-slot-value-order-frontier-"), 6500)
	var output []byte
	output, err = encoder.append(input, output, denseRestLevel)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(input)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		output, err = encoder.append(input, output[:0], denseRestLevel)
		if err != nil {
			b.Fatal(err)
		}
	}
}
