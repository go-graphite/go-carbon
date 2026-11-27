package v2part

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/sstable"
)

// Seal parts use small blocks so a render's per-part seek decompresses less;
// they must read record for record as the same records at the rest layout.
func TestDenseSealBlocksReadAsRest(t *testing.T) {
	spec := fixtureSpec()
	spec.Encoding = DenseEncoding
	const count = 3000
	keys, values := make([][]byte, count), make([][]byte, count)
	for i := range keys {
		key := fixtureKey(spec, uint32(i))
		key.Ordinal = 0
		key.BlockStart = int64(i)
		var err error
		if keys[i], err = Encode(key); err != nil {
			t.Fatal(err)
		}
		values[i] = bytes.Repeat([]byte(fmt.Sprintf("v%07d", i)), 128)
	}
	root := t.TempDir()
	var blocks [2]uint64
	var streams [2][]byte
	for mode, seal := range []bool{false, true} {
		path := filepath.Join(root, fmt.Sprintf("part-%v.sst", seal))
		writer, err := NewWriter(path, spec)
		if err != nil {
			t.Fatal(err)
		}
		if seal {
			(&DenseWriter{writer: writer}).SealCompression()
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
		if err = Read(context.Background(), path, &d, func(k Key, value []byte) error {
			encoded, err := Encode(k)
			streams[mode] = append(append(streams[mode], encoded...), value...)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		readable, err := sstable.NewSimpleReadable(f)
		if err != nil {
			t.Fatal(err)
		}
		r, err := sstable.NewReader(readable, sstable.ReaderOptions{})
		if err != nil {
			t.Fatal(err)
		}
		blocks[mode] = r.Properties.NumDataBlocks
		it, err := r.NewIter(nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		for i := count - 1; i >= 0; i -= 7 {
			k, v := it.SeekGE(keys[i], 0)
			if k == nil {
				t.Fatalf("seal=%v: seek %d missed", seal, i)
			}
			got, _, err := v.Value(nil)
			if err != nil || !bytes.Equal(k.UserKey, keys[i]) || !bytes.Equal(got, values[i]) {
				t.Fatalf("seal=%v: seek %d changed: %v", seal, i, err)
			}
		}
		if err = it.Close(); err != nil {
			t.Fatal(err)
		}
		if err = r.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(streams[0], streams[1]) {
		t.Fatal("seal block layout changed the logical record stream")
	}
	if blocks[1] < 6*blocks[0] {
		t.Fatalf("seal part has %d data blocks, rest part %d: seal blocks are not small", blocks[1], blocks[0])
	}
}
