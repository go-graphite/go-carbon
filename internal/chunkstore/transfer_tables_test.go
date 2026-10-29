package chunkstore

import (
	"bytes"
	"context"
	"testing"
)

func TestTransferTableVerificationRejectsMismatch(t *testing.T) {
	s, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, s)
	ctx := context.Background()
	keys := []RelayoutKV{{[]byte("c/one"), []byte("value")}, {[]byte("c/two"), []byte("other")}}
	path := s.fs.PathJoin(s.dir, "test.sst")
	if err := s.writeTransferTable(ctx, path, keys); err != nil {
		t.Fatal(err)
	}
	for _, changed := range [][]RelayoutKV{keys[:1], {{[]byte("c/one"), []byte("wrong")}, keys[1]}, {{[]byte("c/missing"), keys[0].Value}, keys[1]}} {
		if err := s.verifyTransferTable(ctx, path, changed); err == nil {
			t.Fatal("staged mismatch accepted")
		}
	}
	f, err := s.fs.OpenReadWrite(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(bytes.Repeat([]byte{255}, 16), 0); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.verifyTransferTable(ctx, path, keys); err == nil {
		t.Fatal("corrupt table accepted")
	}
}
