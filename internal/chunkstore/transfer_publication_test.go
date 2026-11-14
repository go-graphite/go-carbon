package chunkstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math/rand"
	"testing"

	"github.com/cockroachdb/pebble"
)

// Freeze the previous single atomic commit as the oracle. Compare every stored
// byte after staging duplicates, private deletes, old-generation retirement,
// partitioned continuation keys, and indivisible oversized values.
func TestTransferPublicationStoredBytesDifferential(t *testing.T) {
	open := func() *Store {
		s, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
		t.Cleanup(func() { closeTestStore(t, s) })
		return s
	}
	oracle, staged := open(), open()
	metas := []Metadata{{ID: 100, Generation: 1}, {ID: 101, Generation: 2}}
	batch := oracle.db.NewBatch()
	defer batch.Close()
	rng := rand.New(rand.NewSource(8144))
	for i := range 600 {
		m := metas[i%2]
		key := append(chunkPrefix(m.ID, m.Generation), byte(i>>8), byte(i))
		if i%3 == 0 {
			key[0] = 'u'
		}
		if i%5 == 0 {
			key = append([]byte{'t', 0, 0, 0, byte(i)}, key...)
		}
		value := make([]byte, 2048)
		if i == 30 {
			value = make([]byte, 2*transferPublicationBytes)
		}
		rng.Read(value)
		// Include raw NaN payload, -0, infinities and subnormal bits.
		for j, bits := range []uint64{0x7ff8000000000042, 1 << 63, 0x7ff0000000000000, 0xfff0000000000000, 1} {
			binary.BigEndian.PutUint64(value[j*8:], bits)
		}
		if err := batch.Set(key, value, nil); err != nil {
			t.Fatal(err)
		}
		if i%7 == 0 {
			if err := batch.Delete(key, nil); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, key := range [][]byte{catalogKey("published"), revisionKey(metas[0]), transferReceiptKey(metas[0]), sidecarCatalogKey("published")} {
		if err := batch.Set(key, []byte("final"), nil); err != nil {
			t.Fatal(err)
		}
	}
	old := append(chunkPrefix(99, 1), 1)
	for _, s := range []*Store{oracle, staged} {
		if err := s.db.Set(old, []byte("old"), pebble.NoSync); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Delete(old, nil); err != nil {
		t.Fatal(err)
	}
	if err := staged.commitTransferPublication(context.Background(), batch, metas, nil); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	a, err := oracle.db.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := staged.db.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	x, y := a.First(), b.First()
	for x && y {
		if !bytes.Equal(a.Key(), b.Key()) || !bytes.Equal(a.Value(), b.Value()) {
			t.Fatalf("stored bytes differ at %x / %x", a.Key(), b.Key())
		}
		x, y = a.Next(), b.Next()
	}
	if x != y || a.Error() != nil || b.Error() != nil {
		t.Fatal("iterator length/error differs", a.Error(), b.Error())
	}
}

func TestTransferPublicationCanceledDoesNotPublish(t *testing.T) {
	s, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true})
	defer closeTestStore(t, s)
	b := s.db.NewBatch()
	defer b.Close()
	m := Metadata{ID: 100, Generation: 1}
	for _, key := range [][]byte{append(chunkPrefix(100, 1), 1), catalogKey("unpublished"), transferReceiptKey(m)} {
		if err := b.Set(key, []byte("data"), nil); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.commitTransferPublication(ctx, b, []Metadata{m}, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for _, key := range [][]byte{catalogKey("unpublished"), transferReceiptKey(m)} {
		_, closer, err := s.db.Get(key)
		if closer != nil {
			closer.Close()
		}
		if !errors.Is(err, pebble.ErrNotFound) {
			t.Fatal("cancellation published visibility key", err)
		}
	}
}
