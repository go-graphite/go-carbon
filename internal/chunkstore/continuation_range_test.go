package chunkstore

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

func TestContinuationRangeSnapshot(t *testing.T) {
	s, _ := openTestStore(t, t.TempDir(), Options{fs: vfs.NewMem(), ExactBlocks: true})
	defer closeTestStore(t, s)
	m := Metadata{ID: 41, Generation: 3}
	io := newContinuationIO(context.Background(), s.db, nil, m)
	for _, suffix := range []string{"a", "b", "d", "e"} {
		if err := s.db.Set(io.key([]byte(suffix)), []byte(suffix), pebble.NoSync); err != nil {
			t.Fatal(err)
		}
	}
	view := s.db.NewSnapshot()
	defer view.Close()
	reader := newContinuationIO(context.Background(), view, nil, m)
	if err := s.db.Set(io.key([]byte("c")), []byte("new"), pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	var got []string
	if err := reader.VisitRange([]byte("b"), []byte("e"), func(k, v []byte) error { got = append(got, string(k)+string(v)); return nil }); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []string{"bb", "dd"}) {
		t.Fatal(got)
	}
	sentinel := errors.New("stop")
	if err := reader.VisitRange([]byte("b"), []byte("e"), func(k, v []byte) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	reader.ctx = ctx
	if err := reader.VisitRange([]byte("b"), []byte("e"), func(k, v []byte) error { t.Fatal("called after cancellation"); return nil }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
