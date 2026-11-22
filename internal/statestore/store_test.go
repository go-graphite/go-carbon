package statestore

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"
)

func TestCommitUniqueAndReplay(t *testing.T) {
	for _, kind := range []string{"sqlite", "pebble"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state")
			s, err := Open(kind, path)
			if err != nil {
				t.Fatal(err)
			}
			for _, x := range []struct {
				token uint64
				ids   []uint64
				added uint64
			}{{1, []uint64{1, 2, 2}, 2}, {1, []uint64{1, 2, 2}, 0}, {2, []uint64{2, 3}, 1}} {
				n, e := s.CommitWork(1, x.token, x.ids, nil)
				if e != nil || n != x.added {
					t.Fatalf("added=%d err=%v", n, e)
				}
			}
			if _, e := s.CommitWork(1, 1, []uint64{99}, nil); e == nil {
				t.Fatal("accepted token with changed membership")
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = Open(kind, path)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			b, e := s.Get("window", 1)
			if e != nil || string(b) != "3" {
				t.Fatalf("count=%s err=%v", b, e)
			}
			n, e := s.CommitWork(1, 2, []uint64{2, 3}, nil)
			if e != nil || n != 0 {
				t.Fatalf("reopen replay=%d %v", n, e)
			}
			if _, e = s.Get("done/1", 99); !errors.Is(e, ErrNotFound) {
				t.Fatal(e)
			}
		})
	}
}
func TestImportParityAndRollback(t *testing.T) {
	for _, kind := range []string{"sqlite", "pebble"} {
		t.Run(kind, func(t *testing.T) {
			s, e := Open(kind, filepath.Join(t.TempDir(), "db"))
			if e != nil {
				t.Fatal(e)
			}
			defer s.Close()
			want := []Record{{"a", 0, "label", []byte{0, 1, 255}}, {"a", 1, "", []byte("{}")}, {"z", 9, "", []byte("[]")}}
			if e = s.Apply(want); e != nil {
				t.Fatal(e)
			}
			var got []Record
			if e = s.Scan(func(r Record) error { r.Payload = append([]byte{}, r.Payload...); got = append(got, r); return nil }); e != nil {
				t.Fatal(e)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("got %#v", got)
			}
			func() {
				defer func() {
					if recover() == nil {
						t.Fatal("hook did not panic")
					}
				}()
				_, _ = s.CommitWork(8, 1, []uint64{1, 2}, func() { panic("power-cut boundary") })
			}()
			if _, e = s.Get("window", 8); !errors.Is(e, ErrNotFound) {
				t.Fatal("partial transaction", e)
			}
			n, e := s.CommitWork(8, 1, []uint64{1, 2}, nil)
			if e != nil || n != 2 {
				t.Fatal(n, e)
			}
		})
	}
}
