package statestore

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path/filepath"
	"testing"
)

func TestImportCursorAtomicityAndSourceBinding(t *testing.T) {
	for _, backend := range []string{"sqlite", "pebble"} {
		t.Run(backend, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state")
			s, err := Open(backend, path)
			if err != nil {
				t.Fatal(err)
			}
			h := sha256.Sum256([]byte("immutable input"))
			source := hex.EncodeToString(h[:])
			before, err := s.ImportPosition(source)
			if err != nil {
				t.Fatal(err)
			}
			after := before
			after.Offset, after.Records = 100, 1
			rows := []Record{{Bucket: "proofs", ID: 5, Payload: []byte("exact proof")}}
			func() {
				defer func() {
					if recover() == nil {
						t.Error("missing injected interruption")
					}
				}()
				_ = s.ImportBatch(before, after, rows, func() { panic("before commit") })
			}()
			if _, err = s.Get("proofs", 5); !errors.Is(err, ErrNotFound) {
				t.Fatalf("partial batch: %v", err)
			}
			if got, err := s.ImportPosition(source); err != nil || got != before {
				t.Fatal(got, err)
			}
			if err = s.ImportBatch(before, after, rows, nil); err != nil {
				t.Fatal(err)
			}
			if err = s.ImportBatch(before, after, rows, nil); err == nil {
				t.Fatal("accepted stale cursor")
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = Open(backend, path)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if got, err := s.ImportPosition(source); err != nil || got != after {
				t.Fatal(got, err)
			}
			if _, err = s.ImportPosition(hex.EncodeToString(make([]byte, 32))); err == nil {
				t.Fatal("accepted changed input")
			}
		})
	}
}
