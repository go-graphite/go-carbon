package main

import (
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

func TestScanOriginalReceipts(t *testing.T) {
	db, err := pebble.Open("receipts", &pebble.Options{FS: vfs.NewMem()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, x := range []struct {
		name string
		id   uint64
	}{{"a", 11}, {"b", 3}} {
		var data [65]byte
		data[0] = 1
		binary.BigEndian.PutUint64(data[1:9], x.id)
		binary.BigEndian.PutUint64(data[9:17], 1)
		binary.BigEndian.PutUint64(data[17:25], 2)
		if err = db.Set([]byte(x.name), data[:], pebble.Sync); err != nil {
			t.Fatal(err)
		}
	}
	count, bound, err := scan(db, sha256.New())
	if err != nil || count != 2 || bound != 11 {
		t.Fatalf("%d/%d/%v", count, bound, err)
	}
	if err = db.Set([]byte("broken"), []byte{1}, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if _, _, err = scan(db, sha256.New()); err == nil {
		t.Fatal("partial proof accepted")
	}
}
