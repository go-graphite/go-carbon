package v2part

import (
	"bytes"
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

func TestDenseCopiedRunsAndReusableDecode(t *testing.T) {
	spec := fixtureSpec()
	spec.Encoding = DenseEncoding
	key := fixtureKey(spec, 0)
	key.Ordinal = 0
	key.BlockStart = 1
	var records []sealrollup.Record
	for i := int64(1); i <= 32; i++ {
		records = append(records, sealrollup.Record{Delta: sealrollup.Delta{Archive: int(key.Archive), Slot: i, Order: uint64(i), Sample: sealrollup.Sample{Time: 1000 + i, Bits: uint64(i) * 0x1000000001, Sequence: uint64(i)}}, Correction: i%3 == 0})
	}
	encoded, err := EncodeSlots(records)
	if err != nil {
		t.Fatal(err)
	}
	scratch := make([]sealrollup.Record, 0, 64)
	for pass := 0; pass < 3; pass++ {
		got, e := DecodeSlotsInto(scratch, uint32(records[0].Archive), 1, encoded)
		if e != nil {
			t.Fatal(e)
		}
		if !reflect.DeepEqual(got, records) {
			t.Fatal("reused decode changed exact bits/flags")
		}
		if &got[0] != &scratch[:cap(scratch)][0] {
			t.Fatal("decode did not reuse capacity")
		}
		// Dirty backing storage must not contaminate the next XOR/bit-plane decode.
		for i := range got {
			got[i].Sample.Bits = ^uint64(0)
		}
	}
	path := filepath.Join(t.TempDir(), "copied.sst")
	w, err := NewDenseWriter(path, spec)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	k, err := Encode(key)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.SetSlotsEncoded(k, encoded); err != nil {
		t.Fatal(err)
	}
	key.BlockStart = 33
	k, err = Encode(key)
	if err != nil {
		t.Fatal(err)
	}
	next := records[0]
	next.Slot = 33
	if err = w.SetRecordEncoded(k, next); err != nil {
		t.Fatal(err)
	}
	d, err := w.Close()
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	if err = Read(context.Background(), path, &d, func(_ Key, b []byte) error {
		if seen == 0 && !bytes.Equal(b, encoded) {
			t.Fatal("copied run was repacked")
		}
		seen++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if seen != 2 {
		t.Fatalf("physical runs=%d", seen)
	}
	for _, bad := range []string{"overlap", "malformed"} {
		t.Run(bad, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "bad.sst")
			w, e := NewDenseWriter(path, spec)
			if e != nil {
				t.Fatal(e)
			}
			defer w.Abort()
			key.BlockStart = 1
			k, e := Encode(key)
			if e != nil {
				t.Fatal(e)
			}
			if bad == "malformed" {
				if e = w.SetSlotsEncoded(k, encoded[:len(encoded)-1]); e == nil {
					t.Fatal("accepted truncated run")
				}
				return
			}
			if e = w.SetSlotsEncoded(k, encoded); e != nil {
				t.Fatal(e)
			}
			key.BlockStart = 16
			k, e = Encode(key)
			if e != nil {
				t.Fatal(e)
			}
			next.Slot = 16
			if e = w.SetRecordEncoded(k, next); e == nil {
				t.Fatal("accepted overlapping physical slots")
			}
		})
	}
}
