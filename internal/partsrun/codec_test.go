package partsrun

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math/rand"
	"reflect"
	"testing"
)

func TestExact(t *testing.T) {
	r := rand.New(rand.NewSource(14))
	for _, n := range []int{0, 1, 24, 118, 513} {
		for _, regular := range []bool{false, true} {
			want := make([]Sample, n)
			for i := range want {
				want[i] = Sample{Time: int64(r.Uint64()), Bits: r.Uint64(), Sequence: r.Uint64()}
				if regular {
					want[i].Time = 1794700800 + int64(i*60)
					want[i].Sequence = uint64(i + 1)
					want[i].Bits = uint64(i % 3)
				}
			}
			prefix := []byte("prefix")
			encoded, err := Append(prefix, 1794701000, n, func(i int) Sample { return want[i] })
			if err != nil || !bytes.Equal(encoded[:len(prefix)], prefix) {
				t.Fatal(err)
			}
			encoded = encoded[len(prefix):]
			h, err := Inspect(encoded, n)
			if err != nil || h.Count != n || h.Clock != 1794701000 {
				t.Fatal(h, err)
			}
			got := make([]Sample, 0, n)
			err = Visit(encoded, n, func(p Sample) error { got = append(got, p); return nil })
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatal(n, regular, err)
			}
			for end := 0; end < len(encoded); end++ {
				if Visit(encoded[:end], n, func(Sample) error { return nil }) == nil {
					t.Fatalf("accepted truncation %d/%d", end, len(encoded))
				}
			}
			if n > 0 {
				if _, err = Inspect(encoded, n-1); err == nil {
					t.Fatal("ignored manifest bound")
				}
			}
		}
	}
}
func TestInvalidAndVisitorFailure(t *testing.T) {
	encoded, err := Append(nil, 0, 1, func(int) Sample { return Sample{Bits: 0x7ff80000000000ab} })
	if err != nil {
		t.Fatal(err)
	}
	stop := errors.New("stop")
	if err = Visit(encoded, 1, func(Sample) error { return stop }); !errors.Is(err, stop) {
		t.Fatal(err)
	}
	if Visit(encoded, 1, nil) == nil {
		t.Fatal("nil visitor")
	}
	binary.BigEndian.PutUint64(encoded[2:], 1<<40)
	if _, err = Inspect(encoded, 1<<30); err == nil {
		t.Fatal("unbounded count")
	}
	if _, err = Append(nil, 0, -1, nil); err == nil {
		t.Fatal("negative count")
	}
}
