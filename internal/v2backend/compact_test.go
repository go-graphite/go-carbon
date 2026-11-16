package v2backend

import (
	"encoding/binary"
	"math"
	"math/rand/v2"
	"reflect"
	"testing"

	"github.com/go-graphite/go-carbon/internal/daemonstore"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

func TestCompactArchiveRecordsExactAndBounded(t *testing.T) {
	policy := daemonstore.Metric{Archives: []daemonstore.Archive{{Step: 1, Points: 60}, {Step: 10, Points: 60}}}
	rng := rand.New(rand.NewPCG(13, 42))
	want := make([]sealrollup.Record, compactRecords)
	for i := range want {
		a := i % 2
		stamp := (int64(1794700800) + int64(i)*10) / int64(policy.Archives[a].Step) * int64(policy.Archives[a].Step)
		r := &want[i]
		r.Archive, r.Slot, r.Order = a, stamp/int64(policy.Archives[a].Step)%60, rng.Uint64()|1
		r.Sample = sealrollup.Sample{Time: stamp, Bits: rng.Uint64(), Sequence: r.Order}
		r.Correction = i%3 == 0
	}
	for i, bits := range []uint64{0, 1, 0x8000000000000000, 0x7ff8000000000042, math.Float64bits(math.Inf(-1))} {
		want[i].Sample.Bits = bits
	}
	encoded, err := appendCompactRecords(nil, want, nil)
	if err != nil {
		t.Fatal(err)
	}
	var got []sealrollup.Record
	if err = visitCompactRecords(encoded, policy, nil, func(r sealrollup.Record) error { got = append(got, r); return nil }); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("compact records changed exact physical slots, bits, order or flags")
	}
	for _, size := range []int{0, 7, 8, 15, len(encoded) - 1} {
		if err = visitCompactRecords(encoded[:size], policy, nil, func(sealrollup.Record) error { return nil }); err == nil {
			t.Fatalf("accepted truncation at %d", size)
		}
	}
	bad := append([]byte(nil), encoded...)
	binary.BigEndian.PutUint32(bad[4:], compactRecords+1)
	if err = visitCompactRecords(bad, policy, nil, func(sealrollup.Record) error { return nil }); err == nil {
		t.Fatal("accepted oversized count")
	}
	if _, err = appendCompactRecords(nil, append(want, want[0]), nil); err == nil {
		t.Fatal("encoded oversized run")
	}
}
