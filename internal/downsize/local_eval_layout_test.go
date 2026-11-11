package downsize

import (
	"bytes"
	"encoding/binary"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestEvalRecordLayoutDigestReboundIndex(t *testing.T) {
	key := make([]byte, 10)
	key[0], key[9] = 'f', 'w'
	binary.BigEndian.PutUint64(key[1:], 1)
	value := make([]byte, 17)
	value[0] = 2
	binary.BigEndian.PutUint64(value[1:], 7)
	binary.BigEndian.PutUint32(value[9:], 100)
	binary.BigEndian.PutUint32(value[13:], 200)
	digest := func(key, value []byte) string {
		return evalRecordLayoutDigest(store.TransferMetric{Keys: []store.RelayoutKV{{Key: key, Value: value}}})
	}
	want := digest(key, value)
	rebound := bytes.Clone(value)
	binary.BigEndian.PutUint64(rebound[1:], 42)
	if digest(key, rebound) != want {
		t.Fatal("receiving revision changed the physical-layout proof")
	}
	if binary.BigEndian.Uint64(value[1:]) != 7 {
		t.Fatal("layout proof mutated captured input")
	}
	for _, offset := range []int{9, 13} {
		changed := bytes.Clone(value)
		changed[offset] ^= 1
		if digest(key, changed) == want {
			t.Fatalf("index anchor/bound change at %d escaped the proof", offset)
		}
	}
	physicalKey := bytes.Clone(key)
	physicalKey[9] = 'h'
	if digest(physicalKey, value) == digest(physicalKey, rebound) {
		t.Fatal("physical-header changes escaped the proof")
	}
}
