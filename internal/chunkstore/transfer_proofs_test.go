package chunkstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"reflect"
	"testing"
)

func TestTransferProofBinaryMatchesJSONAndRejectsDamage(t *testing.T) {
	proofs := []TransferProof{
		{KeyCount: 3, Sampled: true, Source: TransferVersion{"source", 7, 2, 9}, Target: TransferVersion{"target", 44, 1, 3}, Digest: "digest", Counts: []int{1, 7}, Merged: true},
		{Counts: []int{}}, {},
	}
	encoded, err := EncodeTransferProofs(proofs)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ReadTransferProofs(bytes.NewReader(encoded))
	if err != nil || !reflect.DeepEqual(got, proofs) {
		t.Fatal("binary proof mismatch", got, err)
	}
	old, err := json.Marshal(proofs)
	if err != nil {
		t.Fatal(err)
	}
	var reference []TransferProof
	if err := json.Unmarshal(old, &reference); err != nil || !reflect.DeepEqual(got, reference) {
		t.Fatal("JSON proof mismatch", reference, err)
	}
	for n := range len(encoded) {
		if _, err := ReadTransferProofs(bytes.NewReader(encoded[:n])); err == nil {
			t.Fatal("truncated proof accepted", n)
		}
	}
	for i := range encoded {
		bad := bytes.Clone(encoded)
		bad[i] ^= 1
		if _, err := ReadTransferProofs(bytes.NewReader(bad)); err == nil {
			t.Fatal("corrupt proof accepted", i)
		}
	}
	bad := bytes.Clone(encoded)
	binary.BigEndian.PutUint32(bad[8:12], ^uint32(0))
	sum := sha256.Sum256(bad[:len(bad)-32])
	copy(bad[len(bad)-32:], sum[:])
	if _, err := ReadTransferProofs(bytes.NewReader(bad)); err == nil {
		t.Fatal("unbounded proof count accepted")
	}
	if _, err := ReadTransferProofs(bytes.NewReader(append(encoded, 0))); err == nil {
		t.Fatal("trailing proof bytes accepted")
	}
}
