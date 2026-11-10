package chunkstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"math"
	"testing"
)

func TestReadTransferRejectsOversizedKeyCount(t *testing.T) {
	header, err := json.Marshal(transferHeader{Keys: math.MaxInt})
	if err != nil {
		t.Fatal(err)
	}
	wire := append([]byte("BKTFR001"), 0, 0, 0, 1)
	wire = binary.BigEndian.AppendUint32(wire, uint32(len(header)))
	wire = append(wire, header...)
	sum := sha256.Sum256(wire)
	wire = append(wire, sum[:]...)
	if _, err := ReadTransfer(bytes.NewReader(wire), int64(len(wire))); err == nil {
		t.Fatal("accepted a key count larger than the remaining transfer body")
	}
}
