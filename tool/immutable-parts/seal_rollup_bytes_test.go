package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"testing"
)

// This logical SST content receipt is also run against the preceding adapter.
// Pebble file numbers and compaction layout are excluded; every stored key and
// value (including raw receipts, archive run bytes and frontier) is included.
func TestCompactStoredLogicalBytes(t *testing.T) {
	cfg := coreRollupTestConfig()
	cfg.Shards = 1
	e, _, err := openCoreRollups(t.TempDir(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e.close()
	var h compactRollupHead
	inputs := atomicBenchInput(24, 118, 4)
	for seal := 1; seal <= 3; seal++ {
		advanceAtomicInputs(inputs, uint64(seal), 118)
		for _, in := range inputs {
			policy, _ := json.Marshal(in.Config)
			for _, b := range in.Batches {
				rows := make([]row, len(b.Points))
				for i, p := range b.Points {
					bits := p.Bits
					if i == 3 {
						bits = 1 << 63
					}
					if i == 5 {
						bits = 0x7ff80000000000ab
					}
					if i == 7 {
						bits = math.Float64bits(math.Inf(1))
					}
					rows[i] = row{ID: in.ID, Time: p.Time, Bits: bits, Seq: p.Sequence}
				}
				if err := h.append(coreBatch{ID: in.ID, Order: b.Order, Clock: b.Now, Policy: policy, Points: rows}); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err = e.submitCompactRollupGroup(0, &h, uint64(seal)); err != nil {
			t.Fatal(err)
		}
		h.reset()
	}
	it, err := e.owners[0].db.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	digest := sha256.New()
	count := 0
	for it.First(); it.Valid(); it.Next() {
		for _, v := range [][]byte{it.Key(), it.Value()} {
			digest.Write(binary.BigEndian.AppendUint64(nil, uint64(len(v))))
			digest.Write(v)
		}
		count++
	}
	if err = it.Error(); err != nil {
		t.Fatal(err)
	}
	fmt.Printf("COMPACT_LOGICAL_BYTES entries=%d sha256=%x\n", count, digest.Sum(nil))
	if count != 410 || fmt.Sprintf("%x", digest.Sum(nil)) != "9446c95d19ac1cbdb25b0d73e16867d92c04040e7288b23c2d91a91bb9a55fd4" {
		t.Fatal("persisted keys/values differ from the preceding adapter")
	}
}
