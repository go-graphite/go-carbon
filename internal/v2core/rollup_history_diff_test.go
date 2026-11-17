package v2core

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// This fixture deliberately uses only pre-history-cache APIs so it can also run
// against an overlay of the unchanged reducer runtime.
func TestRollupHistoryDifferential(t *testing.T) {
	// This golden guards the no-merge physical layout, including superseded
	// replacements. Compaction legitimately changes that layout. Logical exactness
	// with merges, retirement and reopen is TestSealedMergeHistoryDifferential.
	t.Setenv("CORE_MERGE_WORKERS", "0")
	for method := sealrollup.Average; method <= sealrollup.First; method++ {
		for _, schema := range []string{"1s:1h,1m:1d,1h:1y", "10s:6h,5m:7d,1h:1y,1d:10y"} {
			t.Run(fmt.Sprintf("%d/%s", method, schema), func(t *testing.T) {
				_, policy := rollupTestPolicy(t, "history.metric", schema, method, .1)
				cfg := coreTestConfig()
				cfg.Shards = 1
				cfg.Producers = 1
				cfg.Rollups = true
				cfg.MaxRows = 1024

				root := t.TempDir()
				e, err := Open(root, cfg, Hooks{Now: func() time.Time { return time.Unix(1, 0) }})
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if e != nil {
						_ = e.Close()
					}
				}()
				h := sha256.New()
				var scratch [8]byte
				put := func(v uint64) { binary.LittleEndian.PutUint64(scratch[:], v); h.Write(scratch[:]) }
				now := int64(1794700800)
				var order uint64
				bits := []uint64{0x7ff8000000000042, 1 << 63, 0, 0x7ff0000000000000, 0xfff0000000000000, 1, math.Float64bits(7)}
				for epoch := int64(0); epoch < 40; epoch++ {
					now += 600
					order++
					var rows []Row
					for j := int64(0); j < 60; j++ {
						rows = append(rows, Row{ID: 1, Time: now - 600 + j*10, Bits: bits[(j+epoch)%int64(len(bits))], Seq: uint64(j + 1)})
					}
					for _, age := range []int64{120, 1800, 3500, 7200, 18000, 100000} {
						stamp := now - age
						rows = append(rows, Row{ID: 1, Time: stamp, Bits: math.Float64bits(float64(epoch)), Seq: 100}, Row{ID: 1, Time: stamp, Bits: bits[epoch%int64(len(bits))], Seq: 101})
					}
					if err = e.AppendBatches(0, []Batch{{ID: 1, Policy: policy, Order: order, Clock: now, Points: rows}}, order, true); err != nil {
						t.Fatal(err)
					}
					snap := e.DBs()[0].NewSnapshot()
					// Hash full physical replacements as published, normalizing only the
					// random part ID. A second digest surface is resolved archive reads.
					it, err := newArchiveIter(context.Background(), snap, nil)
					if err != nil {
						t.Fatal(err)
					}
					var entries []string
					for valid := it.First(); valid; valid = it.Next() {
						key := it.Key()
						if len(key) < 4 || string(key[:4]) != "GCP\x03" {
							continue
						}
						k, err := decodeHistoryKey(key)
						if err != nil {
							t.Fatal(err)
						}
						entries = append(entries, k+string(it.Value()))
					}
					if err = it.Close(); err != nil {
						t.Fatal(err)
					}
					sort.Strings(entries)
					for _, entry := range entries {
						put(uint64(len(entry)))
						h.Write([]byte(entry))
					}
					index, err := NewRollupSnapshot(context.Background(), snap, nil, 0)
					if err != nil {
						t.Fatal(err)
					}
					rs, err := index.Records(context.Background(), "history.metric")
					if err != nil {
						t.Fatal(err)
					}
					sort.Slice(rs, func(i, j int) bool {
						if rs[i].Archive != rs[j].Archive {
							return rs[i].Archive < rs[j].Archive
						}
						return rs[i].Slot < rs[j].Slot
					})
					for _, r := range rs {
						put(uint64(r.Archive))
						put(uint64(r.Slot))
						h.Write(sealrollup.EncodeRecord(r))
					}
					snap.Close()
					if epoch == 17 || epoch == 31 {
						if err = e.Close(); err != nil {
							t.Fatal(err)
						}
						e, err = Open(root, cfg, Hooks{Now: func() time.Time { return time.Unix(1, 0) }})
						if err != nil {
							t.Fatal(err)
						}
					}
				}
				digest := fmt.Sprintf("%x", h.Sum(nil))
				// Closed-window layout: open coarse replacements are projected at read
				// time. TestClosedHistoryLogicalDifferential proves the resolved values
				// equal the original eager golden at every epoch and restart cut.
				want := map[string]string{
					"1|1s:1h,1m:1d,1h:1y":         "5ff4627495805dfa52671347ff9c9fda65844389d6eaf5e11c3793030135c222",
					"1|10s:6h,5m:7d,1h:1y,1d:10y": "a1cdd4e044beb9c75df507f883b05cf88d8342dce193dc7fe8e5a1b7b87235e1",
					"2|1s:1h,1m:1d,1h:1y":         "3803b50b07deb9babbfa7815a97177e3e7a47108f87f1378d3db55774203f140",
					"2|10s:6h,5m:7d,1h:1y,1d:10y": "ba3b0de4e0ffc6ddbd9744226407af15b70cff4e4a0592ec6965549c16502137",
					"3|1s:1h,1m:1d,1h:1y":         "abd5a6d99173f927f0e741c6fdc343a74ad21ba5d38339874d0b6571c551be87",
					"3|10s:6h,5m:7d,1h:1y,1d:10y": "0d1f878a458e830802f1807a069af6927028a2cfe97da3cf468b8e0a260a389f",
					"4|1s:1h,1m:1d,1h:1y":         "008bf53abf9608def2dc5f581f6a2fd4c8cce03f4cf000d4547fe290c5672005",
					"4|10s:6h,5m:7d,1h:1y,1d:10y": "889ec59e38afa0b1836aefdfaf779a0130516cabcf233cbb95c579773fe85be8",
					"5|1s:1h,1m:1d,1h:1y":         "4aad9a654fe1fee00c53bf117f0b5f758aa80bb3a929530c296a1de14b3143e8",
					"5|10s:6h,5m:7d,1h:1y,1d:10y": "88c9def57cd5e9d5708ea4a03ed96121126de5a0156e749c24fb0e474a660059",
					"6|1s:1h,1m:1d,1h:1y":         "944432f1baab5a0fce6ee9f3f9b7788d3a1f84181280c24efc558d16ef9b38d3",
					"6|10s:6h,5m:7d,1h:1y,1d:10y": "c3bbff239946ea79ed39be7a002bf038069f2279ee358158b8a597570f1ce815"}[fmt.Sprintf("%d|%s", method, schema)]
				if digest != want {
					t.Fatalf("stored/read bit differential got %s want %s", digest, want)
				}
				t.Logf("history digest %s", digest)
			})
		}
	}
}

func decodeHistoryKey(key []byte) (string, error) {
	k, err := v2part.Decode(key)
	if err != nil {
		return "", err
	}
	k.Part = v2part.PartID{}
	b, err := v2part.Encode(k)
	return string(b), err
}
