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
	if coreTestProcess(t) {
		return
	}
	// This golden guards the no-merge physical layout, including superseded
	// replacements. Compaction legitimately changes that layout. Logical exactness
	// with merges, retirement and reopen is TestSealedMergeHistoryDifferential.
	t.Setenv("CORE_MERGE_WORKERS", "0")
	for method := sealrollup.Average; method <= sealrollup.First; method++ {
		for _, schema := range []string{"1s:1h,1m:1d,1h:1y", "10s:6h,5m:7d,1h:1y,1d:10y"} {
			t.Run(fmt.Sprintf("%d/%s", method, schema), func(t *testing.T) {
				// Subtests own their engines; the parent's environment stays set
				// until all of them finish.
				t.Parallel()
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
						// Slim envelopes are hashed as the full encoding of what
						// they decode to, so the goldens prove decoded metadata,
						// including definition bytes, is unchanged.
						value := it.Value()
						if dk, err := v2part.Decode(key); err != nil {
							t.Fatal(err)
						} else if dk.Archive == v2part.MetadataArchive {
							m, err := DecodeRollupMetadata(dk.Metric, dk.Group, value)
							if err != nil {
								t.Fatal(err)
							}
							value = AppendRollupMetadata(nil, m)
						}
						entries = append(entries, k+string(value))
					}
					if err = it.Close(); err != nil {
						t.Fatal(err)
					}
					sort.Strings(entries)
					for _, entry := range entries {
						put(uint64(len(entry)))
						h.Write([]byte(entry))
					}
					index, err := NewRollupSnapshot(context.Background(), snap, nil, nil, nil, 0)
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
				// Closed-window layout in monthly partitions (#1546 keys and envelopes);
				// Hooks.Now caps age retirement, so the physical image is stable.
				// Open coarse replacements are projected at read
				// time. TestClosedHistoryLogicalDifferential proves the resolved values
				// equal the original eager golden at every epoch and restart cut.
				want := map[string]string{
					"1|1s:1h,1m:1d,1h:1y":         "27eff56f749b398edee4a0c95010870c10f2343a2f41c9ecf78e612a8854342e",
					"1|10s:6h,5m:7d,1h:1y,1d:10y": "456a99886c398e2609c32d9b069790e778e19e4dd10bb6eed2ca4e51dfbc176d",
					"2|1s:1h,1m:1d,1h:1y":         "9791e5be5c30b9533dfd4781cb425d414aac2e02e7bbb139ee12613e03150f1d",
					"2|10s:6h,5m:7d,1h:1y,1d:10y": "88cf4d606d332e820afc8f9c1acea432b3233514ebbc86a26fc6306dd3b30b0b",
					"3|1s:1h,1m:1d,1h:1y":         "846fffd8bbd5b082f9fbb91891f46288afa9d50e1da3c4b53f463448eaf12b27",
					"3|10s:6h,5m:7d,1h:1y,1d:10y": "a92bd0f9b43bb33ad1ab128c792466f722189c745c0406d30998d1770ba550b2",
					"4|1s:1h,1m:1d,1h:1y":         "50088ea3d8cde560c2debd1f73394988224236ab887b5a6f8eeb59ec1d5395ad",
					"4|10s:6h,5m:7d,1h:1y,1d:10y": "9ae1834a7c455cbd4a174509f78c59f5b1b3581f5b4eae600247c014f788e219",
					"5|1s:1h,1m:1d,1h:1y":         "c7418f9fb9c73b374c6c1966e5145f27a6b225ffcd04d4ad20b3dafeabd316ad",
					"5|10s:6h,5m:7d,1h:1y,1d:10y": "64a804304717a1d682e5fc072f21a36ffa7b415406e5dc4765b53f471226018e",
					"6|1s:1h,1m:1d,1h:1y":         "bfd00d5e469f75f00719857c7bc06dc0d0fef1a01a4dc8a3dcfba7f28ee1358c",
					"6|10s:6h,5m:7d,1h:1y,1d:10y": "1fa7741bce3b1a640c99ccb8e60f7efaf4c6b0f5e8f4da7befd20f27d47896ac"}[fmt.Sprintf("%d|%s", method, schema)]
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
