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
					"1|1s:1h,1m:1d,1h:1y":         "01accbf6a1d9cb2abc53794bc83d80b060831ca12836bc08f57800213fe65275",
					"1|10s:6h,5m:7d,1h:1y,1d:10y": "6bbac1a8f952b51b7f6851c492139967d9c60e3544a6393f3735d5ae31f33bb5",
					"2|1s:1h,1m:1d,1h:1y":         "e486b8fec32a0168f4a2ec10e425246d4b672120f53d5e183d567bc171475e06",
					"2|10s:6h,5m:7d,1h:1y,1d:10y": "3b890b4fc755759d2b64fb7b4685911f079c0cfeaa97377079359701e8a8d710",
					"3|1s:1h,1m:1d,1h:1y":         "622017fa321e4aa780af44af056e02e75c07a20aab46740398c3d7f447aa0bf4",
					"3|10s:6h,5m:7d,1h:1y,1d:10y": "44d287d32cc86f12a47ce7b31c3b0845d00b8a1efdb603d1eb7c8eb242710753",
					"4|1s:1h,1m:1d,1h:1y":         "05ce7a5925cba87b97235b710ba6aa584a524f9800dedf22fc4aa9e248758642",
					"4|10s:6h,5m:7d,1h:1y,1d:10y": "380215e96af296cdd1fea6ebdb052757440a38ac3ab9cc45bcd756d4b44506c8",
					"5|1s:1h,1m:1d,1h:1y":         "18085a1390c0735facf28f6699da1fe6616ade5c00a723feb2c55914b19dc9a9",
					"5|10s:6h,5m:7d,1h:1y,1d:10y": "3a44390467c449d3e959f8aaef15b8c86840adf9dd91ee09cf5aa2c93b409b3c",
					"6|1s:1h,1m:1d,1h:1y":         "d6f8e3de2df05f04243069956dec0fe5ef37b5134bae46516a9cacf18d8308aa",
					"6|10s:6h,5m:7d,1h:1y,1d:10y": "f43b0781e16de59ec1d8e5c894b21306268eae84857ef7e63e90d9847b32af52"}[fmt.Sprintf("%d|%s", method, schema)]
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
