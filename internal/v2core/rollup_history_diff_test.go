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
				// Closed-window layout in monthly partitions (#1546 keys and envelopes);
				// Hooks.Now caps age retirement, so the physical image is stable.
				// Open coarse replacements are projected at read
				// time. TestClosedHistoryLogicalDifferential proves the resolved values
				// equal the original eager golden at every epoch and restart cut.
				want := map[string]string{
					"1|1s:1h,1m:1d,1h:1y":         "711d843de47a3543728e05a9cf3db2943a3e6df58c459d1d10ef860867bf8f86",
					"1|10s:6h,5m:7d,1h:1y,1d:10y": "90a8a8637c45b3fcc696cbcecd43aa67848c67c275232d5778c51bba537d90c1",
					"2|1s:1h,1m:1d,1h:1y":         "1689867a0ea6614475de9230b82f630baea11be8d17614816f75bf1254fe3d4c",
					"2|10s:6h,5m:7d,1h:1y,1d:10y": "e20af49bc0966e7dc1547614b04532f9a9fda4abc9db88faf9ac8348591d9c6b",
					"3|1s:1h,1m:1d,1h:1y":         "2aa37fd3ce0ff4d2f2bb36198408dcedf60cd308272161cbdfbe15e6730e7a00",
					"3|10s:6h,5m:7d,1h:1y,1d:10y": "0d926c55f549bf6fa4eeb8de11933a80a8b97ec0644ed700348ea6e2eabd469e",
					"4|1s:1h,1m:1d,1h:1y":         "766c834575f4b86cf29495746f74d9c7c6df9fdffc13321988f1c3ca21c162d6",
					"4|10s:6h,5m:7d,1h:1y,1d:10y": "40fd7a65da98d4adbbb7c3e9b1b10b5d4017b277dadd758fbc992a4acd090be1",
					"5|1s:1h,1m:1d,1h:1y":         "eb4b7a9b26aeb15803826f45c8f5d9d7f2f33f79071cacf04406d36640bd5029",
					"5|10s:6h,5m:7d,1h:1y,1d:10y": "2e44270165585e6d4e76e3398d96097582995f926f89d7e0c6f58677bbafd9c1",
					"6|1s:1h,1m:1d,1h:1y":         "371e618d44f5c031f82542b6b31157ab792a67d99461e3007316e14ca9de32a8",
					"6|10s:6h,5m:7d,1h:1y,1d:10y": "8847ea828d04c7c7de06f703cda1f431ad2cb90772e8afe6ce43fe204b5c8adb"}[fmt.Sprintf("%d|%s", method, schema)]
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
