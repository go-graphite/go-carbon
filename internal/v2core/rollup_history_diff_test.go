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
	rollupHistoryDifferential(t, rollupHistoryGolden)
}

// TestRollupHistoryDifferentialLegacyKinds proves the record-kind format
// changed nothing else: parts written without kinds keep the goldens recorded
// before kinds existed, byte for byte.
func TestRollupHistoryDifferentialLegacyKinds(t *testing.T) {
	if coreTestProcess(t) {
		return
	}
	// Parallel subtests outlive this function: reset in a cleanup.
	v2part.LegacyKinds.Store(true)
	t.Cleanup(func() { v2part.LegacyKinds.Store(false) })
	rollupHistoryDifferential(t, rollupHistoryLegacyGolden)
}

func rollupHistoryDifferential(t *testing.T, golden map[string]string) {
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
					orderProofObserve(t, t.Name(), e)
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
				id := fmt.Sprintf("%d|%s", method, schema)
				if want := golden[id]; digest != want {
					t.Fatalf("stored/read bit differential %q got %s want %s", id, digest, want)
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

// rollupHistoryLegacyGolden was recorded before record kinds existed.
var rollupHistoryLegacyGolden = map[string]string{
	"1|1s:1h,1m:1d,1h:1y":         "01accbf6a1d9cb2abc53794bc83d80b060831ca12836bc08f57800213fe65275",
	"1|10s:6h,5m:7d,1h:1y,1d:10y": "1b0e7bec63f57c7d63437c0df0145c759941894498952f281150cd1670a77302",
	"2|1s:1h,1m:1d,1h:1y":         "e486b8fec32a0168f4a2ec10e425246d4b672120f53d5e183d567bc171475e06",
	"2|10s:6h,5m:7d,1h:1y,1d:10y": "2b206843a35585dbb11b861e77b78f05cc480c0afae43c50d53e0cfe9a5e6c5c",
	"3|1s:1h,1m:1d,1h:1y":         "622017fa321e4aa780af44af056e02e75c07a20aab46740398c3d7f447aa0bf4",
	"3|10s:6h,5m:7d,1h:1y,1d:10y": "aa48520e6485ceefe46790e045a82ea6cdd9f59e624c2a59d27cbfdc4fc42a63",
	"4|1s:1h,1m:1d,1h:1y":         "05ce7a5925cba87b97235b710ba6aa584a524f9800dedf22fc4aa9e248758642",
	"4|10s:6h,5m:7d,1h:1y,1d:10y": "30f5d846ef7dd26c9c7c078e53baca02e2a1d32d02a0d3b2f180cdde8ca44432",
	"5|1s:1h,1m:1d,1h:1y":         "18085a1390c0735facf28f6699da1fe6616ade5c00a723feb2c55914b19dc9a9",
	"5|10s:6h,5m:7d,1h:1y,1d:10y": "a34b90a26ff9d152a54e3bf7d307a145f79065aac596616823dae69370fd4531",
	"6|1s:1h,1m:1d,1h:1y":         "d6f8e3de2df05f04243069956dec0fe5ef37b5134bae46516a9cacf18d8308aa",
	"6|10s:6h,5m:7d,1h:1y,1d:10y": "efa3119cc25ec2a8b1a928fbfe1707e2b7952b252e26dec17e4af8c0bde59ac0"}

// rollupHistoryGolden adds the record versions and kinds column of runs that
// hold superseded versions or the seal's closed windows (Mat); the resolved
// records are the legacy ones (TestRollupHistoryDifferentialLegacyKinds).
var rollupHistoryGolden = map[string]string{
	"1|1s:1h,1m:1d,1h:1y":         "2440ffc621b36523dd3c14629244905851e25dfedd553ea17e6732b82fc105a1",
	"1|10s:6h,5m:7d,1h:1y,1d:10y": "0038ea065ec22b1508a85fa6780bad3bee08b6b547a8956b2c76831a1657e445",
	"2|1s:1h,1m:1d,1h:1y":         "4dcda2f13f43192eb4ac164d4e4354fd99045855dcfa7176f524f8f8364e0f6d",
	"2|10s:6h,5m:7d,1h:1y,1d:10y": "d42688fa5e78be2c23624bac7afe3e316e81a49bf19daf408afc696bf4ac3c9b",
	"3|1s:1h,1m:1d,1h:1y":         "09e8e11baee3432b97366f573dc80cda11ee12a8cb68727928593bf6e3e9e2e8",
	"3|10s:6h,5m:7d,1h:1y,1d:10y": "9bed0f9c77f8b4ade555fb3a4e8a4ed30c703f4ff6327058bdfef33de3c82af1",
	"4|1s:1h,1m:1d,1h:1y":         "312b2da0496b4c2055c01fe9315924faea2c15c0ece108730e2310385fa936be",
	"4|10s:6h,5m:7d,1h:1y,1d:10y": "35faec7ce58f1da47ab1f0a5926bbd0e305293e6012dc63db746bdf27f2cda5e",
	"5|1s:1h,1m:1d,1h:1y":         "09d8fb4918e6ba2b94f0f86fafa8f8bba82f6dd76a5185ba9ecd0c089e67bb39",
	"5|10s:6h,5m:7d,1h:1y,1d:10y": "6145fc75706f854d207b7036e9d84e3010df88845d7c7a6c2fa9833195059cc6",
	"6|1s:1h,1m:1d,1h:1y":         "75cafb322e2fe7c95fb9ebb866913c80f8578c3f9922010193bc6de2fe526974",
	"6|10s:6h,5m:7d,1h:1y,1d:10y": "cb072cced78337817d1ace7e160053b44e7e69ebf8f67a10136e134f1117a15e"}
