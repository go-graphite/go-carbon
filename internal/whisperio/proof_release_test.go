package whisperio

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestContinuationProofReleaseDifferential(t *testing.T) {
	for _, mode := range []string{"release", "reset", "ttl"} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			now := int64(1700000000)
			proofFrom, proofUntil := now-100, now-50
			clock := func() time.Time { return time.Unix(now, 0) }
			var stores [2]*store.Store
			for i := range stores {
				var ttl *time.Duration
				if mode == "ttl" && i == 0 {
					d := time.Second
					ttl = &d
				}
				db, err := store.Open(t.TempDir(), store.Options{MutationProofScopeTTL: ttl, ExactBlocks: true, Continuation: &ContinuationCodec{ReuseBlockKeys: true, DirectLateWrites: true}, Now: clock, SyncInterval: time.Hour, CacheSize: 1 << 20, MemTableSize: 1 << 20})
				if err != nil {
					t.Fatal(err)
				}
				stores[i] = db
				t.Cleanup(func() {
					if err := db.Close(); err != nil {
						t.Error(err)
					}
				})
				config := store.MetricConfig{Name: "proof", Retentions: []store.Retention{{Step: 1, Count: 512}, {Step: 8, Count: 256}}, AggregationMethod: store.Sum, XFilesFactor: .5}
				if _, err := db.Create(t.Context(), config); err != nil {
					t.Fatal(err)
				}
				var points []store.Point
				for ts := now - 200; ts < now; ts++ {
					points = append(points, store.Point{Timestamp: ts, Value: float64(ts % 31)})
				}
				if err := db.UpdateMany(t.Context(), "proof", points); err != nil {
					t.Fatal(err)
				}
			}
			proof := func(db *store.Store) string {
				t.Helper()
				p, _, err := db.MutationProof(t.Context(), "proof", proofFrom, proofUntil)
				if err != nil || p == "" {
					t.Fatal(p, err)
				}
				return p
			}
			old := proof(stores[0])
			proof(stores[1])
			if mode == "ttl" {
				now += 2
			} else if mode == "reset" {
				if _, err := stores[0].ResetMutationProofs(t.Context()); err != nil {
					t.Fatal(err)
				}
			} else {
				if r := stores[0].ReleaseMutationProofs(t.Context(), []store.ProofRelease{{Name: "proof"}}); r[0].Released != 1 {
					t.Fatal(r)
				}
			}
			stats := stores[0].ProofStats()
			snapshots := func(db *store.Store) []byte {
				t.Helper()
				s, err := db.Snapshot(t.Context(), "proof")
				if err != nil {
					t.Fatal(err)
				}
				var b bytes.Buffer
				for _, a := range s.Archives {
					if err := binary.Write(&b, binary.BigEndian, a.Points); err != nil {
						t.Fatal(err)
					}
				}
				return b.Bytes()
			}
			for seq, points := range [][]store.Point{
				{{Timestamp: now - 80, Value: 99}}, // In-window late correction while untracked.
				{{Timestamp: now - 79, Value: math.Copysign(0, -1)}, {Timestamp: now - 78, Value: math.Float64frombits(1)}},
				{{Timestamp: now + 8, Value: 7}},    // Propagate across the rollup boundary.
				{{Timestamp: now + 512, Value: 11}}, // Evict the circular owner.
			} {
				for _, db := range stores {
					if err := db.UpdateMany(context.Background(), "proof", points); err != nil {
						t.Fatalf("%d: %v", seq, err)
					}
				}
				if !bytes.Equal(snapshots(stores[0]), snapshots(stores[1])) {
					t.Fatalf("point bytes differ after batch %d", seq)
				}
			}
			end := stores[0].ProofStats()
			if mode == "ttl" && (end.ExpiredScopes != 1 || end.ExpiredHistories != 1) {
				t.Fatal(end)
			}
			if end.TrackedWrites != stats.TrackedWrites || end.UntrackedWrites != stats.UntrackedWrites+4 {
				t.Fatal("tracking did not stop", stats, end)
			}
			if fresh := proof(stores[0]); fresh == old {
				t.Fatal("old token survived untracked in-window correction")
			}
			if r := stores[0].ProofStats(); r.Registered != stats.Registered+1 {
				t.Fatal("scope was not freshly registered", r)
			}
		})
	}
}
