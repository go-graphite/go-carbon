package whisperio

import (
	"context"
	"fmt"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// Identical out-of-scope batches over a durable proof scope. Historical groups
// stay referenced while the hot group's override is warmed before timing.
// Both cases include Pebble's indexed batch, codec and commit, without fsync.
func BenchmarkClassicMutationProof(b *testing.B) {
	for _, refs := range []bool{false, true} {
		for _, legacy := range []bool{true, false} {
			b.Run(fmt.Sprintf("referenced=%v/legacy=%v", refs, legacy), func(b *testing.B) {
				ctx := context.Background()
				const now = int64(1700000000)
				clock := func() time.Time { return time.Unix(now, 0) }
				points := make([]store.Point, 16384)
				for i := range points {
					points[i] = store.Point{Timestamp: now - int64(len(points)) + int64(i), Value: float64(i)}
				}
				kv, m := mutationClassicFixture(b, refs, []store.Retention{{Step: 1, Count: 32768}}, [][]store.Point{points}, clock)
				var codec store.ContinuationCodec = &ContinuationCodec{}
				if legacy {
					codec = &legacyMutationCodec{}
				}
				db, err := store.Open(b.TempDir(), store.Options{ExactBlocks: true, Continuation: codec, Now: clock, SyncInterval: time.Hour, CacheSize: 8 << 20, MemTableSize: 4 << 20})
				if err != nil {
					b.Fatal(err)
				}
				defer db.Close()
				if _, err = db.ImportTransfer(ctx, []store.TransferMetric{classicTransfer(kv, store.Snapshot{Metadata: m})}); err != nil {
					b.Fatal(err)
				}
				if p, _, err := db.MutationProof(ctx, m.Name, now-16000, now-4000); err != nil || p == "" {
					b.Fatal(p, err)
				}
				input := make([]store.Point, 30)
				for i := range input {
					input[i] = store.Point{Timestamp: now - 30 + int64(i), Value: 42}
				}
				if err := db.UpdateMany(ctx, m.Name, input); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					for j := range input {
						input[j].Value = float64(i + j)
					}
					if err := db.UpdateMany(ctx, m.Name, input); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
