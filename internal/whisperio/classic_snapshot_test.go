package whisperio

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func requireSnapshotPointBits(t testing.TB, want, got store.Snapshot) {
	t.Helper()
	if len(want.Archives) != len(got.Archives) {
		t.Fatal("archive count differs")
	}
	for i, a := range want.Archives {
		b := got.Archives[i]
		if a.Retention != b.Retention || len(a.Points) != len(b.Points) {
			t.Fatalf("archive %d shape differs: %d != %d", i, len(a.Points), len(b.Points))
		}
		for j, p := range a.Points {
			q := b.Points[j]
			if p.Timestamp != q.Timestamp || math.Float64bits(p.Value) != math.Float64bits(q.Value) {
				t.Fatalf("archive %d point %d: (%d,%x) != (%d,%x)", i, j, p.Timestamp, math.Float64bits(p.Value), q.Timestamp, math.Float64bits(q.Value))
			}
		}
	}
}

func TestClassicSnapshotMatchesWhisper(t *testing.T) {
	ctx := context.Background()
	fast, old := &ContinuationCodec{}, &ContinuationCodec{keepClassicSnapshotReads: true}
	fixtures := classicDiffFixtures(t)
	// Exercise wire timestamp truncation, zero slots and all float bit classes.
	rng := rand.New(rand.NewSource(9881))
	slots := map[int]store.Point{}
	bits := []uint64{0, 1, 1 << 63, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000042, 0x7ff0000000000001, 0xffffffffffffffff}
	for i := 0; i < 513; i++ {
		ts := int64(rng.Intn(30))
		if i%3 == 0 {
			ts += 1 << 32
		}
		if i%7 == 0 {
			ts = -ts
		}
		slots[i] = store.Point{Timestamp: ts, Value: math.Float64frombits(bits[i%len(bits)])}
	}
	kv, m := placedClassicImage(t, []store.Retention{{Step: 1, Count: 513}}, []map[int]store.Point{slots})
	fixtures = append(fixtures, classicDiffFixture{"wire-bits", kv, m, nil})
	for _, fx := range fixtures {
		t.Run(fx.name, func(t *testing.T) {
			want, err := old.Snapshot(ctx, fx.kv, fx.m, classicTestClock)
			if err != nil {
				t.Fatal(err)
			}
			got, err := fast.Snapshot(ctx, fx.kv, fx.m, classicTestClock)
			if err != nil {
				t.Fatal(err)
			}
			requireSnapshotPointBits(t, want, got)
		})
	}
}

// Both stores run the real import/merge/restore path; only snapshot decoding
// differs. Repeated overlapping imports exercise existing-target precedence.
func TestClassicSnapshotImportStoredPoints(t *testing.T) {
	ctx := context.Background()
	dbs := make([]*store.Store, 2)
	for i, codec := range []*ContinuationCodec{{keepClassicSnapshotReads: true}, {}} {
		opts := coldOptions(codec)
		opts.Now = classicTestClock
		db, err := store.Open(t.TempDir(), opts)
		if err != nil {
			t.Fatal(err)
		}
		db.StopBackground()
		defer db.Close()
		dbs[i] = db
	}
	rng := rand.New(rand.NewSource(334))
	for round := 0; round < 5; round++ {
		var records []store.TransferMetric
		for n := 0; n < 8; n++ {
			slots := map[int]store.Point{}
			for j := 0; j < 200; j++ {
				slots[rng.Intn(513)] = store.Point{Timestamp: int64(999000 + rng.Intn(70)*10), Value: math.Float64frombits(rng.Uint64())}
			}
			kv, m := placedClassicImage(t, []store.Retention{{Step: 10, Count: 513}}, []map[int]store.Point{slots})
			m.Name = fmt.Sprintf("snapshot.%d", n)
			records = append(records, classicTransfer(kv, store.Snapshot{Metadata: m}))
		}
		for _, db := range dbs {
			if _, err := db.ImportTransfer(ctx, records); err != nil {
				t.Fatal(err)
			}
		}
		for _, r := range records {
			a, err := dbs[0].Snapshot(ctx, r.Config.Name)
			if err != nil {
				t.Fatal(err)
			}
			b, err := dbs[1].Snapshot(ctx, r.Config.Name)
			if err != nil {
				t.Fatal(err)
			}
			requireSnapshotPointBits(t, a, b)
		}
	}
}

func BenchmarkClassicSnapshot(b *testing.B) {
	slots := map[int]store.Point{}
	for i := 0; i < 50400; i++ {
		slots[i] = store.Point{Timestamp: int64(1000000 + i*60), Value: float64(i) + .1}
	}
	kv, m := placedClassicImage(b, []store.Retention{{Step: 60, Count: 50400}}, []map[int]store.Point{slots})
	for _, old := range []bool{true, false} {
		b.Run(fmt.Sprintf("byte-reader=%t", old), func(b *testing.B) {
			c := &ContinuationCodec{keepClassicSnapshotReads: old}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := c.Snapshot(context.Background(), kv, m, classicTestClock); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
