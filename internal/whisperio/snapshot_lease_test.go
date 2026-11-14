package whisperio

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// Preserve all codec capabilities but use the previous allocating Snapshot
// for every import read, as an executable ownership oracle.
type allocatingSnapshotCodec struct{ *ContinuationCodec }

func (c allocatingSnapshotCodec) LeaseSnapshot(ctx context.Context, kv store.ContinuationIO, m store.Metadata, clock func() time.Time) (store.Snapshot, func(), error) {
	snapshot, err := c.Snapshot(ctx, kv, m, clock)
	return snapshot, nil, err
}

func TestSnapshotLeaseImportDifferential(t *testing.T) {
	for _, buckets := range []bool{false, true} {
		t.Run(fmt.Sprintf("buckets=%t", buckets), func(t *testing.T) {
			ctx := context.Background()
			donorOptions := coldOptions(&ContinuationCodec{})
			donorOptions.TimeBuckets = buckets
			donor, err := store.Open(t.TempDir(), donorOptions)
			if err != nil {
				t.Fatal(err)
			}
			defer donor.Close()
			donor.StopBackground()
			snap := coldFixture(t, donor)
			compressed, err := donor.ExportTransfer(ctx, []string{snap.Metadata.Name})
			if err != nil {
				t.Fatal(err)
			}
			var dbs []*store.Store
			for _, codec := range []store.ContinuationCodec{allocatingSnapshotCodec{&ContinuationCodec{keepSnapshotReadAllocations: true}}, &ContinuationCodec{}} {
				opts := coldOptions(codec)
				opts.TimeBuckets = buckets
				db, err := store.Open(t.TempDir(), opts)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				db.StopBackground()
				dbs = append(dbs, db)
			}
			for round := range 4 {
				var records []store.TransferMetric
				for n := range 6 {
					r := compressed[0]
					if n%2 == 0 {
						bits := []uint64{0, 1 << 63, 1, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000042}
						slots := map[int]store.Point{}
						for i := range 160 {
							slots[i] = store.Point{Timestamp: coldBase + int64((i*31+round*7)*60), Value: math.Float64frombits(bits[i%len(bits)])}
						}
						kv, m := placedClassicImage(t, snap.Metadata.Retentions, []map[int]store.Point{slots, {0: {Timestamp: coldBase / 3600 * 3600, Value: math.Copysign(0, -1)}}})
						r = classicTransfer(kv, store.Snapshot{Metadata: m})
					}
					r.Config.Name = fmt.Sprintf("lease.%d", n)
					r.Version.Name = r.Config.Name
					records = append(records, r)
				}
				var expected []store.TransferProof
				for i, db := range dbs {
					proofs, err := db.ImportTransfer(ctx, records)
					if err != nil {
						t.Fatalf("round %d store %d: %v", round, i, err)
					}
					if err := db.VerifyTransfer(ctx, proofs); err != nil {
						t.Fatal(err)
					}
					if i == 0 {
						expected = proofs
					} else if !reflect.DeepEqual(expected, proofs) {
						t.Fatal("proofs differ from allocating import")
					}
				}
				for _, r := range records {
					want, err := dbs[0].Snapshot(ctx, r.Config.Name)
					if err != nil {
						t.Fatal(err)
					}
					got, err := dbs[1].Snapshot(ctx, r.Config.Name)
					if err != nil {
						t.Fatal(err)
					}
					requireSnapshotPointBits(t, want, got)
				}
			}
		})
	}
}

func TestSnapshotLeasesDoNotAlias(t *testing.T) {
	codec := &ContinuationCodec{}
	for _, fixture := range classicDiffFixtures(t) {
		want, err := codec.Snapshot(context.Background(), fixture.kv, fixture.m, classicTestClock)
		if err != nil {
			t.Fatal(err)
		}
		a, releaseA, err := codec.LeaseSnapshot(context.Background(), fixture.kv, fixture.m, classicTestClock)
		if err != nil {
			t.Fatal(err)
		}
		b, releaseB, err := codec.LeaseSnapshot(context.Background(), fixture.kv, fixture.m, classicTestClock)
		if err != nil {
			t.Fatal(err)
		}
		for _, archive := range a.Archives {
			for i := range archive.Points {
				archive.Points[i] = store.Point{}
			}
		}
		requireSnapshotPointBits(t, want, b)
		releaseA()
		releaseB()
		c, releaseC, err := codec.LeaseSnapshot(context.Background(), fixture.kv, fixture.m, classicTestClock)
		if err != nil {
			t.Fatal(err)
		}
		requireSnapshotPointBits(t, want, c)
		releaseC()
	}
}

func BenchmarkSnapshotLease(b *testing.B) {
	slots := map[int]store.Point{}
	for i := range 50400 {
		slots[i] = store.Point{Timestamp: int64(1000000 + i*60), Value: float64(i) + .1}
	}
	kv, m := placedClassicImage(b, []store.Retention{{Step: 60, Count: 50400}}, []map[int]store.Point{slots})
	for _, leased := range []bool{false, true} {
		b.Run(fmt.Sprintf("leased=%t", leased), func(b *testing.B) {
			codec := &ContinuationCodec{}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if leased {
					_, release, err := codec.LeaseSnapshot(context.Background(), kv, m, classicTestClock)
					if err != nil {
						b.Fatal(err)
					}
					release()
				} else if _, err := codec.Snapshot(context.Background(), kv, m, classicTestClock); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkCompressedSnapshotLease(b *testing.B) {
	ctx := context.Background()
	base := int64(1794000000)
	clock := func() time.Time { return time.Unix(base, 0) }
	m := store.Metadata{MetricConfig: store.MetricConfig{Name: "snapshot.compressed", Retentions: []store.Retention{{Step: 60, Count: 20160}, {Step: 300, Count: 17280}, {Step: 3600, Count: 17520}}, AggregationMethod: store.Average, XFilesFactor: .5}}
	kv := mapContinuationIO{}
	codec := &ContinuationCodec{}
	if err := codec.Create(ctx, kv, m, clock); err != nil {
		b.Fatal(err)
	}
	points := make([]store.Point, 20000)
	for i := range points {
		points[i] = store.Point{Timestamp: base - int64(len(points)-i)*60, Value: float64(i%997) + .1}
	}
	if err := codec.Update(ctx, kv, m, points, -1, clock, nil); err != nil {
		b.Fatal(err)
	}
	for _, leased := range []bool{false, true} {
		b.Run(fmt.Sprintf("leased=%t", leased), func(b *testing.B) {
			codec := &ContinuationCodec{keepSnapshotReadAllocations: !leased}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if leased {
					_, release, err := codec.LeaseSnapshot(ctx, kv, m, clock)
					if err != nil {
						b.Fatal(err)
					}
					release()
				} else if _, err := codec.Snapshot(ctx, kv, m, clock); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
