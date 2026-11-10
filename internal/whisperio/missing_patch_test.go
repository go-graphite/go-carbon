package whisperio

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/points"
)

func missingRequest(p store.FinitePatch) store.MissingPatch {
	r := store.MissingPatch{Expected: p.Expected, PlanSHA256: p.PlanSHA256, Nonce: p.Nonce, ParentProofSHA256: fmt.Sprintf("%064x", 2), ProjectionSHA256: fmt.Sprintf("%064x", 3), SourceBindingSHA256: fmt.Sprintf("%064x", 4), Points: []store.MissingPoint{
		{Archive: 0, Step: 60, Count: 20160, Timestamp: 1793976600, BeforeBits: "7ff8000000000042", DonorBits: "3ff0000000000000"},
		{Archive: 0, Step: 60, Count: 20160, Timestamp: 1793976660, DonorBits: "8000000000000000"},
		{Archive: 1, Step: 1800, Count: 35040, Timestamp: 1793973600, DonorBits: "402a000000000000"},
	}}
	r.BatchSHA256 = store.MissingPointHash(r.Points, "requested")
	return r
}
func TestMissingOnlyWholeArchiveAndDurableReceipt(t *testing.T) {
	db, before, p := finiteFixture(t)
	r := missingRequest(p)
	ctx := context.Background()
	core := cache.New()
	res, err := db.PatchMissing(ctx, r, core.FenceEmptyMetric)
	if err != nil || res.Applied || res.PointsApplied != 0 {
		t.Fatalf("dry-run %+v %v", res, err)
	}
	unchanged, _ := db.Snapshot(ctx, p.Expected.Name)
	if err := store.EqualSnapshots(before, unchanged); err != nil {
		t.Fatal(err)
	}
	r.Apply = true
	res, err = db.PatchMissing(ctx, r, core.FenceEmptyMetric)
	if err != nil || !res.Applied || res.PointsApplied != 3 || res.AppliedSHA256 != store.MissingPointHash(r.Points, "applied") {
		t.Fatalf("apply %+v %v", res, err)
	}
	after, err := db.Snapshot(ctx, p.Expected.Name)
	if err != nil {
		t.Fatal(err)
	}
	before.Archives[0].Points[0].Value = 1
	before.Archives[0].Points = append(before.Archives[0].Points, store.Point{Timestamp: 1793976660, Value: math.Copysign(0, -1)})
	before.Archives[1].Points = append(before.Archives[1].Points, store.Point{Timestamp: 1793973600, Value: 13})
	for i := range before.Archives {
		sort.Slice(before.Archives[i].Points, func(a, b int) bool {
			return before.Archives[i].Points[a].Timestamp < before.Archives[i].Points[b].Timestamp
		})
	}
	if err := store.EqualSnapshots(before, after); err != nil {
		t.Fatal(err)
	}
	dir, _ := finiteDirs.LoadAndDelete(db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(dir.(string), coldOptions(&ContinuationCodec{}))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopened.StopBackground()
	r.Expected = res.Version
	if _, err := reopened.PatchMissing(ctx, r, core.FenceEmptyMetric); err == nil {
		t.Fatal("replayed consumed batch after WAL reopen with fresh target identity")
	}
	actual, err := reopened.Snapshot(ctx, p.Expected.Name)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EqualSnapshots(before, actual); err != nil {
		t.Fatal(err)
	}
}
func TestMissingOnlyRefusesFiniteAndExactPreimageChanges(t *testing.T) {
	for _, kind := range []string{"finite", "wrong-nan", "occupied-ring-slot", "geometry", "future", "expired", "duplicate", "cap", "hash"} {
		t.Run(kind, func(t *testing.T) {
			db, before, p := finiteFixture(t)
			r := missingRequest(p)
			r.Apply = true
			switch kind {
			case "finite":
				r.Points[0].Timestamp = 1793976780
				r.Points = r.Points[:1]
				r.Points[0].BeforeBits = "4166332100000000"
			case "wrong-nan":
				r.Points[0].BeforeBits = "7ff8000000000043"
			case "occupied-ring-slot":
				r.Points = r.Points[1:2]
				r.Points[0].Timestamp = 1793976780
			case "geometry":
				r.Points[0].Count--
			case "future":
				r.Points = r.Points[1:2]
				r.Points[0].Timestamp = 1793976900
			case "expired":
				r.Points = r.Points[1:2]
				r.Points[0].Timestamp = 1793976660 - 20160*60
			case "duplicate":
				r.Points = append(r.Points[:1], r.Points[0])
			case "cap":
				for len(r.Points) <= 128 {
					r.Points = append(r.Points, r.Points[0])
				}
			}
			r.BatchSHA256 = store.MissingPointHash(r.Points, "requested")
			if kind == "hash" {
				r.BatchSHA256 = fmt.Sprintf("%064x", 99)
			}
			if _, err := db.PatchMissing(context.Background(), r, cache.New().FenceEmptyMetric); err == nil {
				t.Fatal("accepted invalid/conflicting request")
			}
			after, _ := db.Snapshot(context.Background(), p.Expected.Name)
			if err := store.EqualSnapshots(before, after); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestMissingOnlyRefusesConcurrentIntakeAndInFlight(t *testing.T) {
	db, before, p := finiteFixture(t)
	r := missingRequest(p)
	r.Apply = true
	core := cache.New()
	ctx := context.Background()
	core.Add(points.OnePoint(p.Expected.Name, 1793976660, 99))
	r.Apply = false
	if _, err := db.PatchMissing(ctx, r, core.FenceEmptyMetric); err == nil {
		t.Fatal("dry-run accepted pending intake")
	}
	r.Apply = true
	if _, err := db.PatchMissing(ctx, r, core.FenceEmptyMetric); err == nil {
		t.Fatal("accepted pending finite intake")
	}
	batch, _ := core.PopNotConfirmed(p.Expected.Name)
	if _, err := db.PatchMissing(ctx, r, core.FenceEmptyMetric); err == nil {
		t.Fatal("accepted in-flight finite intake")
	}
	core.Confirm(batch)
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		_, err := db.PatchMissing(ctx, r, func(ctx context.Context, name string, publish func() error) error {
			close(entered)
			<-release
			return core.FenceEmptyMetric(ctx, name, publish)
		})
		done <- err
	}()
	<-entered
	core.Add(points.OnePoint(p.Expected.Name, 1793976660, 99))
	close(release)
	if err := <-done; err == nil {
		t.Fatal("concurrent intake overwritten")
	}
	after, _ := db.Snapshot(ctx, p.Expected.Name)
	if err := store.EqualSnapshots(before, after); err != nil {
		t.Fatal(err)
	}
}

type blockingMissingCodec struct {
	*ContinuationCodec
	entered, release chan struct{}
}

func (c blockingMissingCodec) PlanMissingPatch(ctx context.Context, kv store.ContinuationIO, p store.MissingPatch, now int64) ([]store.RelayoutKV, error) {
	w, err := c.ContinuationCodec.PlanMissingPatch(ctx, kv, p, now)
	close(c.entered)
	<-c.release
	return w, err
}
func TestMissingOnlyRefusesConcurrentCommittedWriter(t *testing.T) {
	c := blockingMissingCodec{&ContinuationCodec{}, make(chan struct{}), make(chan struct{})}
	db, _, p := finiteFixtureCodec(t, c)
	r := missingRequest(p)
	r.Apply = true
	done := make(chan error, 1)
	ctx := context.Background()
	go func() { _, err := db.PatchMissing(ctx, r, cache.New().FenceEmptyMetric); done <- err }()
	<-c.entered
	if err := db.UpdateMany(ctx, p.Expected.Name, []store.Point{{Timestamp: 1793976660, Value: 77}}); err != nil {
		t.Fatal(err)
	}
	want, _ := db.Snapshot(ctx, p.Expected.Name)
	close(c.release)
	if err := <-done; err == nil {
		t.Fatal("overwrote concurrent finite committed writer")
	}
	after, _ := db.Snapshot(ctx, p.Expected.Name)
	if err := store.EqualSnapshots(want, after); err != nil {
		t.Fatal(err)
	}
}
func TestMissingOnlyPostPublicationFailureKeepsReceipt(t *testing.T) {
	db, _, p := finiteFixture(t)
	r := missingRequest(p)
	r.Apply = true
	res, err := db.PatchMissing(context.Background(), r, func(ctx context.Context, n string, publish func() error) error {
		if err := cache.New().FenceEmptyMetric(ctx, n, publish); err != nil {
			return err
		}
		return errors.New("lost durability acknowledgement")
	})
	if err == nil || !res.Applied || res.PointsApplied != 3 || res.AppliedSHA256 == "" {
		t.Fatalf("lost applied receipt %+v %v", res, err)
	}
	r.Expected = res.Version
	if _, err := db.PatchMissing(context.Background(), r, cache.New().FenceEmptyMetric); err == nil {
		t.Fatal("replayed ambiguous publication")
	}
}

func TestMissingOnlyEmptyExistingArchiveAndMaxBatch(t *testing.T) {
	t.Run("empty existing archive", func(t *testing.T) {
		ctx := context.Background()
		clock := func() time.Time { return time.Unix(1000080, 0) }
		r := []store.Retention{{Step: 60, Count: 10}, {Step: 600, Count: 10}}
		want := store.Snapshot{Metadata: store.Metadata{MetricConfig: store.MetricConfig{Name: "anchor", Retentions: r, AggregationMethod: store.Sum}}, Archives: []store.Archive{{Retention: r[0]}, {Retention: r[1], Points: []store.Point{{Timestamp: 999600, Value: math.Copysign(0, -1)}}}}}
		kv := mapContinuationIO{}
		codec := &ContinuationCodec{}
		if err := codec.RestoreArchives(ctx, kv, want, clock); err != nil {
			t.Fatal(err)
		}
		p := missingRequest(store.FinitePatch{Expected: store.TransferVersion{Name: "anchor", ID: 1, Generation: 1}, PlanSHA256: fmt.Sprintf("%064x", 1), Nonce: fmt.Sprintf("%032x", 1)})
		p.Points = []store.MissingPoint{{Archive: 0, Step: 60, Count: 10, Timestamp: 1000020, DonorBits: "3ff0000000000000"}}
		p.BatchSHA256 = store.MissingPointHash(p.Points, "requested")
		writes, err := codec.PlanMissingPatch(ctx, kv, p, clock().Unix())
		if err != nil {
			t.Fatal(err)
		}
		for _, w := range writes {
			kv.Set(w.Key, w.Value)
		}
		want.Archives[0].Points = []store.Point{{Timestamp: 1000020, Value: 1}}
		got, err := codec.Snapshot(ctx, kv, want.Metadata, clock)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.EqualSnapshots(want, got); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("all 128 points atomic", func(t *testing.T) {
		db, want, finite := finiteFixture(t)
		p := missingRequest(finite)
		p.Apply, p.Points = true, nil
		for i := 0; i < 128; i++ {
			stamp := int64(1793976600 - (128-i)*60)
			p.Points = append(p.Points, store.MissingPoint{Archive: 0, Step: 60, Count: 20160, Timestamp: stamp, DonorBits: "8000000000000000"})
			want.Archives[0].Points = append(want.Archives[0].Points, store.Point{Timestamp: stamp, Value: math.Copysign(0, -1)})
		}
		p.BatchSHA256 = store.MissingPointHash(p.Points, "requested")
		result, err := db.PatchMissing(context.Background(), p, cache.New().FenceEmptyMetric)
		if err != nil || result.PointsApplied != 128 {
			t.Fatalf("max batch %+v %v", result, err)
		}
		got, err := db.Snapshot(context.Background(), p.Expected.Name)
		if err != nil {
			t.Fatal(err)
		}
		sort.Slice(want.Archives[0].Points, func(i, j int) bool {
			return want.Archives[0].Points[i].Timestamp < want.Archives[0].Points[j].Timestamp
		})
		if err := store.EqualSnapshots(want, got); err != nil {
			t.Fatal(err)
		}
	})
}
