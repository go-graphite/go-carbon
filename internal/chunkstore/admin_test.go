package chunkstore

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
)

func TestSnapshotReplaceDeleteAndList(t *testing.T) {
	s, now := openTestStore(t, t.TempDir(), Options{})
	defer closeTestStore(t, s)
	config := MetricConfig{Name: "admin.metric", Retentions: []Retention{{Step: 60, Count: 256}}, AggregationMethod: Average}
	input := Snapshot{Metadata: Metadata{MetricConfig: config}, Archives: []Archive{{Retention: config.Retentions[0], Points: []Point{{Timestamp: int64(now - 120), Value: 1}, {Timestamp: int64(now - 60), Value: math.Float64frombits(0x7ff8000000000042)}}}}}
	m, err := s.CreateFromSnapshot(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if m.ID != 1 || m.Generation != 1 || m.Revision != 1 {
		t.Fatalf("created metadata = %#v", m)
	}
	snap, err := s.Snapshot(context.Background(), config.Name)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Archives) != 1 || len(snap.Archives[0].Points) != 2 || math.Float64bits(snap.Archives[0].Points[1].Value) != 0x7ff8000000000042 {
		t.Fatalf("snapshot = %#v", snap)
	}
	page, err := s.ListPage(context.Background(), "admin.", "", 1)
	if err != nil || len(page) != 1 || page[0].Revision != 1 {
		t.Fatalf("page = %#v, %v", page, err)
	}

	snap.Archives[0].Points[0].Value = 2
	m, err = s.Replace(context.Background(), snap)
	if err != nil {
		t.Fatal(err)
	}
	if m.Generation != 2 || m.Revision != 2 {
		t.Fatalf("replaced metadata = %#v", m)
	}
	if err := s.DeleteIfUnchanged(context.Background(), config.Name, snap.Metadata); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale delete error = %v", err)
	}
	if err := s.DeleteIfUnchanged(context.Background(), config.Name, m); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Metadata(context.Background(), config.Name); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted metadata error = %v", err)
	}
}

func TestFillPreservesDestinationAndRejectsPolicyChange(t *testing.T) {
	s, now := openTestStore(t, t.TempDir(), Options{})
	defer closeTestStore(t, s)
	config := MetricConfig{Name: "fill.metric", Retentions: []Retention{{Step: 60, Count: 256}}, AggregationMethod: Average}
	negativeZero := math.Float64frombits(1 << 63)
	base := Snapshot{Metadata: Metadata{MetricConfig: config}, Archives: []Archive{{Retention: config.Retentions[0], Points: []Point{{Timestamp: int64(now - 180), Value: negativeZero}, {Timestamp: int64(now - 120), Value: math.NaN()}, {Timestamp: int64(now - 60), Value: 1}}}}}
	if _, err := s.CreateFromSnapshot(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	source := base
	source.Archives[0].Points = []Point{{Timestamp: int64(now - 180), Value: 9}, {Timestamp: int64(now - 120), Value: 2}, {Timestamp: int64(now - 60), Value: math.NaN()}, {Timestamp: int64(now), Value: 3}}
	if _, err := s.Fill(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	got, err := s.Snapshot(context.Background(), config.Name)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Archives[0].Points) != 4 || math.Float64bits(got.Archives[0].Points[0].Value) != math.Float64bits(negativeZero) || got.Archives[0].Points[1].Value != 2 || got.Archives[0].Points[2].Value != 1 || got.Archives[0].Points[3].Value != 3 {
		t.Fatalf("filled points = %#v", got.Archives[0].Points)
	}
	bad := source
	bad.Metadata.AggregationMethod = Sum
	if _, err := s.Fill(context.Background(), bad); !errors.Is(err, ErrConflict) {
		t.Fatalf("policy conflict error = %v", err)
	}
}

func TestConditionalDeleteConflictsAfterUpdateAndRecreate(t *testing.T) {
	s, now := openTestStore(t, t.TempDir(), Options{})
	defer closeTestStore(t, s)
	config := MetricConfig{Name: "lifecycle.metric", Retentions: []Retention{{Step: 60, Count: 256}}, AggregationMethod: Average}
	snapshot := Snapshot{Metadata: Metadata{MetricConfig: config}, Archives: []Archive{{Retention: config.Retentions[0]}}}
	m, err := s.CreateFromSnapshot(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateManyForArchive(context.Background(), config.Name, []Point{{Timestamp: int64(now - 60), Value: 1}}, 60*256); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteIfUnchanged(context.Background(), config.Name, m); !errors.Is(err, ErrConflict) {
		t.Fatalf("delete after update = %v", err)
	}
	current, err := s.Metadata(context.Background(), config.Name)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteIfUnchanged(context.Background(), config.Name, current); err != nil {
		t.Fatal(err)
	}
	recreated, err := s.CreateFromSnapshot(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if recreated.ID == m.ID {
		t.Fatalf("recreated ID = %d, want a new ID", recreated.ID)
	}
	if err := s.DeleteIfUnchanged(context.Background(), config.Name, m); !errors.Is(err, ErrConflict) {
		t.Fatalf("delete after recreate = %v", err)
	}
}

func TestConcurrentReplaceSnapshotIsOneCompleteGeneration(t *testing.T) {
	dir := t.TempDir()
	s, now := openTestStore(t, dir, Options{})
	name := "generation.metric"
	a := Snapshot{Metadata: Metadata{MetricConfig: MetricConfig{Name: name, Retentions: []Retention{{Step: 60, Count: 256}}, AggregationMethod: Average}}, Archives: []Archive{{Retention: Retention{Step: 60, Count: 256}, Points: []Point{{Timestamp: int64(now - 160), Value: 11}}}}}
	b := Snapshot{Metadata: Metadata{MetricConfig: MetricConfig{Name: name, Retentions: []Retention{{Step: 120, Count: 128}}, AggregationMethod: Sum, XFilesFactor: 0.5}}, Archives: []Archive{{Retention: Retention{Step: 120, Count: 128}, Points: []Point{{Timestamp: int64(now - 160), Value: 22}}}}}
	if _, err := s.CreateFromSnapshot(context.Background(), a); err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	errs := make(chan error, 4)
	var readers sync.WaitGroup
	for i := 0; i < cap(errs); i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				snapshot, err := s.Snapshot(context.Background(), name)
				if err != nil {
					errs <- err
					return
				}
				if err := completeGeneration(snapshot, a, b); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	for i := 0; i < 24; i++ {
		candidate := a
		if i%2 == 0 {
			candidate = b
		}
		if _, err := s.Replace(context.Background(), candidate); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	readers.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, _ = openTestStore(t, dir, Options{})
	defer closeTestStore(t, s)
	snapshot, err := s.Snapshot(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	if err := completeGeneration(snapshot, a, b); err != nil {
		t.Fatal(err)
	}
}

func completeGeneration(got Snapshot, a, b Snapshot) error {
	for _, want := range []Snapshot{a, b} {
		if got.Metadata.AggregationMethod != want.Metadata.AggregationMethod || got.Metadata.XFilesFactor != want.Metadata.XFilesFactor || len(got.Metadata.Retentions) != 1 || got.Metadata.Retentions[0] != want.Metadata.Retentions[0] {
			continue
		}
		if len(got.Archives) != 1 || got.Archives[0].Retention != want.Archives[0].Retention || len(got.Archives[0].Points) != 1 {
			return errors.New("snapshot mixed metadata and physical archive generations")
		}
		point, expected := got.Archives[0].Points[0], want.Archives[0].Points[0]
		if point.Timestamp != expected.Timestamp || point.Value != expected.Value {
			return errors.New("snapshot has archive data from another generation")
		}
		return nil
	}
	return errors.New("snapshot has an unknown generation policy")
}
