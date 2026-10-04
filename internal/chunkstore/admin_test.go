package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
)

func TestConcurrentSnapshotImportsAllocateUniqueIDs(t *testing.T) {
	s, now := openTestStore(t, t.TempDir(), Options{})
	defer closeTestStore(t, s)
	const imports = 32
	start := make(chan struct{})
	results := make(chan importedSnapshot, imports)
	for i := 0; i < imports; i++ {
		go importNewSnapshot(s, now, i, start, results)
	}
	close(start)
	seen := make(map[uint64]struct{}, imports)
	for i := 0; i < imports; i++ {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}
		if _, ok := seen[result.metadata.ID]; ok {
			t.Fatalf("duplicate metric ID %d", result.metadata.ID)
		}
		seen[result.metadata.ID] = struct{}{}
		assertImportedSnapshot(t, s, result)
	}
}

type importedSnapshot struct {
	name     string
	value    float64
	metadata Metadata
	err      error
}

func importNewSnapshot(s *Store, now, index int, start <-chan struct{}, results chan<- importedSnapshot) {
	<-start
	name := fmt.Sprintf("concurrent-import.%d", index)
	value := float64(index + 1)
	retention := Retention{Step: 60, Count: 256}
	snapshot := Snapshot{Metadata: Metadata{MetricConfig: MetricConfig{Name: name, Retentions: []Retention{retention}, AggregationMethod: Average}}, Archives: []Archive{{Retention: retention, Points: []Point{{Timestamp: int64(now - 60), Value: value}}}}}
	metadata, err := s.CreateFromSnapshot(context.Background(), snapshot)
	results <- importedSnapshot{name: name, value: value, metadata: metadata, err: err}
}

func assertImportedSnapshot(t *testing.T, s *Store, imported importedSnapshot) {
	t.Helper()
	snapshot, err := s.Snapshot(context.Background(), imported.name)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Metadata.ID != imported.metadata.ID || len(snapshot.Archives) != 1 || len(snapshot.Archives[0].Points) != 1 || snapshot.Archives[0].Points[0].Value != imported.value {
		t.Fatalf("imported snapshot %q = %#v", imported.name, snapshot)
	}
}

func TestSnapshotReplaceDeleteAndList(t *testing.T) {
	s, now := openTestStore(t, t.TempDir(), Options{})
	defer closeTestStore(t, s)
	config := MetricConfig{Name: "admin.metric", Retentions: []Retention{{Step: 60, Count: 256}}, AggregationMethod: Average}
	input := Snapshot{Metadata: Metadata{MetricConfig: config}, Archives: []Archive{{Retention: config.Retentions[0], Points: []Point{{Timestamp: int64(now - 120), Value: 1}, {Timestamp: int64(now - 60), Value: math.Float64frombits(0x7ff8000000000042)}}}}}
	m := createSnapshot(t, s, input)
	assertCreatedSnapshot(t, s, config.Name, m)

	snap, err := s.Snapshot(context.Background(), config.Name)
	if err != nil {
		t.Fatal(err)
	}
	assertSnapshotAndPage(t, s, snap)

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

func createSnapshot(t *testing.T, s *Store, input Snapshot) Metadata {
	t.Helper()
	m, err := s.CreateFromSnapshot(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func assertCreatedSnapshot(t *testing.T, s *Store, name string, m Metadata) {
	t.Helper()
	if m.ID != 1 || m.Generation != 1 || m.Revision != 1 {
		t.Fatalf("created metadata = %#v", m)
	}
	if _, err := s.Metadata(context.Background(), name); err != nil {
		t.Fatal(err)
	}
}

func assertSnapshotAndPage(t *testing.T, s *Store, snap Snapshot) {
	t.Helper()
	if len(snap.Archives) != 1 || len(snap.Archives[0].Points) != 2 || math.Float64bits(snap.Archives[0].Points[1].Value) != 0x7ff8000000000042 {
		t.Fatalf("snapshot = %#v", snap)
	}
	page, err := s.ListPage(context.Background(), "admin.", "", 1)
	if err != nil || len(page) != 1 || page[0].Revision != 1 {
		t.Fatalf("page = %#v, %v", page, err)
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

	stop, errs, readers := startGenerationReaders(s, name, a, b)
	replaceGenerations(t, s, a, b)
	close(stop)
	readers.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	assertGenerationAfterReopen(t, s, dir, name, a, b)
}

func startGenerationReaders(s *Store, name string, a, b Snapshot) (chan struct{}, chan error, *sync.WaitGroup) {
	stop := make(chan struct{})
	errs := make(chan error, 4)
	var readers sync.WaitGroup
	for i := 0; i < cap(errs); i++ {
		readers.Add(1)
		go readGenerations(&readers, stop, errs, s, name, a, b)
	}
	return stop, errs, &readers
}

func readGenerations(readers *sync.WaitGroup, stop <-chan struct{}, errs chan<- error, s *Store, name string, a, b Snapshot) {
	defer readers.Done()
	for {
		select {
		case <-stop:
			return
		default:
		}
		snapshot, err := s.Snapshot(context.Background(), name)
		if err == nil {
			err = completeGeneration(snapshot, a, b)
		}
		if err != nil {
			errs <- err
			return
		}
	}
}

func replaceGenerations(t *testing.T, s *Store, a, b Snapshot) {
	t.Helper()
	for i := 0; i < 24; i++ {
		candidate := a
		if i%2 == 0 {
			candidate = b
		}
		if _, err := s.Replace(context.Background(), candidate); err != nil {
			t.Fatal(err)
		}
	}
}

func assertGenerationAfterReopen(t *testing.T, s *Store, dir, name string, a, b Snapshot) {
	t.Helper()
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, _ := openTestStore(t, dir, Options{})
	defer closeTestStore(t, reopened)
	snapshot, err := reopened.Snapshot(context.Background(), name)
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
