package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/go-graphite/go-carbon/helper"
)

func TestMetadataMemoMatchesFreshSnapshotAcrossMutations(t *testing.T) {
	t.Parallel()
	for _, exact := range []bool{false, true} {
		t.Run(fmt.Sprint(exact), func(t *testing.T) {
			s, now := openTestStore(t, t.TempDir(), Options{ExactBlocks: exact})
			defer closeTestStore(t, s)
			ctx := context.Background()
			created := createTestMetric(t, s, "metadata.memo")
			check := func() Metadata {
				t.Helper()
				fresh, err := s.Snapshot(ctx, created.Name)
				if err != nil {
					t.Fatal(err)
				}
				for range 3 {
					got, err := s.Metadata(ctx, created.Name)
					if err != nil || !reflect.DeepEqual(got, fresh.Metadata) {
						t.Fatalf("cached metadata differs from snapshot: %+v, %+v, %v", got, fresh.Metadata, err)
					}
					// Returning policy must never expose the cached retention slice.
					got.Retentions[0].Step = 999
				}
				return fresh.Metadata
			}
			check()
			if err := s.UpdateMany(ctx, created.Name, []Point{{Timestamp: int64(now - 60), Value: 1.25}}); err != nil {
				t.Fatal(err)
			}
			check()
			if err := s.UpdateManyForArchive(ctx, created.Name, []Point{{Timestamp: int64(now - 120), Value: 2.5}}, 60*256); err != nil {
				t.Fatal(err)
			}
			check()
			if err := s.UpdateManyWithArchiveValues(ctx, created.Name, nil, []ArchiveValue{{Archive: 0, Timestamp: int64((now/60 - 1) * 60), Value: -3.5, Present: true}}); err != nil {
				t.Fatal(err)
			}
			check()
			if err := s.UpdateManyWithArchiveValues(ctx, created.Name, nil, []ArchiveValue{{Archive: 9}}); err == nil {
				t.Fatal("invalid archive accepted")
			}
			check()
			snapshot, err := s.Snapshot(ctx, created.Name)
			if err != nil {
				t.Fatal(err)
			}
			snapshot.Metadata.AggregationMethod = Sum
			if _, err = s.Replace(ctx, snapshot); err != nil {
				t.Fatal(err)
			}
			check()
			if _, err = s.Fill(ctx, snapshot); err != nil {
				t.Fatal(err)
			}
			check()
			snapshot.Archives[0].Points = append(snapshot.Archives[0].Points, Point{Timestamp: int64((now/60 - 3) * 60), Value: 17})
			if _, err = s.ReconcileSnapshot(ctx, snapshot); err != nil {
				t.Fatal(err)
			}
			current := check()
			if err = s.DeleteIfUnchanged(ctx, created.Name, created); !errors.Is(err, ErrConflict) {
				t.Fatalf("stale delete: %v", err)
			}
			check()
			if err = s.DeleteIfUnchanged(ctx, created.Name, current); err != nil {
				t.Fatal(err)
			}
			if _, err = s.Metadata(ctx, created.Name); !errors.Is(err, ErrNotFound) {
				t.Fatalf("deleted name returned: %v", err)
			}
			recreated := createTestMetric(t, s, created.Name)
			if check().ID != recreated.ID || recreated.ID == created.ID {
				t.Fatal("recreated name reused cached identity")
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if _, err = s.Metadata(cancelled, created.Name); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled cached read: %v", err)
			}
			if err = s.Compact(); err != nil {
				t.Fatal(err)
			}
			check()
		})
	}
}

func TestMetadataMemoConcurrentReplaceAndOwnedPolicy(t *testing.T) {
	t.Parallel()
	s, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true})
	defer closeTestStore(t, s)
	ctx := context.Background()
	m := createTestMetric(t, s, "metadata.concurrent")
	var workers sync.WaitGroup
	done := make(chan struct{})
	errorsSeen := make(chan error, 8)
	for range 8 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			var previous uint64
			for {
				select {
				case <-done:
					return
				default:
				}
				got, err := s.Metadata(ctx, m.Name)
				if err != nil {
					errorsSeen <- fmt.Errorf("read metadata: %w", err)
					return
				}
				if got.Generation < previous || got.Revision != got.Generation || got.Retentions[0].Count != 255+int(got.Generation) {
					errorsSeen <- fmt.Errorf("incoherent metadata %+v", got)
					return
				}
				previous = got.Generation
				got.Retentions[0].Count = -1
			}
		}()
	}
	for generation := 2; generation <= 100; generation++ {
		config := cloneConfig(m.MetricConfig)
		config.Retentions[0].Count = 255 + generation
		_, err := s.Replace(ctx, Snapshot{Metadata: Metadata{MetricConfig: config}, Archives: []Archive{{Retention: config.Retentions[0]}}})
		if err != nil {
			t.Error(err)
			break
		}
	}
	close(done)
	workers.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		t.Error(err)
	}
}

func TestMetadataMemoEvictionKeepsCollidingMetricsDistinct(t *testing.T) {
	t.Parallel()
	s, _ := openTestStore(t, t.TempDir(), Options{})
	defer closeTestStore(t, s)
	ctx := context.Background()
	var names []string
	for i := 0; len(names) < 32; i++ {
		name := fmt.Sprintf("metadata.collision.%d", i)
		if (helper.HashString(name)>>32)%256 == 0 {
			names = append(names, name)
		}
	}
	for _, name := range names {
		createTestMetric(t, s, name)
	}
	for repeat := range 3 {
		for _, name := range names {
			got, err := s.Metadata(ctx, name)
			fresh, freshErr := s.Snapshot(ctx, name)
			if err != nil || freshErr != nil || !reflect.DeepEqual(got, fresh.Metadata) {
				t.Fatalf("eviction changed identity at round %d: %+v, %v, %v", repeat, got, err, freshErr)
			}
		}
	}
}
