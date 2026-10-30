package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

func TestExpirationPointDeletesMatchAdministrativeDelete(t *testing.T) {
	for _, buckets := range []bool{false, true} {
		t.Run(fmt.Sprint(buckets), func(t *testing.T) {
			var states []map[string]string
			for _, expiration := range []bool{false, true} {
				opts := Options{fs: vfs.NewMem(), ExactBlocks: true, SyncInterval: time.Hour}
				if buckets {
					opts.Continuation, opts.TimeBuckets = bucketCodec{}, true
				}
				s, now := openTestStore(t, "/store", opts)
				defer closeTestStore(t, s)
				old := createTestMetric(t, s, "expired")
				keep := createTestMetric(t, s, "keep")
				other := createTestMetric(t, s, "expired.second")
				batch := s.db.NewBatch()
				for _, m := range []Metadata{old, keep, other} {
					var c chunk
					c.set(0, Point{Timestamp: int64(now), Value: 42})
					// Include another generation and a sparse, non-first chunk.
					for _, generation := range []uint64{m.Generation, m.Generation + 1} {
						m.Generation = generation
						if err := batch.Set(chunkKey(m, 0, 7), s.encodeChunk(&c), nil); err != nil {
							t.Fatal(err)
						}
						if buckets {
							key := append(continuationPrefix(m.ID, generation), "stale"...)
							if err := batch.Set(key, []byte("state"), nil); err != nil {
								t.Fatal(err)
							}
						}
					}
				}
				if err := batch.Commit(pebble.Sync); err != nil {
					t.Fatal(err)
				}
				batch.Close()
				if expiration {
					outcomes, err := s.DeleteBatchIfUnchanged(context.Background(), []Metadata{other, old})
					if err != nil || len(outcomes) != 2 || outcomes[0] != nil || outcomes[1] != nil {
						t.Fatalf("expiration: %v %v", outcomes, err)
					}
					if err := s.db.Flush(); err != nil {
						t.Fatal(err)
					}
					tables, err := s.db.SSTables(pebble.WithProperties())
					if err != nil {
						t.Fatal(err)
					}
					for _, level := range tables {
						for _, table := range level {
							if table.Properties.NumRangeDeletions != 0 {
								t.Fatal("expiration emitted range tombstones")
							}
						}
					}
				} else {
					for _, m := range []Metadata{old, other} {
						if err := s.Delete(context.Background(), m.Name); err != nil {
							t.Fatal(err)
						}
					}
				}
				it, err := s.db.NewIter(nil)
				if err != nil {
					t.Fatal(err)
				}
				state := make(map[string]string)
				for it.First(); it.Valid(); it.Next() {
					state[string(it.Key())] = string(it.Value())
				}
				if err := errors.Join(it.Error(), it.Close()); err != nil {
					t.Fatal(err)
				}
				states = append(states, state)
			}
			if !reflect.DeepEqual(states[0], states[1]) {
				t.Fatal("expiration and administrative deletion left different keys/values")
			}
		})
	}
}

func TestMetadataDoesNotWaitForMutationStripe(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(fmt.Sprint(cached), func(t *testing.T) {
			opts := Options{fs: vfs.NewMem()}
			if cached {
				opts.MetadataCacheSize = 4 << 20
			}
			s, _ := openTestStore(t, "/store", opts)
			defer closeTestStore(t, s)
			m := createTestMetric(t, s, "hot")
			if _, err := s.Metadata(context.Background(), m.Name); err != nil {
				t.Fatal(err)
			}
			// An in-flight mutation hides cached state. Metadata must read the
			// committed view without waiting for that mutation to finish.
			unlock := s.lockMetricMutation(m.Name)
			done := make(chan error, 1)
			go func() {
				got, err := s.Metadata(context.Background(), m.Name)
				if err == nil && !reflect.DeepEqual(got, m) {
					err = fmt.Errorf("metadata changed: %+v", got)
				}
				done <- err
			}()
			select {
			case err := <-done:
				unlock()
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				unlock()
				<-done
				t.Fatal("metadata waited for a writer stripe")
			}
			if err := s.Delete(context.Background(), m.Name); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Metadata(context.Background(), m.Name); !errors.Is(err, ErrNotFound) {
				t.Fatalf("metadata survived deletion: %v", err)
			}
		})
	}
}

func TestExpirationBatchMatchesAdministrativeDelete(t *testing.T) {
	s := openFileMigrationStore(t, vfs.NewMem())
	defer closeTestStore(t, s)
	ctx := context.Background()
	s.now = func() time.Time { return time.Unix(1790000100, 0) }
	var records []Metadata
	for _, name := range []string{"remove.a", "remove.b", "updated", "recreated", "pending"} {
		snapshot := fileMigrationSnapshot()
		snapshot.Metadata.Name = name
		m, err := s.StageFileSnapshot(ctx, snapshot)
		if err != nil {
			t.Fatal(err)
		}
		m, err = s.PublishFileOwnership(ctx, snapshot, m)
		if err != nil {
			t.Fatal(err)
		}
		if name != "pending" {
			if err := s.CompleteFileCleanup(ctx, m); err != nil {
				t.Fatal(err)
			}
		}
		m, err = s.Metadata(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, m)
	}
	if err := s.UpdateMany(ctx, "updated", []Point{{Timestamp: 1790000001, Value: 3}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "recreated"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Create(ctx, records[3].MetricConfig); err != nil {
		t.Fatal(err)
	}
	result, err := s.DeleteBatchIfUnchanged(ctx, records)
	if err != nil {
		t.Fatal(err)
	}
	for i, outcome := range result {
		if i < 2 && outcome != nil || i >= 2 && !errors.Is(outcome, ErrConflict) {
			t.Fatalf("%d outcome %v", i, outcome)
		}
		_, err := s.Metadata(ctx, records[i].Name)
		if (i < 2 && !errors.Is(err, ErrNotFound)) || (i >= 2 && err != nil) {
			t.Fatalf("%s: %v", records[i].Name, err)
		}
	}
	// The same-name/cache identity must be invalidated after a batch delete.
	m, err := s.Create(ctx, records[0].MetricConfig)
	if err != nil || m.ID == records[0].ID {
		t.Fatalf("recreated %+v: %v", m, err)
	}
	if _, err := s.DeleteBatchIfUnchanged(ctx, make([]Metadata, 513)); err == nil {
		t.Fatal("oversized delete batch accepted")
	}
	if _, err := s.DeleteBatchIfUnchanged(ctx, []Metadata{m, m}); err == nil {
		t.Fatal("duplicate accepted")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.DeleteBatchIfUnchanged(cancelled, []Metadata{m}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func convertedActivity(t *testing.T, s *Store, name string, activity time.Time, points []Point) ActivityBaseline {
	t.Helper()
	snapshot := fileMigrationSnapshot()
	snapshot.Metadata.Name = name
	snapshot.Metadata.LastUpdate = activity
	snapshot.Archives[0].Points = points
	m, err := s.StageFileSnapshot(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	m, err = s.PublishFileOwnership(context.Background(), snapshot, m)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteFileCleanup(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	m, err = s.Metadata(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	return ActivityBaseline{Name: name, ID: m.ID, Generation: m.Generation, Revision: m.Revision, LastUpdate: m.LastUpdate}
}
func TestActivityBackfillOnlyLowersUnchangedConversion(t *testing.T) {
	s := openFileMigrationStore(t, vfs.NewMem())
	defer closeTestStore(t, s)
	ctx := context.Background()
	conversion := time.Unix(1790000100, 0)
	s.now = func() time.Time { return conversion }
	start, end := conversion.Add(-time.Minute), conversion.Add(time.Minute)
	for i, tc := range []struct {
		name   string
		points []Point
		want   int64
	}{
		{"old", []Point{{Timestamp: 1790000001, Value: 42}, {Timestamp: 1790000003, Value: math.NaN()}}, 1790000001},
		{"empty", nil, 0},
		{"future", []Point{{Timestamp: 1790000200, Value: 1}}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			baseline := convertedActivity(t, s, fmt.Sprintf("metric.%d", i), conversion, tc.points)
			got, err := s.BackfillActivity(ctx, baseline, start, end, false)
			if err != nil {
				t.Fatal(err)
			}
			if tc.want == 0 && !got.IsZero() || tc.want != 0 && got.Unix() != tc.want {
				t.Fatalf("newest %s want %d", got, tc.want)
			}
			before, _ := s.Metadata(ctx, baseline.Name)
			if !baseline.matches(before) {
				t.Fatal("dry-run mutated activity")
			}
			applied, err := s.BackfillActivity(ctx, baseline, start, end, true)
			if err != nil || !applied.Equal(got) {
				t.Fatalf("apply %s %v", applied, err)
			}
			after, _ := s.Metadata(ctx, baseline.Name)
			if !got.IsZero() {
				if after.Revision != baseline.Revision+1 || !after.LastUpdate.Equal(got) {
					t.Fatalf("after %+v", after)
				}
				if _, err := s.BackfillActivity(ctx, baseline, start, end, true); !errors.Is(err, ErrConflict) {
					t.Fatalf("repeat %v", err)
				}
			}
		})
	}
	baseline := convertedActivity(t, s, "written", conversion, []Point{{Timestamp: 1790000001, Value: 1}})
	if _, err := s.BackfillActivity(ctx, baseline, start, end, false); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateMany(ctx, baseline.Name, []Point{{Timestamp: 1790000004, Value: 2}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.BackfillActivity(ctx, baseline, start, end, true); !errors.Is(err, ErrConflict) {
		t.Fatalf("overwrote a new write: %v", err)
	}
	if _, err := s.BackfillActivity(ctx, baseline, end, end.Add(time.Hour), true); err == nil {
		t.Fatal("outside window accepted")
	}
}

func TestExpirationBatchesConcurrentWrites(t *testing.T) {
	s, now := openTestStore(t, t.TempDir(), Options{SyncInterval: time.Hour, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const count = 64
	for round := range 8 {
		expected := make([]Metadata, count)
		for i := range expected {
			expected[i] = createTestMetric(t, s, fmt.Sprintf("r%d.metric.%d", round, i))
		}
		start := make(chan struct{})
		failures := make(chan error, count)
		var workers sync.WaitGroup
		for first := 0; first < count; first += 16 {
			workers.Go(func() {
				<-start
				outcomes, err := s.DeleteBatchIfUnchanged(ctx, expected[first:first+16])
				if err != nil {
					failures <- err
					return
				}
				for _, err := range outcomes {
					if err != nil && !errors.Is(err, ErrConflict) {
						failures <- err
					}
				}
			})
		}
		for i := 0; i < count; i += 2 {
			workers.Go(func() {
				<-start
				m := expected[i]
				input := []Point{{Timestamp: int64(now), Value: 7}}
				if err := s.UpdateMany(ctx, m.Name, input); errors.Is(err, ErrNotFound) {
					if _, err := s.Create(ctx, m.MetricConfig); err != nil {
						failures <- err
						return
					}
					if err := s.UpdateMany(ctx, m.Name, input); err != nil {
						failures <- err
					}
				} else if err != nil {
					failures <- err
				}
			})
		}
		close(start)
		workers.Wait()
		close(failures)
		for err := range failures {
			t.Fatal(err)
		}
		for i, m := range expected {
			snap, err := s.Snapshot(ctx, m.Name)
			if i%2 != 0 {
				if !errors.Is(err, ErrNotFound) {
					t.Fatalf("undeleted metric %s: %v", m.Name, err)
				}
			} else if err != nil || len(snap.Archives[0].Points) != 1 || snap.Archives[0].Points[0].Value != 7 {
				t.Fatalf("lost concurrent write %s: %+v %v", m.Name, snap, err)
			}
		}
	}
}
