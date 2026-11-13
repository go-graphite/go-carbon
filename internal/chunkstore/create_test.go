package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

func testCreateMembership(s *Store, ready bool) {
	if s.catalogMembership != nil {
		s.catalogMembership.halt()
	}
	s.catalogMembership = newCatalogMembership(10000)
	s.catalogMembership.ready.Store(ready)
	close(s.catalogMembership.done)
}

func TestCreateConcurrentNames(t *testing.T) {
	for _, mode := range []string{"nil", "not-ready", "ready"} {
		t.Run(mode, func(t *testing.T) {
			s, err := Open("/store", Options{fs: vfs.NewMem(), ExactBlocks: true, SyncInterval: time.Hour})
			if err != nil {
				t.Fatal(err)
			}
			defer closeTestStore(t, s)
			if mode != "nil" {
				testCreateMembership(s, mode == "ready")
			}
			var winners atomic.Int64
			var wg sync.WaitGroup
			for worker := range 96 {
				wg.Go(func() {
					config := MetricConfig{Name: "same", Retentions: []Retention{{Step: 60, Count: 256}}, AggregationMethod: Average}
					if _, err := s.Create(context.Background(), config); err == nil {
						winners.Add(1)
					} else if !errors.Is(err, ErrExists) {
						t.Error(err)
					}
					for i := range 8 {
						config.Name = fmt.Sprintf("distinct.%d.%d", worker, i)
						if _, err := s.Create(context.Background(), config); err != nil {
							t.Error(err)
						}
					}
				})
			}
			wg.Wait()
			if winners.Load() != 1 {
				t.Fatalf("same-name winners: %d", winners.Load())
			}
			list, err := s.List(context.Background(), "")
			if err != nil || len(list) != 769 {
				t.Fatalf("catalog count: %d %v", len(list), err)
			}
			ids := make(map[uint64]bool)
			for _, m := range list {
				if ids[m.ID] || m.ID == 0 || m.ID > 769 {
					t.Fatalf("duplicate/out-of-range ID: %d", m.ID)
				}
				ids[m.ID] = true
			}
		})
	}
}

func TestCreateNotReadyFindsExistingCatalog(t *testing.T) {
	s, err := Open("/store", Options{fs: vfs.NewMem(), ExactBlocks: true})
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestStore(t, s)
	m := createTestMetric(t, s, "existing")
	// Deliberately empty filter: an unfinished scan must consult the catalog.
	testCreateMembership(s, false)
	if _, err := s.Create(context.Background(), m.MetricConfig); !errors.Is(err, ErrExists) {
		t.Fatal(err)
	}
}

func TestCreateExistingDoesNotWaitForAllocation(t *testing.T) {
	s, err := Open("/store", Options{fs: vfs.NewMem(), ExactBlocks: true})
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestStore(t, s)
	m := createTestMetric(t, s, "existing")
	s.mu.Lock()
	done := make(chan error, 1)
	go func() { _, err := s.Create(context.Background(), m.MetricConfig); done <- err }()
	select {
	case err := <-done:
		s.mu.Unlock()
		if !errors.Is(err, ErrExists) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		s.mu.Unlock()
		<-done
		t.Fatal("existing-name lookup waited for global allocation lock")
	}
}

func TestCreateMatchesCatalogReadReference(t *testing.T) {
	for _, mode := range []string{"legacy", "exact", "continuation", "incremental"} {
		t.Run(mode, func(t *testing.T) {
			opts := Options{fs: vfs.NewMem(), ExactBlocks: mode != "legacy", Now: func() time.Time { return time.Unix(1790000000, 0) }}
			if mode == "continuation" {
				opts.Continuation = relayoutCodec{}
			}
			if mode == "incremental" {
				opts.IncrementalSource = "/legacy"
				if err := opts.fs.MkdirAll("/legacy", 0750); err != nil {
					t.Fatal(err)
				}
			}
			fast, err := Open("/fast", opts)
			if err != nil {
				t.Fatal(err)
			}
			defer closeTestStore(t, fast)
			reference, err := Open("/reference", opts)
			if err != nil {
				t.Fatal(err)
			}
			defer closeTestStore(t, reference)
			testCreateMembership(fast, true)
			for i := range 100 {
				config := MetricConfig{Name: fmt.Sprintf("metric.%d", i%50), Retentions: []Retention{{Step: 60, Count: 256}}, AggregationMethod: Average, XFilesFactor: .5}
				got, gotErr := fast.Create(context.Background(), config)
				want, wantErr := reference.createWithCatalogRead(context.Background(), config)
				if !reflect.DeepEqual(got, want) || !errors.Is(gotErr, wantErr) {
					t.Fatalf("Create %d: %+v/%v != %+v/%v", i, got, gotErr, want, wantErr)
				}
				if gotErr == nil && mode != "continuation" {
					points := []Point{{Timestamp: 1789999980, Value: math.Copysign(0, -1)}, {Timestamp: 1789999920, Value: math.Float64frombits(0x7ff8000000000042)}, {Timestamp: 1789999860, Value: .1}}
					for _, store := range []*Store{fast, reference} {
						if err := store.UpdateMany(context.Background(), config.Name, points); err != nil {
							t.Fatal(err)
						}
					}
				}
				// Delete and recreate (the filter deliberately retains positive bits).
				if i == 24 {
					for _, s := range []*Store{fast, reference} {
						if err := s.Delete(context.Background(), config.Name); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			a, err := fast.db.NewIter(nil)
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			z, err := reference.db.NewIter(nil)
			if err != nil {
				t.Fatal(err)
			}
			defer z.Close()
			for av, zv := a.First(), z.First(); ; av, zv = a.Next(), z.Next() {
				if av != zv {
					t.Fatal("different key count")
				}
				if !av {
					break
				}
				if !bytes.Equal(a.Key(), z.Key()) || !bytes.Equal(a.Value(), z.Value()) {
					t.Fatalf("different persisted bytes: %q != %q", a.Key(), z.Key())
				}
			}
			if err := errors.Join(a.Error(), z.Error()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCreateCrashSequenceAtomicity(t *testing.T) {
	for _, synced := range []bool{false, true} {
		t.Run(fmt.Sprintf("synced=%t", synced), func(t *testing.T) {
			fs := vfs.NewStrictMem()
			opts := Options{fs: fs, ExactBlocks: true, SyncInterval: time.Hour}
			s, err := Open("/store", opts)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Flush(); err != nil {
				t.Fatal(err)
			}
			testCreateMembership(s, true)
			m := createTestMetric(t, s, "before.crash")
			if synced {
				if err := s.db.LogData(nil, pebble.Sync); err != nil {
					t.Fatal(err)
				}
			}
			closeForCrashRecovery(t, s, fs)
			s, err = Open("/store", opts)
			if err != nil {
				t.Fatal(err)
			}
			defer closeTestStore(t, s)
			high, err := s.CatalogHighWatermark(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			got, err := s.Metadata(context.Background(), m.Name)
			if synced || high != 0 {
				if err != nil || !reflect.DeepEqual(got, m) || high != m.ID {
					t.Fatalf("recovered catalog/sequence: %+v %d %v", got, high, err)
				}
			} else if !errors.Is(err, ErrNotFound) {
				t.Fatalf("unsynced catalog without sequence: %v", err)
			}
			next := createTestMetric(t, s, "after.crash")
			if next.ID != high+1 {
				t.Fatalf("reused/skipped ID: %d high=%d", next.ID, high)
			}
		})
	}
}

func TestNativePublicationTakesStripeBeforeAllocation(t *testing.T) {
	s, err := Open("/store", Options{fs: vfs.NewMem(), ExactBlocks: true})
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestStore(t, s)
	testCreateMembership(s, true)
	identity := "stripe-publication"
	intent, err := s.reserveNativeIDs(identity, 1)
	if err != nil {
		t.Fatal(err)
	}
	m := Metadata{MetricConfig: MetricConfig{Name: "native.metric", Retentions: []Retention{{Step: 60, Count: 256}}, AggregationMethod: Average}, ID: intent.First, Generation: 1, Revision: 1}
	receipt := NativeReceipt{Identity: identity, Count: 1, Copied: make([]byte, 1)}
	unlock := s.lockMetricMutation(m.Name)
	done := make(chan error, 1)
	go func() {
		done <- s.publishNative(context.Background(), []Metadata{m}, []int{0}, []SidecarSummary{{}}, &receipt, &nativeTables{})
	}()
	select {
	case err := <-done:
		unlock()
		t.Fatalf("native publication bypassed the mutation stripe: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	// A bulk publisher waiting for a stripe must not hold the global allocator.
	other := "other.metric"
	for s.metricStripe(other) == s.metricStripe(m.Name) {
		other += ".x"
	}
	live := make(chan error, 1)
	go func() {
		_, err := s.Create(context.Background(), MetricConfig{Name: other, Retentions: m.Retentions, AggregationMethod: Average})
		live <- err
	}()
	select {
	case err := <-live:
		if err != nil {
			unlock()
			<-done
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		unlock()
		<-done
		<-live
		t.Fatal("native publisher held allocator while waiting for stripe")
	}
	unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := s.Create(context.Background(), m.MetricConfig); !errors.Is(err, ErrExists) {
		t.Fatalf("native catalog overwritten: %v", err)
	}
}
