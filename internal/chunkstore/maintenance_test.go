package chunkstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/restartcpu"
)

type maintenanceCodec struct{ relayoutCodec }

func (maintenanceCodec) Repack(ctx context.Context, io ContinuationIO, r ColdPackRequest) (RepackStats, uint64, bool, error) {
	return testColdCodec{}.Repack(ctx, io, r)
}

func TestStoreMaintenanceAdmissionAndShutdown(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(map[bool]string{false: "warm-complete", true: "shutdown"}[shutdown], func(t *testing.T) {
			dir := t.TempDir()
			opts := checkpointOptions()
			opts.ExactBlocks, opts.TimeBuckets = true, true
			opts.Continuation = maintenanceCodec{}
			opts.IncrementalSource = t.TempDir()
			s, err := Open(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			createTestMetric(t, s, "maintenance.metric")
			closeTestStore(t, s)
			entered := make(chan string, 8)
			release := make(chan struct{})
			opts.MaintenanceWait = func(ctx context.Context, name string) error {
				entered <- name
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-release:
					return nil
				}
			}
			opts.RelayoutRate = 1 << 20
			opts.RelayoutMinFreeBytes = 1 // the test machine's free space must not pause relayout
			opts.ColdPack = ColdPackOptions{Mode: "exact", Interval: time.Hour}
			opts.CompactOnce = "maintenance-test"
			s, err = Open(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer closeTestStore(t, s)
			purged := make(chan error, 1)
			go func() { _, err := s.PurgeInvalidNames(context.Background()); purged <- err }()
			want := map[string]bool{"catalog-membership": true, "relayout": true, "cold-pack": true, "compact-once": true, "purge-invalid-names": true}
			for len(want) > 0 {
				select {
				case name := <-entered:
					if !want[name] {
						t.Fatal("unexpected maintenance", name)
					}
					delete(want, name)
				case <-time.After(3 * time.Second):
					t.Fatal("maintenance never reached gate", want)
				}
			}
			if s.RelayoutStats().Blocks != 0 || s.ColdPackStats().Pages != 0 || s.catalogMembership.ready.Load() {
				t.Fatal("maintenance ran before warming")
			}
			if token, err := readStoreMarker(s.fs, dir, compactOnceMarker); err != nil || token != "" {
				t.Fatal(token, err)
			}
			budget := restartcpu.NewWithCPU(2, func() float64 { return 0 })
			pacer := budget.NewCheckpoint(nil)
			stats, err := s.LoadMetadataCheckpointAfter(context.Background(), PrewarmOptions{Workers: 1, CheckpointWait: pacer.Wait})
			pacer.Finish()
			if err != nil || !stats.Valid || stats.Entries != 1 {
				t.Fatalf("warming blocked: %+v %v", stats, err)
			}
			if shutdown {
				s.StopBackground()
				if err := <-purged; !errors.Is(err, context.Canceled) {
					t.Fatal("purge did not cancel", err)
				}
				return
			}
			close(release)
			if err := <-purged; err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(3 * time.Second)
			for !s.RelayoutStats().Done || s.ColdPackStats().Pages == 0 || !s.catalogMembership.ready.Load() || s.compactOnce.running.Load() {
				if time.Now().After(deadline) {
					t.Fatalf("maintenance did not start after warming: relayout=%+v cold=%+v membership=%v compact=%v error=%v", s.RelayoutStats(), s.ColdPackStats(), s.catalogMembership.ready.Load(), s.compactOnce.running.Load(), s.compactOnce.err.Load())
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
}

func TestStoreMaintenanceOpenFailureCancelsWaiters(t *testing.T) {
	dir := t.TempDir()
	opts := Options{ExactBlocks: true, TimeBuckets: true, Continuation: maintenanceCodec{}, IncrementalSource: t.TempDir(), CacheSize: 1 << 20, MemTableSize: 1 << 20, SyncInterval: time.Hour}
	s, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	closeTestStore(t, s)
	// Fail compact-once initialization after catalog/relayout workers exist.
	badMarker := filepath.Join(dir, compactOnceMarker)
	if err := os.Mkdir(badMarker, 0700); err != nil {
		t.Fatal(err)
	}
	cancelled := make(chan string, 2)
	opts.RelayoutRate, opts.CompactOnce = 1<<20, "fail-open"
	opts.MaintenanceWait = func(ctx context.Context, name string) error {
		<-ctx.Done()
		cancelled <- name
		return ctx.Err()
	}
	if db, err := Open(dir, opts); err == nil {
		db.Close()
		t.Fatal("expected compact-once marker read failure")
	}
	for range 2 {
		select {
		case <-cancelled:
		case <-time.After(3 * time.Second):
			t.Fatal("open failure leaked a maintenance waiter")
		}
	}
	if err := os.Remove(badMarker); err != nil {
		t.Fatal(err)
	}
	opts.MaintenanceWait, opts.CompactOnce, opts.RelayoutRate = nil, "", 0
	s, err = Open(dir, opts)
	if err != nil {
		t.Fatal("failed open did not release store lock", err)
	}
	closeTestStore(t, s)
}
