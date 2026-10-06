package chunkstore

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

func TestAdministrativeMutationsFollowSyncPolicy(t *testing.T) {
	for _, mutation := range administrativeMutations() {
		t.Run(mutation.name, func(t *testing.T) {
			for _, policy := range adminSyncPolicies() {
				t.Run(policy.name, func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						runAdministrativeMutationSyncPolicy(t, mutation, policy)
					})
				})
			}
		})
	}
}

type administrativeMutation struct {
	name  string
	setup func(*testing.T, *Store, int) (adminState, adminState)
	apply func(*testing.T, *Store, int)
}

type adminState struct {
	exists               bool
	generation, revision uint64
	values               []float64
}

type adminSyncPolicy struct {
	name     string
	interval time.Duration
	tick     bool
	crash    bool
	durable  bool
}

func administrativeMutations() []administrativeMutation {
	return []administrativeMutation{
		{name: "create", setup: absentAndCreatedState, apply: createAdministrativeMetric},
		{name: "snapshot import", setup: absentAndImportedState, apply: importAdministrativeSnapshot},
		{name: "replace", setup: existingAndReplacedState, apply: replaceAdministrativeSnapshot},
		{name: "fill existing", setup: existingAndFilledState, apply: fillExistingAdministrativeSnapshot},
		{name: "fill new", setup: absentAndFilledState, apply: fillNewAdministrativeSnapshot},
		{name: "delete", setup: existingAndDeletedState, apply: deleteAdministrativeMetric},
		{name: "conditional delete", setup: existingAndDeletedState, apply: deleteAdministrativeMetricIfUnchanged},
	}
}

func adminSyncPolicies() []adminSyncPolicy {
	return []adminSyncPolicy{
		{name: "synchronous", crash: true, durable: true},
		{name: "unsynced lost", interval: time.Second, crash: true},
		{name: "periodic", interval: time.Second, tick: true, crash: true, durable: true},
		{name: "close", interval: time.Hour, durable: true},
	}
}

func runAdministrativeMutationSyncPolicy(t *testing.T, mutation administrativeMutation, policy adminSyncPolicy) {
	t.Helper()
	strict := vfs.NewStrictMem()
	fs := &walSyncFS{FS: strict}
	s, now := openTestStore(t, "/store", Options{fs: fs, SyncInterval: policy.interval})
	closed := false
	defer func() {
		if !closed {
			closeTestStore(t, s)
		}
	}()
	before, after := mutation.setup(t, s, now)
	waitForPendingSync(t, policy.interval)
	initialSyncs := fs.syncs.Load()
	mutation.apply(t, s, now)
	assertMutationSyncPolicy(t, fs, initialSyncs, policy.interval)
	if policy.tick {
		waitForPendingSync(t, policy.interval)
		if got := fs.syncs.Load(); got != initialSyncs+1 {
			t.Fatalf("periodic WAL syncs = %d, want %d", got, initialSyncs+1)
		}
	}
	closed = true
	closeForRecovery(t, s, strict, policy.crash)
	reopened, _ := openTestStore(t, "/store", Options{fs: strict})
	defer closeTestStore(t, reopened)
	if policy.durable {
		assertAdminState(t, reopened, after)
		return
	}
	assertAdminState(t, reopened, before)
}

func waitForPendingSync(t *testing.T, interval time.Duration) {
	t.Helper()
	if interval == 0 {
		return
	}
	time.Sleep(interval)
	synctest.Wait()
}

func assertMutationSyncPolicy(t *testing.T, fs *walSyncFS, before int64, interval time.Duration) {
	t.Helper()
	want := before
	if interval == 0 {
		want++
	}
	if got := fs.syncs.Load(); got != want {
		t.Fatalf("mutation WAL syncs = %d, want %d", got, want)
	}
}

func closeForRecovery(t *testing.T, s *Store, strict *vfs.MemFS, crash bool) {
	t.Helper()
	strict.SetIgnoreSyncs(crash)
	closeTestStore(t, s)
	if crash {
		strict.ResetToSyncedState()
		strict.SetIgnoreSyncs(false)
	}
}

func absentAndCreatedState(_ *testing.T, _ *Store, _ int) (adminState, adminState) {
	return adminState{}, adminState{exists: true, generation: 1, revision: 1}
}

func absentAndImportedState(_ *testing.T, _ *Store, _ int) (adminState, adminState) {
	return adminState{}, adminState{exists: true, generation: 1, revision: 1, values: []float64{2}}
}

func absentAndFilledState(_ *testing.T, _ *Store, _ int) (adminState, adminState) {
	return adminState{}, adminState{exists: true, generation: 1, revision: 1, values: []float64{3}}
}

func existingAndReplacedState(t *testing.T, s *Store, now int) (adminState, adminState) {
	createAdministrativeSnapshot(t, s, now, 1)
	return adminState{exists: true, generation: 1, revision: 1, values: []float64{1}}, adminState{exists: true, generation: 2, revision: 2, values: []float64{2}}
}

func existingAndFilledState(t *testing.T, s *Store, now int) (adminState, adminState) {
	createAdministrativeSnapshot(t, s, now, 1)
	return adminState{exists: true, generation: 1, revision: 1, values: []float64{1}}, adminState{exists: true, generation: 2, revision: 2, values: []float64{2, 1}}
}

func existingAndDeletedState(t *testing.T, s *Store, now int) (adminState, adminState) {
	createAdministrativeSnapshot(t, s, now, 1)
	return adminState{exists: true, generation: 1, revision: 1, values: []float64{1}}, adminState{}
}

func createAdministrativeMetric(t *testing.T, s *Store, _ int) {
	t.Helper()
	if _, err := s.Create(context.Background(), administrativeMetricConfig()); err != nil {
		t.Fatal(err)
	}
}

func importAdministrativeSnapshot(t *testing.T, s *Store, now int) {
	t.Helper()
	if _, err := s.CreateFromSnapshot(context.Background(), administrativeSnapshot(now, 2)); err != nil {
		t.Fatal(err)
	}
}

func replaceAdministrativeSnapshot(t *testing.T, s *Store, now int) {
	t.Helper()
	if _, err := s.Replace(context.Background(), administrativeSnapshot(now, 2)); err != nil {
		t.Fatal(err)
	}
}

func fillExistingAdministrativeSnapshot(t *testing.T, s *Store, now int) {
	t.Helper()
	snapshot := administrativeSnapshot(now, 2)
	snapshot.Archives[0].Points[0].Timestamp = int64(now - 120)
	if _, err := s.Fill(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
}

func fillNewAdministrativeSnapshot(t *testing.T, s *Store, now int) {
	t.Helper()
	if _, err := s.Fill(context.Background(), administrativeSnapshot(now, 3)); err != nil {
		t.Fatal(err)
	}
}

func deleteAdministrativeMetric(t *testing.T, s *Store, _ int) {
	t.Helper()
	if err := s.Delete(context.Background(), "admin"); err != nil {
		t.Fatal(err)
	}
}

func deleteAdministrativeMetricIfUnchanged(t *testing.T, s *Store, _ int) {
	t.Helper()
	m, err := s.Metadata(context.Background(), "admin")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteIfUnchanged(context.Background(), "admin", m); err != nil {
		t.Fatal(err)
	}
}

func createAdministrativeSnapshot(t *testing.T, s *Store, now int, values ...float64) {
	t.Helper()
	if _, err := s.CreateFromSnapshot(context.Background(), administrativeSnapshot(now, values...)); err != nil {
		t.Fatal(err)
	}
}

func administrativeMetricConfig() MetricConfig {
	return MetricConfig{Name: "admin", Retentions: []Retention{{Step: 60, Count: 256}}, AggregationMethod: Average}
}

func administrativeSnapshot(now int, values ...float64) Snapshot {
	config := administrativeMetricConfig()
	points := make([]Point, len(values))
	for i, value := range values {
		points[i] = Point{Timestamp: int64(now - (len(values)-i)*60), Value: value}
	}
	return Snapshot{Metadata: Metadata{MetricConfig: config}, Archives: []Archive{{Retention: config.Retentions[0], Points: points}}}
}

func assertAdminState(t *testing.T, s *Store, want adminState) {
	t.Helper()
	snapshot, err := s.Snapshot(context.Background(), "admin")
	if !want.exists {
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("deleted metric recovered: %v", err)
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Metadata.Generation != want.generation || snapshot.Metadata.Revision != want.revision {
		t.Fatalf("metadata = %+v, want generation %d revision %d", snapshot.Metadata, want.generation, want.revision)
	}
	points := snapshot.Archives[0].Points
	if len(points) != len(want.values) {
		t.Fatalf("points = %+v, want values %v", points, want.values)
	}
	for i, point := range points {
		if point.Value != want.values[i] {
			t.Fatalf("point %d = %+v, want %v", i, point, want.values[i])
		}
	}
}

func TestPeriodicSyncWithConcurrentUpdates(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		strict := vfs.NewStrictMem()
		s, now := openTestStore(t, "/store", Options{fs: strict, SyncInterval: time.Millisecond})
		closed := false
		defer func() {
			if !closed {
				closeTestStore(t, s)
			}
		}()
		createTestMetric(t, s, "concurrent")
		const writers = 8
		const updates = 10
		var wg sync.WaitGroup
		for i := range writers {
			wg.Go(func() {
				for j := range updates {
					if err := s.UpdateMany(context.Background(), "concurrent", []Point{{Timestamp: int64(now - (i+1)*60), Value: float64(j)}}); err != nil {
						t.Error(err)
						return
					}
					time.Sleep(time.Millisecond)
				}
			})
		}
		wg.Wait()
		time.Sleep(time.Millisecond)
		synctest.Wait()
		strict.SetIgnoreSyncs(true)
		closed = true
		closeTestStore(t, s)
		strict.ResetToSyncedState()
		strict.SetIgnoreSyncs(false)
		reopened, _ := openTestStore(t, "/store", Options{fs: strict})
		defer closeTestStore(t, reopened)
		snapshot, err := reopened.Snapshot(context.Background(), "concurrent")
		if err != nil {
			t.Fatal(err)
		}
		if len(snapshot.Archives[0].Points) != writers || snapshot.Metadata.Revision != writers*updates+1 {
			t.Fatalf("recovered concurrent writes = %+v", snapshot)
		}
		for _, point := range snapshot.Archives[0].Points {
			if point.Value != updates-1 {
				t.Fatalf("recovered stale point = %+v", point)
			}
		}
	})
}

func TestSyncPolicyCrashRecovery(t *testing.T) {
	for _, tt := range []struct {
		name       string
		interval   time.Duration
		wait       bool
		crash      bool
		wantPoints int
	}{
		{name: "synchronous", crash: true, wantPoints: 2},
		{name: "unsynced updates lost", interval: time.Second, crash: true},
		{name: "periodic sync", interval: time.Second, wait: true, crash: true, wantPoints: 2},
		{name: "shutdown sync", interval: time.Hour, wantPoints: 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				strict := vfs.NewStrictMem()
				fs := &walSyncFS{FS: strict}
				const dir = "/store"
				s, now := openTestStore(t, dir, Options{fs: fs, SyncInterval: tt.interval})
				closed := false
				defer func() {
					if !closed {
						closeTestStore(t, s)
					}
				}()
				createTestMetric(t, s, "sync")
				waitForPendingSync(t, tt.interval)
				before := fs.syncs.Load()
				if err := s.UpdateMany(context.Background(), "sync", []Point{{Timestamp: int64(now - 120), Value: 1}}); err != nil {
					t.Fatal(err)
				}
				if err := s.UpdateManyForArchive(context.Background(), "sync", []Point{{Timestamp: int64(now - 60), Value: 2}}, 60*256); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				wantSyncs := before
				if tt.interval == 0 {
					wantSyncs += 2
				}
				if got := fs.syncs.Load(); got != wantSyncs {
					t.Fatalf("WAL syncs before interval = %d, want %d", got, wantSyncs)
				}
				if tt.wait {
					checkPeriodicSyncBoundary(t, s, fs, before, tt.interval, now)
				}

				// Discard all unsynced bytes, including any writes Pebble makes
				// while closing, to model loss of both process and OS buffers.
				strict.SetIgnoreSyncs(tt.crash)
				closed = true
				closeTestStore(t, s)
				strict.ResetToSyncedState()
				strict.SetIgnoreSyncs(false)
				reopened, _ := openTestStore(t, dir, Options{fs: strict})
				defer closeTestStore(t, reopened)
				snapshot, err := reopened.Snapshot(context.Background(), "sync")
				if err != nil {
					t.Fatal(err)
				}
				points := snapshot.Archives[0].Points
				if len(points) != tt.wantPoints || snapshot.Metadata.Revision != uint64(tt.wantPoints+1) {
					t.Fatalf("recovered snapshot = %+v, want %d points and revision %d", snapshot, tt.wantPoints, tt.wantPoints+1)
				}
				for i, p := range points {
					if p.Value != float64(i+1) {
						t.Fatalf("recovered point %d = %+v", i, p)
					}
				}
			})
		})
	}
}

func checkPeriodicSyncBoundary(t *testing.T, s *Store, fs *walSyncFS, before int64, interval time.Duration, now int) {
	t.Helper()
	time.Sleep(interval - time.Nanosecond)
	synctest.Wait()
	if got := fs.syncs.Load(); got != before {
		t.Fatalf("WAL synced before configured interval: %d", got)
	}
	time.Sleep(time.Nanosecond)
	synctest.Wait()
	if got := fs.syncs.Load(); got != before+1 {
		t.Fatalf("periodic WAL syncs = %d, want %d", got, before+1)
	}
	time.Sleep(2 * interval)
	synctest.Wait()
	if got := fs.syncs.Load(); got != before+1 {
		t.Fatalf("idle store synced its WAL: %d", got)
	}
	// A later, unsynced update must not survive the simulated crash.
	if err := s.UpdateMany(context.Background(), "sync", []Point{{Timestamp: int64(now), Value: 99}}); err != nil {
		t.Fatal(err)
	}
}

func TestPeriodicSyncFailureIsFailStop(t *testing.T) {
	const childEnv = "GO_CARBON_CHUNKSTORE_PERIODIC_SYNC_FAILURE_CHILD"
	if os.Getenv(childEnv) == "1" {
		synctest.Test(t, func(t *testing.T) {
			fs := &syncFailFS{FS: vfs.NewMem()}
			s, now := openTestStore(t, "/store", Options{fs: fs, SyncInterval: time.Second})
			createTestMetric(t, s, "sync-failure")
			if err := s.UpdateMany(context.Background(), "sync-failure", []Point{{Timestamp: int64(now - 60), Value: 1}}); err != nil {
				t.Fatal(err)
			}
			fs.fail.Store(true)
			time.Sleep(2 * time.Second)
			t.Fatal("periodic WAL Sync failure did not stop the process")
		})
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestPeriodicSyncFailureIsFailStop$")
	cmd.Env = append(os.Environ(), childEnv+"=1")
	output, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "pebble: fatal commit error: injected sync failure") {
		t.Fatalf("child did not fail from WAL sync error: %v\n%s", err, output)
	}
}

func TestOpenRejectsNegativeSyncInterval(t *testing.T) {
	fs := vfs.NewMem()
	if _, err := Open("/store", Options{fs: fs, SyncInterval: -time.Second}); err == nil {
		t.Fatal("negative sync interval accepted")
	}
	if _, err := fs.Stat("/store"); !os.IsNotExist(err) {
		t.Fatalf("invalid options created store directory: %v", err)
	}
}

// Count only WAL syncs; manifest and directory syncs do not make mutations durable.
type walSyncFS struct {
	vfs.FS
	syncs atomic.Int64
}

func (fs *walSyncFS) Create(name string) (vfs.File, error) {
	f, err := fs.FS.Create(name)
	if err == nil && strings.HasSuffix(name, ".log") {
		return walSyncFile{File: f, syncs: &fs.syncs}, nil
	}
	return f, err
}

type walSyncFile struct {
	vfs.File
	syncs *atomic.Int64
}

func (f walSyncFile) Sync() error {
	if err := f.File.Sync(); err != nil {
		return err
	}
	f.syncs.Add(1)
	return nil
}

func (f walSyncFile) SyncData() error {
	if err := f.File.SyncData(); err != nil {
		return err
	}
	f.syncs.Add(1)
	return nil
}
