package chunkstore

import (
	"context"
	"errors"
	"math"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

func TestOpenRejectsLegacyOrInvalidMarkerWithoutModification(t *testing.T) {
	for _, marker := range []string{"", "go-carbon-chunks-v2\n", "go-carbon-chunks"} {
		t.Run(marker, func(t *testing.T) {
			fs := vfs.NewMem()
			const dir = "/store"
			if err := fs.MkdirAll(dir, 0755); err != nil {
				t.Fatal(err)
			}
			if marker == "" {
				f, err := fs.Create(fs.PathJoin(dir, "MANIFEST-legacy"))
				if err != nil {
					t.Fatal(err)
				}
				if err := f.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				f, err := fs.Create(fs.PathJoin(dir, "CHUNKSTORE"))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.Write([]byte(marker)); err != nil {
					t.Fatal(err)
				}
				if err := f.Close(); err != nil {
					t.Fatal(err)
				}
			}
			before, err := fs.List(dir)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := Open(dir, Options{fs: fs}); !errors.Is(err, ErrFormat) {
				t.Fatalf("Open error = %v, want ErrFormat", err)
			}
			after, err := fs.List(dir)
			if err != nil {
				t.Fatal(err)
			}
			if len(before) != len(after) {
				t.Fatalf("Open modified rejected directory: before=%v after=%v", before, after)
			}
			for i := range before {
				if before[i] != after[i] {
					t.Fatalf("Open modified rejected directory: before=%v after=%v", before, after)
				}
			}
		})
	}
}

func TestCreateMetadataUpdateAndExactFloatBits(t *testing.T) {
	s, now := openTestStore(t, t.TempDir(), Options{})
	defer closeTestStore(t, s)
	m := createTestMetric(t, s, "bits")
	if m.Revision != 1 || m.ID == 0 || m.Generation != 1 {
		t.Fatalf("unexpected created metadata: %#v", m)
	}
	got, err := s.Metadata(context.Background(), "bits")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, m) {
		t.Fatalf("metadata mismatch: got %#v want %#v", got, m)
	}

	negativeZero := math.Float64frombits(1 << 63)
	payloadNaN := math.Float64frombits(0x7ff8000000000042)
	points := []Point{{Timestamp: int64(now - 120), Value: negativeZero}, {Timestamp: int64(now - 60), Value: payloadNaN}}
	if err := s.UpdateManyForArchive(context.Background(), "bits", points, 60*256); err != nil {
		t.Fatal(err)
	}
	series, err := s.Fetch(context.Background(), "bits", now-180, now)
	if err != nil {
		t.Fatal(err)
	}
	if series.Metadata.Revision != 2 {
		t.Fatalf("fetch observed revision %d, want 2", series.Metadata.Revision)
	}
	if len(series.Values) != 3 {
		t.Fatalf("values = %v", series.Values)
	}
	if math.Float64bits(series.Values[0]) != math.Float64bits(negativeZero) || math.Float64bits(series.Values[1]) != math.Float64bits(payloadNaN) {
		t.Fatalf("float bits changed: %x %x", math.Float64bits(series.Values[0]), math.Float64bits(series.Values[1]))
	}
}

func TestRejectsRetentionBeyondTransferFormat(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("the invalid fields do not fit in int")
	}
	s, _ := openTestStore(t, t.TempDir(), Options{})
	defer closeTestStore(t, s)
	tooLarge := uint64(math.MaxUint32) + 1
	for _, retention := range []Retention{{Step: 1, Count: int(tooLarge)}, {Step: int(tooLarge), Count: 1}, {Step: 2, Count: int(tooLarge / 2)}} {
		_, err := s.Create(context.Background(), MetricConfig{
			Name: "invalid-retention", Retentions: []Retention{retention}, AggregationMethod: Average,
		})
		if err == nil {
			t.Fatalf("created unrepresentable retention: %+v", retention)
		}
	}
}

func TestRejectsTimestampOverflowOn32Bit(t *testing.T) {
	if strconv.IntSize != 32 {
		t.Skip("timestamp fits in int on this architecture")
	}
	s, _ := openTestStore(t, t.TempDir(), Options{})
	defer closeTestStore(t, s)
	m := createTestMetric(t, s, "overflow")
	ctx := context.Background()
	points := []Point{{Timestamp: int64(math.MaxInt32) + 1, Value: 9}}
	if err := s.UpdateMany(ctx, m.Name, points); err == nil {
		t.Fatal("normal update accepted an overflowing timestamp")
	}
	if err := s.UpdateManyForArchive(ctx, m.Name, points, 60*256); err == nil {
		t.Fatal("direct update accepted an overflowing timestamp")
	}
	got, err := s.Metadata(ctx, m.Name)
	if err != nil || got.Revision != m.Revision {
		t.Fatalf("rejected update changed revision: %+v, %v", got, err)
	}
	s.now = func() time.Time { return time.Unix(int64(math.MaxInt32)+1, 0) }
	if err := s.UpdateMany(ctx, m.Name, nil); err == nil {
		t.Fatal("update accepted an overflowing clock")
	}
	if _, err := s.Fetch(ctx, m.Name, 0, 1); err == nil {
		t.Fatal("fetch accepted an overflowing clock")
	}
}

func TestRejectsNegativeDirectArchiveAndSnapshotTimestamps(t *testing.T) {
	s, _ := openTestStore(t, t.TempDir(), Options{})
	defer closeTestStore(t, s)
	m := createTestMetric(t, s, "negative-direct")
	if err := s.UpdateManyForArchive(context.Background(), m.Name, []Point{{Timestamp: -1, Value: 1}}, 60*256); err == nil {
		t.Fatal("direct update accepted negative timestamp")
	}
	snapshot, err := s.Snapshot(context.Background(), m.Name)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Archives[0].Points) != 0 {
		t.Fatalf("negative timestamp wrote physical points: %#v", snapshot.Archives[0].Points)
	}

	config := MetricConfig{Name: "negative-snapshot", Retentions: []Retention{{Step: 60, Count: 256}}, AggregationMethod: Average}
	_, err = s.CreateFromSnapshot(context.Background(), Snapshot{Metadata: Metadata{MetricConfig: config}, Archives: []Archive{{Retention: config.Retentions[0], Points: []Point{{Timestamp: -1, Value: 1}}}}})
	if err == nil {
		t.Fatal("snapshot accepted negative timestamp")
	}
	if _, err := s.Metadata(context.Background(), config.Name); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rejected snapshot published metric: %v", err)
	}
}

func TestNormalUpdateDropsExpiredNegativeTimestamp(t *testing.T) {
	s, _ := openTestStore(t, t.TempDir(), Options{})
	defer closeTestStore(t, s)
	m := createTestMetric(t, s, "expired-negative")
	if err := s.UpdateMany(context.Background(), m.Name, []Point{{Timestamp: -1, Value: 1}}); err != nil {
		t.Fatalf("expired negative update = %v, want classic-compatible drop", err)
	}
	snapshot, err := s.Snapshot(context.Background(), m.Name)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Archives[0].Points) != 0 {
		t.Fatalf("expired negative timestamp wrote physical points: %#v", snapshot.Archives[0].Points)
	}
}

func TestWriteMaterializationAtFourSurvivesCompactionAndReopen(t *testing.T) {
	dir := t.TempDir()
	s, now := openTestStore(t, dir, Options{})
	m := createTestMetric(t, s, "materialize")
	if deltasBeforeSet >= materializeAt {
		t.Fatalf("write threshold %d must remain below codec limit %d", deltasBeforeSet, materializeAt)
	}
	// The initial Set and three full merge chains exercise the shorter write
	// bound without changing the independently validated 32-operand format.
	const chains = 3
	updates := 1 + deltasBeforeSet*chains
	for i := 0; i < updates; i++ {
		if err := s.UpdateManyForArchive(context.Background(), "materialize", []Point{{Timestamp: int64(now - 60), Value: float64(i)}}, 60*256); err != nil {
			t.Fatalf("update %d: %v", i, err)
		}
		if deltas := storedChunkDeltas(t, s, m, now-60); deltas >= deltasBeforeSet {
			t.Fatalf("update %d left %d merge operands, bound is %d", i, deltas, deltasBeforeSet)
		}
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	m, err := s.Metadata(context.Background(), "materialize")
	if err != nil {
		t.Fatal(err)
	}
	v, closer, err := s.db.Get(chunkKey(m, 0, ((now-60)/60)%256/chunkSlots))
	if err != nil {
		t.Fatal(err)
	}
	var c chunk
	err = decodeChunk(v, &c)
	closer.Close()
	if err != nil {
		t.Fatal(err)
	}
	if c.Deltas != 0 {
		t.Fatalf("Deltas = %d, want materialized 0", c.Deltas)
	}
	if got, want := s.Stats().Materializations, uint64(1+chains); got != want {
		t.Fatalf("materializations = %d, want %d", got, want)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	closeTestStore(t, s)
	s, _ = openTestStore(t, dir, Options{})
	defer closeTestStore(t, s)
	series, err := s.Fetch(context.Background(), "materialize", now-120, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(series.Values) != 2 || series.Values[0] != float64(updates-1) || !math.IsNaN(series.Values[1]) {
		t.Fatalf("reopened values = %v", series.Values)
	}
}

const (
	syncFailureChild = "GO_CARBON_CHUNKSTORE_SYNC_FAILURE_CHILD"
	syncFailureDir   = "GO_CARBON_CHUNKSTORE_SYNC_FAILURE_DIR"
)

func TestSyncFailureIsFailStopAndPreservesAcknowledgedWrite(t *testing.T) {
	if os.Getenv(syncFailureChild) == "1" {
		runSyncFailureChild(t, os.Getenv(syncFailureDir))
		return
	}

	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestSyncFailureIsFailStopAndPreservesAcknowledgedWrite$")
	cmd.Env = append(os.Environ(), syncFailureChild+"=1", syncFailureDir+"="+dir)
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("child unexpectedly survived a WAL Sync failure: %s", output)
	}
	if !strings.Contains(string(output), "pebble: fatal commit error: injected sync failure") {
		t.Fatalf("child did not fail from Pebble's fatal commit path: %s", output)
	}
	if strings.Contains(string(output), "WAL Sync failure returned instead of failing the process") {
		t.Fatalf("child reached the post-update fallback: %s", output)
	}

	// Pebble v1.1.5 treats a WAL commit failure as fatal. That fail-stop
	// behavior occurs before go-carbon can acknowledge the update; reopening
	// must retain every write that had already completed a successful Sync.
	s, now := openTestStore(t, dir, Options{})
	defer closeTestStore(t, s)
	series, err := s.Fetch(context.Background(), "recovery", now-180, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(series.Values) < 1 || series.Values[0] != 1 {
		t.Fatalf("recovered values = %v, want acknowledged value", series.Values)
	}
}

func runSyncFailureChild(t *testing.T, dir string) {
	t.Helper()
	if dir == "" {
		t.Fatal("missing child store directory")
	}
	fs := &syncFailFS{FS: vfs.Default}
	s, now := openTestStore(t, dir, Options{fs: fs})
	createTestMetric(t, s, "recovery")
	if err := s.UpdateManyForArchive(context.Background(), "recovery", []Point{{Timestamp: int64(now - 120), Value: 1}}, 60*256); err != nil {
		t.Fatal(err)
	}
	fs.fail.Store(true)
	// The default Pebble logger terminates the process here. Reaching this line
	// would silently acknowledge a write after its requested Sync failed.
	_ = s.UpdateManyForArchive(context.Background(), "recovery", []Point{{Timestamp: int64(now - 60), Value: 2}}, 60*256)
	t.Fatal("WAL Sync failure returned instead of failing the process")
}

func TestStrictMemCrashPreservesSyncedMaterialization(t *testing.T) {
	strict := vfs.NewStrictMem()
	const dir = "/new-parent/store"
	s, now := openTestStore(t, dir, Options{fs: strict})
	m := createTestMetric(t, s, "strict-recovery")
	const strictChains = 3
	strictUpdates := 1 + deltasBeforeSet*strictChains
	for i := 0; i < strictUpdates; i++ {
		if err := s.UpdateManyForArchive(context.Background(), "strict-recovery", []Point{{Timestamp: int64(now - 60), Value: float64(i)}}, 60*256); err != nil {
			t.Fatal(err)
		}
		if deltas := storedChunkDeltas(t, s, m, now-60); deltas >= deltasBeforeSet {
			t.Fatalf("update %d left %d merge operands, bound is %d", i, deltas, deltasBeforeSet)
		}
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}

	// Treat this write as unacknowledged. StrictMem discards it during the
	// simulated crash; code must not rely on its presence after recovery.
	strict.SetIgnoreSyncs(true)
	if err := s.UpdateManyForArchive(context.Background(), "strict-recovery", []Point{{Timestamp: int64(now), Value: 99}}, 60*256); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	strict.ResetToSyncedState()
	strict.SetIgnoreSyncs(false)
	s, _ = openTestStore(t, dir, Options{fs: strict})
	defer closeTestStore(t, s)

	m, err := s.Metadata(context.Background(), "strict-recovery")
	if err != nil {
		t.Fatal(err)
	}
	v, closer, err := s.db.Get(chunkKey(m, 0, ((now-60)/60)%256/chunkSlots))
	if err != nil {
		t.Fatal(err)
	}
	var c chunk
	err = decodeChunk(v, &c)
	closer.Close()
	if err != nil {
		t.Fatal(err)
	}
	if c.Deltas != 0 {
		t.Fatalf("Deltas after strict recovery = %d, want materialized 0", c.Deltas)
	}
	series, err := s.Fetch(context.Background(), "strict-recovery", now-120, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(series.Values) != 2 || series.Values[0] != float64(strictUpdates-1) {
		t.Fatalf("recovered values = %v, want synced materialized value", series.Values)
	}
	if !math.IsNaN(series.Values[1]) && series.Values[1] != 99 {
		t.Fatalf("unacknowledged value = %v, want missing or 99", series.Values[1])
	}
}

func storedChunkDeltas(t *testing.T, s *Store, m Metadata, timestamp int) uint32 {
	t.Helper()
	r := m.Retentions[0]
	slot := (timestamp / r.Step) % r.Count
	v, closer, err := s.db.Get(chunkKey(m, 0, slot/chunkSlots))
	if err != nil {
		t.Fatal(err)
	}
	defer closer.Close()
	var c chunk
	if err := decodeChunk(v, &c); err != nil {
		t.Fatal(err)
	}
	return c.Deltas
}

func TestConcurrentUpdatesHaveConsistentFetchMetadata(t *testing.T) {
	s, now := openTestStore(t, t.TempDir(), Options{})
	defer closeTestStore(t, s)
	createTestMetric(t, s, "concurrent")
	const writers = 16
	var wg sync.WaitGroup
	errCh := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errCh <- s.UpdateManyForArchive(context.Background(), "concurrent", []Point{{Timestamp: int64(now - (i+1)*60), Value: float64(i)}}, 60*256)
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	series, err := s.Fetch(context.Background(), "concurrent", now-(writers+1)*60, now)
	if err != nil {
		t.Fatal(err)
	}
	if series.Metadata.Revision != writers+1 {
		t.Fatalf("revision = %d, want %d", series.Metadata.Revision, writers+1)
	}
	for i, value := range series.Values[:writers] {
		if value != float64(writers-1-i) {
			t.Fatalf("values[%d] = %v, want %d", i, value, writers-1-i)
		}
	}
	if !math.IsNaN(series.Values[writers]) {
		t.Fatalf("values[%d] = %v, want NaN", writers, series.Values[writers])
	}
}

func openTestStore(t *testing.T, dir string, opts Options) (*Store, int) {
	t.Helper()
	const now = 100_000
	if opts.Now == nil {
		opts.Now = func() time.Time { return time.Unix(now, 0) }
	}
	s, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	return s, now
}

func closeTestStore(t *testing.T, s *Store) {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func createTestMetric(t *testing.T, s *Store, name string) Metadata {
	t.Helper()
	m, err := s.Create(context.Background(), MetricConfig{
		Name:              name,
		Retentions:        []Retention{{Step: 60, Count: 256}},
		AggregationMethod: Average,
	})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

type syncFailFS struct {
	vfs.FS
	fail atomic.Bool
}

func (fs *syncFailFS) Create(name string) (vfs.File, error) {
	f, err := fs.FS.Create(name)
	return fs.wrap(f), err
}
func (fs *syncFailFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	f, err := fs.FS.Open(name, opts...)
	return fs.wrap(f), err
}
func (fs *syncFailFS) OpenReadWrite(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	f, err := fs.FS.OpenReadWrite(name, opts...)
	return fs.wrap(f), err
}
func (fs *syncFailFS) OpenDir(name string) (vfs.File, error) {
	f, err := fs.FS.OpenDir(name)
	return fs.wrap(f), err
}
func (fs *syncFailFS) ReuseForWrite(oldname, newname string) (vfs.File, error) {
	f, err := fs.FS.ReuseForWrite(oldname, newname)
	return fs.wrap(f), err
}
func (fs *syncFailFS) wrap(f vfs.File) vfs.File {
	if f == nil {
		return nil
	}
	return syncFailFile{File: f, fail: &fs.fail}
}

type syncFailFile struct {
	vfs.File
	fail *atomic.Bool
}

func (f syncFailFile) Sync() error {
	if f.fail.Load() {
		return errors.New("injected sync failure")
	}
	return f.File.Sync()
}

func (f syncFailFile) SyncData() error {
	if f.fail.Load() {
		return errors.New("injected sync failure")
	}
	return f.File.SyncData()
}

func (f syncFailFile) SyncTo(length int64) (bool, error) {
	if f.fail.Load() {
		return false, errors.New("injected sync failure")
	}
	return f.File.SyncTo(length)
}
