package partsconvert

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/partsformat"
)

type reservationFunc func(uint64) (func(), error)

func (f reservationFunc) Reserve(n uint64) (func(), error) { return f(n) }

type publisherFunc func(context.Context, Receipt) error
type reclaimerFunc func(context.Context, Receipt) error

func (f reclaimerFunc) Reclaim(ctx context.Context, r Receipt) error { return f(ctx, r) }

func (f publisherFunc) Publish(ctx context.Context, r Receipt) error { return f(ctx, r) }

type targetHook struct {
	PebbleTarget
	ingest func() error
	verify func() error
}

func (h targetHook) Ingest(ctx context.Context, t []Table) error {
	if err := h.PebbleTarget.Ingest(ctx, t); err != nil {
		return err
	}
	if h.ingest != nil {
		return h.ingest()
	}
	return nil
}
func (h targetHook) Verify(ctx context.Context, t []Table) error {
	if h.verify != nil {
		if err := h.verify(); err != nil {
			return err
		}
	}
	return h.PebbleTarget.Verify(ctx, t)
}

func fixture() (Unit, []StoredMetric, ArchiveSource) {
	m := StoredMetric{Name: "test.metric", Group: "test", Method: 1, XFilesFactorBits: math.Float32bits(.75)}
	for ai, step := range []uint32{1, 60, 3600, 86400} {
		ar := StoredArchive{Step: step, Slots: 1024}
		for i, bits := range []uint64{0, 1 << 63, 1, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000001, 0x7ff0000000000001, math.Float64bits(float64(ai) + 1.25)} {
			ar.Points = append(ar.Points, StoredPoint{Timestamp: int64(i-2) * int64(step), ValueBits: bits})
		}
		m.Archives = append(m.Archives, ar)
	}
	u := Unit{Group: "test", VShard: partsformat.Route(m.Name)}
	source := ArchiveSource{SnapshotID: "immutable-input-frontier:42;policy:one", PartitionSeconds: 60, BlockPoints: 3,
		UpperBound:  func(context.Context, Unit) (uint64, error) { return 8 << 20, nil },
		ScanMetrics: func(_ context.Context, _ Unit, y func(StoredMetric) error) error { return y(m) },
	}
	return u, []StoredMetric{m}, source
}
func converterFor(t *testing.T, root string, source Source) (*Converter, *pebble.DB) {
	t.Helper()
	db, err := pebble.Open(filepath.Join(root, "db"), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	c := &Converter{Directory: filepath.Join(root, "journal"), Source: source, Target: PebbleTarget{DB: db, DestinationID: "destination-incarnation-1"}, Space: &SpaceGuard{Path: root}, ChunkBytes: 2048, MaxTables: 1000}
	return c, db
}

func TestStoredArchivesBitExact(t *testing.T) {
	u, original, source := fixture()
	c, db := converterFor(t, t.TempDir(), source)
	defer db.Close()
	r, err := c.Run(context.Background(), u)
	if err != nil {
		t.Fatal(err)
	}
	if r.State != "ready" || r.Points != 32 {
		t.Fatalf("unexpected receipt %+v", r)
	}
	got := make([][]StoredPoint, 4)
	for _, table := range r.Tables {
		it, err := db.NewIter(&pebble.IterOptions{LowerBound: table.Prefix, UpperBound: partsformat.PrefixEnd(table.Prefix)})
		if err != nil {
			t.Fatal(err)
		}
		for ok := it.First(); ok; ok = it.Next() {
			key, err := partsformat.Decode(it.Key())
			if err != nil {
				t.Fatal(err)
			}
			meta, archive, points, err := DecodeArchive(it.Value())
			if err != nil {
				t.Fatal(err)
			}
			if meta.Method != original[0].Method || meta.XFilesFactorBits != original[0].XFilesFactorBits || archive != key.Archive {
				t.Fatal("policy changed")
			}
			for ai, ar := range meta.Archives {
				if ar.Step != original[0].Archives[ai].Step || ar.Slots != original[0].Archives[ai].Slots {
					t.Fatal("schema changed")
				}
			}
			got[archive] = append(got[archive], points...)
		}
		if err := errors.Join(it.Error(), it.Close()); err != nil {
			t.Fatal(err)
		}
	}
	// SST part order is intentionally independent from logical archive order.
	for ai, points := range got {
		want := make(map[int64]uint64)
		for _, p := range original[0].Archives[ai].Points {
			want[p.Timestamp] = p.ValueBits
		}
		if len(points) != len(want) {
			t.Fatal("point count changed")
		}
		for _, p := range points {
			if bits, ok := want[p.Timestamp]; !ok || bits != p.ValueBits {
				t.Fatalf("archive%d timestamp%d bits changed: %016x", ai, p.Timestamp, p.ValueBits)
			}
		}
	}
	if _, err := Ready(c.Directory, u, source.Identity(), c.Target.Identity()); err != nil {
		t.Fatal(err)
	}
	if _, err := Ready(c.Directory, u, source.Identity(), "other-incarnation"); err == nil {
		t.Fatal("accepted wrong destination")
	}
}

func TestVerificationFailureNeverPublishes(t *testing.T) {
	u, _, source := fixture()
	c, db := converterFor(t, t.TempDir(), source)
	defer db.Close()
	called := false
	c.Publisher = publisherFunc(func(context.Context, Receipt) error { called = true; return nil })
	base := c.Target.(PebbleTarget)
	c.Target = targetHook{PebbleTarget: base, verify: func() error {
		it, err := db.NewIter(nil)
		if err != nil {
			return err
		}
		if !it.First() {
			return errors.New("missing fixture")
		}
		key := bytes.Clone(it.Key())
		_ = it.Close()
		return db.Set(key, []byte("corrupt"), pebble.Sync)
	}}
	r, err := c.Run(context.Background(), u)
	if err == nil || !strings.Contains(err.Error(), "digest mismatch") || called || r.State != "ingested" {
		t.Fatalf("bad verification gate: %+v %v published%v", r, err, called)
	}
	if _, err := Ready(c.Directory, u, source.Identity(), base.Identity()); err == nil {
		t.Fatal("unverified shard ready")
	}
}

func TestCrashAfterDurableIngest(t *testing.T) {
	if root := os.Getenv("PARTSCONVERT_CRASH_ROOT"); root != "" {
		u, _, source := fixture()
		c, _ := converterFor(t, root, source)
		c.Target = targetHook{PebbleTarget: c.Target.(PebbleTarget), ingest: func() error { os.Exit(77); return nil }}
		_, err := c.Run(context.Background(), u)
		t.Fatalf("crash hook not reached: %v", err)
	}
	root := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashAfterDurableIngest$")
	cmd.Env = append(os.Environ(), "PARTSCONVERT_CRASH_ROOT="+root)
	output, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 77 {
		t.Fatalf("child: %v %s", err, output)
	}
	u, _, source := fixture()
	source.ScanMetrics = func(context.Context, Unit, func(StoredMetric) error) error {
		t.Fatal("resumed prepared unit rescanned legacy")
		return nil
	}
	c, db := converterFor(t, root, source)
	defer db.Close()
	r, err := c.Run(context.Background(), u)
	if err != nil || r.State != "ready" || r.Points != 32 {
		t.Fatalf("resume: state%s points%d: %v", r.State, r.Points, err)
	}
	if err := c.Target.Verify(context.Background(), r.Tables); err != nil {
		t.Fatal(err)
	}
}

func TestResumePublicationAndIdentityFences(t *testing.T) {
	u, _, source := fixture()
	c, db := converterFor(t, t.TempDir(), source)
	defer db.Close()
	calls := 0
	c.Publisher = publisherFunc(func(context.Context, Receipt) error {
		calls++
		if calls == 1 {
			return errors.New("publication failed")
		}
		return nil
	})
	r, err := c.Run(context.Background(), u)
	if err == nil || r.State != "ready" {
		t.Fatalf("%+v %v", r, err)
	}
	source.ScanMetrics = func(context.Context, Unit, func(StoredMetric) error) error { t.Fatal("unexpected rescan"); return nil }
	c.Source = source
	r, err = c.Run(context.Background(), u)
	if err != nil || r.State != "published" || calls != 2 {
		t.Fatalf("%+v %v calls%d", r, err, calls)
	}
	r2, err := c.Run(context.Background(), u)
	if err != nil || !reflect.DeepEqual(r, r2) || calls != 2 {
		t.Fatal("publication not idempotent")
	}
	source.SnapshotID = "changed"
	c.Source = source
	if _, err = c.Run(context.Background(), u); err == nil {
		t.Fatal("source identity fence missing")
	}
}

func TestDiskGuardBeforeSourceScan(t *testing.T) {
	u, _, source := fixture()
	source.ScanMetrics = func(context.Context, Unit, func(StoredMetric) error) error {
		t.Fatal("source scanned with no disk space")
		return nil
	}
	c, db := converterFor(t, t.TempDir(), source)
	defer db.Close()
	c.Space = reservationFunc(func(uint64) (func(), error) { return nil, errors.New("out of space") })
	if _, err := c.Run(context.Background(), u); err == nil || !strings.Contains(err.Error(), "out of space") {
		t.Fatal(err)
	}
}

func TestBoundsAndEmptyArchive(t *testing.T) {
	u, metrics, source := fixture()
	m := metrics[0]
	m.Archives[2].Points = nil
	source.ScanMetrics = func(_ context.Context, _ Unit, y func(StoredMetric) error) error { return y(m) }
	var empty bool
	if err := source.Scan(context.Background(), u, func(r Record) error {
		_, ai, points, err := DecodeArchive(r.Value)
		if ai == 2 && len(points) == 0 {
			empty = true
		}
		return err
	}); err != nil || !empty {
		t.Fatalf("empty archive lost: %v", err)
	}
	for _, value := range [][]byte{nil, []byte("GCA1"), make([]byte, 24)} {
		if _, _, _, err := DecodeArchive(value); err == nil {
			t.Fatal("invalid payload accepted")
		}
	}
	root := t.TempDir()
	c, db := converterFor(t, root, source)
	defer db.Close()
	u.Group = "wrong"
	if _, err := c.Run(context.Background(), u); err == nil {
		t.Fatal("ownership mismatch accepted")
	}
	if _, err := os.Stat(filepath.Join(root, "journal")); err != nil {
		t.Fatal(err)
	}
}

func TestParallelUnitsAndJournalLock(t *testing.T) {
	u, _, source := fixture()
	c, db := converterFor(t, t.TempDir(), source)
	defer db.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	source.ScanMetrics = func(_ context.Context, _ Unit, y func(StoredMetric) error) error {
		close(entered)
		<-release
		_, m, _ := fixture()
		return y(m[0])
	}
	c.Source = source
	done := make(chan error, 1)
	go func() { _, err := c.Run(context.Background(), u); done <- err }()
	<-entered
	c2 := &Converter{Directory: c.Directory, Source: c.Source, Target: c.Target, Space: c.Space, ChunkBytes: 2048, MaxTables: 1000}
	if _, err := c2.Run(context.Background(), u); err == nil {
		t.Fatal("second converter acquired journal lock")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func ExampleArchiveSource() {
	fmt.Println("Supply stored raw and coarse archive slots; keep legacy immutable until verified publication.")
	// Output: Supply stored raw and coarse archive slots; keep legacy immutable until verified publication.
}

func TestReclaimOnlyAfterVerifiedPublishedAndRetry(t *testing.T) {
	u, _, source := fixture()
	c, db := converterFor(t, t.TempDir(), source)
	defer db.Close()
	u.Lower = "test."
	u.Upper = "test/~"
	published := false
	calls := 0
	c.Publisher = publisherFunc(func(ctx context.Context, r Receipt) error {
		if r.State != "ready" {
			t.Fatal("published unverified")
		}
		if err := c.Target.Verify(ctx, r.Tables); err != nil {
			return err
		}
		published = true
		return nil
	})
	c.Reclaimer = reclaimerFunc(func(_ context.Context, r Receipt) error {
		if !published || r.State != "published" {
			t.Fatal("deleted before publication")
		}
		calls++
		if calls == 1 {
			return errors.New("old readers still pinned")
		}
		return nil
	})
	c.MaxBatchBytes = 16 << 20
	r, err := c.Run(context.Background(), u)
	if err == nil || r.State != "published" {
		t.Fatalf("state%s err%v", r.State, err)
	}
	for _, table := range r.Tables {
		if _, err := os.Stat(table.Path); err != nil {
			t.Fatal("retry source removed early")
		}
	}
	r, err = c.Run(context.Background(), u)
	if err != nil || r.State != "reclaimed" || calls != 2 {
		t.Fatalf("state%s err%v calls%d", r.State, err, calls)
	}
	for _, table := range r.Tables {
		if _, err := os.Stat(table.Path); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("staging SST retained after reclaim")
		}
	}
	if err := c.Target.Verify(context.Background(), r.Tables); err != nil {
		t.Fatal("cleanup removed target data")
	}
	if _, err := Ready(c.Directory, u, source.Identity(), c.Target.Identity()); err != nil {
		t.Fatal(err)
	}
}

func TestCursorPreservesArbitraryNameBytes(t *testing.T) {
	u := Unit{Group: "\xfe", VShard: 123, Lower: "\x80.\x00", Upper: "\xff.\x00"}
	b, err := json.Marshal(u)
	if err != nil {
		t.Fatal(err)
	}
	var got Unit
	if err = json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got != u {
		t.Fatalf("cursor corrupted bytes: %#v -> %#v", u, got)
	}
}
