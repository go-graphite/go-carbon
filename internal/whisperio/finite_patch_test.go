package whisperio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/points"
)

var finiteDirs sync.Map

func finiteFixture(t *testing.T) (*store.Store, store.Snapshot, store.FinitePatch) {
	t.Helper()
	return finiteFixtureCodec(t, &ContinuationCodec{})
}
func finiteFixtureCodec(t *testing.T, codec store.ContinuationCodec) (*store.Store, store.Snapshot, store.FinitePatch) {
	t.Helper()
	ctx := context.Background()
	opts := coldOptions(codec)
	opts.Now = func() time.Time { return time.Unix(1793976840, 0) }
	dir := t.TempDir()
	db, err := store.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, ok := finiteDirs.LoadAndDelete(db); ok {
			db.Close()
		}
	})
	db.StopBackground()
	finiteDirs.Store(db, dir)
	r := []store.Retention{{Step: 60, Count: 20160}, {Step: 1800, Count: 35040}}
	snapshot := store.Snapshot{Metadata: store.Metadata{MetricConfig: store.MetricConfig{Name: "finite.metric", Retentions: r, AggregationMethod: store.Average}}, Archives: []store.Archive{
		{Retention: r[0], Points: []store.Point{{Timestamp: 1793976600, Value: math.Float64frombits(0x7ff8000000000042)}, {Timestamp: 1793976720, Value: math.Copysign(0, -1)}, {Timestamp: 1793976780, Value: 11639048}}},
		{Retention: r[1], Points: []store.Point{{Timestamp: 1793975400, Value: math.Float64frombits(0x41663513c9249249)}}},
	}}
	kv := mapContinuationIO{}
	if err := (&ContinuationCodec{}).RestoreArchives(ctx, kv, snapshot, opts.Now); err != nil {
		t.Fatal(err)
	}
	record := store.TransferMetric{Config: snapshot.Metadata.MetricConfig, Version: store.TransferVersion{Name: snapshot.Metadata.Name, ID: 1, Generation: 1, Revision: 1}, Continuation: true}
	for k, v := range kv {
		record.Keys = append(record.Keys, store.RelayoutKV{Key: []byte(k), Value: v})
	}
	slices.SortFunc(record.Keys, func(a, b store.RelayoutKV) int { return bytes.Compare(a.Key, b.Key) })
	if _, err = db.ImportTransfer(ctx, []store.TransferMetric{record}); err != nil {
		t.Fatal(err)
	}
	snapshot, err = db.Snapshot(ctx, snapshot.Metadata.Name)
	if err != nil {
		t.Fatal(err)
	}
	records, err := db.ExportTransfer(ctx, []string{snapshot.Metadata.Name})
	if err != nil {
		t.Fatal(err)
	}
	p := store.FinitePatch{Expected: records[0].Version, Archive: 0, Step: 60, Timestamp: 1793976780, BeforeBits: "4166332100000000", DonorBits: "416632d5c0000000", PlanSHA256: fmt.Sprintf("%064x", 1), Nonce: fmt.Sprintf("%032x", 1)}
	return db, snapshot, p
}

func TestFiniteExistingPathsCannotQualify(t *testing.T) {
	t.Run("intake propagates into protected coarse point", func(t *testing.T) {
		db, before, p := finiteFixture(t)
		if err := db.UpdateMany(context.Background(), p.Expected.Name, []store.Point{{Timestamp: p.Timestamp, Value: 11638446}}); err != nil {
			t.Fatal(err)
		}
		after, err := db.Snapshot(context.Background(), p.Expected.Name)
		if err != nil {
			t.Fatal(err)
		}
		if math.Float64bits(before.Archives[1].Points[0].Value) == math.Float64bits(after.Archives[1].Points[0].Value) {
			t.Fatal("fixture did not expose rollup propagation")
		}
	})
	t.Run("fill keeps wrong finite value", func(t *testing.T) {
		db, donor, p := finiteFixture(t)
		donor.Archives[0].Points[2].Value = 11638446
		if _, err := db.Fill(context.Background(), donor); err == nil {
			t.Fatal("legacy flattened fill unexpectedly accepted continuation")
		}
		after, err := db.Snapshot(context.Background(), p.Expected.Name)
		if err != nil {
			t.Fatal(err)
		}
		if math.Float64bits(after.Archives[0].Points[2].Value) != 0x4166332100000000 {
			t.Fatal("finite fill semantics changed")
		}
	})
	t.Run("whole file replacement changes protected coarse and identity", func(t *testing.T) {
		db, donor, p := finiteFixture(t)
		donor.Archives[0].Points[2].Value = 11638446
		donor.Archives[1].Points[0].Value = math.Float64frombits(0x41662c2b75555555)
		path := filepath.Join(t.TempDir(), "donor.wsp")
		a := New(db)
		if err := a.ExportSnapshot(context.Background(), donor, path); err != nil {
			t.Fatal(err)
		}
		if _, err := a.ImportWSP(context.Background(), p.Expected.Name, path, true); err != nil {
			t.Logf("whole-file replacement refuses continuation: %v", err)
			return
		}
		after, err := db.Snapshot(context.Background(), p.Expected.Name)
		if err != nil {
			t.Fatal(err)
		}
		if math.Float64bits(after.Archives[1].Points[0].Value) != 0x41662c2b75555555 {
			t.Fatal("fixture did not expose whole-file replacement")
		}
	})
}

func TestFinitePatchOnlyOnePointAndDurable(t *testing.T) {
	db, before, p := finiteFixture(t)
	ctx := context.Background()
	core := cache.New()
	r, err := db.PatchFinite(ctx, p, core.FenceEmptyMetric)
	if err != nil || r.Applied {
		t.Fatalf("dry-run: %+v %v", r, err)
	}
	unchanged, _ := db.Snapshot(ctx, p.Expected.Name)
	if err := store.EqualSnapshots(before, unchanged); err != nil {
		t.Fatal(err)
	}
	p.Apply = true
	r, err = db.PatchFinite(ctx, p, core.FenceEmptyMetric)
	if err != nil || !r.Applied {
		t.Fatalf("apply: %+v %v", r, err)
	}
	after, err := db.Snapshot(ctx, p.Expected.Name)
	if err != nil {
		t.Fatal(err)
	}
	before.Archives[0].Points[2].Value = 11638446
	if err := store.EqualSnapshots(before, after); err != nil {
		t.Fatal(err)
	}
	if _, err = db.PatchFinite(ctx, p, core.FenceEmptyMetric); err == nil {
		t.Fatal("replayed stale version")
	}
	// Reopen actual filesystem/WAL and compare the entire exact image.
	dir, _ := finiteDirs.Load(db)
	finiteDirs.Delete(db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(dir.(string), coldOptions(&ContinuationCodec{}))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopened.StopBackground()
	durable, err := reopened.Snapshot(ctx, p.Expected.Name)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EqualSnapshots(before, durable); err != nil {
		t.Fatal(err)
	}
	// The durable receipt also refuses plan reuse after process restart.
	if _, err := reopened.PatchFinite(ctx, p, core.FenceEmptyMetric); err == nil {
		t.Fatal("replayed consumed plan after restart")
	}
	db = reopened
	p.Expected = r.Version
	p.PlanSHA256 = fmt.Sprintf("%064x", 2)
	r, err = db.PatchFinite(ctx, p, core.FenceEmptyMetric)
	if err != nil || !r.Noop || r.Applied {
		t.Fatalf("noop: %+v %v", r, err)
	}
}

func TestFinitePatchRefusesQueuedAndConcurrentIntake(t *testing.T) {
	db, before, p := finiteFixture(t)
	ctx := context.Background()
	core := cache.New()
	p.Apply = true
	core.Add(points.OnePoint(p.Expected.Name, 1793976780, 11639048))
	if _, err := db.PatchFinite(ctx, p, core.FenceEmptyMetric); err == nil {
		t.Fatal("accepted queued conflicting preimage")
	}
	batch, ok := core.PopNotConfirmed(p.Expected.Name)
	if !ok {
		t.Fatal("missing queued batch")
	}
	if _, err := db.PatchFinite(ctx, p, core.FenceEmptyMetric); err == nil {
		t.Fatal("accepted in-flight conflicting preimage")
	}
	core.Confirm(batch)
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := db.PatchFinite(ctx, p, func(ctx context.Context, name string, publish func() error) error {
			close(entered)
			<-release
			return core.FenceEmptyMetric(ctx, name, publish)
		})
		done <- err
	}()
	<-entered
	// New intake between immutable preparation and the actual cache fence
	// must cause refusal; the committed image must remain wholly untouched.
	core.Add(points.OnePoint(p.Expected.Name, 1793976780, 11639048))
	close(release)
	if err := <-done; err == nil {
		t.Fatal("concurrent intake accepted")
	}
	after, _ := db.Snapshot(ctx, p.Expected.Name)
	if err := store.EqualSnapshots(before, after); err != nil {
		t.Fatal(err)
	}
}

type blockingFiniteCodec struct {
	*ContinuationCodec
	entered, release chan struct{}
}

func (c blockingFiniteCodec) PlanFinitePatch(ctx context.Context, kv store.ContinuationIO, p store.FinitePatch) (store.FinitePatchPlan, error) {
	plan, err := c.ContinuationCodec.PlanFinitePatch(ctx, kv, p)
	close(c.entered)
	<-c.release
	return plan, err
}
func TestFinitePatchRefusesConcurrentCommittedWriter(t *testing.T) {
	codec := blockingFiniteCodec{&ContinuationCodec{}, make(chan struct{}), make(chan struct{})}
	db, _, p := finiteFixtureCodec(t, codec)
	p.Apply = true
	core := cache.New()
	done := make(chan error, 1)
	go func() { _, err := db.PatchFinite(context.Background(), p, core.FenceEmptyMetric); done <- err }()
	<-codec.entered
	if err := db.UpdateMany(context.Background(), p.Expected.Name, []store.Point{{Timestamp: p.Timestamp, Value: 123}}); err != nil {
		t.Fatal(err)
	}
	want, err := db.Snapshot(context.Background(), p.Expected.Name)
	if err != nil {
		t.Fatal(err)
	}
	close(codec.release)
	if err := <-done; err == nil {
		t.Fatal("stale immutable image overwrote foreground writer")
	}
	after, err := db.Snapshot(context.Background(), p.Expected.Name)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EqualSnapshots(want, after); err != nil {
		t.Fatal(err)
	}
}

func TestFinitePatchPostCommitErrorKeepsAppliedReceipt(t *testing.T) {
	db, before, p := finiteFixture(t)
	p.Apply = true
	core := cache.New()
	fault := errors.New("injected post-publication fence error")
	r, err := db.PatchFinite(context.Background(), p, func(ctx context.Context, name string, publish func() error) error {
		if err := core.FenceEmptyMetric(ctx, name, publish); err != nil {
			return err
		}
		return fault
	})
	if !errors.Is(err, fault) || !r.Applied {
		t.Fatalf("lost applied state: %+v %v", r, err)
	}
	before.Archives[0].Points[2].Value = 11638446
	after, readErr := db.Snapshot(context.Background(), p.Expected.Name)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if err := store.EqualSnapshots(before, after); err != nil {
		t.Fatal(err)
	}
	if _, err := db.PatchFinite(context.Background(), p, core.FenceEmptyMetric); err == nil {
		t.Fatal("post-commit error permitted replay")
	}
}

func TestFinitePatchInvalidAndAbsentPreimagesRefuse(t *testing.T) {
	db, before, p := finiteFixture(t)
	for _, change := range []func(*store.FinitePatch){
		func(p *store.FinitePatch) { p.BeforeBits = "7ff8000000000001" },
		func(p *store.FinitePatch) { p.DonorBits = "7ff0000000000000" },
		func(p *store.FinitePatch) { p.Archive = 1 },
		func(p *store.FinitePatch) { p.Timestamp-- },
		func(p *store.FinitePatch) { p.BeforeBits = "3ff0000000000000" },
		func(p *store.FinitePatch) { p.Timestamp -= 180 },
		func(p *store.FinitePatch) { p.Apply = true },
	} {
		request := p
		change(&request)
		if _, err := db.PatchFinite(context.Background(), request, nil); err == nil {
			t.Fatalf("accepted %+v", request)
		}
	}
	after, err := db.Snapshot(context.Background(), p.Expected.Name)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EqualSnapshots(before, after); err != nil {
		t.Fatal(err)
	}
}
