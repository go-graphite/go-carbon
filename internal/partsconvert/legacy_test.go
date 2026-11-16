package partsconvert

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/partsformat"
)

func binaryLegacyCatalog(id, generation uint64) []byte {
	b := make([]byte, 37)
	b[0] = 0x81
	binary.LittleEndian.PutUint64(b[1:], id)
	binary.LittleEndian.PutUint64(b[9:], generation)
	binary.LittleEndian.PutUint32(b[17:], 1)
	binary.LittleEndian.PutUint32(b[25:], 1)
	binary.LittleEndian.PutUint32(b[29:], 1)
	binary.LittleEndian.PutUint32(b[33:], 100)
	return b
}

func legacyFixture(t *testing.T, directory string, large bool) LegacyPlan {
	t.Helper()
	db, err := pebble.Open(directory, &pebble.Options{DisableAutomaticCompactions: true})
	if err != nil {
		t.Fatal(err)
	}
	value := []byte("unchanged legacy payload")
	if large {
		value = make([]byte, 128<<10)
		random := rand.New(rand.NewPCG(19, 81))
		for i := range value {
			value[i] = byte(random.Uint32())
		}
	}
	b := db.NewBatch()
	for _, m := range []struct {
		name string
		id   uint64
	}{{"a", 7}, {"m", 100}, {"z", 8}} {
		for _, prefix := range []string{"m/", "o/"} {
			v := []byte("sidecar")
			if prefix == "m/" {
				v = binaryLegacyCatalog(m.id, 2)
			}
			if err := b.Set(append([]byte(prefix), m.name...), v, nil); err != nil {
				t.Fatal(err)
			}
		}
		for generation := uint64(1); generation <= 2; generation++ {
			for _, kind := range []byte{'c', 'r', 'u'} {
				if err := b.Set(legacyGenerationPrefix(kind, m.id, generation), value, nil); err != nil {
					t.Fatal(err)
				}
			}
			for _, bucket := range []uint32{0, 123, ^uint32(0)} {
				key := append(binary.BigEndian.AppendUint32([]byte{'t'}, bucket), legacyGenerationPrefix('u', m.id, generation)...)
				if err := b.Set(key, value, nil); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if err := b.Set([]byte("z/sequence"), []byte("next ID is 101"), nil); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
	_ = b.Close()
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	plan, err := PlanLegacy(context.Background(), db, "source-1", []byte("group"), nil, []byte("n"), false, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Metrics) != 2 || !slices.Equal(plan.Buckets, []uint32{0, 123, ^uint32(0)}) {
		t.Fatalf("unexpected plan %+v", plan)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return plan
}

func legacySSTBytes(t *testing.T, root string) int64 {
	t.Helper()
	var total int64
	if err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(path, ".sst") {
			info, err := d.Info()
			if err != nil {
				return err
			}
			total += info.Size()
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return total
}

func assertLegacyRemainder(t *testing.T, directory string, p LegacyPlan) {
	t.Helper()
	db, err := pebble.Open(directory, &pebble.Options{ReadOnly: true, ErrorIfNotExists: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	it, err := db.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	count := 0
	for ok := it.First(); ok; ok = it.Next() {
		key := it.Key()
		for _, span := range p.spans() {
			if bytes.Compare(key, span.lower) >= 0 && bytes.Compare(key, span.upper) < 0 {
				t.Fatalf("converted archive survived: %x", key)
			}
		}
		if bytes.Equal(key, []byte("m/a")) || bytes.Equal(key, []byte("m/m")) || bytes.Equal(key, []byte("o/a")) || bytes.Equal(key, []byte("o/m")) {
			t.Fatalf("converted metadata survived: %x", key)
		}
		count++
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	// Unconverted ID 8 sits between selected IDs 7 and 100: widening their
	// deletion range would silently destroy this entire metric.
	if count != 15 {
		t.Fatalf("remaining keys=%d, want unconverted metric's 14 plus sequence", count)
	}
}

func TestLegacyPhysicalReclaimIncludesBucketsAndOldGenerations(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "legacy")
	p := legacyFixture(t, directory, true)
	before := legacySSTBytes(t, directory)
	db, err := pebble.Open(directory, &pebble.Options{DisableAutomaticCompactions: true})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := DeleteLegacy(context.Background(), db, p); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	after := legacySSTBytes(t, directory)
	if after >= before/2 {
		t.Fatalf("physical SSTs did not shrink enough: %d -> %d", before, after)
	}
	t.Logf("physical source SST bytes %d -> %d", before, after)
	assertLegacyRemainder(t, directory, p)
}

func TestLegacyChangedCatalogOrRevisionRefusesAllDeletion(t *testing.T) {
	for _, key := range [][]byte{[]byte("m/m"), legacyGenerationPrefix('r', 100, 2)} {
		t.Run(string(key), func(t *testing.T) {
			directory := filepath.Join(t.TempDir(), "legacy")
			p := legacyFixture(t, directory, false)
			db, err := pebble.Open(directory, &pebble.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if err := db.Set(key, []byte("new incarnation or revision"), pebble.Sync); err != nil {
				t.Fatal(err)
			}
			if err := DeleteLegacy(context.Background(), db, p); err == nil {
				t.Fatal("accepted changed source")
			}
			_, closer, err := db.Get([]byte("m/a"))
			if err != nil {
				t.Fatal("deleted the first metric before checking the second", err)
			}
			_ = closer.Close()
		})
	}
}

func TestLegacyReaderPinsAndReopenFailure(t *testing.T) {
	var gate ReaderGate
	unpin, err := gate.Pin()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var called atomic.Bool
	done := make(chan error, 1)
	go func() {
		done <- gate.Exclusive(ctx, func() error { called.Store(true); return nil }, func() error { return nil }, func() error { return nil })
	}()
	// Wait for admission fencing using an eventual assertion, not a sleep
	// as evidence that the old streaming reader has drained.
	deadline := time.After(time.Second)
	for {
		release, err := gate.Pin()
		if errors.Is(err, ErrLegacyFenced) {
			break
		}
		if release != nil {
			release()
		}
		select {
		case <-deadline:
			t.Fatal("exclusive did not fence reader admission")
		default:
		}
	}
	if called.Load() {
		t.Fatal("closed source while old reader was pinned")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	unpin()
	failure := errors.New("reopen failed")
	if err := gate.Exclusive(context.Background(), func() error { return nil }, func() error { return nil }, func() error { return failure }); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if release, err := gate.Pin(); !errors.Is(err, ErrLegacyFenced) {
		if release != nil {
			release()
		}
		t.Fatal("reader admitted after failed reopen", err)
	}
	if err := gate.Exclusive(context.Background(), func() error { return nil }, func() error { return nil }, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	release, err := gate.Pin()
	if err != nil {
		t.Fatal(err)
	}
	release()
}

func globalConverter(t *testing.T, root string, p LegacyPlan, checkpoint func(string) error) (*NameConverter, *pebble.DB) {
	t.Helper()
	source := ArchiveSource{SnapshotID: p.Source, PartitionSeconds: 86400, BlockPoints: 32,
		UpperBound: func(context.Context, Unit) (uint64, error) { return 1 << 20, nil },
		ScanMetrics: func(_ context.Context, u Unit, yield func(StoredMetric) error) error {
			for _, m := range p.Metrics {
				if partsformat.Route(partsformat.ID(m.Name)) != u.VShard {
					continue
				}
				if err := yield(StoredMetric{Name: partsformat.ID(m.Name), Group: partsformat.Group(p.Group), Method: 1, Archives: []StoredArchive{{Step: 1, Slots: 100, Points: []StoredPoint{{Timestamp: 1, ValueBits: 1 << 63}, {Timestamp: 2, ValueBits: 0x7ff8000000000023}}}}}); err != nil {
					return err
				}
			}
			return nil
		},
	}
	c, db := converterFor(t, root, source)
	var gate ReaderGate
	r := LegacyReclaimer{Gate: &gate, CloseReaders: func() error { return nil }, ReopenReaders: func() error { return nil }, Open: func() (*pebble.DB, error) {
		return pebble.Open(filepath.Join(root, "legacy"), &pebble.Options{DisableAutomaticCompactions: true})
	}, Identity: func(*pebble.DB) (string, error) { return "source-1", nil }, Checkpoint: checkpoint}
	nc := &NameConverter{Directory: filepath.Join(root, "names"), Converter: c, MaxOutputBytes: 8 << 20, Checkpoint: checkpoint, Publish: func(_ context.Context, receipt NameReceipt) error {
		if len(receipt.Units) != 2 {
			t.Fatal("global cursor published before all shards verified")
		}
		return nil
	}, Reclaim: r.Reclaim}
	return nc, db
}

func TestLegacyNameBatchCrashResume(t *testing.T) {
	if root := os.Getenv("PARTSCONVERT_LEGACY_CRASH_ROOT"); root != "" {
		var p LegacyPlan
		if err := readJSON(filepath.Join(root, "plan.json"), &p); err != nil {
			t.Fatal(err)
		}
		stage := os.Getenv("PARTSCONVERT_LEGACY_CRASH_STAGE")
		nc, _ := globalConverter(t, root, p, func(point string) error {
			if point == stage {
				process, err := os.FindProcess(os.Getpid())
				if err != nil {
					return err
				}
				if err := process.Kill(); err != nil {
					return err
				}
				select {} // SIGKILL bypasses defers and all database shutdown paths.
			}
			return nil
		})
		_, err := nc.Run(context.Background(), p)
		t.Fatalf("crash stage not hit: %v", err)
	}
	for _, stage := range []string{"names-ready", "cursor-durable", "names-published", "legacy-deleted", "legacy-compacted", "names-reclaimed"} {
		t.Run(stage, func(t *testing.T) {
			root := t.TempDir()
			p := legacyFixture(t, filepath.Join(root, "legacy"), false)
			if len(p.Units()) != 2 {
				t.Fatal("fixture should occupy two routing shards")
			}
			if err := writeJSON(filepath.Join(root, "plan.json"), p); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestLegacyNameBatchCrashResume$")
			cmd.Env = append(os.Environ(), "PARTSCONVERT_LEGACY_CRASH_ROOT="+root, "PARTSCONVERT_LEGACY_CRASH_STAGE="+stage)
			out, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != -1 {
				t.Fatalf("wanted durable crash at %s, got %v\n%s", stage, err, out)
			}
			nc, db := globalConverter(t, root, p, nil)
			defer db.Close()
			r, err := nc.Run(context.Background(), p)
			if err != nil || r.State != "reclaimed" || len(r.Units) != 2 {
				t.Fatalf("resume: %+v %v", r, err)
			}
			for _, unit := range r.Units {
				if unit.Points != 2 {
					t.Fatalf("lost or duplicated stored points: %+v", unit)
				}
			}
			assertLegacyRemainder(t, filepath.Join(root, "legacy"), p)
			if again, err := nc.Run(context.Background(), p); err != nil || again.State != "reclaimed" {
				t.Fatalf("completed retry: %v %v", again, err)
			}
		})
	}
}

func TestLegacyOversizedSavedPlanSplitsWithoutReset(t *testing.T) {
	for _, stop := range []string{"split-published", "split-reclaimed"} {
		t.Run(stop, func(t *testing.T) {
			ctx := context.Background()
			root := t.TempDir()
			plan := legacyFixture(t, filepath.Join(root, "legacy"), false)
			nc, db := globalConverter(t, root, plan, nil)
			defer db.Close()
			// Simulate work ingested before the old aggregate budget refused resume.
			unit, err := nc.Converter.Run(ctx, plan.Units()[0])
			if err != nil {
				t.Fatal(err)
			}
			nc.MaxOutputBytes = 1 << 20
			interrupted := errors.New("stop split")
			nc.Checkpoint = func(stage string) error {
				if stage == stop {
					return interrupted
				}
				return nil
			}
			published := 0
			nc.Publish = func(_ context.Context, r NameReceipt) error {
				published++
				if len(r.Plan.Metrics) != 1 || len(r.Units) != 1 {
					t.Fatal("unbounded child", r)
				}
				return nil
			}
			r, err := nc.Run(ctx, plan)
			if !errors.Is(err, interrupted) || r.State != "splitting" || len(r.Batches) != 2 {
				t.Fatalf("split %+v %v", r, err)
			}
			cursor, pending, err := NameState(nc.Directory)
			if err != nil || pending == nil || len(pending.Metrics) != len(plan.Metrics) || !bytes.Equal(cursor.Before, plan.Metrics[1].Name) {
				t.Fatalf("lost authority %+v %+v %v", cursor, pending, err)
			}
			// Restart with the exact same parent and its existing token/receipts.
			nc.Checkpoint = nil
			r, err = nc.Run(ctx, *pending)
			if err != nil || r.State != "reclaimed" || len(r.Units) != 2 {
				t.Fatalf("resume %+v %v", r, err)
			}
			found := false
			for _, got := range r.Units {
				if got.Unit == unit.Unit {
					found = true
					if got.Token != unit.Token {
						t.Fatal("reset existing unit token")
					}
				}
			}
			if !found || published < 2 {
				t.Fatal("lost original receipt or publication")
			}
			assertLegacyRemainder(t, filepath.Join(root, "legacy"), plan)
			if _, err = nc.Run(ctx, plan); err != nil {
				t.Fatal("completed retry", err)
			}
		})
	}
}
