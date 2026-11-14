package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"reflect"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

// CheckTransferViewForTest compares the complete prepared batch, encoded point
// bytes, proofs and sidecars with the previous snapshot-backed preparation.
// Exported only from the test build so continuation-codec fixtures can use it.
func CheckTransferViewForTest(t *testing.T, s *Store, records []TransferMetric) {
	t.Helper()
	ctx := context.Background()
	first, err := s.reserveFileMetricIDs(uint64(len(records)))
	if err != nil {
		t.Fatal(err)
	}
	metas := make([]Metadata, len(records))
	for i, r := range records {
		metas[i] = Metadata{MetricConfig: r.Config, ID: first + uint64(i), Generation: 1, Revision: 1, LastUpdate: s.nextActivity(s.now()), Continuation: r.Continuation}
		if s.incrementalSource != "" {
			metas[i].FileMigration = FileReclaimed
		}
	}
	stage := s.stageTransfer(first, records, metas)
	b, err := s.writeTransferBatch(ctx, stage)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	oracle := s.db.NewSnapshot()
	defer oracle.Close()
	view, err := s.newTransferView()
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	want, err := s.prepareTransfer(ctx, records, metas, oracle)
	if err != nil {
		t.Fatal(err)
	}
	defer want.batch.Close()
	got, err := s.prepareTransfer(ctx, records, metas, view)
	if err != nil {
		t.Fatal(err)
	}
	defer got.batch.Close()
	if !bytes.Equal(want.batch.Repr(), got.batch.Repr()) || !reflect.DeepEqual(want.proofs, got.proofs) || !reflect.DeepEqual(want.sidecars, got.sidecars) || !reflect.DeepEqual(want.expected, got.expected) || !reflect.DeepEqual(want.metas, got.metas) {
		t.Fatal("iterator preparation differs from snapshot oracle")
	}
}

func TestTransferViewMatchesSnapshotAcrossMutations(t *testing.T) {
	db, err := pebble.Open("db", &pebble.Options{FS: vfs.NewMem()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rng := rand.New(rand.NewSource(8144))
	for round := 0; round < 12; round++ {
		for i := 0; i < 100; i++ {
			key := []byte{byte(rng.Intn(64)), 0, 255}
			value := make([]byte, 32)
			rng.Read(value)
			if err := db.Set(key, value, pebble.NoSync); err != nil {
				t.Fatal(err)
			}
		}
		oracle := db.NewSnapshot()
		s := &Store{db: db}
		view, err := s.newTransferView()
		if err != nil {
			t.Fatal(err)
		}
		// Overwrite and delete all current keys, flush and compact while both old
		// views are alive. Clones opened afterwards must still observe the old bits.
		if err := db.DeleteRange([]byte{0}, []byte{255}, pebble.NoSync); err != nil {
			t.Fatal(err)
		}
		if err := db.Set([]byte{63, 0, 255}, []byte("replacement"), pebble.NoSync); err != nil {
			t.Fatal(err)
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
		if err := db.Compact([]byte{0}, []byte{255}, false); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 66; i++ {
			key := []byte{byte(i), 0, 255}
			want, wc, we := oracle.Get(key)
			got, gc, ge := view.Get(key)
			if !bytes.Equal(want, got) || errors.Is(we, pebble.ErrNotFound) != errors.Is(ge, pebble.ErrNotFound) {
				t.Fatalf("round %d key %x changed", round, key)
			}
			if wc != nil {
				wc.Close()
			}
			if gc != nil {
				gc.Close()
			}
		}
		opts := &pebble.IterOptions{LowerBound: []byte{10}, UpperBound: []byte{50}}
		wi, we := oracle.NewIter(opts)
		gi, ge := view.NewIter(opts)
		if we != nil || ge != nil {
			t.Fatal(we, ge)
		}
		w, g := wi.First(), gi.First()
		for w && g {
			if !bytes.Equal(wi.Key(), gi.Key()) || !bytes.Equal(wi.Value(), gi.Value()) {
				t.Fatal("bounded clone differs")
			}
			w, g = wi.Next(), gi.Next()
		}
		if w != g || wi.Error() != nil || gi.Error() != nil {
			t.Fatal("iterator length/error differs")
		}
		wi.Close()
		gi.Close()
		view.Close()
		oracle.Close()
	}
}
