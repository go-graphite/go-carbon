package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

const continuationOracleRounds, continuationOracleSteps = 9, 120

type continuationOracleOperation struct {
	Key, Value, Prefix string
	Delete             bool
}

type continuationOracleFixture struct {
	Want       map[string]string
	Operations []continuationOracleOperation
}

// Compare with the original range tombstone operation across indexed writes,
// old reader snapshots, flush/compaction and reopen. The map is independent of
// both implementations and includes keys outside each deleted prefix.
func TestContinuationPrefixDeletionMatchesRangeOracle(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(202610240058))
	want := map[string]string{}
	for round := 0; round < continuationOracleRounds; round++ {
		fixture := continuationOracleFixture{Want: maps.Clone(want)}
		// A boundary's large keys are all deleted before the random walk.
		// Precompute the same live image carried from one compacted round to
		// the next, so each round can run independently with its own stores.
		want["large/recreated"] = "after-deletion"
		for step := 0; step < continuationOracleSteps; step++ {
			k := fmt.Sprintf("%s/%02d", []string{"main", "ooo", "sibling", "\x00\xff"}[rng.Intn(4)], rng.Intn(18))
			var operation continuationOracleOperation
			if rng.Intn(3) != 0 {
				operation = continuationOracleOperation{Key: k, Value: fmt.Sprint(round, ":", step)}
				want[k] = operation.Value
			} else {
				operation = continuationOracleOperation{Delete: true, Prefix: []string{"main/", "ooo/", "sibling/1", "absent/", "\x00", ""}[rng.Intn(6)]}
				for key := range want {
					if strings.HasPrefix(key, operation.Prefix) {
						delete(want, key)
					}
				}
			}
			fixture.Operations = append(fixture.Operations, operation)
		}
		final := maps.Clone(want)
		t.Run(fmt.Sprintf("round=%d", round), func(t *testing.T) {
			t.Parallel()
			checkContinuationPrefixDeletionMatchesRangeOracle(t, round, fixture, final)
		})
	}
}

func checkContinuationPrefixDeletionMatchesRangeOracle(t *testing.T, selectedRound int, fixture continuationOracleFixture, final map[string]string) {
	ctx := context.Background()
	dirs := []string{t.TempDir(), t.TempDir()}
	stores := make([]*Store, 2)
	// Each logical store retains its own files across reopen, without disk I/O.
	options := []Options{{ExactBlocks: true, fs: vfs.NewMem()}, {ExactBlocks: true, fs: vfs.NewMem()}}
	if selectedRound < 3 {
		// Spill the million-key WAL/table images to real files without
		// device syncs; all reopen/flush assertions still read those files.
		for i := range options {
			options[i].fs = noSyncFS{FS: vfs.Default}
		}
	}
	for i := range stores {
		stores[i], _ = openTestStore(t, dirs[i], options[i])
	}
	defer func() {
		for _, s := range stores {
			closeTestStore(t, s)
		}
	}()
	m := Metadata{ID: 73, Generation: 19}
	read := func(t *testing.T, reader pebble.Reader) map[string]string {
		t.Helper()
		got := map[string]string{}
		err := newContinuationIO(ctx, reader, nil, m).Visit(nil, func(k, v []byte) error {
			got[string(k)] = string(v)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	want := maps.Clone(fixture.Want)
	// Each starting image is the previous round's compacted, reopened live
	// state. Keep that same persisted input and verify both stores against it.
	for i, store := range stores {
		batch := store.db.NewBatch()
		io := newContinuationIO(ctx, batch, batch, m)
		for _, k := range slices.Sorted(maps.Keys(want)) {
			if err := io.Set([]byte(k), []byte(want[k])); err != nil {
				t.Fatal(err)
			}
		}
		if err := batch.Commit(pebble.Sync); err != nil {
			t.Fatal(err)
		}
		if err := batch.Close(); err != nil {
			t.Fatal(err)
		}
		if err := store.db.Flush(); err != nil {
			t.Fatal(err)
		}
		if err := store.db.Compact(continuationPrefix(m.ID, m.Generation), prefixEnd(continuationPrefix(m.ID, m.Generation)), false); err != nil {
			t.Fatal(err)
		}
		closeTestStore(t, store)
		stores[i], _ = openTestStore(t, dirs[i], options[i])
	}
	firstRound, endRound := selectedRound, selectedRound+1
	const stepsPerRound = continuationOracleSteps
	boundarySizes := [...]int{continuationPointDeleteLimit - 1, continuationPointDeleteLimit, continuationPointDeleteLimit + 1}
	var boundaryCoverage [len(boundarySizes)]int
	var laterDeleteRecreates, randomizedSteps, checkedSteps, boundaryDeleteRecreates int
	for round := firstRound; round < endRound; round++ {
		snaps := []*pebble.Snapshot{stores[0].db.NewSnapshot(), stores[1].db.NewSnapshot()}
		old := read(t, snaps[0])
		if !reflect.DeepEqual(old, want) || !reflect.DeepEqual(read(t, snaps[1]), want) {
			t.Fatal("round's starting snapshots differ from the independent oracle")
		}
		operations := fixture.Operations
		if len(operations) != stepsPerRound {
			t.Fatalf("round%d operations %d, want %d", round, len(operations), stepsPerRound)
		}
		batches := make([]*pebble.Batch, len(stores))
		for i, store := range stores {
			capacity := 0
			if round < len(boundarySizes) && i == 0 {
				size := boundarySizes[round]
				digits := len(strconv.Itoa(size - 1))
				keyBytes := len(continuationPrefix(m.ID, m.Generation)) + len("large/") + max(5, digits)
				// Each record has a kind and one-byte key/value lengths.
				capacity = size*(3+keyBytes+len(strconv.Itoa(round))+1+digits) + (64 << 10)
				if i == 0 && size <= continuationPointDeleteLimit {
					capacity += size * (2 + keyBytes)
				}
			}
			batches[i] = store.db.NewIndexedBatchWithSize(capacity)
		}
		ios := []*continuationIO{newContinuationIO(ctx, batches[0], batches[0], m), newContinuationIO(ctx, batches[1], batches[1], m)}
		set := func(k, v string) {
			t.Helper()
			for _, io := range ios {
				if err := io.Set([]byte(k), []byte(v)); err != nil {
					t.Fatal(err)
				}
			}
			want[k] = v
		}
		remove := func(prefix string) {
			t.Helper()
			if err := ios[0].DeletePrefix([]byte(prefix)); err != nil {
				t.Fatal(err)
			}
			start := ios[1].key([]byte(prefix))
			if err := batches[1].DeleteRange(start, prefixEnd(start), nil); err != nil {
				t.Fatal(err)
			}
			for k := range want {
				if strings.HasPrefix(k, prefix) {
					delete(want, k)
				}
			}
		}
		checkLater := func(phase string) {
			t.Helper()
			t.Run(phase, func(t *testing.T) {
				for i, b := range batches {
					t.Run(fmt.Sprintf("implementation=%d", i), func(t *testing.T) {
						t.Parallel()
						if got := read(t, b); !reflect.DeepEqual(got, want) {
							t.Fatalf("round%d implementation%d %s: got%v want%v", round, i, phase, got, want)
						}
					})
				}
			})
		}
		// Exercise every boundary of the actual key limit once.
		// Repeating the large indexed batch in all nine rounds makes every
		// later oracle read scan the same mass of pending tombstones under -race.
		// The remaining rounds retain the same randomized operations and
		// held-snapshot/flush/compaction/reopen checks on a small batch.
		if round < len(boundarySizes) {
			key := make([]byte, len("large/"), 32)
			copy(key, "large/")
			var decimal [20]byte
			value := []byte(fmt.Sprint(round, ":"))
			valuePrefix := len(value)
			for i := 0; i < boundarySizes[round]; i++ {
				digits := strconv.AppendInt(decimal[:0], int64(i), 10)
				key = append(key[:len("large/")], "00000"[:max(0, 5-len(digits))]...)
				key = append(key, digits...)
				value = strconv.AppendInt(value[:valuePrefix], int64(i), 10)
				if err := ios[0].Set(key, value); err != nil {
					t.Fatal(err)
				}
			}
			if err := batches[1].SetRepr(bytes.Clone(batches[0].Repr())); err != nil {
				t.Fatal(err)
			}
			// Every generated key starts with large/ and is deleted before
			// the first readback. Its oracle contribution is therefore empty;
			// avoid building a million-entry map only to erase it immediately.

			boundaryCoverage[round]++
		} else {
			set("large/recreated", fmt.Sprint("before-deletion-", round))
		}
		remove("large/")
		checkLater("prefix deletion")
		set("large/recreated", "after-deletion")
		checkLater("recreation")
		if round >= len(boundarySizes) {
			laterDeleteRecreates++
		} else {
			// Check a later whole-prefix deletion and recreation while the
			// large boundary batch is still pending. Drain its deleted keys
			// before the random walk: scanning those same dead keys after
			// every unrelated mutation adds work without a new boundary case.
			saved := maps.Clone(want)
			remove("")
			checkLater("whole-prefix deletion after boundary")
			for _, k := range slices.Sorted(maps.Keys(saved)) {
				set(k, saved[k])
			}
			checkLater("recreation after whole-prefix deletion")
			boundaryDeleteRecreates++
			for i, b := range batches {
				if err := b.Commit(pebble.Sync); err != nil {
					t.Fatal(err)
				}
				if err := b.Close(); err != nil {
					t.Fatal(err)
				}
				if err := stores[i].db.Flush(); err != nil {
					t.Fatal(err)
				}
				if got := read(t, snaps[i]); !reflect.DeepEqual(got, old) {
					t.Fatal("boundary commit changed a held snapshot")
				}
				if err := snaps[i].Close(); err != nil {
					t.Fatal(err)
				}
				if err := stores[i].db.Compact(continuationPrefix(m.ID, m.Generation), prefixEnd(continuationPrefix(m.ID, m.Generation)), false); err != nil {
					t.Fatal(err)
				}
				if got := read(t, stores[i].db); !reflect.DeepEqual(got, want) {
					t.Fatalf("round%d implementation%d boundary flush: got%v want%v", round, i, got, want)
				}
				// The randomized batch gets its own held snapshot, after
				// compaction can discard the validated boundary's dead keys.
				snaps[i] = stores[i].db.NewSnapshot()
				batches[i] = stores[i].db.NewIndexedBatch()
				ios[i] = newContinuationIO(ctx, batches[i], batches[i], m)
			}
			old = maps.Clone(want)
		}
		for step, operation := range operations {
			if operation.Delete {
				remove(operation.Prefix)
			} else {
				set(operation.Key, operation.Value)
			}
			for i, b := range batches {
				if got := read(t, b); !reflect.DeepEqual(got, want) {
					t.Fatalf("round%d step%d implementation%d: got%v want%v", round, step, i, got, want)
				}
			}
			checkedSteps++
			randomizedSteps++
		}
		for i, b := range batches {
			if err := b.Commit(pebble.Sync); err != nil {
				t.Fatal(err)
			}
			if err := b.Close(); err != nil {
				t.Fatal(err)
			}
			if got := read(t, snaps[i]); !reflect.DeepEqual(got, old) {
				t.Fatal("committed prefix deletion changed a held snapshot")
			}
			if err := snaps[i].Close(); err != nil {
				t.Fatal(err)
			}
			if err := stores[i].db.Flush(); err != nil {
				t.Fatal(err)
			}
			if err := stores[i].db.Compact(continuationPrefix(m.ID, m.Generation), prefixEnd(continuationPrefix(m.ID, m.Generation)), false); err != nil {
				t.Fatal(err)
			}
			closeTestStore(t, stores[i])
			stores[i], _ = openTestStore(t, dirs[i], options[i])
			if got := read(t, stores[i].db); !reflect.DeepEqual(got, want) {
				t.Fatalf("round%d implementation%d reopen: got%v want%v", round, i, got, want)
			}
		}
	}
	for i, size := range boundarySizes {
		wantCount := 0
		if i >= firstRound && i < endRound {
			wantCount = 1
		}
		if boundaryCoverage[i] != wantCount {
			t.Fatalf("boundary size %d exercised %d times, want %d", size, boundaryCoverage[i], wantCount)
		}
	}
	if want := max(0, endRound-max(firstRound, len(boundarySizes))); laterDeleteRecreates != want {
		t.Fatalf("later delete/recreate rounds %d, want %d", laterDeleteRecreates, want)
	}
	if want := max(0, min(endRound, len(boundarySizes))-firstRound); boundaryDeleteRecreates != want {
		t.Fatalf("boundary delete/recreate rounds %d, want %d", boundaryDeleteRecreates, want)
	}
	if want := (endRound - firstRound) * stepsPerRound; randomizedSteps != want {
		t.Fatalf("randomized steps %d, want %d", randomizedSteps, want)
	}
	if want := (endRound - firstRound) * stepsPerRound; checkedSteps != want {
		t.Fatalf("checked steps %d, want %d", checkedSteps, want)
	}
	if !reflect.DeepEqual(want, final) {
		t.Fatalf("round%d final oracle image differs: got%v want%v", selectedRound, want, final)
	}
	t.Logf("checked %d randomized images and %d boundary delete/recreates across %d rounds", randomizedSteps, boundaryDeleteRecreates, endRound-firstRound)
}

// A plain Batch can record deletes but cannot expose an iterator. Keep the
// original range operation for this valid write-only use of ContinuationIO.
func TestContinuationPrefixDeletionPlainBatchFallback(t *testing.T) {
	t.Parallel()
	s, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true})
	defer closeTestStore(t, s)
	b := s.db.NewBatch()
	defer b.Close()
	io := newContinuationIO(context.Background(), b, b, Metadata{ID: 91, Generation: 2})
	if err := io.Set([]byte("main/one"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := io.DeletePrefix([]byte("main/")); err != nil {
		t.Fatal(err)
	}
	if got := batchRangeDeletes(t, b); got != 1 {
		t.Fatalf("plain batch wrote%d range deletes", got)
	}
	if err := b.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if _, err := newContinuationIO(context.Background(), s.db, nil, Metadata{ID: 91, Generation: 2}).Get([]byte("main/one")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("point survived plain batch prefix deletion:%v", err)
	}
}
