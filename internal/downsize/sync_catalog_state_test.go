package downsize

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

type catalogEntry struct {
	ID   int64
	Name string
	Mask uint64
}

func readCatalogSource(t *testing.T, source syncNameSource) (out []catalogEntry) {
	t.Helper()
	defer source.close()
	for {
		name, mask, err := source.next()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, catalogEntry{source.id(), name, mask})
	}
}

func TestSyncCatalogPersistenceDifferential(t *testing.T) {
	count := int64(2*statePageNames + 3)
	store := newTypedState(t, count)
	var union, missing bytes.Buffer
	for first := int64(1); first <= count; first += statePageNames {
		var page []syncName
		for id := first; id < first+statePageNames && id <= count; id++ {
			n := syncName{Name: fmt.Sprintf("metric.%06d", id), Sources: uint64(id%3) | 1<<62, OnTarget: id%2 == 0}
			if id%7 == 0 {
				n.Sources = 0
			}
			page = append(page, n)
			fmt.Fprintf(&union, "%s\t%x\t%d\n", n.Name, n.Sources, stateBool(n.OnTarget))
			if !n.OnTarget {
				fmt.Fprintf(&missing, "%s\t%x\n", n.Name, n.Sources)
			}
		}
		if err := store.update(func(tx *stateTx) error { return tx.putCatalog(first, page) }); err != nil {
			t.Fatal(err)
		}
	}
	path := t.TempDir()
	for name, data := range map[string][]byte{"union.tsv": union.Bytes(), "missing.tsv": missing.Bytes()} {
		if err := os.WriteFile(filepath.Join(path, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	legacy := &syncer{o: syncOptions{state: path}, sum: syncSummary{Discovered: int(count)}, copied: map[string]bool{"metric.000001": true}}
	typed := &syncer{o: syncOptions{state: t.TempDir()}, sum: legacy.sum, copied: legacy.copied, catalogState: sqliteSyncCatalog{store}}
	if !reflect.DeepEqual(readCatalogSource(t, legacy.unionSource()), readCatalogSource(t, typed.unionSource())) {
		t.Fatal("union IDs/masks differ")
	}
	for _, s := range []*syncer{legacy, typed} {
		s.legacyCopyReceipts = true
	}
	if !reflect.DeepEqual(readCatalogSource(t, legacy.unionSource()), readCatalogSource(t, typed.unionSource())) {
		t.Fatal("legacy copy compatibility membership differs")
	}
	wanted := []string{"metric.008195", "metric.000001", "metric.004097", "metric.000001"}
	a, err := legacy.setSource(wanted)
	if err != nil {
		t.Fatal(err)
	}
	b, err := typed.setSource(wanted)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(readCatalogSource(t, a), readCatalogSource(t, b)) {
		t.Fatal("sparse selection IDs/order/masks differ")
	}
	for _, s := range []*syncer{legacy, typed} {
		if _, err := s.setSource([]string{"absent"}); err == nil {
			t.Fatal("missing sparse name accepted")
		}
	}
	lm, err := legacy.readMissing()
	if err != nil {
		t.Fatal(err)
	}
	tm, err := typed.readMissing()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(lm, tm) {
		t.Fatal("missing membership differs")
	}
}

// Run discovery, verification and fill together, rather than projecting a
// precomputed fixture. SQLite owns all three receipt streams and the dictionary;
// phase/READY/native-child construction is deliberately not activated here.
func TestSyncDiscoveryVerifyFillPersistenceDifferential(t *testing.T) {
	type outcome struct {
		Progress           progressSnapshot
		Fill, Verify       []syncRow
		Copies             []string
		Discovered, Filled int
	}
	outcomes := map[string]outcome{}
	for _, backend := range []string{"legacy", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			b := &fillPersistenceCut{fakeSync: newFakeSync()}
			for i, source := range []string{syncD1, syncD2} {
				name := []string{"a", "b"}[i]
				b.put(source, name, []syncArchive{{3600, 240}})
				b.put(syncT, name, []syncArchive{{3600, 240}}, syncTestNow-5*3600)
			}
			o := convergeOptions(t)
			o.from, o.streams = []string{syncD1, syncD2}, 1
			identity, _ := syncIdentity(o)
			var store *stateStore
			if backend == "sqlite" {
				store = newTypedState(t, 0)
			}
			open := func() (*syncJournal, *syncer) {
				j, err := openSyncJournal(filepath.Join(o.state, "sync.jsonl"), identity)
				if err != nil {
					t.Fatal(err)
				}
				s := &syncer{o: o, b: b, j: j, forbid: map[string]bool{}, copied: map[string]bool{}, sum: syncSummary{phases: map[string]*syncPhase{}}}
				s.redo = newSyncRedoGuard(j)
				if store != nil {
					s.discoveryState = &sqliteSyncDiscovery{store: store, scope: "root"}
					s.catalogState = sqliteSyncCatalog{store}
					s.fillState = sqliteSyncFill{store, "root"}
					s.verificationState = sqliteSyncVerification{store, "root"}
				}
				return j, s
			}
			j, s := open()
			if err := s.discover(context.Background()); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			b.cancel = cancel
			if err := s.verifyAndFill(ctx); !errors.Is(err, context.Canceled) {
				t.Fatal("missing mid-fill interruption", err)
			}
			if len(b.copies) != 1 {
				t.Fatal("fixture failed to commit exactly one fill", b.copies)
			}
			j.close()
			if store != nil {
				if err := store.close(); err != nil {
					t.Fatal(err)
				}
				var err error
				store, err = openStateStore(store.path, store.currentUnit(), false)
				if err != nil {
					t.Fatal(err)
				}
				defer store.close()
			}
			j, s = open()
			defer j.close()
			if err := s.discover(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := s.verifyAndFill(context.Background()); err != nil {
				t.Fatal(err)
			}
			if len(b.copies) != 2 || b.copies[0] != syncD1+" a" || b.copies[1] != syncD2+" b" {
				t.Fatal("finished fill repeated", b.copies)
			}
			out := outcome{Copies: b.copies, Discovered: s.sum.Discovered, Filled: s.sum.Filled}
			for round := 1; round <= 2; round++ {
				rows, err := s.readVerificationRound(round)
				if err != nil {
					t.Fatal(err)
				}
				out.Verify = append(out.Verify, rows...)
			}
			if store == nil {
				out.Fill = j.find(func(r syncRow) bool { return r.Kind == "copied" && r.Phase == "fill" })
				out.Progress = j.progress
			} else {
				stateReadTx(t, store, func(tx *sql.Tx) {
					var err error
					out.Progress, err = readStateProgress(tx, "root")
					if err != nil {
						t.Fatal(err)
					}
					for i := range 2 {
						r, err := readStateFillReceipt(tx, "root", int64(i))
						if err != nil {
							t.Fatal(err)
						}
						out.Fill = append(out.Fill, r)
					}
				})
				if rows := j.find(func(r syncRow) bool {
					return r.Kind == "discovered" || r.Kind == "discover_checkpoint" || r.Kind == "verified" || r.Kind == "copied"
				}); len(rows) != 0 {
					t.Fatal("SQLite phases dual-wrote legacy receipt authority", rows)
				}
				for _, name := range []string{"union.tsv", "missing.tsv"} {
					if _, err := os.Stat(filepath.Join(o.state, name)); !errors.Is(err, os.ErrNotExist) {
						t.Fatal("SQLite phases reconstructed a legacy catalog", name, err)
					}
				}
			}
			for _, rows := range [][]syncRow{out.Fill, out.Verify} {
				for i := range rows {
					rows[i].Identity = ""
					rows[i].Time = ""
				}
			}
			if out.Progress.Discovered != 2 || out.Progress.Filled != 2 || out.Progress.Verified != 4 {
				t.Fatal("lost or duplicated phase progress", out.Progress)
			}
			for _, n := range out.Progress.RedoNames {
				if n != 0 {
					t.Fatal("redo names", out.Progress)
				}
			}
			outcomes[backend] = out
		})
	}
	if !reflect.DeepEqual(outcomes["legacy"], outcomes["sqlite"]) {
		t.Fatalf("combined controller differential mismatch:\nlegacy=%+v\nsqlite=%+v", outcomes["legacy"], outcomes["sqlite"])
	}
}
