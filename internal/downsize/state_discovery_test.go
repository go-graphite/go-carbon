package downsize

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

func typedDiscovery(t *testing.T, s *stateStore) (d stateDiscovery) {
	t.Helper()
	stateReadTx(t, s, func(tx *sql.Tx) {
		var err error
		d, err = readStateDiscovery(tx, "root")
		if err != nil {
			t.Fatal(err)
		}
	})
	return d
}

func discoveryNext(d stateDiscovery, names []syncName) syncDiscoveryCheckpoint {
	c := d.Checkpoint
	c.Cursors = cloneCursors(c.Cursors)
	for _, n := range names {
		c.Names++
		c.UnionBytes += int64(len(fmt.Sprintf("%s\t%x\t%d\n", n.Name, n.Sources, stateBool(n.OnTarget))))
		if n.OnTarget {
			c.OnTarget++
			if n.Sources != 0 {
				c.Both++
			}
		} else {
			c.Missing++
			c.MissingBytes += int64(len(fmt.Sprintf("%s\t%x\n", n.Name, n.Sources)))
		}
	}
	return c
}

func TestTypedStateDiscoveryUnknownSizeRestartAndSeal(t *testing.T) {
	s := newTypedState(t, 0)
	if err := s.update(func(tx *stateTx) error { return tx.beginDiscovery("root") }); err != nil {
		t.Fatal(err)
	}
	var all []syncName
	for i := range 2*syncListPage + 3 {
		all = append(all, syncName{Name: fmt.Sprintf("metric.%05d", i), Sources: uint64(i) | 1<<62, OnTarget: i%3 == 0})
	}
	// Grow an incomplete tail across several transactions and process restarts.
	for _, bounds := range [][2]int{{0, 3}, {3, 4097}, {4097, 14097}, {14097, 20003}} {
		d := typedDiscovery(t, s)
		next := discoveryNext(d, all[bounds[0]:bounds[1]])
		next.Cursors["source"] = all[bounds[1]-1].Name
		if err := s.update(func(tx *stateTx) error {
			return tx.advanceDiscovery(d, next, all[bounds[0]:bounds[1]])
		}); err != nil {
			t.Fatal(err)
		}
		if s.unit.CatalogNames != 0 {
			t.Fatal("unsealed discovery exposed an immutable catalog")
		}
		if err := s.close(); err != nil {
			t.Fatal(err)
		}
		var err error
		expected := s.unit
		expected.CatalogNames = -1
		s, err = openStateStore(s.path, expected, false)
		if err != nil {
			t.Fatal(err)
		}
	}
	defer s.close()
	d := typedDiscovery(t, s)
	var names []syncName
	stateReadTx(t, s, func(tx *sql.Tx) {
		for first := int64(1); first <= int64(len(all)); first += statePageNames {
			page, err := readStateDiscoveryPage(tx, d, first)
			if err != nil {
				t.Fatal(err)
			}
			names = append(names, page...)
		}
	})
	if !reflect.DeepEqual(names, all) {
		t.Fatal("growing catalog lost sorted IDs, source bits or target membership")
	}
	before := typedDigests(t, s)
	abort := errors.New("interrupted seal")
	if err := s.update(func(tx *stateTx) error {
		if err := tx.sealDiscovery(d); err != nil {
			return err
		}
		if tx.unit.CatalogNames != int64(len(all)) {
			t.Fatal("seal did not publish transaction-local unit size")
		}
		return abort
	}); !errors.Is(err, abort) {
		t.Fatal(err)
	}
	if s.unit.CatalogNames != 0 || !reflect.DeepEqual(before, typedDigests(t, s)) {
		t.Fatal("failed seal published a catalog, count or partial checkpoint")
	}
	if err := s.update(func(tx *stateTx) error { return tx.sealDiscovery(d) }); err != nil {
		t.Fatal(err)
	}
	if s.unit.CatalogNames != int64(len(all)) {
		t.Fatal("committed final size missing from writer")
	}
	sealed := typedDiscovery(t, s)
	if !sealed.Complete || !reflect.DeepEqual(sealed.Checkpoint, d.Checkpoint) {
		t.Fatal(sealed)
	}
	for _, id := range []int64{1, 3, 4096, 4097, 8195, 20003} {
		n, err := s.catalogName(id)
		if err != nil || n != all[id-1] {
			t.Fatal(id, n, err)
		}
	}
	before = typedDigests(t, s)
	if err := s.update(func(tx *stateTx) error { return tx.sealDiscovery(sealed) }); err != nil || !reflect.DeepEqual(before, typedDigests(t, s)) {
		t.Fatal("seal replay changed catalog", err)
	}
	if err := s.update(func(tx *stateTx) error { return tx.advanceDiscovery(sealed, sealed.Checkpoint, nil) }); err == nil {
		t.Fatal("sealed catalog reopened")
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	var err error
	expected := s.unit
	expected.CatalogNames = -1
	s, err = openStateStore(s.path, expected, false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	if got := typedDiscovery(t, s); !reflect.DeepEqual(got, sealed) {
		t.Fatal("final discovery did not survive reopen", got)
	}
}

func TestTypedStateDiscoveryEmptyAndReadOnlyInspection(t *testing.T) {
	s := newTypedState(t, 0)
	if err := s.update(func(tx *stateTx) error { return tx.beginDiscovery("root") }); err != nil {
		t.Fatal(err)
	}
	d := typedDiscovery(t, s)
	if d.Checkpoint.Cursors != nil {
		t.Fatal("initial absent cursor map changed")
	}
	next := syncDiscoveryCheckpoint{Cursors: map[string]string{}, NotOwned: 5}
	if err := s.update(func(tx *stateTx) error { return tx.advanceDiscovery(d, next, nil) }); err != nil {
		t.Fatal(err)
	}
	d = typedDiscovery(t, s)
	if !reflect.DeepEqual(next, d.Checkpoint) {
		t.Fatal("empty catalog lost unowned count or empty cursor map")
	}
	if err := s.update(func(tx *stateTx) error { return tx.sealDiscovery(d) }); err != nil {
		t.Fatal(err)
	}
	sealed := typedDiscovery(t, s)
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(s.path)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := stateMain(context.Background(), []string{"inspect", "--database", s.path, "--hashes"}, &output); err != nil {
		t.Fatal(err)
	}
	var got stateInspection
	if err := json.Unmarshal(output.Bytes(), &got); err != nil || len(got.Discoveries) != 1 || !reflect.DeepEqual(got.Discoveries[0], sealed) {
		t.Fatal("inspection lost committed empty discovery", output.String(), err)
	}
	after, err := os.ReadFile(s.path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("inspection changed discovery database", err)
	}
}

func TestTypedStateDiscoveryUnknownCountRequiresMatchingSeal(t *testing.T) {
	s := newTypedState(t, 0)
	p := &sqliteSyncDiscovery{store: s, scope: "root"}
	if _, _, err := p.load(); err != nil {
		t.Fatal(err)
	}
	names := []syncName{{Name: "metric.a", Sources: 1}}
	next := discoveryNext(p.saved, names)
	if err := p.append(names[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := p.commit(next, true); err != nil {
		t.Fatal(err)
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	db, err := connectStateDB(s.path, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE unit SET catalog_names=2"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(s.path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := openStateStore(s.path, stateUnit{"identity", "options", "catalog-v1", -1}, false); err == nil {
		t.Fatal("unknown expected size ignored a contradictory discovery seal")
	}
	after, err := os.ReadFile(s.path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("rejected discovery seal changed the database", err)
	}
}

func TestTypedStateDiscoveryConcurrentCatalogReaders(t *testing.T) {
	s := newTypedState(t, 0)
	p := &sqliteSyncDiscovery{store: s, scope: "root"}
	if _, _, err := p.load(); err != nil {
		t.Fatal(err)
	}
	want := syncName{Name: "metric.a", Sources: 1}
	if err := p.append(want); err != nil {
		t.Fatal(err)
	}
	if _, err := p.commit(discoveryNext(p.saved, []syncName{want}), false); err != nil {
		t.Fatal(err)
	}
	started, stop, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		close(started)
		for {
			select {
			case <-stop:
				done <- nil
				return
			default:
				n, err := s.catalogName(1)
				if err == nil && n != want {
					done <- fmt.Errorf("catalog changed during seal: %+v", n)
					return
				}
			}
		}
	}()
	<-started
	err := s.update(func(tx *stateTx) error { return tx.sealDiscovery(p.saved) })
	close(stop)
	if readerErr := <-done; err != nil || readerErr != nil {
		t.Fatal(err, readerErr)
	}
	if got, err := s.catalogName(1); err != nil || got != want {
		t.Fatal("sealed catalog unavailable", got, err)
	}
}

func TestTypedStateDiscoveryCrashBoundary(t *testing.T) {
	if mode := os.Getenv("TYPED_DISCOVERY_CRASH"); mode != "" {
		s, err := openStateStore(os.Getenv("TYPED_DISCOVERY_PATH"), stateUnit{"identity", "options", "catalog-v1", -1}, false)
		if err != nil {
			t.Fatal(err)
		}
		d := typedDiscovery(t, s)
		added := []syncName{{Name: "metric.b", Sources: 1}}
		next := discoveryNext(d, added)
		next.Cursors["source"] = "metric.b"
		err = s.update(func(tx *stateTx) error {
			if err := tx.advanceDiscovery(d, next, added); err != nil {
				return err
			}
			if err := tx.reduceProgress("root", syncRow{Kind: "discover_checkpoint", Discovery: &next}); err != nil {
				return err
			}
			if strings.HasSuffix(mode, "seal") {
				current, err := readStateDiscovery(tx.tx, "root")
				if err != nil {
					return err
				}
				if err := tx.sealDiscovery(current); err != nil {
					return err
				}
			}
			if strings.HasPrefix(mode, "before") {
				os.Exit(76)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		os.Exit(77) // no graceful checkpoint/close, including after a seal
	}
	for _, mode := range []string{"before-prefix", "after-prefix", "before-seal", "after-seal"} {
		t.Run(mode, func(t *testing.T) {
			s := newTypedState(t, 0)
			p := &sqliteSyncDiscovery{store: s, scope: "root"}
			if _, _, err := p.load(); err != nil {
				t.Fatal(err)
			}
			name := syncName{Name: "metric.a", Sources: 1, OnTarget: true}
			next := discoveryNext(p.saved, []syncName{name})
			next.Cursors["source"] = name.Name
			if err := p.append(name); err != nil {
				t.Fatal(err)
			}
			if _, err := p.commit(next, false); err != nil {
				t.Fatal(err)
			}
			before := typedDigests(t, s)
			if err := s.close(); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestTypedStateDiscoveryCrashBoundary$")
			cmd.Env = append(os.Environ(), "TYPED_DISCOVERY_CRASH="+mode, "TYPED_DISCOVERY_PATH="+s.path)
			out, err := cmd.CombinedOutput()
			var code *exec.ExitError
			wantExit := 77
			if strings.HasPrefix(mode, "before") {
				wantExit = 76
			}
			if !errors.As(err, &code) || code.ExitCode() != wantExit {
				t.Fatalf("crash cut did not run: %v %s", err, out)
			}
			s, err = openStateStore(s.path, stateUnit{"identity", "options", "catalog-v1", -1}, false)
			if err != nil {
				t.Fatal(err)
			}
			defer s.close()
			if strings.HasPrefix(mode, "before") {
				if !reflect.DeepEqual(before, typedDigests(t, s)) {
					t.Fatal("crash published a partial prefix, cursor, counter or seal")
				}
				return
			}
			d := typedDiscovery(t, s)
			if d.Checkpoint.Names != 2 || d.Checkpoint.Cursors["source"] != "metric.b" || d.Complete != (mode == "after-seal") || d.Complete && s.unit.CatalogNames != 2 {
				t.Fatal("acknowledged prefix or seal lost", d)
			}
			stateReadTx(t, s, func(tx *sql.Tx) {
				progress, err := readStateProgress(tx, "root")
				if err != nil || progress.Discovered != 2 || progress.Cursors["source"] != "metric.b" {
					t.Fatal("checkpoint and progress diverged", progress, err)
				}
			})
		})
	}
}

func TestTypedStateDiscoveryCASValidationAndAtomicity(t *testing.T) {
	s := newTypedState(t, 0)
	if err := s.update(func(tx *stateTx) error { return tx.beginDiscovery("root") }); err != nil {
		t.Fatal(err)
	}
	d := typedDiscovery(t, s)
	page := []syncName{{Name: "metric.a", Sources: 1, OnTarget: true}}
	next := discoveryNext(d, page)
	next.Cursors["source"] = "metric.a"
	before := typedDigests(t, s)
	abort := errors.New("interrupted prefix commit")
	if err := s.update(func(tx *stateTx) error {
		if err := tx.advanceDiscovery(d, next, page); err != nil {
			return err
		}
		return abort
	}); !errors.Is(err, abort) || !reflect.DeepEqual(before, typedDigests(t, s)) {
		t.Fatal("checkpoint/cursor/page commit was not atomic", err)
	}
	if err := s.update(func(tx *stateTx) error { return tx.advanceDiscovery(d, next, page) }); err != nil {
		t.Fatal(err)
	}
	if err := s.update(func(tx *stateTx) error { return tx.advanceDiscovery(d, next, page) }); err == nil {
		t.Fatal("stale checkpoint accepted")
	}
	d = typedDiscovery(t, s)
	for _, modify := range []func(*syncDiscoveryCheckpoint){
		func(c *syncDiscoveryCheckpoint) { c.UnionBytes++ },
		func(c *syncDiscoveryCheckpoint) { c.Missing++ },
		func(c *syncDiscoveryCheckpoint) { c.Both++ },
		func(c *syncDiscoveryCheckpoint) { c.Cursors["source"] = "metric.0" },
		func(c *syncDiscoveryCheckpoint) { delete(c.Cursors, "source") },
	} {
		next := discoveryNext(d, nil)
		modify(&next)
		if err := s.update(func(tx *stateTx) error { return tx.advanceDiscovery(d, next, nil) }); err == nil {
			t.Fatal("counter/extent/cursor regression accepted", next)
		}
	}
	before = typedDigests(t, s)
	// Force a failure after inserting the tail's replacement page. Rollback must
	// retain the exact prior page, counters and source cursors.
	next = discoveryNext(d, []syncName{{Name: "metric.b", Sources: 1}})
	next.Cursors[""] = "invalid identity"
	if err := s.update(func(tx *stateTx) error {
		return tx.advanceDiscovery(d, next, []syncName{{Name: "metric.b", Sources: 1}})
	}); err == nil || !reflect.DeepEqual(before, typedDigests(t, s)) {
		t.Fatal("failure after catalog update leaked changes", err)
	}
	if _, err := s.db.Exec("UPDATE catalog_page SET data=x'00'"); err != nil {
		t.Fatal(err)
	}
	if err := s.update(func(tx *stateTx) error { return tx.sealDiscovery(d) }); err == nil {
		t.Fatal("corrupt discovery page sealed")
	}
}

type typedDiscoveryCut struct {
	*fakeSync
	cut     bool
	noCalls bool
	calls   atomic.Int64
}

func (b *typedDiscoveryCut) List(ctx context.Context, store, prefix, after string, limit int) ([]string, error) {
	b.calls.Add(1)
	if b.noCalls {
		return nil, errors.New("completed discovery contacted backend")
	}
	if b.cut && store == syncD1 && after >= "metric.005" {
		return nil, context.Canceled
	}
	return b.fakeSync.List(ctx, store, prefix, after, limit)
}

// Run the same discovery controller against each persistence implementation,
// interrupt after a consumed prefix, reopen its actual store and finish. This
// deliberately qualifies only discovery; sync still has no SQLite selector.
func TestSyncDiscoveryPersistenceDifferential(t *testing.T) {
	type outcome struct {
		Partial, Final syncDiscoveryCheckpoint
		Union, Missing []byte
		Progress       progressSnapshot
	}
	outcomes := map[string]outcome{}
	for _, backend := range []string{"legacy", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			b := &typedDiscoveryCut{fakeSync: newFakeSync(), cut: true}
			b.listPageSize = 3
			for i := range 15 {
				name := fmt.Sprintf("metric.%03d", i)
				b.put(syncD1, name, []syncArchive{{3600, 240}})
				if i%3 != 0 {
					b.put(syncT, name, []syncArchive{{3600, 240}})
				}
			}
			path := t.TempDir()
			j, err := openSyncJournal(filepath.Join(path, "sync.jsonl"), "discovery-oracle")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { j.close() }()
			source := &syncer{o: syncOptions{to: syncT, from: []string{syncD1}, state: path}, b: b, j: j, sum: syncSummary{phases: map[string]*syncPhase{}}}
			source.owner = func(n string) bool { return n != "metric.006" }
			var s *stateStore
			if backend == "sqlite" {
				s = newTypedState(t, 0)
				source.discoveryState = &sqliteSyncDiscovery{store: s, scope: "root"}
			}
			checkpoint := func() syncDiscoveryCheckpoint {
				if s != nil {
					return typedDiscovery(t, s).Checkpoint
				}
				rows := j.find(func(r syncRow) bool { return r.Kind == "discover_checkpoint" })
				if len(rows) == 0 {
					t.Fatal("fixture has no committed checkpoint")
				}
				return *rows[len(rows)-1].Discovery
			}
			if err := source.discover(context.Background()); !errors.Is(err, context.Canceled) {
				t.Fatal("discovery interruption missing", err)
			}
			partial := checkpoint()
			if partial.Names == 0 || partial.Names >= 15 {
				t.Fatal("fixture did not commit a partial prefix", partial)
			}
			if s != nil {
				if err := s.close(); err != nil {
					t.Fatal(err)
				}
				s, err = openStateStore(s.path, s.unit, false)
				if err != nil {
					t.Fatal(err)
				}
				source.discoveryState = &sqliteSyncDiscovery{store: s, scope: "root"}
			}
			j.close()
			j, err = openSyncJournal(filepath.Join(path, "sync.jsonl"), "discovery-oracle")
			if err != nil {
				t.Fatal(err)
			}
			source.j, b.cut = j, false
			if err := source.discover(context.Background()); err != nil {
				t.Fatal(err)
			}
			final := checkpoint()
			var progress progressSnapshot
			var union, missing []byte
			if s == nil {
				progress = j.progress
				union, err = os.ReadFile(filepath.Join(path, "union.tsv"))
				if err != nil {
					t.Fatal(err)
				}
				missing, err = os.ReadFile(filepath.Join(path, "missing.tsv"))
				if err != nil {
					t.Fatal(err)
				}
			} else {
				stateReadTx(t, s, func(tx *sql.Tx) {
					var err error
					progress, err = readStateProgress(tx, "root")
					if err != nil {
						t.Fatal(err)
					}
				})
				var u, m bytes.Buffer
				for id := int64(1); id <= s.unit.CatalogNames; id++ {
					n, err := s.catalogName(id)
					if err != nil {
						t.Fatal(err)
					}
					fmt.Fprintf(&u, "%s\t%x\t%d\n", n.Name, n.Sources, stateBool(n.OnTarget))
					if !n.OnTarget {
						fmt.Fprintf(&m, "%s\t%x\n", n.Name, n.Sources)
					}
				}
				union, missing = u.Bytes(), m.Bytes()
				for _, name := range []string{"union.tsv", "missing.tsv"} {
					if _, err := os.Stat(filepath.Join(path, name)); !errors.Is(err, os.ErrNotExist) {
						t.Fatal("SQLite discovery wrote a legacy catalog", name, err)
					}
				}
				if rows := j.find(func(r syncRow) bool { return r.Kind == "discovered" || r.Kind == "discover_checkpoint" }); len(rows) != 0 {
					t.Fatal("SQLite discovery dual-wrote its journal reducer")
				}
				if err := s.close(); err != nil {
					t.Fatal(err)
				}
				s, err = openStateStore(s.path, s.unit, false)
				if err != nil {
					t.Fatal(err)
				}
				defer s.close()
				source.discoveryState = &sqliteSyncDiscovery{store: s, scope: "root"}
			}
			// A completed reopen must load its receipt without contacting a
			// source; make every future List request fail to prove it.
			b.noCalls = true
			calls := b.calls.Load()
			if err := source.discover(context.Background()); err != nil {
				t.Fatal("completed discovery repeated backend work", err)
			}
			if b.calls.Load() != calls {
				t.Fatal("completed discovery repeated backend calls")
			}
			outcomes[backend] = outcome{partial, final, union, missing, progress}
		})
	}
	if !reflect.DeepEqual(outcomes["legacy"], outcomes["sqlite"]) {
		t.Fatalf("discovery persistence differs: legacy=%+v sqlite=%+v", outcomes["legacy"], outcomes["sqlite"])
	}
}
