package downsize

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

type stateSelectionBackend struct {
	*retrySelectionBackend
	requests [][]string
}

func (b *stateSelectionBackend) WindowPresence(ctx context.Context, source string, names []string, w syncRange) (map[string][]syncDigest, error) {
	b.requests = append(b.requests, append([]string(nil), names...))
	return b.retrySelectionBackend.WindowPresence(ctx, source, names, w)
}

func stateProjectWindowRows(t *testing.T, s *stateStore, rows []syncRow, offset int) {
	t.Helper()
	for i, r := range rows[offset:] {
		err := s.update(func(tx *stateTx) error {
			switch r.Kind {
			case "window-selected":
				return tx.putWindowSelection(stateSelection{Scope: "root", Ordinal: int64(offset + i), Group: r.Round, Holder: r.Source, First: r.First, Last: r.Last, Names: r.Names, Complete: r.SelectionComplete, SavedAt: r.Time, Selected: r.Selected, Skipped: r.Skipped})
			case "window-resolved":
				return tx.resolveWindowSelection("root", fmt.Sprintf("row-%d", offset+i), r.Round, r.Source, r.Selected, r.Resolved, r.Copied)
			case "window-retry-visited":
				return tx.visitWindowSelection("root", r.Round, r.Source)
			case "window-retry-floor":
				return tx.putWindowRetryFloor("root", int64(r.Names))
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestTypedStateWindowSelectionRealRetryRestart(t *testing.T) {
	s := newTypedState(t, 3)
	path := t.TempDir()
	identity := "typed-selection-retry"
	j, err := openSyncJournal(filepath.Join(path, "sync.jsonl"), identity)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { j.close() }()
	b := &stateSelectionBackend{retrySelectionBackend: &retrySelectionBackend{newFakeSync(), true}}
	w := syncRange{Name: "gap", From: syncTestNow - 600, Until: syncTestNow - 120}
	for _, n := range []string{"done", "pending", "later"} {
		b.store(syncD1).archives[n] = []syncArchive{{Step: 60, Points: 10000}}
		b.store(syncD1).points[n] = map[int64]map[int64]float64{60: {w.From: 42}}
	}
	rows := []syncRow{
		{Kind: "window-selected", Round: 1, Source: syncD1, First: "done", Last: "pending", Names: 2, Selected: []string{}, Skipped: []syncSkip{{"done", syncWindowSelectionReason}, {"pending", syncWindowSelectionReason}, {"pending", syncWindowSelectionReason}}, SelectionComplete: true},
		{Kind: "window-selected", Round: 2, Source: syncD1, First: "later", Last: "later", Names: 1, Skipped: []syncSkip{{"later", syncWindowSelectionReason}}, SelectionComplete: true},
		{Kind: "window-resolved", Round: 1, Source: syncD1, Selected: []string{"done"}},
		{Kind: "window-retry-floor", Names: 99}, {Kind: "window-retry-floor", Names: 1},
	}
	for _, r := range rows {
		if err = j.append(r); err != nil {
			t.Fatal(err)
		}
	}
	stateProjectWindowRows(t, s, j.rows, 0)
	driver := &syncer{o: syncOptions{to: syncT, from: []string{syncD1}, state: path, windows: syncRanges{w}, includeShort: true, now: func() time.Time { return time.Unix(syncTestNow, 0) }, streams: 1, batch: 2}, b: b, j: j, forbid: map[string]bool{}}
	offset := len(j.rows)
	if err = driver.retryWindowSelections(context.Background()); err != nil {
		t.Fatal(err)
	}
	stateProjectWindowRows(t, s, j.rows, offset)
	stateReadTx(t, s, func(tx *sql.Tx) {
		counts, err := readStateWindowRetryCounts(tx, "root")
		if err != nil || counts.Total != 4 || counts.Durable != driver.prog.done.Load() || counts.Floor != 4 {
			t.Fatal(counts, err, driver.prog.done.Load())
		}
		// A visited row must not retire unresolved selection coverage. Duplicate
		// pending occurrences advance the old display count but yield one request.
		pending, err := readStatePendingSelection(tx, "root", 1, -1, 10, true)
		if err != nil || len(pending) != 1 || pending[0].Name != "pending" {
			t.Fatal(pending, err)
		}
		got, err := readStateLatestWindowSelection(tx, "root", 1, syncD1)
		if err != nil || !reflect.DeepEqual(got.Selected, []string{}) || len(got.Skipped) != 3 {
			t.Fatal(got, err)
		}
	})
	j.close()
	j, err = openSyncJournal(filepath.Join(path, "sync.jsonl"), identity)
	if err != nil {
		t.Fatal(err)
	}
	driver.j = j
	if err = s.close(); err != nil {
		t.Fatal(err)
	}
	s, err = openStateStore(s.path, s.unit, false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	b.pending = false
	b.requests = nil
	offset = len(j.rows)
	if err = driver.retryWindowSelections(context.Background()); err != nil {
		t.Fatal(err)
	}
	stateProjectWindowRows(t, s, j.rows, offset)
	expected := driver.resolvedWindowSelections()
	stateReadTx(t, s, func(tx *sql.Tx) {
		counts, err := readStateWindowRetryCounts(tx, "root")
		if err != nil || counts.Total != 4 || counts.Durable != 4 || counts.Copied != 2 {
			t.Fatal(counts, err)
		}
		for _, ordinal := range []int64{1, 2} {
			pending, err := readStatePendingSelection(tx, "root", ordinal, -1, 10, true)
			if err != nil || len(pending) != 0 {
				t.Fatal(pending, err)
			}
		}
		got := map[syncWindowSelectionKey]map[string]bool{}
		err = stateTaskRows(tx, "SELECT group_id,holder,name FROM window_resolved WHERE scope=?", []any{"root"}, func(rows *sql.Rows) error {
			var k syncWindowSelectionKey
			var name string
			if err := rows.Scan(&k.group, &k.source, &name); err != nil {
				return err
			}
			if got[k] == nil {
				got[k] = map[string]bool{}
			}
			got[k][name] = true
			return nil
		})
		if err != nil || !reflect.DeepEqual(got, expected) {
			t.Fatal(got, expected, err)
		}
	})
	for _, names := range b.requests {
		if len(names) != 1 || names[0] == "done" {
			t.Fatal("completed/duplicate selection reached backend", b.requests)
		}
	}
	calls := len(b.requests)
	copies := len(b.copies)
	if err = driver.retryWindowSelections(context.Background()); err != nil || len(b.requests) != calls || len(b.copies) != copies {
		t.Fatal("resolved work repeated", err)
	}
}

func TestTypedStateWindowSelectionAtomicAndImmutable(t *testing.T) {
	s := newTypedState(t, 0)
	v := stateSelection{Scope: "root", Ordinal: 7, Group: 4, Holder: "source", First: "a", Last: "z", Names: 3, Complete: true, Selected: []string{"z", "a"}, Skipped: []syncSkip{{"pending", "unknown"}}}
	if err := s.update(func(tx *stateTx) error { return tx.putWindowSelection(v) }); err != nil {
		t.Fatal(err)
	}
	before := typedDigests(t, s)
	abort := errors.New("failed outcome commit")
	if err := s.update(func(tx *stateTx) error {
		if err := tx.resolveWindowSelection("root", "token", 4, "source", []string{"pending"}, []syncSkip{{"pending", "source_absent_no_pending"}}, 0); err != nil {
			return err
		}
		if err := tx.visitWindowSelection("root", 4, "source"); err != nil {
			return err
		}
		return abort
	}); !errors.Is(err, abort) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, typedDigests(t, s)) {
		t.Fatal("resolution/proof/visit partially committed")
	}
	for _, mutate := range []func(*stateSelection){func(v *stateSelection) { v.Selected = []string{"a", "z"} }, func(v *stateSelection) { v.Complete = false }, func(v *stateSelection) { v.Skipped = nil }} {
		next := v
		mutate(&next)
		if err := s.update(func(tx *stateTx) error { return tx.putWindowSelection(next) }); err == nil {
			t.Fatal("selection changed after issue")
		}
	}
	if err := s.update(func(tx *stateTx) error {
		return tx.resolveWindowSelection("root", "token", 4, "source", []string{"pending"}, nil, 1)
	}); err != nil {
		t.Fatal(err)
	}
	before = typedDigests(t, s)
	if err := s.update(func(tx *stateTx) error {
		return tx.resolveWindowSelection("root", "token", 4, "source", []string{"pending"}, nil, 1)
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, typedDigests(t, s)) {
		t.Fatal("resolution replay counted twice")
	}
	if err := s.update(func(tx *stateTx) error {
		return tx.resolveWindowSelection("root", "token", 4, "source", []string{"other"}, nil, 1)
	}); err == nil {
		t.Fatal("resolution token rebound")
	}
	if _, err := s.db.Exec("UPDATE window_selection_name SET name='corrupt' WHERE selection=7 AND kind=0 AND ordinal=0"); err != nil {
		t.Fatal(err)
	}
	if err := s.update(func(tx *stateTx) error { return tx.putWindowSelection(v) }); err == nil {
		t.Fatal("corrupt selection projection accepted")
	}
	if _, err := s.db.Exec("UPDATE window_resolution_name SET name='corrupt' WHERE token='token'"); err != nil {
		t.Fatal(err)
	}
	if err := s.update(func(tx *stateTx) error {
		return tx.resolveWindowSelection("root", "token", 4, "source", []string{"pending"}, nil, 1)
	}); err == nil {
		t.Fatal("corrupt resolution projection accepted")
	}
}
