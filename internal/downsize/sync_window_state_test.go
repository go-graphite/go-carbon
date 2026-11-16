package downsize

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

type windowVisitCut struct {
	syncWindowPersistence
	cancel context.CancelFunc
}

func (p *windowVisitCut) append(r syncRow) error {
	if err := p.syncWindowPersistence.append(r); err != nil {
		return err
	}
	if r.Kind == "window-retry-visited" && p.cancel != nil {
		p.cancel()
		p.cancel = nil
	}
	return nil
}

func TestSyncWindowPersistenceDifferential(t *testing.T) {
	type outcome struct {
		Snapshot              syncWindowSnapshot
		Copies                []string
		Requests              [][]string
		Done, Replayed, Floor int64
		Residual              []syncResidual
	}
	outcomes := map[string]outcome{}
	for _, backend := range []string{"legacy", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			b := &stateSelectionBackend{retrySelectionBackend: &retrySelectionBackend{newFakeSync(), true}}
			w := syncRange{Name: "gap", From: syncTestNow - 600, Until: syncTestNow - 120}
			for _, name := range []string{"done", "pending", "later"} {
				b.store(syncD1).archives[name] = []syncArchive{{Step: 60, Points: 10000}}
				b.store(syncD1).points[name] = map[int64]map[int64]float64{60: {w.From: 42}}
			}
			path := t.TempDir()
			o := syncOptions{to: syncT, from: []string{syncD1}, state: path, windows: syncRanges{w}, includeShort: true, now: func() time.Time { return time.Unix(syncTestNow, 0) }, streams: 1, batch: 2}
			var j *syncJournal
			var store *stateStore
			open := func() (*syncer, syncWindowPersistence) {
				t.Helper()
				var err error
				j, err = openSyncJournal(filepath.Join(path, "sync.jsonl"), "window-persistence")
				if err != nil {
					t.Fatal(err)
				}
				s := &syncer{o: o, b: b, j: j, forbid: map[string]bool{}}
				var p syncWindowPersistence = legacySyncWindow{j}
				if backend == "sqlite" {
					if store == nil {
						store = newTypedState(t, 0)
					} else {
						store, err = openStateStore(store.path, store.unit, false)
						if err != nil {
							t.Fatal(err)
						}
					}
					p = sqliteSyncWindow{store, "root"}
					s.windowState = p
				}
				return s, p
			}
			s, p := open()
			for _, row := range []syncRow{
				{Kind: "window-selected", Round: 1, Source: syncD1, First: "done", Last: "pending", Names: 2, Skipped: []syncSkip{{"done", syncWindowSelectionReason}, {"pending", syncWindowSelectionReason}, {"pending", syncWindowSelectionReason}}, SelectionComplete: true},
				{Kind: "window-selected", Round: 2, Source: syncD1, First: "later", Last: "later", Names: 1, Skipped: []syncSkip{{"later", syncWindowSelectionReason}}, SelectionComplete: true},
				{Kind: "window-resolved", Round: 1, Source: syncD1, Selected: []string{"done"}},
				{Kind: "window-retry-floor", Names: 99}, {Kind: "window-retry-floor", Names: 1},
			} {
				if err := p.append(row); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s.windowState = &windowVisitCut{p, cancel}
			if err := s.retryWindowSelections(ctx); !errors.Is(err, context.Canceled) {
				t.Fatal("missing interruption", err)
			}
			saved, err := p.load()
			if err != nil || !saved.visited[syncWindowSelectionKey{1, syncD1}] || saved.resolved[syncWindowSelectionKey{1, syncD1}]["pending"] {
				t.Fatal("visit retired unknown coverage", saved, err)
			}
			beforeReceipts := append([]syncRow(nil), saved.receipts...)
			j.close()
			if store != nil {
				if err := store.close(); err != nil {
					t.Fatal(err)
				}
			}
			b.pending = false
			b.requests = nil
			s, p = open()
			defer j.close()
			if store != nil {
				defer store.close()
			}
			if err := s.retryWindowSelections(context.Background()); err != nil {
				t.Fatal(err)
			}
			pending, err := s.hasPendingWindowSelections()
			if err != nil || pending {
				t.Fatal("resolved selections remain pending", err)
			}
			saved, err = p.load()
			if err != nil {
				t.Fatal(err)
			}
			if len(saved.receipts) < len(beforeReceipts) || !reflect.DeepEqual(saved.receipts[:len(beforeReceipts)], beforeReceipts) {
				t.Fatal("saved receipt order or timestamps changed across reopen")
			}
			for i := range saved.selections {
				saved.selections[i].Time = ""
			}
			for i := range saved.receipts {
				saved.receipts[i].Time = ""
			}
			s.prog.mu.Lock()
			floor := s.prog.durableFloor
			s.prog.mu.Unlock()
			outcomes[backend] = outcome{saved, append([]string(nil), b.copies...), b.requests, s.prog.done.Load(), s.prog.replayed.Load(), floor, s.sum.residual}
			if len(b.copies) != 2 {
				t.Fatal("native imports repeated", b.copies)
			}
			for _, names := range b.requests {
				if len(names) != 1 || names[0] == "done" {
					t.Fatal("completed or duplicate selection reached backend", b.requests)
				}
			}
			calls, copies := len(b.requests), len(b.copies)
			if err := s.retryWindowSelections(context.Background()); err != nil || len(b.requests) != calls || len(b.copies) != copies {
				t.Fatal("completed work repeated", err)
			}
			if store != nil {
				if rows := j.find(func(r syncRow) bool {
					return r.Kind == "window-selected" || r.Kind == "window-resolved" || r.Kind == "window-retry-visited" || r.Kind == "window-retry-floor"
				}); len(rows) != 0 {
					t.Fatal("SQLite window state dual-written", rows)
				}
				stateReadTx(t, store, func(tx *sql.Tx) {
					counts, err := readStateWindowRetryCounts(tx, "root")
					if err != nil || counts.Total != 4 || counts.Durable != 4 || counts.Copied != 2 {
						t.Fatal(counts, err)
					}
				})
			}
		})
	}
	if !reflect.DeepEqual(outcomes["legacy"], outcomes["sqlite"]) {
		t.Fatalf("window persistence differs\nlegacy: %+v\nsqlite: %+v", outcomes["legacy"], outcomes["sqlite"])
	}
}

func TestSyncWindowPersistenceFailsClosed(t *testing.T) {
	for _, kind := range []string{"selection", "resolution", "membership", "header"} {
		t.Run(kind, func(t *testing.T) {
			store := newTypedState(t, 0)
			p := sqliteSyncWindow{store, "root"}
			if err := p.append(syncRow{Kind: "window-selected", Round: 1, Source: syncD1, Skipped: []syncSkip{{"pending", syncWindowSelectionReason}}, SelectionComplete: true}); err != nil {
				t.Fatal(err)
			}
			if err := p.append(syncRow{Kind: "window-resolved", Round: 1, Source: syncD1, Selected: []string{"done"}, Resolved: []syncSkip{{"done", "source_absent_no_pending"}}}); err != nil {
				t.Fatal(err)
			}
			before := typedDigests(t, store)
			if err := p.append(syncRow{Kind: "window-selected", Round: 1, Source: syncD1, Failed: []string{}}); err == nil || !reflect.DeepEqual(before, typedDigests(t, store)) {
				t.Fatal("unmapped field accepted", err)
			}
			var query string
			switch kind {
			case "selection":
				query = "UPDATE window_selection_name SET name='corrupt'"
			case "resolution":
				query = "UPDATE window_resolution_proof SET reason='corrupt'"
			case "header":
				query = "UPDATE window_runtime_receipt SET saved_at='corrupt' WHERE kind='window-resolved'"
			case "membership":
				query = "INSERT INTO window_resolved VALUES('root',1,'" + syncD1 + "','phantom',1)"
			}
			if _, err := store.db.Exec(query); err != nil {
				t.Fatal(err)
			}
			if _, err := p.load(); err == nil {
				t.Fatal("corrupt retry state authorized scheduling")
			}
		})
	}
}
