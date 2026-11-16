package downsize

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"testing"
)

// Exit without defers while a receipt is staged or immediately after its commit.
// This is process-crash qualification, not a physical power-loss simulation.
func TestTypedReceiptProcessCut(t *testing.T) {
	if mode := os.Getenv("STORECTL_TYPED_RECEIPT_CUT"); mode != "" {
		store, err := openStateStore(os.Getenv("STORECTL_TYPED_RECEIPT_DB"), stateUnit{"identity", "options", "catalog-v1", 0}, false)
		if err != nil {
			t.Fatal(err)
		}
		kind := os.Getenv("STORECTL_TYPED_RECEIPT_KIND")
		r := syncRow{Kind: "copied", Phase: "fill", Identity: "observation", Round: 1, Source: "source", First: "a", Last: "b", Names: 2, Copied: 2, Selected: []string{"a", "b"}}
		if kind == "verify" {
			r = syncRow{Kind: "verified", Identity: "observation", Round: 1, First: "a", Last: "b", Names: 2, Holes: []syncHole{{Name: "a", Source: "source", Step: 60, Points: 1, First: 600, Last: 600}}}
		}
		err = store.update(func(tx *stateTx) error {
			var err error
			if kind == "window" {
				err = tx.resolveWindowSelection("root", "resolution", 1, "source", []string{"a", "b"}, []syncSkip{{"a", "source_absent_no_pending"}}, 2)
				if err == nil {
					err = tx.appendWindowRuntimeReceipt("root", syncRow{Kind: "window-resolved", Round: 1, Source: "source", Selected: []string{"a", "b"}, Resolved: []syncSkip{{"a", "source_absent_no_pending"}}, Copied: 2, Time: "2026-11-16T20:00:00Z"}, nil, "resolution")
				}
				if err == nil {
					err = tx.visitWindowSelection("root", 1, "source")
				}
				if err == nil {
					err = tx.appendWindowRuntimeReceipt("root", syncRow{Kind: "window-retry-visited", Round: 1, Source: "source", Names: 2, Time: "2026-11-16T20:00:01Z"}, nil, nil)
				}
			} else if kind == "fill" {
				err = tx.appendFillReceipt("root", r)
			} else {
				err = tx.appendVerification("root", r)
			}
			if err != nil {
				return err
			}
			if kind != "window" {
				if err = tx.reduceProgress("root", r); err != nil {
					return err
				}
			}
			if mode == "before" {
				os.Exit(86)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		os.Exit(86)
	}
	for _, kind := range []string{"fill", "verify", "window"} {
		for _, mode := range []string{"before", "after"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				store := newTypedState(t, 0)
				path, unit := store.path, store.currentUnit()
				if err := store.close(); err != nil {
					t.Fatal(err)
				}
				cmd := exec.Command(os.Args[0], "-test.run=^TestTypedReceiptProcessCut$")
				cmd.Env = append(os.Environ(), "STORECTL_TYPED_RECEIPT_CUT="+mode, "STORECTL_TYPED_RECEIPT_KIND="+kind, "STORECTL_TYPED_RECEIPT_DB="+path)
				out, err := cmd.CombinedOutput()
				var exit *exec.ExitError
				if !errors.As(err, &exit) || exit.ExitCode() != 86 {
					t.Fatalf("process cut failed: %v %s", err, out)
				}
				reopened, err := openStateStore(path, unit, false)
				if err != nil {
					t.Fatal(err)
				}
				defer reopened.close()
				stateReadTx(t, reopened, func(tx *sql.Tx) {
					if kind == "window" {
						want := 0
						if mode == "after" {
							want = 1
						}
						for _, table := range []string{"window_resolution_batch", "window_resolution_proof", "window_retry_visited"} {
							var n int
							if err := tx.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil || n != want {
								t.Fatal("window transaction partially survived", table, n, err)
							}
						}
						var headers int
						if err := tx.QueryRow("SELECT count(*) FROM window_runtime_receipt").Scan(&headers); err != nil || headers != 2*want {
							t.Fatal("receipt header partly survived", headers, err)
						}
						if mode == "after" {
							for _, id := range []int64{0, 1} {
								if _, err := readStateWindowRuntimeReceipt(tx, "root", id); err != nil {
									t.Fatal(err)
								}
							}
						}
						if mode == "after" {
							if err := checkStateWindowResolution(tx, "root", "resolution"); err != nil {
								t.Fatal(err)
							}
						}
						return
					}
					p, err := readStateProgress(tx, "root")
					if mode == "before" {
						if !errors.Is(err, sql.ErrNoRows) {
							t.Fatal("uncommitted progress survived", p, err)
						}
						var n int
						for _, table := range []string{"fill_receipt", "fill_receipt_name", "verification_receipt", "verification_name", "verification_hole"} {
							if err := tx.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil || n != 0 {
								t.Fatal("uncommitted receipt membership survived", table, n, err)
							}
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					if kind == "fill" {
						if p.Filled != 2 {
							t.Fatal("committed fill counter lost", p)
						}
						if _, err := readStateFillReceipt(tx, "root", 0); err != nil {
							t.Fatal(err)
						}
					} else {
						if p.Verified != 2 || p.Holes != 1 {
							t.Fatal("committed verification counters lost", p)
						}
						if _, err := readStateVerification(tx, "root", 0); err != nil {
							t.Fatal(err)
						}
					}
				})
				if err := reopened.update(func(tx *stateTx) error { return context.Canceled }); !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			})
		}
	}
}
