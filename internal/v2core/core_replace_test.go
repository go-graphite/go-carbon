package v2core

import (
	"errors"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble"
)

func TestCoreReplacementAtomic(t *testing.T) {
	t.Parallel()
	for _, blind := range []bool{false, true} {
		t.Run(map[bool]string{false: "sst", true: "batch"}[blind], func(t *testing.T) {
			cfg := coreTestConfig()
			cfg.Shards = 1
			cfg.Producers = 1
			cfg.BlindBatch = blind
			root := t.TempDir()
			commits, aborts := 0, 0
			var e *coreEngine
			hooks := coreHooks{PlanSeal: func(owner int, in coreSealInput) (coreSealPlan, error) {
				if owner != 0 || len(in.Batches) != 1 || string(in.Batches[0].Policy) != "opaque-frame-receipt" || in.Batches[0].Clock != 1234 {
					t.Errorf("lost original boundary: %+v", in)
				}
				return coreSealPlan{ReplaceRaw: true, Files: [][]coreKV{{{[]byte("G-data"), []byte("exact-replacement"), nil, ""}}, {{[]byte("I-catalog"), []byte("canonical-name"), nil, ""}}, {{[]byte("R-receipt"), slices.Clone(in.Batches[0].Policy), nil, ""}}, {{[]byte{255, 'V', '2', '/'}, []byte("checkpoint"), nil, ""}}}, Commit: func() {
					commits++
					for _, key := range []string{"G-data", "I-catalog", "R-receipt", string(coreMetaKey)} {
						_, c, err := e.owners[0].db.Get([]byte(key))
						if err != nil {
							t.Error("commit before durable data", key, err)
						}
						if c != nil {
							c.Close()
						}
					}
				}, Abort: func() { aborts++ }}, nil
			}}
			var err error
			e, err = openCoreWithHooks(root, cfg, hooks)
			if err != nil {
				t.Fatal(err)
			}
			dbs := e.DBs()
			if err = dbs[0].Set([]byte("I-recovered"), []byte("before-intake"), pebble.Sync); err != nil {
				t.Fatal(err)
			}
			dbs[0] = nil // The accessor must detach its slice.
			old := e.owners[0].db.NewSnapshot()
			// Replacement planning retains original unsorted negative timestamps.
			rows := []row{{7, 19, 0x8000000000000000, 12}, {7, -1, 0x7ff8000000000042, 11}}
			batch := []coreBatch{{ID: 7, Order: 9, Clock: 1234, Policy: []byte("opaque-frame-receipt"), Points: rows}}
			if err = e.submitBatches(0, batch, 101, true); err != nil {
				t.Fatal(err)
			}
			if commits != 1 || aborts != 0 || e.durableFrontiers()[0][0] != 101 {
				t.Fatalf("commits%d aborts%d frontier%v", commits, aborts, e.durableFrontiers())
			}
			if _, c, err := old.Get([]byte("G-data")); !errors.Is(err, pebble.ErrNotFound) {
				if c != nil {
					c.Close()
				}
				t.Fatal("mutated old snapshot", err)
			}
			old.Close()
			if err = e.close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := openCoreWithHooks(root, cfg, hooks)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.close()
			if reopened.durableFrontiers()[0][0] != 101 {
				t.Fatal("lost durable frontier")
			}
			snap := reopened.owners[0].db.NewSnapshot()
			defer snap.Close()
			var count int
			if err = ScanRaw(snap, func(Row) error { count++; return nil }); err != nil || count != 0 {
				t.Fatalf("numeric raw leaked: %d %v", count, err)
			}
			for key, want := range map[string]string{"G-data": "exact-replacement", "I-catalog": "canonical-name", "R-receipt": "opaque-frame-receipt", "I-recovered": "before-intake", string([]byte{255, 'V', '2', '/'}): "checkpoint"} {
				v, c, err := snap.Get([]byte(key))
				if err != nil {
					t.Fatal(key, err)
				}
				if string(v) != want {
					t.Error(key, string(v))
				}
				c.Close()
			}
		})
	}
}

func TestCoreReplacementAbort(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"planner", "reserved", "missing"} {
		t.Run(kind, func(t *testing.T) {
			cfg := coreTestConfig()
			cfg.Shards = 1
			cfg.Producers = 1
			commits, aborts := 0, 0
			sentinel := errors.New("planner failed")
			e, err := openCoreWithHooks(t.TempDir(), cfg, coreHooks{PlanSeal: func(int, coreSealInput) (coreSealPlan, error) {
				p := coreSealPlan{ReplaceRaw: true, Commit: func() { commits++ }, Abort: func() { aborts++ }}
				switch kind {
				case "planner":
					return p, sentinel
				case "reserved":
					p.Files = [][]coreKV{{{[]byte{1}, []byte("invalid"), nil, ""}}}
				}
				return p, nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			err = e.submitBatches(0, []coreBatch{{ID: 7, Points: []row{{7, 1, 1, 1}}}}, 1, true)
			if err == nil || commits != 0 || aborts != 1 || e.durableFrontiers()[0][0] != 0 {
				t.Fatalf("err%v commit%d abort%d frontier%v", err, commits, aborts, e.durableFrontiers())
			}
			if kind == "planner" && !errors.Is(err, sentinel) {
				t.Fatal(err)
			}
			if _, c, getErr := e.owners[0].db.Get(coreMetaKey); !errors.Is(getErr, pebble.ErrNotFound) {
				if c != nil {
					c.Close()
				}
				t.Fatal("failed seal persisted frontier", getErr)
			}
			if e.close() == nil {
				t.Fatal("lost owner error")
			}
		})
	}
}
