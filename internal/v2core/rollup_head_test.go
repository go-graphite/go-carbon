package v2core

import (
	"math"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// A rollup owner without base rows keeps only its head's point count and
// oldest time. A late point in an expired partition must still fail the seal
// wherever it sits in the head, exactly as with base rows, and the oldest
// time must restart with every head.
func TestRollupCountOnlyHeadRejectsExpiredPartition(t *testing.T) {
	t.Parallel()
	_, policy := rollupTestPolicy(t, "test.metric", "1s:1d", sealrollup.Sum, 0)
	clock := int64(1794700800)
	fresh := func(seq uint64) Row {
		return Row{ID: 7, Time: clock - 10 - int64(seq), Bits: math.Float64bits(float64(seq)), Seq: seq}
	}
	late := Row{ID: 7, Time: clock - 3600, Bits: math.Float64bits(1), Seq: 99}
	type packet struct {
		rows []Row
		seal bool
	}
	cases := []struct {
		name    string
		packets []packet
		reject  bool
	}{
		{"fresh across seals", []packet{{[]Row{fresh(1), fresh(2)}, true}, {[]Row{fresh(3)}, false}, {[]Row{fresh(4)}, true}}, false},
		{"late mid packet", []packet{{[]Row{fresh(1), late, fresh(2)}, true}}, true},
		{"late in later packet", []packet{{[]Row{fresh(1)}, false}, {[]Row{fresh(2), late, fresh(3)}, true}}, true},
		{"late after clean seal", []packet{{[]Row{fresh(1)}, true}, {[]Row{late, fresh(2)}, true}}, true},
	}
	for _, keepBaseRows := range []bool{false, true} {
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				cfg := coreTestConfig()
				cfg.Shards, cfg.Producers, cfg.MaxRows = 1, 1, 1024
				cfg.Rollups, cfg.KeepBaseRows = true, keepBaseRows
				root := t.TempDir()
				order := uint64(0)
				submit := func(core *Engine, rows []Row, seal bool) error {
					order++
					return core.AppendBatches(0, []Batch{{ID: 7, Order: order, Clock: clock, Policy: policy, Points: rows}}, order, seal)
				}
				// Establish the owner frontier, then expire partitions older
				// than ten minutes offline, as retention does.
				core, err := Open(root, cfg, Hooks{})
				if err != nil {
					t.Fatal(err)
				}
				if err = submit(core, []Row{fresh(0)}, true); err != nil {
					t.Fatal(err)
				}
				if err = core.Close(); err != nil {
					t.Fatal(err)
				}
				db, err := pebble.Open(filepath.Join(root, "owner-000"), &pebble.Options{})
				if err != nil {
					t.Fatal(err)
				}
				if err = coreExpire(db, corePartition(clock-600, cfg.PartitionSeconds)); err != nil {
					t.Fatal(err)
				}
				if err = db.Close(); err != nil {
					t.Fatal(err)
				}
				if core, err = Open(root, cfg, Hooks{}); err != nil {
					t.Fatal(err)
				}
				defer func() { _ = core.Close() }()
				for _, p := range tc.packets {
					if err = submit(core, p.rows, p.seal); err != nil {
						break
					}
				}
				if got := err != nil && strings.Contains(err.Error(), "late point targets expired partition"); got != tc.reject {
					t.Fatalf("keepBaseRows=%v: seal error %v, want rejection %v", keepBaseRows, err, tc.reject)
				}
			})
		}
	}
}
