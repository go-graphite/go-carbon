package v2core

import (
	"bytes"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble"
)

func TestAppendFrontierBoundAndAtomicRetirement(t *testing.T) {
	for _, blind := range []bool{false, true} {
		cfg := coreTestConfig()
		cfg.Shards = 1
		cfg.Owners = 1
		cfg.Producers = 1
		cfg.BlindBatch = blind
		cfg.AppendFrontiers = true
		root := t.TempDir()
		hooks := coreHooks{PlanSeal: func(_ int, in coreSealInput) (coreSealPlan, error) {
			key := []byte{'R', byte(in.Next.NextPart)}
			p := coreSealPlan{ReplaceRaw: true, Files: [][]coreKV{{{Key: key, Value: []byte("current")}}}}
			if in.Next.NextPart > 1 {
				p.RangeDeletes = [][2][]byte{{[]byte{'R'}, key}}
			}
			return p, nil
		}}
		e, err := openCoreWithHooks(root, cfg, hooks)
		if err != nil {
			t.Fatal(err)
		}
		for n := 1; n <= 66; n++ {
			if err = e.AppendBatches(0, []Batch{{ID: 1, Order: uint64(n), Clock: int64(n), Points: []Row{{ID: 1, Time: int64(n), Bits: 1, Seq: uint64(n)}}}}, uint64(n), true); err != nil {
				t.Fatal(err)
			}
		}
		it, err := e.DBs()[0].NewIter(nil)
		if err != nil {
			t.Fatal(err)
		}
		frontiers, records := 0, 0
		for it.First(); it.Valid(); it.Next() {
			if bytes.HasPrefix(it.Key(), coreMetaKey) {
				frontiers++
			}
			if it.Key()[0] == 'R' {
				records++
			}
		}
		if err = it.Close(); err != nil {
			t.Fatal(err)
		}
		if frontiers > 64 || records != 1 {
			t.Fatal(blind, frontiers, records)
		}
		if _, c, err := e.DBs()[0].Get([]byte{'R', 65}); !errors.Is(err, pebble.ErrNotFound) {
			if c != nil {
				c.Close()
			}
			t.Fatal("retired extension survived", err)
		}
		if err = e.Close(); err != nil {
			t.Fatal(err)
		}
		e, err = openCoreWithHooks(root, cfg, hooks)
		if err != nil {
			t.Fatal(err)
		}
		if got := e.DurableFrontiers()[0][0]; got != 66 {
			t.Fatal("restart frontier", got)
		}
		if err = e.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
func TestSealRetirementRejectsReservedOrCrossNamespace(t *testing.T) {
	for _, span := range [][2][]byte{{{1}, {2}}, {{255, 'V'}, {255, 'W'}}, {{'R'}, {'Q'}}, {{'R'}, {'S'}}} {
		if validateCoreSealPlan(coreSealPlan{RangeDeletes: [][2][]byte{span}}) == nil {
			t.Fatal(span)
		}
	}
}
