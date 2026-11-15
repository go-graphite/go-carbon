package main

import (
	"errors"
	"github.com/cockroachdb/pebble"
	"slices"
	"sync"
	"testing"
)

func TestCoreOriginalBatchesAtomicExtension(t *testing.T) {
	for _, blind := range []bool{false, true} {
		cfg := coreTestConfig()
		cfg.Shards = 1
		cfg.Producers = 1
		cfg.BlindBatch = blind
		var got []coreBatch
		var committed int
		e, err := openCoreWithHooks(t.TempDir(), cfg, coreHooks{PlanSeal: func(owner int, in coreSealInput) (coreSealPlan, error) {
			if owner != 0 {
				t.Error(owner)
			}
			for _, b := range in.Batches {
				b.Policy = slices.Clone(b.Policy)
				b.Points = slices.Clone(b.Points)
				got = append(got, b)
			}
			return coreSealPlan{Files: [][]coreKV{{{[]byte{'R', byte(in.Next.NextPart)}, []byte("archive")}}}, Commit: func() { committed++ }}, nil
		}})
		if err != nil {
			t.Fatal(err)
		}
		want := []coreBatch{{ID: 7, Order: 11, Clock: 1234, Policy: []byte("exact-policy"), Points: []row{{7, -1, 0x7ff8000000000042, 11}, {7, 19, 0x8000000000000000, 12}}}, {ID: 7, Order: 12, Clock: 9876, Policy: []byte("exact-policy"), Points: []row{{7, 0, 1, 13}}}}
		if err = e.submitBatches(0, want, 101, true); err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 || got[0].Clock != 1234 || got[1].Clock != 9876 || !slices.Equal(got[0].Points, want[0].Points) || !slices.Equal(got[1].Points, want[1].Points) || string(got[0].Policy) != "exact-policy" || committed != 1 {
			t.Fatalf("lost batch metadata: %+v commit%d", got, committed)
		}
		if g := e.durableFrontiers(); g[0][0] != 101 {
			t.Fatal(g)
		}
		snap := e.owners[0].db.NewSnapshot()
		v, c, err := snap.Get([]byte{'R', 1})
		if err != nil {
			t.Fatal(err)
		}
		if string(v) != "archive" {
			t.Fatal(string(v))
		}
		c.Close()
		rows, err := coreRead(snap, 7)
		if err != nil || len(rows) != 3 {
			t.Fatalf("raw rows %v %v", rows, err)
		}
		snap.Close()
		if err = e.close(); err != nil {
			t.Fatal(err)
		}
	}
}
func TestCorePlanFailureDoesNotCommit(t *testing.T) {
	cfg := coreTestConfig()
	cfg.Shards = 1
	cfg.Producers = 1
	sentinel := errors.New("planner failed")
	e, err := openCoreWithHooks(t.TempDir(), cfg, coreHooks{PlanSeal: func(int, coreSealInput) (coreSealPlan, error) { return coreSealPlan{}, sentinel }})
	if err != nil {
		t.Fatal(err)
	}
	if err = e.submitBatches(0, []coreBatch{{ID: 1, Points: []row{{1, 1, 1, 1}}}}, 1, true); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	if e.durableFrontiers()[0][0] != 0 {
		t.Fatal("advanced failed frontier")
	}
	if _, c, err := e.owners[0].db.Get(coreMetaKey); !errors.Is(err, pebble.ErrNotFound) {
		if c != nil {
			c.Close()
		}
		t.Fatal(err)
	}
	if err = e.close(); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
}
func TestCorePlanValidation(t *testing.T) {
	for _, files := range [][][]coreKV{{{}}, {{{[]byte{1}, nil}}}, {{{[]byte{255}, nil}}}, {{{[]byte{'R', 2}, nil}, {[]byte{'R', 1}, nil}}}, {{{[]byte{'R', 1}, nil}}, {{[]byte{'R', 1}, nil}}}} {
		if validateCoreSealPlan(coreSealPlan{Files: files}) == nil {
			t.Fatal(files)
		}
	}
}
func TestCoreConcurrentDurableFrontier(t *testing.T) {
	cfg := coreTestConfig()
	e, err := openCore(t.TempDir(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		last := make([]uint64, cfg.Producers)
		for {
			select {
			case <-done:
				return
			default:
			}
			f := e.durableFrontiers()
			for p := range last {
				var low uint64 = ^uint64(0)
				for _, v := range f {
					low = min(low, v[p])
				}
				if low < last[p] {
					t.Error("frontier reversed")
				}
				last[p] = low
			}
		}
	}()
	for n := 1; n <= 12; n++ {
		for p := 0; p < cfg.Producers; p++ {
			if err = e.submit(p, coreRows(p, n), uint64(n), true); err != nil {
				t.Fatal(err)
			}
		}
	}
	close(done)
	wg.Wait()
	if err = e.close(); err != nil {
		t.Fatal(err)
	}
}
