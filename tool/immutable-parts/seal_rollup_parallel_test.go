package main

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"
)

func TestParallelRollupGroupExact(t *testing.T) {
	for _, shape := range [][2]int{{4, 2}, {48, 6}} {
		t.Run(fmt.Sprintf("owners%d/producers%d", shape[0], shape[1]), func(t *testing.T) {
			checkParallelRollupGroupExact(t, shape[0], shape[1])
		})
	}
}

func checkParallelRollupGroupExact(t *testing.T, owners, producers int) {
	t.Helper()
	cfg := coreConfig{Shards: owners, Producers: producers, MaxRows: 10000, PartitionSeconds: 3600, AppendParts: true, JournalIdentity: "parallel-exact"}
	parallel, _, err := openCoreRollups(t.TempDir(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	classic, _, err := openCoreRollups(t.TempDir(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	inputs := atomicBenchInput(1, 1, 4)
	policy, err := json.Marshal(inputs[0].Config)
	if err != nil {
		t.Fatal(err)
	}
	for n := range 3 {
		all := make([]coreBatch, 32)
		heads := make([]compactRollupHead, producers)
		for i := range all {
			b := coreBatch{ID: uint64(i), Order: uint64(n + 1), Clock: 1794700800 + int64((n+1)*118), Policy: policy, Points: make([]row, 118)}
			for j := range b.Points {
				b.Points[j] = row{ID: b.ID, Time: b.Clock - 118 + int64(j), Bits: math.Float64bits(float64(j % 17)), Seq: uint64(j + 1)}
			}
			all[i] = b
			p := coreShard(b.ID, cfg.Shards) % cfg.Producers
			if err = heads[p].append(b); err != nil {
				t.Fatal(err)
			}
		}
		var workers sync.WaitGroup
		errs := make(chan error, producers)
		for p := range heads {
			workers.Go(func() {
				bs, err := heads[p].decode()
				if err == nil {
					err = parallel.submitRollupGroup(p, bs, uint64(n+1))
				}
				errs <- err
			})
		}
		workers.Wait()
		for range heads {
			if err = <-errs; err != nil {
				t.Fatal(err)
			}
		}
		if err = classic.submitBatches(0, all, uint64(n+1), true); err != nil {
			t.Fatal(err)
		}
	}
	// Compare raw + persisted rollup bytes, including original batch receipts.
	dump := func(e *coreEngine) map[string]string {
		out := map[string]string{}
		for owner, o := range e.owners {
			it, err := o.db.NewIter(nil)
			if err != nil {
				t.Fatal(err)
			}
			for it.First(); it.Valid(); it.Next() {
				k := it.Key()
				if k[0] == 2 || k[0] == 'S' {
					out[fmt.Sprintf("%d/%x", owner, k)] = string(it.Value())
				}
			}
			if err = it.Error(); err != nil {
				t.Fatal(err)
			}
			if err = it.Close(); err != nil {
				t.Fatal(err)
			}
		}
		return out
	}
	want, got := dump(classic), dump(parallel)
	if !reflect.DeepEqual(want, got) {
		t.Fatal("parallel/serial persisted data differ", len(want), len(got))
	}
	for i, o := range parallel.owners {
		if o.parts.Load() != 3 {
			t.Fatalf("owner%d got%d seals, want3", i, o.parts.Load())
		}
	}
	if err = parallel.close(); err != nil {
		t.Fatal(err)
	}
	if err = classic.close(); err != nil {
		t.Fatal(err)
	}
}
