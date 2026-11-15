package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
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
				err := parallel.submitCompactRollupGroup(p, &heads[p], uint64(n+1))
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
	// Compare exact decoded raw receipts and physical archive timestamp/value/order bytes.
	// The compact path stores one encoded run instead of duplicate raw rows.
	dump := func(e *coreEngine) map[string]string {
		out := map[string]string{}
		for owner, o := range e.owners {
			it, err := o.db.NewIter(nil)
			if err != nil {
				t.Fatal(err)
			}
			for it.First(); it.Valid(); it.Next() {
				k := it.Key()
				if len(k) == 31 && k[0] == 'S' {
					id := binary.BigEndian.Uint64(k[10:])
					kind := k[18]
					switch kind {
					case 'r':
						out[fmt.Sprintf("%d/%x", owner, k)] = string(it.Value())
					case 'c':
						b, e := decodeCompactRun(it.Value(), id, binary.BigEndian.Uint64(k[23:]), nil)
						if e != nil {
							t.Fatal(e)
						}
						key := append([]byte(nil), k...)
						key[18] = 'r'
						out[fmt.Sprintf("%d/%x", owner, key)] = string(encodeRollupBatch(b))
					case 'a':
						out[fmt.Sprintf("%d/%x", owner, k)] = string(it.Value()[2:])
					case 'b':
						archive := int(binary.BigEndian.Uint32(k[19:]))
						a := inputs[0].Config.Archives[archive]
						b, e := decodeCompactRun(it.Value(), id, 0, nil)
						if e != nil {
							t.Fatal(e)
						}
						for _, p := range b.Points {
							key := append([]byte(nil), k...)
							key[18] = 'a'
							binary.BigEndian.PutUint64(key[23:], uint64((p.Time/a.Step)%a.Count))
							r := sealrollup.Record{Delta: sealrollup.Delta{Archive: archive, Slot: (p.Time / a.Step) % a.Count, Order: p.Sequence, Sample: p}}
							out[fmt.Sprintf("%d/%x", owner, key)] = string(sealrollup.EncodeRecord(r)[2:])
						}
					}
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
