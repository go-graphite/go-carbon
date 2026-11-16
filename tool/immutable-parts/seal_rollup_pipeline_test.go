package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"runtime/metrics"
	"runtime/pprof"
	"runtime/trace"
	"strconv"
	"sync"
	"testing"
	"time"
)

// Keep execution and physical storage geometry independently measurable. A
// database per CPU changes fixed publication/close work with every curve arm.
func sealBenchmarkGeometry(t testing.TB, cores int) (shards, owners, producers int) {
	t.Helper()
	read := func(name string, fallback int) int {
		s := os.Getenv(name)
		if s == "" {
			return fallback
		}
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 {
			t.Fatalf("invalid %s=%q", name, s)
		}
		return n
	}
	shards = read("SEAL_BENCH_DBS", max(1, cores/8))
	owners = read("SEAL_BENCH_OWNERS", shards)
	producers = read("SEAL_BENCH_PRODUCERS", min(cores/2, 48))
	if owners > shards {
		t.Fatal("execution owners must not exceed physical databases")
	}
	return
}

func sealBenchmarkIDOffset(t testing.TB) uint64 {
	t.Helper()
	s := os.Getenv("SEAL_BENCH_ID_OFFSET")
	if s == "" {
		return 0
	}
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// Each child opens its independent engine before the common start barrier.
// Calibration and measured passes have separate barriers, so process startup
// and a fast child cannot inflate the combined throughput.
func sealBenchmarkBarrier(b *testing.B) {
	b.Helper()
	prefix := os.Getenv("SEAL_BENCH_BARRIER")
	if prefix == "" {
		return
	}
	base := prefix + "." + strconv.Itoa(b.N)
	if err := os.WriteFile(base+".ready", nil, 0600); err != nil {
		b.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, err := os.Stat(base + ".start"); err == nil {
			break
		} else if !os.IsNotExist(err) {
			b.Fatal(err)
		}
		if time.Now().After(deadline) {
			b.Fatal("pipeline start barrier timed out")
		}
		time.Sleep(time.Millisecond)
	}
	fmt.Printf("PIPELINE_START pass=%d unix_ns=%d\n", b.N, time.Now().UnixNano())
}

func sealBenchmarkGCCPU() float64 {
	s := []metrics.Sample{{Name: "/cpu/classes/gc/total:cpu-seconds"}}
	metrics.Read(s)
	return s[0].Value.Float64()
}

// Profiles cover the timed seal/close phase, excluding engine open and the
// explicit post-timing GC used by the memory receipt. Whole-process profiles
// can otherwise attribute that forced GC to the write path.
func sealBenchmarkProfile(b *testing.B) func() {
	b.Helper()
	prefix := os.Getenv("SEAL_BENCH_PROFILE")
	if prefix == "" || b.N == 1 {
		return func() {}
	}
	prefix += "." + strconv.Itoa(b.N)
	create := func(suffix string) *os.File {
		f, err := os.Create(prefix + suffix)
		if err != nil {
			b.Fatal(err)
		}
		return f
	}
	cpu := create(".cpu")
	var tr *os.File
	if os.Getenv("SEAL_BENCH_PROFILE_CPU_ONLY") != "1" {
		tr = create(".trace")
	}
	runtime.SetBlockProfileRate(1)
	prior := runtime.SetMutexProfileFraction(1)
	if err := pprof.StartCPUProfile(cpu); err != nil {
		b.Fatal(err)
	}
	if tr != nil {
		if err := trace.Start(tr); err != nil {
			b.Fatal(err)
		}
	}
	return func() {
		if tr != nil {
			trace.Stop()
		}
		pprof.StopCPUProfile()
		_ = cpu.Close()
		if tr != nil {
			_ = tr.Close()
		}
		for _, name := range []string{"mutex", "block"} {
			f := create("." + name)
			if err := pprof.Lookup(name).WriteTo(f, 0); err != nil {
				b.Fatal(err)
			}
			_ = f.Close()
		}
		runtime.SetBlockProfileRate(0)
		runtime.SetMutexProfileFraction(prior)
	}
}

// Changing execution geometry must preserve every persisted key and value,
// including policy/frontier records. Database routing and input stay fixed.
func TestRollupExecutionGeometryExact(t *testing.T) {
	var want [32]byte
	for _, owners := range []int{1, 2, 4, 8} {
		t.Run(fmt.Sprint(owners), func(t *testing.T) {
			cfg := coreConfig{Shards: 8, Owners: owners, Producers: 2, MaxRows: 10000, PartitionSeconds: 3600, AppendParts: true, JournalIdentity: "geometry-exact"}
			e, _, err := openCoreRollups(t.TempDir(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			inputs := atomicBenchInput(32, 118, 4)
			policy, err := json.Marshal(inputs[0].Config)
			if err != nil {
				t.Fatal(err)
			}
			for n := range 3 {
				advanceAtomicInputs(inputs, uint64(n+1), 118)
				heads := make([]compactRollupHead, cfg.Producers)
				for _, in := range inputs {
					b := in.Batches[0]
					ps := make([]row, len(b.Points))
					for i, p := range b.Points {
						ps[i] = row{ID: in.ID, Time: p.Time, Bits: p.Bits, Seq: p.Sequence}
					}
					p := coreShard(in.ID, cfg.Shards) % cfg.Producers
					if err = heads[p].append(coreBatch{ID: in.ID, Order: b.Order, Clock: b.Now, Policy: policy, Points: ps}); err != nil {
						t.Fatal(err)
					}
				}
				for p := range heads {
					if err = e.submitCompactRollupGroup(p, &heads[p], uint64(n+1)); err != nil {
						t.Fatal(err)
					}
				}
			}
			h := sha256.New()
			for _, o := range e.owners {
				it, err := o.db.NewIter(nil)
				if err != nil {
					t.Fatal(err)
				}
				for it.First(); it.Valid(); it.Next() {
					fmt.Fprintf(h, "%x:%x\n", it.Key(), it.Value())
				}
				if err = it.Error(); err != nil {
					t.Fatal(err)
				}
				if err = it.Close(); err != nil {
					t.Fatal(err)
				}
			}
			got := [32]byte(h.Sum(nil))
			if owners == 1 {
				want = got
			} else if got != want {
				t.Fatalf("execution geometry changed persisted bytes: %x != %x", got, want)
			}
			if err = e.close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCompactPlanOwnsPackedBytes(t *testing.T) {
	inputs := atomicBenchInput(1, 118, 4)
	policy, err := json.Marshal(inputs[0].Config)
	if err != nil {
		t.Fatal(err)
	}
	var h compactRollupHead
	ps := make([]row, 118)
	fillSealRows(ps, 7, 1794700800, 0, nil)
	if err = h.append(coreBatch{ID: 7, Order: 1, Clock: 1794700800, Policy: policy, Points: ps}); err != nil {
		t.Fatal(err)
	}
	o := &atomicRollupOwner{}
	in := coreSealInput{Next: coreFrontier{NextPart: 1, Config: coreConfig{PartitionSeconds: 3600}, ExpiredBefore: -1 << 63}, Compact: &compactRollupSelection{head: &h, batches: h.batches}}
	first, err := planCompactCoreRollups(o, in)
	if err != nil {
		t.Fatal(err)
	}
	copyBytes := func(p coreSealPlan) []string {
		var out []string
		for _, file := range p.Files {
			for _, kv := range file {
				out = append(out, string(kv.Key), string(kv.Value))
			}
		}
		return out
	}
	want := copyBytes(first)
	// Plan another part before committing the first, then destroy admission
	// storage. Neither action may change bytes borrowed by the first SST writer.
	in.Next.NextPart++
	if _, err = planCompactCoreRollups(o, in); err != nil {
		t.Fatal(err)
	}
	clear(h.data)
	if got := copyBytes(first); !reflect.DeepEqual(got, want) {
		t.Fatal("a later plan or input reuse mutated planned SST bytes")
	}
}

// More producers than databases used to leave producers without an owner.
// Exercise every independent queue and verify exact raw sample bits on reopen.
func TestCompactMoreProducersThanDBs(t *testing.T) {
	cfg := coreConfig{Shards: 2, Owners: 2, Producers: 8, MaxRows: 10000, PartitionSeconds: 3600, AppendParts: true, JournalIdentity: "producer-geometry"}
	root := t.TempDir()
	e, _, err := openCoreRollups(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	policy, _ := json.Marshal(atomicBenchInput(1, 1, 4)[0].Config)
	var heads [8]compactRollupHead
	var ids [8]uint64
	rows := make([]row, 118)
	for p := range heads {
		id := uint64(p)
		for coreShard(id, len(heads)) != p {
			id++
		}
		ids[p] = id
		fillSealRows(rows, id, 1794700800, 0, nil)
		if err = heads[p].append(coreBatch{ID: id, Order: 1, Clock: 1794700800, Policy: policy, Points: rows}); err != nil {
			t.Fatal(err)
		}
	}
	var workers sync.WaitGroup
	errs := make(chan error, len(heads))
	for p := range heads {
		workers.Go(func() { errs <- e.submitCompactRollupGroup(p, &heads[p], 1) })
	}
	workers.Wait()
	for range heads {
		if err = <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if err = e.close(); err != nil {
		t.Fatal(err)
	}
	e, _, err = openCoreRollups(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e.close()
	for p := range heads {
		o := e.owners[coreShard(ids[p], cfg.Shards)]
		snap := o.db.NewSnapshot()
		got, err := appendCompactCoreRows(snap, ids[p], nil)
		snap.Close()
		if err != nil {
			t.Fatal(err)
		}
		fillSealRows(rows, ids[p], 1794700800, 0, nil)
		if !reflect.DeepEqual(got, rows) {
			t.Fatalf("producer %d changed points after reopen", p)
		}
	}
}
