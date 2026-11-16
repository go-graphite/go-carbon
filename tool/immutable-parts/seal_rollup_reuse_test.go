package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

func compactReuseFixture(t testing.TB, density int) (*atomicRollupOwner, coreSealInput) {
	t.Helper()
	policy, err := json.Marshal(atomicBenchInput(1, 1, 4)[0].Config)
	if err != nil {
		t.Fatal(err)
	}
	h := new(compactRollupHead)
	ps := make([]row, density)
	for id := range 64 {
		fillSealRows(ps, uint64(id), 1794700800, 0, nil)
		if err = h.append(coreBatch{ID: uint64(id), Order: 1, Clock: 1794700800, Policy: policy, Points: ps}); err != nil {
			t.Fatal(err)
		}
	}
	return &atomicRollupOwner{}, coreSealInput{Next: coreFrontier{NextPart: 1, Config: coreConfig{PartitionSeconds: 3600}, ExpiredBefore: math.MinInt64}, Compact: &compactRollupSelection{head: h, batches: h.batches}}
}
func reuseCompactSeal(t testing.TB, o *atomicRollupOwner, in *coreSealInput) {
	t.Helper()
	p, err := planCompactCoreRollups(o, *in)
	if err != nil {
		t.Fatal(err)
	}
	p.Commit()
	in.Next.NextPart++
	for i := range in.Compact.batches {
		in.Compact.batches[i].order++
	}
}

// Repeated replacements hold occupied history constant. Both inactive history
// buffers, reducer outputs and plan arenas must be warmed before measuring.
func TestCompactPlannerSteadyAllocs(t *testing.T) {
	for _, density := range []int{118, 513} {
		t.Run(fmt.Sprint(density), func(t *testing.T) {
			o, in := compactReuseFixture(t, density)
			for range 8 {
				reuseCompactSeal(t, o, &in)
			}
			n := testing.AllocsPerRun(20, func() { reuseCompactSeal(t, o, &in) })
			t.Logf("density=%d metrics=64 allocs/seal=%.0f", density, n)
			if n != 0 {
				t.Fatalf("steady compact planner allocated %.0f times/seal", n)
			}
		})
	}
}
func BenchmarkCompactPlannerSteady(b *testing.B) {
	for _, d := range []int{118, 513} {
		b.Run(fmt.Sprint(d), func(b *testing.B) {
			o, in := compactReuseFixture(b, d)
			for range 8 {
				reuseCompactSeal(b, o, &in)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				reuseCompactSeal(b, o, &in)
			}
		})
	}
}
func TestCompactFrontierJSONExact(t *testing.T) {
	for _, f := range []sealrollup.Frontier{{}, {Order: math.MaxUint64, MaxTime: math.MinInt64}} {
		for i := range f.Policy {
			f.Policy[i] = byte(i * 9)
		}
		want, err := json.Marshal(f)
		if err != nil {
			t.Fatal(err)
		}
		prefix := []byte("prefix")
		got := appendCompactFrontier(prefix, f)
		if !bytes.Equal(got[len(prefix):], want) {
			t.Fatalf("got %s want %s", got, want)
		}
	}
}

// The arena should hold a canonical schema once, independently of metric count.
func TestCompactPolicyBytesShared(t *testing.T) {
	o, in := compactReuseFixture(t, 118)
	p, err := planCompactCoreRollups(o, in)
	if err != nil {
		t.Fatal(err)
	}
	var policy []byte
	count := 0
	for _, kv := range p.Files[len(p.Files)-1] {
		if len(kv.Key) == 10 && kv.Key[1] == 'p' {
			count++
			if policy == nil {
				policy = kv.Value
			} else if &policy[0] != &kv.Value[0] {
				t.Fatal("policy cloned per metric")
			}
		}
	}
	if count != 64 {
		t.Fatalf("policy keys=%d", count)
	}
	p.Commit()
}

// Exclude the storage dependency here so this guard covers exactly admission
// routing, stable packet/ACK storage, callbacks and waiting for every borrower.
func TestCompactSubmissionSteadyAllocsAndRebind(t *testing.T) {
	_, in := compactReuseFixture(t, 118)
	h := in.Compact.head
	for range 2 {
		e := &coreEngine{cfg: coreConfig{Producers: 1, Shards: 2, MaxRows: 100000}}
		var calls atomic.Int64
		var done []chan struct{}
		for range 2 {
			o := &coreOwner{queues: []*coreQueue{{}}, dead: make(chan struct{}), plan: func(coreSealInput) (coreSealPlan, error) { calls.Add(1); return coreSealPlan{}, nil }}
			e.owners = append(e.owners, o)
			d := make(chan struct{})
			done = append(done, d)
			go func() {
				defer close(d)
				for {
					select {
					case <-o.dead:
						return
					default:
					}
					if p := o.queues[0].pop(); p != nil {
						_, err := p.encoded.Plan(coreFrontier{})
						p.ack <- err
					} else {
						runtime.Gosched()
					}
				}
			}()
		}
		run := func() {
			if err := e.submitCompactRollupGroup(0, h, 1); err != nil {
				t.Fatal(err)
			}
		}
		run()
		if n := testing.AllocsPerRun(20, run); n != 0 {
			t.Errorf("submission allocated %.0f times", n)
		}
		if calls.Load() == 0 {
			t.Error("callbacks still bound to previous engine")
		}
		for i, o := range e.owners {
			close(o.dead)
			<-done[i]
		}
	}
}
func TestCompactHeadSteadyAllocs(t *testing.T) {
	_, in := compactReuseFixture(t, 513)
	h := in.Compact.head
	rows := make([]row, 513)
	fillSealRows(rows, 0, 1794700800, 0, nil)
	batch := coreBatch{ID: 0, Order: 1, Clock: 1794700800, Policy: h.policies[0], Points: rows}
	run := func() {
		h.reset()
		if err := h.append(batch); err != nil {
			t.Fatal(err)
		}
	}
	run()
	if n := testing.AllocsPerRun(20, run); n != 0 {
		t.Fatalf("head allocated %.0f times", n)
	}
}
