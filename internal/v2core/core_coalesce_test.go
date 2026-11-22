package v2core

import (
	"math/rand/v2"
	"slices"
	"testing"
	"time"
)

// Scatter is a transport change: compare every packet, including per-owner
// ordering and IEEE payloads, before either path can sort or encode anything.
func TestCoreIndexedScatterPreservesPackets(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 192))
	input := make([]row, 32768)
	for i := range input {
		input[i] = row{rng.Uint64N(901), int64(rng.Uint64()), rng.Uint64(), uint64(i)}
	}
	for _, shards := range []int{1, 7, 48, 96} {
		var want [][]row
		for _, indexed := range []bool{false, true} {
			cfg := coreConfig{Shards: shards, Producers: 1, MaxRows: len(input)}
			e := &coreEngine{cfg: cfg, indexScatter: indexed, routers: make([]coreRouter, 1)}
			for range shards {
				e.owners = append(e.owners, &coreOwner{dead: make(chan struct{}), queues: []*coreQueue{newCoreQueue(coreQueueBytes)}})
			}
			// A single indivisible packet may exceed an empty edge's byte quota.
			if err := e.submit(0, input, 19, false); err != nil {
				t.Fatal(err)
			}
			got := make([][]row, shards)
			for s, o := range e.owners {
				p := o.queues[0].pop()
				if p == nil || p.cursor != 19 {
					t.Fatal("missing cursor/packet", s)
				}
				got[s] = slices.Clone(p.rows)
				for _, r := range p.rows {
					if coreShard(r.ID, shards) != s {
						t.Fatal("wrong owner")
					}
				}
				p.lease.release()
			}
			clear(e.routers[0].slots[0].storage) // detached expected packets must survive lease reuse
			if e.routers[0].bytes.Load() != 0 {
				t.Fatal("credit leak")
			}
			if !indexed {
				want = got
				continue
			}
			for s := range want {
				if !slices.Equal(want[s], got[s]) {
					t.Fatal("packet bytes/order changed", shards, s)
				}
			}
		}
	}
}

func TestCoreWakeHintsCannotBlockOrLoseCredit(t *testing.T) {
	wake := make(chan struct{}, 1)
	for range 100 {
		coreWake(wake)
	}
	if len(wake) != 1 {
		t.Fatal("notification did not coalesce")
	}
	coreWait(wake)
	if len(wake) != 0 {
		t.Fatal("notification was not consumed")
	}
	// Missing notifications must permit a retry so stopped owners are observed.
	done := make(chan struct{})
	go func() { coreWait(wake); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("lost notification blocked retry")
	}
	router := &coreRouter{wake: wake}
	router.bytes.Store(100)
	lease := &coreDispatch{router: router, bytes: 100}
	lease.remaining.Store(2)
	lease.release()
	if router.bytes.Load() != 100 {
		t.Fatal("early credit return")
	}
	lease.release()
	if router.bytes.Load() != 0 || len(wake) != 1 {
		t.Fatal("last owner did not return credit and notify")
	}
}
