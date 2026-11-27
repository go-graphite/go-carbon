package v2core

import (
	"strings"
	"testing"
	"time"
)

func TestCoreLeaseWaitIsSeparateFromQueueWait(t *testing.T) {
	for _, stop := range []bool{false, true} {
		t.Run(map[bool]string{false: "released", true: "owner-stopped"}[stop], func(t *testing.T) {
			owner := &coreOwner{dead: make(chan struct{}), queues: []*coreQueue{newCoreQueue(coreQueueBytes)}}
			e := &coreEngine{cfg: coreConfig{Shards: 1, Producers: 1, MaxRows: 8}, owners: []*coreOwner{owner}, routers: make([]coreRouter, 1)}
			router := &e.routers[0]
			router.slots = make([]coreDispatch, 1)
			lease := &router.slots[0]
			lease.router, lease.bytes = router, coreStreamBytes
			lease.remaining.Store(1)
			router.bytes.Store(coreStreamBytes)
			done := make(chan error, 1)
			go func() { done <- e.submit(0, []row{{ID: 1, Bits: 0x8000000000000000}}, 1, false) }()
			deadline := time.Now().Add(5 * time.Second)
			for !router.leaseWaiting.Load() {
				if time.Now().After(deadline) {
					t.Fatal("producer did not enter credit wait")
				}
				time.Sleep(time.Microsecond)
			}
			if router.leaseWaitCount.Load() != 1 || owner.queues[0].blockedNS.Load() != 0 {
				t.Fatal("credit wait was not recorded separately")
			}
			if stop {
				close(owner.dead)
			} else {
				lease.release()
			}
			select {
			case err := <-done:
				if stop && (err == nil || !strings.Contains(err.Error(), "owner stopped during buffer reuse")) {
					t.Fatalf("stopped owner: %v", err)
				}
				if !stop && err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("producer remained blocked after release/stop")
			}
			if router.leaseWaiting.Load() || router.leaseWaitNS.Load() == 0 || router.leaseWaitCount.Load() != 1 {
				t.Fatal("credit wait duration/count/gauge was not finalized")
			}
			if !stop {
				packet := owner.queues[0].pop()
				if packet == nil || len(packet.rows) != 1 || packet.rows[0].Bits != 0x8000000000000000 {
					t.Fatal("credit instrumentation changed submitted bits")
				}
				packet.lease.release()
				if router.bytes.Load() != 0 {
					t.Fatal("lease credit leaked")
				}
			}
		})
	}
}
