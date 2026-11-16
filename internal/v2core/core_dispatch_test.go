package v2core

import (
	"reflect"
	"testing"
	"time"
)

func TestCoreQueueByteQuotaAndFIFO(t *testing.T) {
	q := newCoreQueue(1024)
	small := &corePacket{cursor: 1}
	large := &corePacket{cursor: 2, rows: make([]row, 20)}
	if !q.tryPush(small) || !q.tryPush(large) {
		t.Fatal("byte quota rejected packets with room")
	}
	if q.tryPush(large) {
		t.Fatal("byte quota exceeded")
	}
	if q.pop() != small || q.pop() != large || q.pop() != nil {
		t.Fatal("FIFO changed")
	}
	oversized := &corePacket{rows: make([]row, 100)}
	if !q.tryPush(oversized) || q.tryPush(small) || q.pop() != oversized {
		t.Fatal("oversized group must be alone")
	}
	for i := range 10000 {
		p := &corePacket{cursor: uint64(i)}
		if !q.tryPush(p) || q.pop() != p {
			t.Fatal("wrap", i)
		}
	}
}

// One full owner must not prevent any ready owner from receiving the current
// original group. This fails the old ascending-owner blocking dispatch.
func TestCoreDispatchSkipsFullOwnerAndPreservesGroups(t *testing.T) {
	cfg := coreTestConfig()
	cfg.Shards, cfg.Producers = 2, 1
	e := &coreEngine{cfg: cfg, routers: make([]coreRouter, 1)}
	for range cfg.Shards {
		e.owners = append(e.owners, &coreOwner{queues: []*coreQueue{newCoreQueue(corePacketBytes)}, dead: make(chan struct{})})
	}
	var ids [2]uint64
	for s := range ids {
		for id := uint64(1); ; id++ {
			if coreShard(id, 2) == s {
				ids[s] = id
				break
			}
		}
	}
	for n := uint64(1); n <= 12; n++ {
		full := &corePacket{cursor: 999}
		if !e.owners[0].queues[0].tryPush(full) {
			t.Fatal("fill")
		}
		want := []coreBatch{
			{ID: ids[0], Order: n, Clock: -17, Policy: []byte("policy0"), Points: []row{{ID: ids[0], Time: -91, Bits: 0x7ff8000000000042, Seq: n}}},
			{ID: ids[1], Order: n, Clock: 123, Policy: []byte("policy1"), Points: []row{{ID: ids[1], Time: 92, Bits: 0x8000000000000000, Seq: n}}},
		}
		done := make(chan error, 1)
		go func() { done <- e.submitBatches(0, want, n, true) }()
		deadline := time.Now().Add(time.Second)
		var ready *corePacket
		for ready == nil && time.Now().Before(deadline) {
			ready = e.owners[1].queues[0].pop()
			if ready == nil {
				time.Sleep(time.Millisecond)
			}
		}
		if ready == nil {
			t.Fatal("ready owner starved behind full owner")
		}
		if ready.cursor != n || !reflect.DeepEqual(ready.batches, want[1:]) {
			t.Fatal("ready owner group changed")
		}
		// Completing owner1 cannot satisfy the owner0 seal barrier.
		ready.ack <- nil
		ready.lease.release()
		select {
		case <-done:
			t.Fatal("seal ack associated with wrong owner")
		default:
		}
		if e.owners[0].queues[0].pop() != full {
			t.Fatal("full queue reordered")
		}
		var slow *corePacket
		for slow == nil && time.Now().Before(deadline) {
			slow = e.owners[0].queues[0].pop()
			if slow == nil {
				time.Sleep(time.Millisecond)
			}
		}
		if slow == nil || slow.cursor != n || !reflect.DeepEqual(slow.batches, want[:1]) {
			t.Fatal("slow owner group changed")
		}
		slow.ack <- nil
		slow.lease.release()
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("seal stalled")
		}
		// Caller storage may be reused only after submit returns; owners received copies.
		clear(want[0].Points)
		clear(want[1].Policy)
	}
	if got := e.routers[0].bytes.Load(); got != 0 {
		t.Fatal("lease byte leak", got)
	}
}

func TestCoreDispatchLeaseDepthFollowsBytes(t *testing.T) {
	cfg := coreTestConfig()
	cfg.Shards, cfg.Producers = 1, 1
	e := &coreEngine{cfg: cfg, routers: make([]coreRouter, 1), owners: []*coreOwner{{queues: []*coreQueue{newCoreQueue(coreQueueBytes)}, dead: make(chan struct{})}}}
	done := make(chan error, 1)
	go func() {
		for i := uint64(1); i <= 12; i++ {
			if err := e.submit(0, []row{{ID: i}}, i, false); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("fixed lease/slot count blocked byte-small packets")
	}
	for i := uint64(1); i <= 12; i++ {
		p := e.owners[0].queues[0].pop()
		if p == nil || p.cursor != i {
			t.Fatal("lease FIFO changed", i)
		}
		p.lease.release()
	}
	if e.routers[0].bytes.Load() != 0 {
		t.Fatal("lease byte credit lost")
	}
}
