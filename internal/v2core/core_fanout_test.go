package v2core

import (
	"testing"
	"time"
)

// A full queue must backpressure its producer without hiding the current
// packet from other ready owners. Their per-producer FIFO still cannot pass
// the current fanout, so cursors and original apply order remain unchanged.
func TestCoreFanoutOffersReadyOwnersBeforeWaiting(t *testing.T) {
	e := &coreEngine{cfg: coreConfig{Shards: 3, Producers: 1, MaxRows: 8}, routers: make([]coreRouter, 1)}
	for range 3 {
		e.owners = append(e.owners, &coreOwner{dead: make(chan struct{}), queues: []*coreQueue{newCoreQueue(2 * corePacketBytes)}})
	}
	q := e.owners[0].queues[0]
	if err := q.push(new(corePacket), e.owners[0].dead); err != nil {
		t.Fatal(err)
	}
	if err := q.push(new(corePacket), e.owners[0].dead); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- e.submit(0, []row{{ID: 7, Bits: 0x8000000000000000, Seq: 1}}, 1, false) }()
	defer func() {
		// Also unblock the producer when an assertion fails.
		q.pop()
		q.pop()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("fanout did not finish")
		}
	}()
	for _, owner := range []int{1, 2} {
		deadline := time.Now().Add(2 * time.Second)
		var p *corePacket
		for p == nil && time.Now().Before(deadline) {
			p = e.owners[owner].queues[0].pop()
			if p == nil {
				time.Sleep(time.Millisecond)
			}
		}
		if p == nil {
			t.Fatalf("ready owner %d hidden behind full owner 0", owner)
		}
		if p.cursor != 1 || p.lease == nil {
			t.Fatalf("owner %d invalid packet", owner)
		}
		p.lease.release()
	}
}
