package v2core

import (
	"runtime"
	"testing"
	"time"
	"unsafe"

	"github.com/cockroachdb/pebble"
)

// TestCorePublicationCopiesOnlyNewBatches checks that a pending publication
// costs O(batches added since the previous one), not O(head). Each pure
// publication allocates a bounded number of bytes however large the head
// grows, and the visible observer sees each batch exactly once across
// publications, synchronous seals and background seals. A seal repeats its
// whole set only after an expiry, so observers can re-add an expired name.
func TestCorePublicationCopiesOnlyNewBatches(t *testing.T) {
	t.Run("allocation", testPendingPublicationAllocation)
	t.Run("visibility", testCorePublicationVisibility)
}

// Background merges allocate independently of publication. Measure the same
// immutable-view publisher in isolation, joining the worker outside each
// TotalAlloc interval. The owner/observer/seal integration is checked below.
func testPendingPublicationAllocation(t *testing.T) {
	const perPacket, packets = 256, 256
	const head = perPacket * packets
	lookup := func(uint64) *ApplicationPolicy { return nil }
	packet := make([]coreBatch, perPacket)
	for i := range packet {
		packet[i] = coreBatch{ID: uint64(i%17 + 1), Order: uint64(i + 1), Clock: 1,
			Points: []row{{ID: uint64(i%17 + 1), Time: 1000, Seq: uint64(i + 1)}}}
	}
	maxBytes, maxAt := uint64(0), 0
	for _, compact := range []bool{false, true} {
		var h pendingHead
		var c pendingCompaction
		for range packets {
			h.add(packet, lookup)
			var stats runtime.MemStats
			runtime.ReadMemStats(&stats)
			before := stats.TotalAlloc
			v := h.view(lookup)
			runtime.ReadMemStats(&stats)
			if n := stats.TotalAlloc - before; n > maxBytes {
				maxBytes, maxAt = n, v.Len()
			}
			if compact {
				c.start(&h)
				c.wait(&h)
			}
		}
	}
	clone := uint64(head) * uint64(unsafe.Sizeof(coreBatch{}))
	if limit := clone / 32; maxBytes > limit {
		t.Fatalf("a publication allocated %d bytes at %d published batches; limit %d (a full clone is %d)", maxBytes, maxAt, limit, clone)
	}
	t.Logf("max bytes per publication %d at %d batches (full clone %d)", maxBytes, maxAt, clone)
}

func testCorePublicationVisibility(t *testing.T) {
	dir := t.TempDir()
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := coreConfig{Producers: 1, Shards: 1, MaxRows: 1 << 22, PartitionSeconds: 60, SealInterval: time.Hour}
	o := &coreOwner{db: db, dir: dir, cfg: cfg, sealRows: 1 << 22}
	o.meta = coreFrontier{Version: 1, Config: cfg, Cursors: []uint64{0}, ExpiredBefore: -1 << 63}
	o.publishFrontier(o.meta)
	defer func() { o.pub.clear(); db.Close() }()

	const perPacket, packets, epochs = 256, 256, 4
	const head = perPacket * packets // batches per seal
	// The reader keeps the latest publication. Every application here holds
	// one point: positions count batches.
	var published PendingView
	o.batchHook = func(pv PendingView, _ uint64) error {
		published = pv
		return nil
	}
	seen := make([]uint8, head*epochs+1) // by order
	visibleCalls, lastCall, observed := 0, 0, 0
	o.visible = func(pv PendingView) {
		visibleCalls++
		lastCall = 0
		pv.Each(func(a *PendingApplication) bool {
			lastCall++
			seen[a.Order]++
			return true
		})
		observed += lastCall
	}
	q := newCoreQueue(coreQueueBytes)
	o.queues = []*coreQueue{q}
	poll := o.poller()

	point := []row{{ID: 1, Time: 1000, Bits: 1, Seq: 1}}
	rows := make([]row, perPacket)
	for i := range rows {
		rows[i] = point[0]
	}
	var cursor, order uint64
	push := func() {
		cursor++
		batches := make([]coreBatch, perPacket)
		for i := range batches {
			order++
			batches[i] = coreBatch{ID: 1, Order: order, Clock: int64(order), Points: point}
		}
		if !q.tryPush(&corePacket{rows: rows, batches: batches, cursor: cursor}) {
			t.Fatal("queue full")
		}
		poll()
	}
	awaitSeals := func(n uint64) {
		for deadline := time.Now().Add(30 * time.Second); o.parts.Load() < n; time.Sleep(time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatalf("seal %d did not complete", n)
			}
			poll()
		}
	}
	for epoch := range epochs {
		async := epoch%2 == 1
		if async {
			// The last packet reaches the row threshold: a background seal.
			o.sealRows = head
		} else {
			o.sealRows = 1 << 22
		}
		for i := range packets {
			push()
			if async && i == packets-1 {
				awaitSeals(uint64(epoch + 1))
				break
			}
			o.gcDirty.Store(true)
			poll()
			if published.Len() != (i+1)*perPacket {
				t.Fatalf("epoch %d packet %d: published %d batches, want %d", epoch, i, published.Len(), (i+1)*perPacket)
			}
			if lastCall > perPacket || observed != int(order)+head*min(1, epoch/3) {
				t.Fatalf("epoch %d packet %d: observer got %d batches in its last call (want at most the %d new ones), %d in all of %d", epoch, i, lastCall, perPacket, observed, order)
			}
		}
		if !async {
			if epoch == 2 {
				// An expiry while the head was pending: the seal repeats its set.
				o.expiries.Add(1)
			}
			calls := visibleCalls
			ack := make(chan error, 1)
			if !q.tryPush(&corePacket{cursor: cursor, seal: true, ack: ack}) {
				t.Fatal("queue full")
			}
			poll()
			if err := <-ack; err != nil {
				t.Fatal(err)
			}
			if epoch == 2 && (visibleCalls == calls || lastCall != head) {
				t.Fatalf("seal after an expiry repeated %d batches, want %d", lastCall, head)
			}
		}
	}
	if got := o.committed.Load(); got != uint64(head*epochs) {
		t.Fatalf("committed %d rows, want %d", got, head*epochs)
	}
	for ord := 1; ord <= head*epochs; ord++ {
		want := uint8(1)
		if (ord-1)/head == 2 {
			want = 2
		}
		if seen[ord] != want {
			t.Fatalf("order %d reached the observer %d times, want %d", ord, seen[ord], want)
		}
	}

}

// TestCoreSealDeadlinesStaggerByOwner checks that owners which start together
// spread their first time-based seal across one interval, by owner index.
func TestCoreSealDeadlinesStaggerByOwner(t *testing.T) {
	const shards, interval = 4, 8 * time.Second
	cfg := coreConfig{Producers: 1, Shards: shards, MaxRows: 1 << 10, PartitionSeconds: 60, SealInterval: interval}
	for index := range shards {
		o := &coreOwner{cfg: cfg, sealRows: 1 << 10, index: index, queues: []*coreQueue{newCoreQueue(coreQueueBytes)}}
		o.meta = coreFrontier{Cursors: []uint64{0}}
		start := time.Now()
		o.poller()
		want := interval - interval*time.Duration(index)/shards
		if got := o.nextPoll.Sub(start); got < want-time.Second/2 || got > want+time.Second/2 {
			t.Fatalf("owner %d: first seal deadline in %v, want %v", index, got, want)
		}
	}
}

// Pending visibility has its own sub-second budget; a long seal interval
// must not make the second admitted application wait for the next seal.
func TestCorePublicationDefaultDeadline(t *testing.T) {
	dir := t.TempDir()
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := coreConfig{Producers: 1, Shards: 1, MaxRows: 1 << 20, PartitionSeconds: 60, SealInterval: time.Hour}
	o := &coreOwner{db: db, dir: dir, cfg: cfg, sealRows: 1 << 20}
	o.meta = coreFrontier{Version: 1, Config: cfg, Cursors: []uint64{0}}
	o.publishFrontier(o.meta)
	defer func() { o.pub.clear(); db.Close() }()
	var visible uint64
	o.batchHook = func(PendingView, uint64) error { return nil }
	o.visible = func(v PendingView) {
		v.Each(func(a *PendingApplication) bool { visible = max(visible, a.Order); return true })
	}
	q := newCoreQueue(coreQueueBytes)
	o.queues = []*coreQueue{q}
	poll := o.poller()
	push := func(order uint64) {
		rows := []row{{ID: 1, Time: int64(order), Bits: 1, Seq: order}}
		if !q.tryPush(&corePacket{rows: rows, batches: []coreBatch{{ID: 1, Order: order, Clock: 1700000000, Points: rows}}, cursor: order}) {
			t.Fatal("queue full")
		}
		poll()
	}
	push(1)
	if visible != 1 {
		t.Fatalf("first publication: %d", visible)
	}
	push(2)
	if visible != 1 {
		t.Fatal("publication no longer batches intake")
	}
	time.Sleep(600 * time.Millisecond)
	poll()
	if visible != 2 {
		t.Fatalf("second application still invisible after the sub-second publication budget: %d", visible)
	}
	if o.parts.Load() != 0 {
		t.Fatal("visibility required a seal")
	}
}
