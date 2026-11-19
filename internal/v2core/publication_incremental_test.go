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
	var published []coreBatch        // the reader keeps the latest publication
	o.batchHook = func(b []coreBatch, _ uint64) error {
		published = b
		return nil
	}
	seen := make([]uint8, head*epochs+1) // by order
	visibleCalls, lastCall, observed := 0, 0, 0
	o.visible = func(b []coreBatch) {
		visibleCalls++
		lastCall = len(b)
		observed += len(b)
		for i := range b {
			seen[b[i].Order]++
		}
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
	var stats runtime.MemStats
	maxBytes, maxAt := uint64(0), 0
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
			// A pure publication of the grown head: its cost must not grow with it.
			runtime.ReadMemStats(&stats)
			before := stats.TotalAlloc
			o.gcDirty.Store(true)
			poll()
			runtime.ReadMemStats(&stats)
			if b := stats.TotalAlloc - before; b > maxBytes {
				maxBytes, maxAt = b, len(published)
			}
			if len(published) != (i+1)*perPacket {
				t.Fatalf("epoch %d packet %d: published %d batches, want %d", epoch, i, len(published), (i+1)*perPacket)
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
	// A full-head clone costs head x sizeof(coreBatch), 5.8 MB here.
	clone := uint64(head) * uint64(unsafe.Sizeof(coreBatch{}))
	if limit := clone / 32; maxBytes > limit {
		t.Fatalf("a publication allocated %d bytes at %d published batches; limit %d (a full clone is %d)", maxBytes, maxAt, limit, clone)
	}
	t.Logf("max bytes per publication %d at %d batches (full clone %d)", maxBytes, maxAt, clone)
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
