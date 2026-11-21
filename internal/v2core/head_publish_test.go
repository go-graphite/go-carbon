package v2core

import (
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

// publishedBatch is a batch's content. Row segments decode fresh arenas on
// every read, so point addresses are not part of it.
type publishedBatch struct {
	id, order uint64
	n         int
	first     row
}

func describeBatches(b []coreBatch) []publishedBatch {
	out := make([]publishedBatch, len(b))
	for i := range b {
		out[i] = publishedBatch{id: b[i].ID, order: b[i].Order, n: len(b[i].Points), first: b[i].Points[0]}
	}
	return out
}

// checkPublished re-decodes a publication from the owner's shared arrays.
func checkPublished(v PendingView, want []publishedBatch) error {
	got := v.Batches()
	if len(got) != len(want) || cap(got) != len(got) {
		return fmt.Errorf("publication len/cap %d/%d, want %d", len(got), cap(got), len(want))
	}
	for i := range got {
		if describeBatches(got[i : i+1])[0] != want[i] {
			return fmt.Errorf("published batch %d mutated", i)
		}
	}
	return nil
}

// TestCorePendingPublicationSharesImmutablePrefix publishes pending heads while
// the owner keeps appending and sealing, and concurrent readers re-check every
// publication. Shared prefixes must never be mutated, each publication must
// extend its predecessor until a seal, and each seal must retire exactly the
// batches its final pre-seal publication exposed.
func TestCorePendingPublicationSharesImmutablePrefix(t *testing.T) {
	dir := t.TempDir()
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := coreConfig{Producers: 1, Shards: 1, MaxRows: 1 << 20, PartitionSeconds: 60, SealInterval: time.Hour}
	o := &coreOwner{db: db, dir: dir, cfg: cfg, sealRows: 1 << 20}
	o.meta = coreFrontier{Version: 1, Config: cfg, Cursors: []uint64{0}, ExpiredBefore: -1 << 63}
	o.publishFrontier(o.meta)
	defer func() { o.pub.clear(); db.Close() }()

	type publication struct {
		view  PendingView
		want  []publishedBatch
		seals uint64
	}
	var pubs []publication
	feed := make(chan publication, 1024)
	visible := map[uint64]int{}
	o.batchHook = func(pv PendingView, _ uint64) error {
		p := publication{view: pv, want: describeBatches(pv.Batches()), seals: o.parts.Load()}
		pubs = append(pubs, p)
		feed <- p
		return nil
	}
	o.visible = func(pv PendingView) {
		pv.Each(func(a *PendingApplication) bool {
			visible[a.Order]++
			return true
		})
	}
	stop := make(chan struct{})
	errs := make(chan error, 8)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var live []publication
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				case p := <-feed:
					mu.Lock()
					live = append(live, p)
					mu.Unlock()
				default:
				}
				mu.Lock()
				snapshot := slices.Clone(live)
				mu.Unlock()
				for _, p := range snapshot {
					if err := checkPublished(p.view, p.want); err != nil {
						errs <- err
						return
					}
				}
				time.Sleep(time.Millisecond)
			}
		})
	}

	q := newCoreQueue(coreQueueBytes)
	o.queues = []*coreQueue{q}
	poll := o.poller()
	var cursor, order, rows, seq uint64
	epochs := [][]uint64{nil}
	seal := func() {
		ack := make(chan error, 1)
		if !q.tryPush(&corePacket{cursor: cursor, seal: true, ack: ack}) {
			t.Fatal("queue full")
		}
		poll()
		if err := <-ack; err != nil {
			t.Fatal(err)
		}
		epochs = append(epochs, nil)
	}
	for i := 1; i <= 3000; i++ {
		cursor++
		var packetRows []row
		var batches []coreBatch
		for b := 0; b < 1+i%3; b++ {
			order++
			var points []row
			for k := 0; k < 1+(i+b)%4; k++ {
				seq++
				points = append(points, row{ID: uint64(1 + (i+b)%17), Time: int64(1000 + seq), Bits: seq * 7, Seq: seq})
			}
			packetRows = append(packetRows, points...)
			batches = append(batches, coreBatch{ID: points[0].ID, Order: order, Clock: int64(i), Points: points})
			epochs[len(epochs)-1] = append(epochs[len(epochs)-1], order)
		}
		rows += uint64(len(packetRows))
		if !q.tryPush(&corePacket{rows: packetRows, batches: batches, cursor: cursor}) {
			t.Fatal("queue full")
		}
		if i%5 == 0 {
			// Reuse the GC publication path: it republishes the pending head
			// without sealing, which the 1 s timer would make slow to test.
			o.gcDirty.Store(true)
		}
		poll()
		if i%400 == 0 {
			seal()
		}
	}
	seal()
	close(stop)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if got := o.committed.Load(); got != rows {
		t.Fatalf("committed %d rows, want %d", got, rows)
	}
	if f := o.frontier.Load().Cursors[0]; f != cursor {
		t.Fatalf("frontier %d, want %d", f, cursor)
	}
	for o := uint64(1); o <= order; o++ {
		if visible[o] == 0 {
			t.Fatalf("order %d never visible", o)
		}
	}
	// Every publication is still intact after all later appends and seals, and
	// extends its predecessor within one seal epoch.
	lastBySeal := map[uint64][]publishedBatch{}
	for i, p := range pubs {
		if err := checkPublished(p.view, p.want); err != nil {
			t.Fatal(i, err)
		}
		if i > 0 && pubs[i-1].seals == p.seals {
			prev := pubs[i-1].want
			if len(prev) > len(p.want) || !slices.Equal(prev, p.want[:len(prev)]) {
				t.Fatalf("publication %d does not extend its predecessor", i)
			}
		}
		if len(p.want) > 0 {
			lastBySeal[p.seals] = p.want
		}
	}
	for k, want := range epochs[:len(epochs)-1] {
		got := lastBySeal[uint64(k)]
		orders := make([]uint64, len(got))
		for i := range got {
			orders[i] = got[i].order
		}
		if !slices.Equal(orders, want) {
			t.Fatalf("seal %d retired %d batches, final publication exposed %d", k, len(want), len(orders))
		}
	}
}

// BenchmarkCorePendingPublication measures one pending publication against
// the size of the already-published head.
func BenchmarkCorePendingPublication(b *testing.B) {
	for _, head := range []int{1 << 10, 1 << 14, 1 << 17, 1 << 20} {
		b.Run(fmt.Sprintf("head=%d", head), func(b *testing.B) {
			dir := b.TempDir()
			db, err := pebble.Open(dir, &pebble.Options{})
			if err != nil {
				b.Fatal(err)
			}
			cfg := coreConfig{Producers: 1, Shards: 1, MaxRows: 1 << 22, PartitionSeconds: 60, SealInterval: time.Hour}
			o := &coreOwner{db: db, dir: dir, cfg: cfg, sealRows: 1 << 22}
			o.meta = coreFrontier{Version: 1, Config: cfg, Cursors: []uint64{0}, ExpiredBefore: -1 << 63}
			o.publishFrontier(o.meta)
			defer func() { o.pub.clear(); db.Close() }()
			var retained []coreBatch
			o.batchHook = func(pv PendingView, _ uint64) error {
				batches := pv.Batches()
				retained = batches
				return nil
			}
			q := newCoreQueue(coreQueueBytes)
			o.queues = []*coreQueue{q}
			poll := o.poller()
			point := []row{{ID: 1, Time: 1, Bits: 1, Seq: 1}}
			batches := make([]coreBatch, 1024)
			for i := range batches {
				batches[i] = coreBatch{ID: 1, Order: uint64(i + 1), Points: point}
			}
			cursor := uint64(0)
			push := func(n int) {
				cursor++
				if !q.tryPush(&corePacket{rows: point, batches: batches[:n], cursor: cursor}) {
					b.Fatal("queue full")
				}
				poll()
			}
			for n := 0; n < head; n += len(batches) {
				push(len(batches))
			}
			b.ResetTimer()
			for b.Loop() {
				push(1)
				o.gcDirty.Store(true)
				poll()
			}
			b.StopTimer()
			_ = retained
		})
	}
}
