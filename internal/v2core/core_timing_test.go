package v2core

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

func TestCoreSealTimingPreservesPublication(t *testing.T) {
	var mu sync.Mutex
	phases := map[string]time.Duration{}
	e, err := openCoreWithHooks(t.TempDir(), coreConfig{Shards: 1, Producers: 1, MaxRows: 16, PartitionSeconds: 3600}, coreHooks{SealTiming: func(phase string, d time.Duration) { mu.Lock(); defer mu.Unlock(); phases[phase] += d }})
	if err != nil {
		t.Fatal(err)
	}
	if err = e.submit(0, []row{{ID: 1, Time: 1, Bits: 42, Seq: 1}}, 1, true); err != nil {
		t.Fatal(err)
	}
	if e.durableFrontiers()[0][0] != 1 {
		t.Fatal("timing observer changed publication frontier")
	}
	if err = e.close(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if phases["seal_total"] <= 0 || phases["publication"] <= 0 || phases["seal_total"] < phases["rollup_plan"]+phases["publication"] {
		t.Fatalf("invalid phase durations: %v", phases)
	}
}

func TestCoreVisibleObserverCoversSealedAndPending(t *testing.T) {
	for _, maxRows := range []int{1, 100} {
		t.Run(fmt.Sprint(maxRows), func(t *testing.T) {
			cfg := coreTestConfig()
			cfg.Shards = 1
			cfg.Producers = 1
			cfg.MaxRows = maxRows
			var mu sync.Mutex
			publications := 0
			seen := map[uint64]uint64{}
			e, err := openCoreWithHooks(t.TempDir(), cfg, coreHooks{
				PublishBatches: func(_ int, pin *pebble.Snapshot, _ []coreBatch, _ uint64) error {
					defer pin.Close()
					mu.Lock()
					publications++
					mu.Unlock()
					return nil
				},
				Visible: func(_ int, batches []coreBatch) {
					mu.Lock()
					defer mu.Unlock()
					if publications == 0 {
						t.Error("observed before publication")
					}
					for _, b := range batches {
						seen[b.Order] = b.Points[0].Bits
					}
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			for i := uint64(1); i <= 2; i++ {
				if err = e.AppendBatches(0, []Batch{{ID: 1, Order: i, Points: []Row{{ID: 1, Bits: i, Seq: i}}}}, i, false); err != nil {
					t.Fatal(err)
				}
			}
			if err = e.Close(); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(seen) != 2 || seen[1] != 1 || seen[2] != 2 {
				t.Fatal(seen)
			}
		})
	}
}

// Pending publications share the owner's one-second cadence. Mandatory seal
// publication still happens immediately and contains every original batch.
func TestCorePendingPublicationCoalesces(t *testing.T) {
	dir := t.TempDir()
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := coreConfig{Producers: 1, Shards: 1, MaxRows: 2048, PartitionSeconds: 60}
	o := &coreOwner{db: db, dir: dir, cfg: cfg, sealRows: 2048}
	o.meta = coreFrontier{Version: 1, Config: cfg, Cursors: []uint64{0}, ExpiredBefore: -1 << 63}
	o.publishFrontier(o.meta)
	defer func() { o.pub.clear(); db.Close() }()
	calls := 0
	o.batchHook = func(_ PendingView, _ uint64) error { calls++; return nil }
	q := newCoreQueue(coreQueueBytes)
	o.queues = []*coreQueue{q}
	poll := o.poller()
	for i := uint64(1); i <= 32; i++ {
		point := row{ID: 1, Time: int64(i), Bits: i, Seq: i}
		if !q.tryPush(&corePacket{rows: []row{point}, batches: []coreBatch{{ID: 1, Order: i, Points: []row{point}}}, cursor: i}) {
			t.Fatal("queue full")
		}
		poll()
	}
	if calls != 1 {
		t.Fatal("republished growing head on every packet", calls)
	}
	ack := make(chan error, 1)
	if !q.tryPush(&corePacket{cursor: 32, seal: true, ack: ack}) {
		t.Fatal("queue full")
	}
	poll()
	if err = <-ack; err != nil {
		t.Fatal(err)
	}
	// The final tail is published before seal work, then the cleared head after it.
	if calls != 3 || o.frontier.Load().Cursors[0] != 32 {
		t.Fatal("seal did not publish every packet", calls)
	}
}
