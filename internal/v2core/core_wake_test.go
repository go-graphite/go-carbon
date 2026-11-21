package v2core

import (
	"slices"
	"testing"
	"time"
)

func TestCoreQueueReadyRearms(t *testing.T) {
	o := &coreOwner{}
	for range 96 {
		o.queues = append(o.queues, newCoreQueue(coreQueueBytes))
	}
	wake := make(chan struct{}, 1)
	o.initQueueSignals(wake)
	a, b, c := &corePacket{cursor: 1}, &corePacket{cursor: 2}, &corePacket{cursor: 3}
	if !o.queues[95].tryPush(a) || !o.queues[95].tryPush(b) || !o.queues[0].tryPush(c) {
		t.Fatal("push failed")
	}
	select {
	case <-wake:
	default:
		t.Fatal("missing owner notification")
	}
	if o.ready[0].Swap(0) != 1 || o.ready[1].Swap(0) != 1<<31 {
		t.Fatal("wrong ready producers")
	}
	if o.queues[95].pop() != a || o.ready[1].Swap(0) != 1<<31 {
		t.Fatal("queued successor lost readiness")
	}
	if o.queues[95].pop() != b || o.ready[1].Load() != 0 {
		t.Fatal("drained queue stayed ready")
	}
	if o.queues[0].pop() != c {
		t.Fatal("wrong packet")
	}
	if !o.queues[95].tryPush(a) {
		t.Fatal("push after idle failed")
	}
	select {
	case <-wake:
	default:
		t.Fatal("idle queue did not wake owner")
	}
}

// Drive real sleeping execution groups, the second ready word, FIFO completion,
// shutdown and recovery. A missing enqueue wake would stall until the seal timer.
func TestCoreWakeGroupedOwnersRestart(t *testing.T) {
	cfg := coreConfig{Shards: 6, Owners: 2, Producers: 96, MaxRows: 128, PartitionSeconds: 3600, AppendParts: true}
	root := t.TempDir()
	e, err := openCore(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if e != nil {
			e.close()
		}
	}()
	want := []row{{ID: 7, Time: 1000, Bits: 1 << 63, Seq: 1}, {ID: 7, Time: 999, Bits: 0x7ff8000000000042, Seq: 2}}
	for i, r := range want {
		time.Sleep(20 * time.Millisecond)
		done := make(chan error, 1)
		go func() { done <- e.submit(95, []row{r}, uint64(i+1), true) }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatal("sleeping owner missed work notification")
		}
	}
	if err = e.close(); err != nil {
		t.Fatal(err)
	}
	e = nil
	e, err = openCore(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	snap := e.owners[coreShard(7, cfg.Shards)].db.NewSnapshot()
	got, err := coreRead(snap, 7)
	snap.Close()
	slices.SortFunc(want, less)
	if err != nil || !slices.Equal(got, want) {
		t.Fatal("wake/restart changed durable rows", got, err)
	}
}
