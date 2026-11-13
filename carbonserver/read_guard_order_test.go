package carbonserver

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestQueuedReadDoesNotBlockPersist reproduces a production stall: every
// store read slot is busy, so a render read queues. Before the fix the queued
// read already held OriginalReadGuard, a stripe read lock, and the persister's
// SharedWriteGuard on the same metric waited for the read queue. The persister
// must get its stripe while the read is still queued.
func TestQueuedReadDoesNotBlockPersist(t *testing.T) {
	l, db, _, names, _ := newRangeScreenFixture(t, 60, 1<<20)
	// Hold every read slot, as readers parked on one block-cache shard did.
	held := fillReadSlots(t, db)
	defer func() {
		for _, release := range held {
			release()
		}
	}()

	read := make(chan error, 1)
	go func() {
		_, err := l.fetchFromDisk(names[0], rangeScreenNow-3600, rangeScreenNow)
		read <- err
	}()
	time.Sleep(100 * time.Millisecond) // let the read reach the slot queue
	select {
	case err := <-read:
		t.Fatalf("read finished without a free slot: %v", err)
	default:
	}

	persisted := make(chan struct{})
	go func() {
		release := db.SharedWriteGuard(names[0])
		release()
		close(persisted)
	}()
	select {
	case <-persisted:
	case <-time.After(2 * time.Second):
		t.Fatalf("persister write guard blocked behind a queued read (%d slots held)", len(held))
	}

	for _, release := range held {
		release()
	}
	held = nil
	select {
	case err := <-read:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("queued read did not finish after slots were released")
	}
}

// TestAdmittedReadReusesSlot checks that Fetch and Metadata with an admitted
// context do not wait for a second slot, and that a released admission no
// longer bypasses the queue.
func TestAdmittedReadReusesSlot(t *testing.T) {
	_, db, _, names, _ := newRangeScreenFixture(t, 60, 1<<20)
	ctx := context.Background()
	admittedCtx, release, err := db.AdmitRead(ctx, names[1])
	if err != nil {
		t.Fatal(err)
	}
	held := fillReadSlots(t, db)
	defer func() {
		release()
		for _, r := range held {
			r()
		}
	}()
	done := make(chan error, 1)
	go func() {
		if _, err := db.Fetch(admittedCtx, names[1], rangeScreenNow-3600, rangeScreenNow); err != nil {
			done <- err
			return
		}
		_, err := db.Metadata(admittedCtx, names[1])
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("admitted read waited for another slot")
	}
	release()
	held = append(held, fillReadSlots(t, db)...)
	blocked, cancel := context.WithTimeout(admittedCtx, 100*time.Millisecond)
	defer cancel()
	if _, err := db.Fetch(blocked, names[1], rangeScreenNow-3600, rangeScreenNow); err == nil {
		t.Fatal("released admission still bypassed the full read queue")
	}
}

// fillReadSlots takes every free store read slot through AdmitRead and returns
// their releases. The one admission left waiting is released when it lands.
func fillReadSlots(t *testing.T, db interface {
	AdmitRead(context.Context, string) (context.Context, func(), error)
}) []func() {
	t.Helper()
	var held []func()
	for i := 0; ; i++ {
		got := make(chan func(), 1)
		ctx, cancel := context.WithCancel(context.Background())
		name := fmt.Sprintf("holder.%d.%d", len(held), i)
		go func() {
			_, release, err := db.AdmitRead(ctx, name)
			if err != nil && ctx.Err() == nil {
				t.Error(err)
			}
			got <- func() { release(); cancel() }
		}()
		select {
		case release := <-got:
			held = append(held, release)
			continue
		case <-time.After(200 * time.Millisecond):
			// Cancel and join the probe before store cleanup; it owns no slot.
			cancel()
			(<-got)()
		}
		return held
	}
}
