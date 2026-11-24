package v2core

import (
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2part"
)

// Synced replacement plus snapshot/part references are sufficient to protect
// readers. Once those readers close, input copies need no wall-clock delay.
func TestSealedMergeReclaimsReleasedInputs(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	root := t.TempDir()
	e := mergeFixtureIdle(t, root, nil)
	defer func() {
		if e != nil {
			_ = e.close()
		}
	}()
	for order := uint64(1); order <= 64; order++ {
		mergeAppend(t, e, order)
	}
	o := e.owners[0]
	o.gc.mu.Lock()
	frozen := time.Now()
	o.gc.now = func() time.Time { return frozen }
	o.gc.mu.Unlock()
	pin := o.db.NewSnapshot()
	defer func() {
		if pin != nil {
			_ = pin.Close()
		}
	}()
	want := mergeRead(t, pin)
	if len(want) == 0 {
		t.Fatal("empty differential")
	}
	if mergeDrain(t, o) == 0 {
		t.Fatal("no replacement")
	}
	o.gc.mu.Lock()
	var inputs []v2part.Digest
	var lastFence uint64
	for id, p := range o.gc.pending {
		if p.sequence == 0 {
			o.gc.mu.Unlock()
			t.Fatal("runtime retirement has no fence")
		}
		inputs = append(inputs, id)
		lastFence = max(lastFence, p.sequence)
	}
	o.gc.mu.Unlock()
	if len(inputs) == 0 {
		t.Fatal("merge retired no inputs")
	}

	current := o.db.NewSnapshot()
	got := mergeRead(t, current)
	_ = current.Close()
	if !reflect.DeepEqual(got, want) {
		t.Fatal("replacement changed exact records")
	}
	if _, err := o.gc.sweep(); err != nil {
		t.Fatal(err)
	}
	for _, id := range inputs {
		if _, err := os.Stat(filepath.Join(o.gc.dir, hex.EncodeToString(id[:])+".sst")); err != nil {
			t.Fatalf("pinned input lost: %v", err)
		}
	}
	if !reflect.DeepEqual(mergeRead(t, pin), want) {
		t.Fatal("pinned generation changed")
	}
	_ = pin.Close()
	pin = nil
	// Wait for all old snapshot fences, including the owner's publication,
	// rather than its acknowledgement, which precedes snapshot replacement.
	deadline := time.Now().Add(10 * time.Second)
	for {
		snapshots := o.db.Metrics().Snapshots
		if snapshots.Count == 0 || snapshots.EarliestSeqNum > lastFence {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("retired generation still pinned by a snapshot")
		}
		time.Sleep(time.Millisecond)
	}
	// A sweep processes at most 64 retired files; drain that existing bound.
	for pass := 0; pass <= len(inputs)/64; pass++ {
		if _, err := o.gc.sweep(); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range inputs {
		if _, err := os.Stat(filepath.Join(o.gc.dir, hex.EncodeToString(id[:])+".sst")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("released input retained without readers: %v", err)
		}
	}
	if err := e.close(); err != nil {
		t.Fatal(err)
	}
	e = nil
	e = mergeFixtureIdle(t, root, nil)
	restarted := e.owners[0].db.NewSnapshot()
	defer restarted.Close()
	if !reflect.DeepEqual(mergeRead(t, restarted), want) {
		t.Fatal("restart changed exact records after input deletion")
	}
}
