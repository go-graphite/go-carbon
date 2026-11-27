package v2core

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

// This trace also runs unchanged against the base revision. An attachment
// fence must not invalidate ordinary proposals or change merge selection.
func TestNoMoveMergePublication(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	e := mergeFixtureIdle(t, t.TempDir(), nil)
	defer e.close()
	o := e.owners[0]
	for n := uint64(1); n <= 32; n++ {
		mergeAppend(t, e, n)
		if n%4 != 0 {
			continue
		}
		pin := o.db.NewSnapshot()
		selected, err := o.selectMerge(pin)
		if err != nil {
			t.Fatal(err)
		}
		o.gc.mu.Lock()
		pendingBefore := len(o.gc.pending)
		o.gc.mu.Unlock()
		started := time.Now()
		changed, err := o.mergeSealedParts(nil, false)
		finished := time.Now()
		if err != nil {
			t.Fatal(err)
		}
		if len(selected) > 0 && !changed {
			t.Fatal("ordinary merge proposal invalidated")
		}
		// Every selected input gets this publication's synced retirement fence.
		// No unselected proposal output may add another pending file.
		o.gc.mu.Lock()
		if got := len(o.gc.pending); got != pendingBefore+len(selected) {
			t.Errorf("pending=%d want=%d", got, pendingBefore+len(selected))
		}
		var fence uint64
		for _, input := range selected {
			pending, ok := o.gc.pending[input.SHA256]
			if !ok || pending.at.Before(started) || pending.at.After(finished) || pending.sequence == 0 {
				t.Errorf("input %x has invalid publication retirement: %+v, present=%v", input.SHA256, pending, ok)
			}
			if fence != 0 && pending.sequence != fence {
				t.Error("one publication used different retirement fences")
			}
			fence = pending.sequence
			if _, live := o.gc.inventory.bindings[string(partBinding(sealedPartPrefix, input.SHA256))]; live {
				t.Error("retired input remains live")
			}
		}
		o.gc.mu.Unlock()
		pin.Close()
		if epoch := reflect.ValueOf(o.gc).Elem().FieldByName("moveEpoch"); epoch.IsValid() && epoch.Uint() != 0 {
			t.Fatal("ordinary publication advanced attachment epoch")
		}
		scope := "none"
		if len(selected) > 0 {
			scope = fmt.Sprintf("%s/%d/%d", selected[0].Scope.Group, selected[0].Scope.VShard, selected[0].Scope.Partition)
		}
		t.Logf("MERGE_TRACE seal=%d scope=%s selected=%d published=%v invalidated=0 live=%d", n, scope, len(selected), changed, mergeCount(o))
	}
}
