package v2core

import (
	"fmt"
	"reflect"
	"testing"
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
		pin.Close()
		changed, err := o.mergeSealedParts(nil, false)
		if err != nil {
			t.Fatal(err)
		}
		if len(selected) > 0 && !changed {
			t.Fatal("ordinary merge proposal invalidated")
		}
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
