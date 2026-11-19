package v2part

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func TestDensePlannedKeyRouteOracle(t *testing.T) {
	spec := fixtureSpec()
	spec.Scope.VShard = GroupVShard
	w, err := NewDenseWriter(filepath.Join(t.TempDir(), "part.sst"), spec)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	for i := range 4096 {
		name := ID(fmt.Sprintf("%s.%d", strings.Repeat("metric.", i%40), i))
		w.havePrev = false
		k, err := w.plannedKey(Route(name), uint32(i+1), name, 0, int64(i))
		if err != nil {
			t.Fatal(err)
		}
		want, err := Encode(k) // Checked format encoder rehashes in the test.
		if err != nil || !bytes.Equal(want, w.appendKey(nil, k)) {
			t.Fatalf("planned key differs from checked encoder: %v", err)
		}
		// A bad planner route must trip the checked oracle, even though the
		// production writer trusts the planner and avoids that second hash.
		k.VShard ^= 1
		if _, err := Encode(k); err == nil {
			t.Fatal("foreign-vshard mutation escaped the oracle")
		}
	}
}

func TestDensePlannedKeyBounds(t *testing.T) {
	for _, kind := range []string{"empty", "long", "vshard", "scope"} {
		t.Run(kind, func(t *testing.T) {
			spec := fixtureSpec()
			name := ID("metric.name")
			shard := Route(name)
			spec.Scope.VShard = shard
			switch kind {
			case "empty":
				name = ""
			case "long":
				name = ID(strings.Repeat("x", MaxNameBytes+1))
			case "vshard":
				shard = VirtualShards
			case "scope":
				shard ^= 1
			}
			w, err := NewDenseWriter(filepath.Join(t.TempDir(), "part.sst"), spec)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Abort()
			if _, err = w.plannedKey(shard, 1, name, 0, 0); err == nil {
				t.Fatal("accepted invalid planned key")
			}
		})
	}
}
