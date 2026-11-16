package v2backend

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"
)

func TestCatalogSegmentsPreserveFrozenViewsAndIDs(t *testing.T) {
	o := &owner{metrics: make(map[string]*metricState)}
	saved := make(map[*catalog]map[string]uint64)
	for generation := 0; generation < 12; generation++ {
		for i := 0; i <= generation; i++ {
			name := fmt.Sprintf("root.host%d.metric%d", generation%3, generation*100+i)
			o.metrics[name] = &metricState{Policy: simplePolicy()}
		}
		c, err := o.publishCatalog()
		if err != nil {
			t.Fatal(err)
		}
		for old, want := range saved {
			for name, id := range want {
				m, ok := c.Lookup(name)
				if !ok || m.ID != id {
					t.Fatalf("unstable id %q", name)
				}
				m, ok = old.Lookup(name)
				if !ok || m.ID != id {
					t.Fatal("old view mutated")
				}
			}
			if old.count == c.count {
				t.Fatal("new names not counted")
			}
			var oldNames []string
			if err := old.Find(context.Background(), "root.*.*", func(n string) error { oldNames = append(oldNames, n); return nil }); err != nil {
				t.Fatal(err)
			}
			if len(oldNames) != len(want) {
				t.Fatal("new names leaked into frozen view")
			}
		}
		ids := make(map[string]uint64)
		var wantNames []string
		for name := range o.metrics {
			m, ok := c.Lookup(name)
			if !ok {
				t.Fatal(name)
			}
			ids[name] = m.ID
			wantNames = append(wantNames, name)
			if got, ok := c.nameForID(m.ID); !ok || got != name {
				t.Fatal("id lookup")
			}
		}
		var names []string
		if err := c.Find(context.Background(), "root.*.*", func(n string) error { names = append(names, n); return nil }); err != nil {
			t.Fatal(err)
		}
		sort.Strings(names)
		sort.Strings(wantNames)
		if !reflect.DeepEqual(names, wantNames) {
			t.Fatal("segmented find changed leaf names")
		}
		saved[c] = ids
		o.readCatalog.Store(c)
		same, err := o.publishCatalog()
		if err != nil || same != c {
			t.Fatal("point-only seal rebuilt catalog", err)
		}
	}
}
func BenchmarkCatalogPointOnlyPublication(b *testing.B) {
	o := &owner{metrics: make(map[string]*metricState)}
	for i := 0; i < 10000; i++ {
		o.metrics[fmt.Sprintf("root.host%d.metric", i)] = &metricState{Policy: simplePolicy()}
	}
	c, err := o.publishCatalog()
	if err != nil {
		b.Fatal(err)
	}
	o.readCatalog.Store(c)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := o.publishCatalog(); err != nil {
			b.Fatal(err)
		}
	}
}
