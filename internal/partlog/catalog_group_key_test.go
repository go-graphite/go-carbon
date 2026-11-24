package partlog

import (
	"testing"
	"unsafe"
)

func TestCatalogGroupKeyMatchesFrontCoding(t *testing.T) {
	for _, tc := range []struct {
		name, previous, suffix, definition string
		prefix                             int
		bound, borrowed                    bool
	}{
		{"plain", "metric.old", "new", `{"Name":"metric.new","Config":{}}`, 7, true, true},
		{"unicode", "metric.old", "é", `{"Name":"metric.é"}`, 7, true, true},
		{"first", "", "metric", `{"Name":"metric"}`, 0, true, true},
		{"prefix differs", "metric.old", "new", `{"Name":"metrix.new"}`, 7, true, false},
		{"suffix differs", "metric.old", "new", `{"Name":"metric.bad"}`, 7, true, false},
		{"short", "metric.old", "new", `{"Name":"x"}`, 7, true, false},
		{"delimiter", "metric.old", "new", `{"Name":"metric.newX}`, 7, true, false},
		{"opaque", "metric.old", "new", "opaque metadata", 7, true, false},
		{"escaped", "metric.old", `a"b`, `{"Name":"metric.a\"b"}`, 7, true, false},
		{"unbound", "metric.old", "new", `{"Name":"metric.new"}`, 7, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			definition := []byte(tc.definition)
			suffix := []byte(tc.suffix)
			want := tc.previous[:tc.prefix] + tc.suffix
			got := catalogGroupKey(tc.previous, tc.prefix, suffix, definition, tc.bound)
			if got != want {
				t.Fatalf("got %q, want %q", got, want)
			}
			at := unsafe.StringData(got)
			aliases := at == unsafe.SliceData(definition[len(definitionNamePrefix):])
			if aliases != tc.borrowed {
				t.Fatalf("borrowed=%v, want %v", aliases, tc.borrowed)
			}
			if !tc.borrowed {
				clear(definition)
				if got != want {
					t.Fatal("owned fallback name changed with definition")
				}
			}
		})
	}
}

func TestCatalogGroupKeyNoAllocation(t *testing.T) {
	definition := []byte(`{"Name":"metric.new","Config":{}}`)
	suffix := []byte("new")
	var result string
	allocs := testing.AllocsPerRun(100, func() {
		result = catalogGroupKey("metric.old", 7, suffix, definition, true)
	})
	if result != "metric.new" || allocs != 0 {
		t.Fatalf("name reconstruction: %g allocations, got %q", allocs, result)
	}
}
