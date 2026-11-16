package main

import (
	"reflect"
	"strings"
	"testing"
)

func TestCatalogRoundTrip(t *testing.T) {
	want := []catalogName{{"alpha.a", 1, false}, {"alpha.b", 7, true}, {"z", 1 << 62, false}}
	got, e := decodeNames(encodeNames(want))
	if e != nil || !reflect.DeepEqual(got, want) {
		t.Fatal(got, e)
	}
	d, e := newDictionary()
	if e != nil {
		t.Fatal(e)
	}
	defer d.close()
	d.add(want[:2])
	d.add(want[2:])
	for i, n := range want {
		id, ok, e := d.lookup(n.Name)
		if e != nil || !ok || id != uint64(i+1) {
			t.Fatal(id, ok, e)
		}
	}
	if _, ok, e := d.lookup("alpha.aa"); e != nil || ok {
		t.Fatal(ok, e)
	}
}
func TestCatalogCorrupt(t *testing.T) {
	for _, b := range [][]byte{{255}, {1, 0, 0, 0}, {0, 9}, {0, 0, 0, 2}} {
		if _, e := decodeNames(b); e == nil {
			t.Fatalf("accepted %x", b)
		}
	}
}

func TestCanonicalNoLoss(t *testing.T) {
	d, e := newDictionary()
	if e != nil {
		t.Fatal(e)
	}
	defer d.close()
	d.add([]catalogName{{"a.metric", 1, false}, {"z.metric", 2, true}})
	v, e := decode([]byte(`{"names":["a.metric","unknown.metric","z.metric"],"first":9007199254740993,"reason":"not known"}`))
	if e != nil {
		t.Fatal(e)
	}
	b, e := d.canonical(v)
	if e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(string(b), `"$catalog_id":1`) || !strings.Contains(string(b), `9007199254740993`) {
		t.Fatal(string(b))
	}
}
