package main

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/go-graphite/go-carbon/internal/statestore"
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

func TestPersistedCanonicalEscapesAndExactNumbers(t *testing.T) {
	for _, backend := range []string{"sqlite", "pebble"} {
		t.Run(backend, func(t *testing.T) {
			d, err := newDictionary()
			if err != nil {
				t.Fatal(err)
			}
			defer d.close()
			d.add([]catalogName{{"metric", 1, false}})
			s, err := statestore.Open(backend, filepath.Join(t.TempDir(), "state"))
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			for id, input := range []string{
				`{"$catalog_id":1}`,
				`{"$catalog_id":"metric"}`,
				`{"$catalog_object":{"$catalog_id":1}}`,
				`{"$catalog_object":{"$catalog_object":{"$catalog_id":"metric"}}}`,
				`{"known":"metric","unknown":"other","large":18446744073709551615,"negative":-9007199254740993,"null":null}`,
				`["metric",{"$catalog_id":9007199254740993},null,true,0]`,
			} {
				v, err := decode([]byte(input))
				if err != nil {
					t.Fatal(err)
				}
				want, _ := json.Marshal(v)
				b, err := d.canonical(v)
				if err != nil {
					t.Fatal(err)
				}
				if err = s.Apply([]statestore.Record{{Bucket: "json", ID: uint64(id), Payload: b}}); err != nil {
					t.Fatal(err)
				}
				b, err = s.Get("json", uint64(id))
				if err != nil {
					t.Fatal(err)
				}
				got, err := d.decodeCanonical(b)
				if err != nil {
					t.Fatal(err)
				}
				raw, _ := json.Marshal(got)
				if !bytes.Equal(raw, want) {
					t.Fatalf("%s != %s", raw, want)
				}
			}
		})
	}
}

func TestCanonicalRejectsUnknownVersionAndInvalidIDs(t *testing.T) {
	d, err := newDictionary()
	if err != nil {
		t.Fatal(err)
	}
	defer d.close()
	d.add([]catalogName{{"metric", 1, false}})
	for _, raw := range []string{
		`{"version":2,"value":"metric"}`,
		`{"version":1}`,
		`{"version":1,"value":{"$catalog_id":0}}`,
		`{"version":1,"value":{"$catalog_id":2}}`,
		`{"version":1,"value":{"$catalog_id":-1}}`,
		`{"version":1,"value":{"$catalog_id":1.1}}`,
		`{"version":1,"value":{"$catalog_object":null}}`,
	} {
		if _, err := d.decodeCanonical([]byte(raw)); err == nil {
			t.Fatalf("accepted %s", raw)
		}
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

func TestCatalogHexMask(t *testing.T) {
	n, e := parseCatalog("metric\t3f\t0")
	if e != nil || n.Sources != 63 || n.Target {
		t.Fatal(n, e)
	}
}
