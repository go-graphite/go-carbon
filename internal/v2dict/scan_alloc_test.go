//go:build !race

package v2dict

import "testing"

func TestScanWithoutLevelsMatchesIteratorAllocations(t *testing.T) {
	d, err := Open(t.TempDir(), Options{NoMerge: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	callback := func([]byte, Entry) bool { return true }
	check := func(want int) {
		t.Helper()
		seen := 0
		d.Scan("g", 0, nil, func(name []byte, e Entry) bool {
			if string(name) != "name" || e.ID != 0 {
				t.Fatalf("unexpected entry %q: %+v", name, e)
			}
			seen++
			return true
		})
		if seen != want {
			t.Fatalf("got %d names, want %d", seen, want)
		}
		scan := testing.AllocsPerRun(1000, func() { d.Scan("g", 0, nil, callback) })
		iter := testing.AllocsPerRun(1000, func() {
			it := d.NewIter("g", 0)
			for ok := it.SeekGE(nil); ok; ok = it.Next() {
				callback(it.Name(), it.Entry())
			}
		})
		if scan != iter {
			t.Fatalf("scan without levels allocated %g objects; iterator allocated %g", scan, iter)
		}
	}
	check(0)
	if _, _, err := d.Append("g", 0, []byte("name"), 0); err != nil {
		t.Fatal(err)
	}
	check(1)
}
