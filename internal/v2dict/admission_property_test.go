package v2dict

import (
	"fmt"
	"math/rand/v2"
	"testing"
)

// Recovered dictionaries can contain names whose part/catalog identity has not
// hydrated yet. A fresh in-memory caller must still obtain the persisted ID.
func TestAdmissionNeverDuplicatesExistingName(t *testing.T) {
	rng := rand.New(rand.NewPCG(827, 921))
	dir := t.TempDir()
	d, err := Open(dir, Options{NoMerge: true, BufferBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { d.Close() }()
	type identity struct {
		group string
		vs    uint16
		name  string
	}
	oracle := map[identity]Entry{}
	for step := 0; step < 6000; step++ {
		n := rng.IntN(400)
		k := identity{fmt.Sprintf("g%d", n%3), uint16(n % 8), fmt.Sprintf("metric.%04d", n)}
		old, exists := oracle[k]
		e, created, err := d.Append(k.group, k.vs, []byte(k.name), uint16(step%7))
		if err != nil {
			t.Fatal(err)
		}
		if exists && (created || e != old) {
			t.Fatalf("assigned a second ID to existing name: step=%d old=%+v new=%+v created=%v", step, old, e, created)
		}
		if !exists && !created {
			t.Fatal("new name not created")
		}
		oracle[k] = e
		if step%137 == 0 {
			if err = d.Flush(); err != nil {
				t.Fatal(err)
			}
		}
		if step%521 == 0 {
			if err = d.MergeAll(); err != nil {
				t.Fatal(err)
			}
		}
		if step%997 == 0 {
			if err = d.Close(); err != nil {
				t.Fatal(err)
			}
			d, err = Open(dir, Options{NoMerge: true, BufferBytes: 1 << 20})
			if err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestAdmissionDirectCompression(t *testing.T) {
	d, err := Open(t.TempDir(), Options{BufferBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for pass := range 4 {
		for i := range 100 {
			if _, _, err = d.Append("g", 0, fmt.Appendf(nil, "n%04d", pass*100+i), 0); err != nil {
				t.Fatal(err)
			}
		}
		if err = d.Flush(); err != nil {
			t.Fatal(err)
		}

	}
	d.mergeWG.Wait()
	s := d.snap.Load()
	if len(s.levels) != 1 || !s.levels[0].compact {
		t.Fatal("final run not compressed")
	}
	// Give a subsequent flush its own worker and prove the old run stays readable.
	if _, _, err = d.Append("g", 0, []byte("z"), 0); err != nil {
		t.Fatal(err)
	}
	if err = d.Flush(); err != nil {
		t.Fatal(err)
	}
	d.mergeWG.Wait()
	for i := range 400 {
		e, ok := d.Lookup("g", 0, fmt.Appendf(nil, "n%04d", i))
		if !ok || e.ID != uint32(i) {
			t.Fatal(i, e, ok)
		}
	}
}
