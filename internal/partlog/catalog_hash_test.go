package partlog

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"testing"
)

func TestCatalogIndexCollisionsAndGrowth(t *testing.T) {
	var index catalogIndex
	catalog := &NameCatalog{entries: []catalogEntry{{}}}
	// Force zero hashes, full-hash collisions, wraparound and repeated growth.
	for i := 1; i <= 4096; i++ {
		key := fmt.Sprintf("series.%d\x00", i)
		hash := []uint64{0, ^uint64(0), 7}[i%3]
		if index.lookup(key, hash, catalog) != 0 {
			t.Fatal("unseen name found", key)
		}
		catalog.entries = append(catalog.entries, catalogEntry{key: key})
		index.insert(hash, uint64(i))
	}
	for i := 1; i < len(catalog.entries); i++ {
		hash := []uint64{0, ^uint64(0), 7}[i%3]
		if id := index.lookup(catalog.entries[i].key, hash, catalog); id != uint64(i) {
			t.Fatal(i, id)
		}
	}
	if index.lookup("absent", 7, catalog) != 0 {
		t.Fatal("hash collision admitted as a name")
	}
}

func TestCatalogHashedRecoveryOracle(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	c, err := OpenNameCatalog(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	rng := rand.New(rand.NewSource(8182))
	want := make(map[string]uint64)
	for round := range 3 {
		for range 20000 {
			key := fmt.Sprintf("namespace.%d.metric.%d\x00", rng.Intn(7), rng.Intn(2000))
			id, err := c.InternHashed(key, []byte{0, 255}, NameHash(key))
			if err != nil {
				t.Fatal(err)
			}
			if old := want[key]; old != 0 && id != old {
				t.Fatal(key, old, id)
			}
			if want[key] == 0 {
				if id != uint64(len(want)+1) {
					t.Fatal("arrival IDs changed")
				}
				want[key] = id
			}
		}
		if round == 1 {
			err = c.Checkpoint()
		} else {
			err = c.Sync()
		}
		if err != nil {
			t.Fatal(err)
		}
		if err = c.Close(); err != nil {
			t.Fatal(err)
		}
		c, err = OpenNameCatalog(dir)
		if err != nil {
			t.Fatal(err)
		}
		for key, id := range want {
			got, err := c.Intern(key, []byte{0, 255})
			name, def, ok := c.Lookup(id)
			if err != nil || got != id || !ok || name != key || !bytes.Equal(def, []byte{0, 255}) {
				t.Fatal(key, id, got, err)
			}
		}
	}
	if _, err = c.Intern("new", nil); err != nil {
		t.Fatal(err)
	}
	for key := range want {
		if _, err = c.Intern(key, []byte{1}); err == nil {
			t.Fatal("changed definition accepted")
		}
		break
	}
}

func TestCatalogInternBatchOracle(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	c, err := OpenNameCatalog(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	rng := rand.New(rand.NewSource(19))
	want := make(map[string]uint64)
	for round := range 8 {
		keys := make([]string, 4097)
		for i := range keys {
			keys[i] = fmt.Sprintf("metric.%d\x00", rng.Intn(7000))
		}
		ids, err := c.InternBatch(keys, []byte{0, 255})
		if err != nil {
			t.Fatal(err)
		}
		for i, key := range keys {
			if want[key] == 0 {
				want[key] = uint64(len(want) + 1)
			}
			if ids[i] != want[key] {
				t.Fatal("batch changed arrival order", round, i, ids[i], want[key])
			}
			scalar, err := c.Intern(key, []byte{0, 255})
			if err != nil || scalar != ids[i] {
				t.Fatal("scalar/batch mismatch", key, err)
			}
		}
		if round%2 == 0 {
			err = c.Sync()
		} else {
			err = c.Checkpoint()
		}
		if err != nil {
			t.Fatal(err)
		}
		if err = c.Close(); err != nil {
			t.Fatal(err)
		}
		c, err = OpenNameCatalog(dir)
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.InternBatch([]string{"metric.0\x00"}, []byte{1}); err == nil {
		t.Fatal("changed batch definition accepted")
	}
	c.failed = errCatalog
	if _, err := c.InternBatch([]string{"other"}, nil); !errors.Is(err, errCatalog) {
		t.Fatal("sticky failure lost", err)
	}
	c.failed = nil
}
