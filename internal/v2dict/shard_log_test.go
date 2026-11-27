package v2dict

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestShardAppendTakesNoStoreLock: an append below shardLogBytes keeps its
// log record in its shard, so producers admitting new names never queue on
// the store-wide lock (first-sight admission of 10M names, one Append each,
// was serialized on it).
func TestShardAppendTakesNoStoreLock(t *testing.T) {
	d, err := Open(t.TempDir(), Options{Shards: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	d.mu.Lock()
	done := make(chan error, 1)
	go func() {
		_, _, err := d.Append("g", 1, []byte("held.name"), 0)
		done <- err
	}()
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		d.mu.Unlock()
		<-done
		t.Fatal("Append waited on the store-wide lock")
	}
	d.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
}

// TestSyncLogsShardRecords: Sync makes the records shards keep durable. The
// directory as Sync left it (a kill right after) reopens with every name.
func TestSyncLogsShardRecords(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, Options{Shards: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	ids := map[string]uint32{}
	for v := range uint16(8) {
		name := fmt.Sprintf("shard.log.v%d", v)
		e, _, err := d.Append("g", v, []byte(name), 0)
		if err != nil {
			t.Fatal(err)
		}
		ids[name] = e.ID
	}
	before := d.Generation()
	if err = d.Sync(); err != nil {
		t.Fatal(err)
	}
	if d.Generation() == before {
		t.Fatal("Sync left the generation unchanged after appends")
	}
	killed := t.TempDir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(killed, e.Name()), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	r, err := Open(killed, Options{Shards: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for v := range uint16(8) {
		name := fmt.Sprintf("shard.log.v%d", v)
		e, ok := r.Lookup("g", v, []byte(name))
		if !ok || e.ID != ids[name] {
			t.Fatalf("%s after a kill past Sync: %+v %v, want ID %d", name, e, ok, ids[name])
		}
	}
}

// TestKnownPolicyTakesNoLock: every admitted name interns its policy and
// group; a known one is a lock-free map read, not a store-wide lock.
func TestKnownPolicyTakesNoLock(t *testing.T) {
	d, err := Open(t.TempDir(), Options{Shards: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	want, err := d.InternPolicy([]byte(`,"p":1}`))
	if err != nil {
		t.Fatal(err)
	}
	d.pol.mu.Lock()
	done := make(chan uint16, 1)
	go func() {
		i, _ := d.InternPolicy([]byte(`,"p":1}`))
		done <- i
	}()
	var got uint16
	select {
	case got = <-done:
	case <-time.After(5 * time.Second):
		d.pol.mu.Unlock()
		<-done
		t.Fatal("a known policy waited on the table lock")
	}
	d.pol.mu.Unlock()
	if got != want {
		t.Fatalf("known policy index %d, want %d", got, want)
	}
}
