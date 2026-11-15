package partsops

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/partsformat"
)

func TestPartsCatalogDurableReopen(t *testing.T) {
	dir := t.TempDir()
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	m := testMetric("service.durable")
	m.Metadata.Continuation = true
	m.Metadata.FileMigration = store.FileCleanupPending
	o, err := OpenCatalog(db, m.Scope)
	if err != nil {
		t.Fatal(err)
	}
	if err := o.Apply(context.Background(), []Metric{m}, nil); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	o, err = OpenCatalog(db, m.Scope)
	if err != nil {
		t.Fatal(err)
	}
	got := o.Snapshot().Metrics[0].Metadata
	if got.Revision != m.Metadata.Revision || !got.LastUpdate.Equal(m.Metadata.LastUpdate) || !got.Continuation || got.FileMigration != m.Metadata.FileMigration {
		t.Fatal("lost activity metadata", got)
	}
	plan, err := o.Expire(context.Background(), ExpirationPolicy{Fallback: time.Hour}, time.Unix(10000, 0), nil)
	if err != nil || len(plan.Delete) != 1 {
		t.Fatal(plan, err)
	}
	restored, err := OpenCatalog(db, m.Scope)
	if err != nil {
		t.Fatal(err)
	}
	if len(restored.Snapshot().Metrics) != 0 || len(restored.Snapshot().Tombstones) != 1 {
		t.Fatal(restored.Snapshot())
	}
	if err := restored.Apply(context.Background(), []Metric{m}, nil); !errors.Is(err, ErrConflict) {
		t.Fatal("resurrected old part", err)
	}
}

func TestPartsRetentionDropsOnlyWholeScopedPartition(t *testing.T) {
	db, err := pebble.Open(t.TempDir(), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	m := testMetric("service.retention")
	o, err := OpenCatalog(db, m.Scope)
	if err != nil {
		t.Fatal(err)
	}
	key := partsformat.Key{Group: m.Scope.PlacementGroup, VShard: m.Scope.VShard, Partition: 100, Metric: partsformat.ID(m.Metadata.Name), BlockStart: 100}
	old, _ := partsformat.Encode(key)
	key.Partition = 200
	live, _ := partsformat.Encode(key)
	key.Partition = 100
	key.Group = "foreign"
	foreign, _ := partsformat.Encode(key)
	payload := []byte{0x7f, 0xf8, 0, 0, 0, 0, 0, 7} // occupied NaN payload, never a deletion marker
	for _, k := range [][]byte{old, live, foreign} {
		if err := db.Set(k, payload, pebble.Sync); err != nil {
			t.Fatal(err)
		}
	}
	pin := db.NewSnapshot()
	defer pin.Close()
	part := Partition{Scope: m.Scope, ID: "old", Key: 100, Start: 100, End: 200, LongestSeconds: 100, Sealed: true}
	if drops, err := o.Reclaim(context.Background(), []Partition{part}, time.Unix(300, 0)); err != nil || len(drops) != 1 {
		t.Fatal(drops, err)
	}
	if _, closer, err := db.Get(old); !errors.Is(err, pebble.ErrNotFound) {
		if closer != nil {
			closer.Close()
		}
		t.Fatal("old partition still visible", err)
	}
	for _, k := range [][]byte{live, foreign} {
		v, c, e := db.Get(k)
		if e != nil {
			t.Fatal(e)
		}
		if !bytes.Equal(v, payload) {
			t.Fatal("unrelated part modified")
		}
		c.Close()
	}
	v, c, err := pin.Get(old)
	if err != nil {
		t.Fatal("pinned reader lost old part", err)
	}
	if !bytes.Equal(v, payload) {
		t.Fatal(v)
	}
	c.Close()
	restored, err := OpenCatalog(db, m.Scope)
	if err != nil || len(restored.Snapshot().DroppedPartitions) != 1 {
		t.Fatal("retirement record missing", err)
	}
}

func TestPartsCatalogCrashRecovery(t *testing.T) {
	m := testMetric("service.crash")
	if dir := os.Getenv("PARTSOPS_CRASH_CHILD"); dir != "" {
		db, err := pebble.Open(dir, &pebble.Options{})
		if err != nil {
			t.Fatal(err)
		}
		o, err := OpenCatalog(db, m.Scope)
		if err != nil {
			t.Fatal(err)
		}
		if err := o.Apply(context.Background(), []Metric{m}, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := o.Expire(context.Background(), ExpirationPolicy{Fallback: time.Hour}, time.Unix(10000, 0), nil); err != nil {
			t.Fatal(err)
		}
		os.Exit(0) // abrupt process exit: no Close/flush/deferred cleanup
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestPartsCatalogCrashRecovery$")
	cmd.Env = append(os.Environ(), "PARTSOPS_CRASH_CHILD="+dir)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("child: %v %s", err, output)
	}
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	o, err := OpenCatalog(db, m.Scope)
	if err != nil {
		t.Fatal(err)
	}
	s := o.Snapshot()
	if s.Revision != 2 || len(s.Metrics) != 0 || len(s.Tombstones) != 1 {
		t.Fatal("torn catalog/tombstone commit", s)
	}
}
