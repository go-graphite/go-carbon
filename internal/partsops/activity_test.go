package partsops

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

func TestActivityPreservesExactMetadataAcrossReopenAndRecreate(t *testing.T) {
	dir := t.TempDir()
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	m := testMetric("service.activity")
	o, err := OpenCatalog(db, m.Scope)
	if err != nil {
		t.Fatal(err)
	}
	if err = o.Apply(context.Background(), []Metric{m}, nil); err != nil {
		t.Fatal(err)
	}
	stamp := time.Unix(1794700800, 987654321)
	a := Activity{Name: m.Metadata.Name, Generation: m.Metadata.Generation, Revision: 9, LastUpdate: stamp}
	edits, err := EncodeEdits(Batch{Scope: m.Scope, ExpectedRevision: 1, Revision: 2, Activity: []Activity{a}})
	if err != nil {
		t.Fatal(err)
	}
	batch := db.NewBatch()
	for _, e := range edits {
		if err = batch.Set(e.Key, e.Value, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err = batch.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
	_ = batch.Close()
	if err = db.Close(); err != nil {
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
	got := o.Snapshot().Metrics[0]
	if got.Metadata.Revision != 9 || !got.Metadata.LastUpdate.Equal(stamp) || got.Metadata.Generation != m.Metadata.Generation || got.Metadata.Retentions[0] != m.Metadata.Retentions[0] {
		t.Fatal("activity lost policy or exact arrival", got)
	}
	// A later full definition wins over an older retained activity record.
	m.Metadata.Revision = 10
	m.Metadata.LastUpdate = stamp.Add(time.Second)
	if err = o.Apply(context.Background(), []Metric{m}, nil); err != nil {
		t.Fatal(err)
	}
	o, err = OpenCatalog(db, m.Scope)
	if err != nil {
		t.Fatal(err)
	}
	if got := o.Snapshot().Metrics[0].Metadata; got.Revision != 10 || !got.LastUpdate.Equal(m.Metadata.LastUpdate) {
		t.Fatal("stale activity won", got)
	}
	d := Tombstone{Scope: m.Scope, Name: m.Metadata.Name, ThroughGeneration: m.Metadata.Generation, Revision: 10}
	if err = o.Apply(context.Background(), nil, []Tombstone{d}); err != nil {
		t.Fatal(err)
	}
	o, err = OpenCatalog(db, m.Scope)
	if err != nil {
		t.Fatal(err)
	}
	if len(o.Snapshot().Metrics) != 0 {
		t.Fatal("activity resurrected deletion")
	}
	m.Metadata.Generation++
	m.Metadata.Revision = 12
	if err = o.Apply(context.Background(), []Metric{m}, nil); err != nil {
		t.Fatal(err)
	}
	o, err = OpenCatalog(db, m.Scope)
	if err != nil {
		t.Fatal(err)
	}
	if got := o.Snapshot().Metrics[0].Metadata; got.Generation != m.Metadata.Generation || got.Revision != 12 {
		t.Fatal("old incarnation activity won", got)
	}
}

func TestActivityRejectsMissingDefinitionAndMalformedEncoding(t *testing.T) {
	m := testMetric("service.missing")
	a := Activity{Name: m.Metadata.Name, Generation: 1, Revision: 2, LastUpdate: time.Unix(1000, 1)}
	b, err := encodeActivity(m.Scope, a)
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]byte{nil, b[:29], append(append([]byte(nil), b...), 0)} {
		if _, err := decodeActivity(a.Name, bad); err == nil {
			t.Fatal("accepted malformed activity")
		}
	}
	db, err := pebble.Open(t.TempDir(), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Set(metadataKey(m.Scope, 'a', a.Name), b, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if _, err = OpenCatalog(db, m.Scope); err == nil {
		t.Fatal("accepted missing definition")
	}
}
