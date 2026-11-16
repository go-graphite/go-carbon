package v2core

import (
	"bytes"
	"context"
	"math"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

func TestPortableRollupPartPublishedGCP3Compatibility(t *testing.T) {
	_, policy := rollupTestPolicy(t, "test.portable", "1s:1d,1m:30d", sealrollup.Last, .5)
	rows := []Row{{ID: 7, Time: 1700000000, Bits: 0x7ff8000000000042, Seq: 1}, {ID: 7, Time: 1700000001, Bits: 1 << 63, Seq: 2}, {ID: 7, Time: 1700000002, Bits: math.Float64bits(3), Seq: 3}}
	r := newCoreRollups()
	plan, err := r.plan(coreSealInput{Batches: []Batch{{ID: 7, Order: 1, Clock: 1700000003, Policy: policy, Points: rows}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Files) != 1 {
		t.Fatalf("files %d", len(plan.Files))
	}
	file := plan.Files[0]
	// Reproduce the published core's SST writer, without the new facade.
	legacy := filepath.Join(t.TempDir(), "published.sst")
	f, err := vfs.Default.Create(legacy)
	if err != nil {
		t.Fatal(err)
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{Compression: sstable.SnappyCompression})
	for _, kv := range file {
		if err = w.Set(kv.Key, kv.Value); err != nil {
			t.Fatal(err)
		}
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	check := func(path string, d *v2part.Descriptor) {
		t.Helper()
		i := 0
		err := v2part.Read(context.Background(), path, d, func(k v2part.Key, value []byte) error {
			key, e := v2part.Encode(k)
			if e != nil {
				return e
			}
			if i >= len(file) || !bytes.Equal(key, file[i].Key) || !bytes.Equal(value, file[i].Value) {
				t.Fatal("published key/value changed")
			}
			i++
			return nil
		})
		if err != nil || i != len(file) {
			t.Fatalf("read %d records: %v", i, err)
		}
	}
	check(legacy, nil)
	path := filepath.Join(t.TempDir(), "portable.sst")
	d, err := WritePortableRollupPart(path, file, v2part.Provenance{Incarnation: v2part.PartID{9}, Stream: "journal-0"})
	if err != nil {
		t.Fatal(err)
	}
	if d.Coverage.MinTime != rows[0].Time || d.Coverage.MaxTime != rows[len(rows)-1].Time || d.Coverage.Names != 1 || d.Coverage.Records != uint64(len(file)) || len(d.Coverage.Policies) != 2 {
		t.Fatalf("bad coverage %+v", d.Coverage)
	}
	check(path, &d)
	db, err := pebble.Open(filepath.Join(t.TempDir(), "different-owner"), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Ingest([]string{path}); err != nil {
		t.Fatal(err)
	}
	snap := db.NewSnapshot()
	defer snap.Close()
	names := 0
	if err = ScanRollups(snap, func(p RollupPolicy, m RollupMetadata, records []sealrollup.Record) error {
		names++
		if p.Name != "test.portable" || m.ID != 7 || m.Frontier.Order != 1 {
			t.Fatal("metadata changed")
		}
		found := map[int64]uint64{}
		for _, r := range records {
			if r.Archive == 0 {
				found[r.Sample.Time] = r.Sample.Bits
			}
		}
		for _, p := range rows {
			if bits, ok := found[p.Time]; !ok || bits != p.Bits {
				t.Fatalf("value bits changed at %d", p.Time)
			}
		}
		return nil
	}); err != nil || names != 1 {
		t.Fatalf("recovered %d names: %v", names, err)
	}
	for _, kv := range file {
		k, _ := v2part.Decode(kv.Key)
		if k.Archive == v2part.MetadataArchive {
			continue
		}
		if _, err := WritePortableRollupPart(filepath.Join(t.TempDir(), "bad.sst"), []KV{kv}, v2part.Provenance{Incarnation: v2part.PartID{9}, Stream: "journal-0"}); err == nil {
			t.Fatal("accepted archive without metadata")
		}
		break
	}
}
