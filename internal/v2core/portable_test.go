package v2core

import (
	"bytes"
	"context"
	"encoding/json"
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
	defer plan.Abort()
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
		if err = w.Set(kv.Key, kv.encodedValue()); err != nil {
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
			if i >= len(file) || !bytes.Equal(key, file[i].Key) || !bytes.Equal(value, file[i].encodedValue()) {
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

func TestPortableDenseRollupPartPublishedGCP3Compatibility(t *testing.T) {
	_, policy := rollupTestPolicy(t, "test.portable", "1s:1d,1m:30d", sealrollup.Last, .5)
	rows := []Row{{ID: 7, Time: 1700000000, Bits: 0x7ff8000000000042, Seq: 1}, {ID: 7, Time: 1700000001, Bits: 1 << 63, Seq: 2}, {ID: 7, Time: 1700000002, Bits: math.Float64bits(3), Seq: 3}}
	r := newCoreRollups()
	plan, err := r.plan(coreSealInput{Batches: []Batch{{ID: 7, Order: 1, Clock: 1700000003, Policy: policy, Points: rows}}})
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Abort()
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
		if err = w.Set(kv.Key, kv.encodedValue()); err != nil {
			t.Fatal(err)
		}
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	check := func(path string, d *v2part.Descriptor) {
		t.Helper()
		i := 0
		err := v2part.ReadExpanded(context.Background(), path, d, func(k v2part.Key, value []byte) error {
			key, e := v2part.Encode(k)
			if e != nil {
				return e
			}
			if i >= len(file) || !bytes.Equal(key, file[i].Key) || !bytes.Equal(value, file[i].encodedValue()) {
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
	d, err := WriteDenseRollupPart(path, file, v2part.Provenance{Incarnation: v2part.PartID{9}, Stream: "journal-0"})
	if err != nil {
		t.Fatal(err)
	}
	if d.Coverage.MinTime != rows[0].Time || d.Coverage.MaxTime != rows[len(rows)-1].Time || d.Coverage.Names != 1 || d.Coverage.Records >= uint64(len(file)) || len(d.Coverage.Policies) != 2 {
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

func TestPortableDenseNewestSingletonWins(t *testing.T) {
	_, policy := rollupTestPolicy(t, "test.overlay", "1s:1d", sealrollup.Last, .5)
	plan, e := newCoreRollups().plan(coreSealInput{Batches: []Batch{{ID: 7, Order: 1, Clock: 1700000002, Policy: policy, Points: []Row{{ID: 7, Time: 1700000000, Bits: 1 << 63, Seq: 1}, {ID: 7, Time: 1700000001, Bits: 0x7ff8000000000042, Seq: 2}}}}})
	if e != nil {
		t.Fatal(e)
	}
	defer plan.Abort()
	dir := t.TempDir()
	older := filepath.Join(dir, "older.sst")
	source := v2part.Provenance{Incarnation: v2part.PartID{9}, Stream: "original"}
	if _, e = WriteDenseRollupPart(older, plan.Files[0], source); e != nil {
		t.Fatal(e)
	}
	var latest []KV
	for _, kv := range plan.Files[0] {
		k, _ := v2part.Decode(kv.Key)
		k.Part = v2part.PartID{8}
		value := bytes.Clone(kv.encodedValue())
		if k.Archive == v2part.MetadataArchive {
			m, err := DecodeRollupMetadata(k.Metric, k.Group, value)
			if err != nil {
				t.Fatal(err)
			}
			m.Frontier.Order = 2
			m.Revision++
			value, e = json.Marshal(m)
			if e != nil {
				t.Fatal(e)
			}
		} else {
			record, err := sealrollup.DecodeRecord(int(k.Archive), k.BlockStart, value)
			if err != nil {
				t.Fatal(err)
			}
			if record.Sample.Time != 1700000000 {
				continue
			}
			record.Order = 2
			record.Correction = true
			record.Sample.Bits = math.Float64bits(17.125)
			value = sealrollup.EncodeRecord(record)
		}
		key, _ := v2part.Encode(k)
		latest = append(latest, KV{Key: key, Value: value})
	}
	newer := filepath.Join(dir, "newer.sst")
	if _, e = WritePortableRollupPart(newer, latest, source); e != nil {
		t.Fatal(e)
	}
	db, e := pebble.Open(filepath.Join(dir, "db"), &pebble.Options{})
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	if e = db.Ingest([]string{older, newer}); e != nil {
		t.Fatal(e)
	}
	snapshot := db.NewSnapshot()
	defer snapshot.Close()
	n := 0
	e = ScanRollups(snapshot, func(_ RollupPolicy, m RollupMetadata, rs []sealrollup.Record) error {
		if m.Frontier.Order != 2 || len(rs) != 2 {
			t.Fatalf("bad frontier/slots: %+v %d", m.Frontier, len(rs))
		}
		for _, r := range rs {
			if r.Sample.Time == 1700000000 {
				if r.Order != 2 || r.Sample.Bits != math.Float64bits(17.125) || !r.Correction {
					t.Fatal("stale dense value won")
				}
			} else if r.Sample.Bits != 0x7ff8000000000042 {
				t.Fatal("occupied NaN changed")
			}
			n++
		}
		return nil
	})
	if e != nil || n != 2 {
		t.Fatalf("read %d: %v", n, e)
	}
}

func TestDenseRuntimeSealExport(t *testing.T) {
	_, policy := rollupTestPolicy(t, "test.runtime.dense", "1s:1d,1m:30d", sealrollup.Last, .5)
	r := newCoreRollups()
	rows := []Row{{ID: 7, Time: 1700000000, Bits: 0x7ff8000000000042, Seq: 1}, {ID: 7, Time: 1700000001, Bits: 1 << 63, Seq: 2}, {ID: 7, Time: 1700000002, Bits: 1, Seq: 3}}
	plan, err := r.plan(SealInput{Batches: []Batch{{ID: 7, Order: 1, Clock: 1700000003, Policy: policy, Points: rows}}})
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Abort()
	if len(plan.Files) != 1 {
		t.Fatalf("files %d", len(plan.Files))
	}
	source := v2part.Provenance{Incarnation: v2part.PartID{9}, Stream: "journal-0"}
	path := filepath.Join(t.TempDir(), "runtime.sst")
	d, err := WriteDenseRollupPart(path, plan.Files[0], source)
	if err != nil {
		t.Fatal(err)
	}
	actual := map[string][]byte{}
	if err := v2part.ReadExpanded(context.Background(), path, &d, func(k v2part.Key, value []byte) error {
		key, err := v2part.Encode(k)
		if err != nil {
			return err
		}
		actual[string(key)] = bytes.Clone(value)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, kv := range plan.Files[0] {
		key, err := v2part.Decode(kv.Key)
		if err != nil {
			t.Fatal(err)
		}
		if key.Archive == v2part.MetadataArchive {
			count++
			if !bytes.Equal(actual[string(kv.Key)], kv.encodedValue()) {
				t.Fatal("metadata changed")
			}
			continue
		}
		records, err := v2part.DecodeSlots(key.Archive, key.BlockStart, kv.encodedValue())
		if err != nil {
			t.Fatal(err)
		}
		for _, record := range records {
			count++
			key.BlockStart = record.Slot
			encoded, _ := v2part.Encode(key)
			if !bytes.Equal(actual[string(encoded)], sealrollup.EncodeRecord(record)) {
				t.Fatalf("runtime slot changed %+v", record)
			}
		}
	}
	if count != len(actual) {
		t.Fatalf("expanded slots %d want%d", len(actual), count)
	}
}
