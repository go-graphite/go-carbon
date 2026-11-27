package v2core_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2convert"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// A first live seal and a certified native baseline of the same initial input
// must produce identical parts at the same boundary. Existing native history
// lacks original per-point orders and needs an explicit new baseline authority.
func TestFormat3LiveSealConverterIdentity(t *testing.T) {
	ctx := context.Background()
	source := v2part.Provenance{Incarnation: v2part.PartID{9, 3}, Stream: "format3-fixture-input"}
	native, err := chunkstore.Open("native", chunkstore.Options{ExactBlocks: true, Now: func() time.Time { return time.Unix(1700000020, 0) }, CacheSize: 1 << 20, MemTableSize: 1 << 20}.WithFS(vfs.NewMem()))
	if err != nil {
		t.Fatal(err)
	}
	defer native.Close()
	nativeMeta, err := native.Create(ctx, chunkstore.MetricConfig{Name: "format3.identity", Retentions: []chunkstore.Retention{{Step: 1, Count: 86400}, {Step: 60, Count: 43200}}, AggregationMethod: chunkstore.Last, XFilesFactor: .5})
	if err != nil {
		t.Fatal(err)
	}
	policy, err := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: "format3.identity", Group: "format3-fixture", Config: sealrollup.Config{Method: sealrollup.Last, XFilesFactor: .5, Archives: []sealrollup.Archive{{Step: 1, Count: 86400}, {Step: 60, Count: 43200}}}})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	cfg := v2core.Config{Rollups: true, AppendParts: true, JournalIdentity: source.Stream, Shards: 1, Producers: 1, MaxRows: 1024, PartitionSeconds: 60}
	hooks := v2core.Hooks{PartSource: source}
	engine, err := v2core.Open(root, cfg, hooks)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if engine != nil {
			_ = engine.Close()
		}
	}()
	bits := []uint64{0, 1 << 63, 1, 0x7ff8000000000042, 0xfff8000000000017, 0x7ff0000000000000, 0xfff0000000000000, 0x3ff4000000000000}
	rows := make([]v2core.Row, len(bits))
	nativePoints := make([]chunkstore.Point, len(bits))
	for i, value := range bits {
		rows[i] = v2core.Row{ID: nativeMeta.ID, Time: 1700000000 + int64(i), Bits: value, Seq: uint64(i + 1)}
		nativePoints[i] = chunkstore.Point{Timestamp: rows[i].Time, Value: math.Float64frombits(value)}
	}
	if err := native.UpdateMany(ctx, nativeMeta.Name, nativePoints); err != nil {
		t.Fatal(err)
	}
	if err := engine.AppendBatches(0, []v2core.Batch{{ID: nativeMeta.ID, Order: 1, Clock: 1700000020, Policy: policy, Points: rows}}, 1, true); err != nil {
		t.Fatal(err)
	}
	var descriptors []string
	if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err == nil && !entry.IsDir() && strings.HasSuffix(path, ".sst.descriptor") {
			descriptors = append(descriptors, path)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if len(descriptors) != 1 {
		t.Fatalf("live seal retained %d descriptors, want one", len(descriptors))
	}
	descriptorBytes, err := os.ReadFile(descriptors[0])
	if err != nil {
		t.Fatal(err)
	}
	d, err := v2part.ParseDescriptor(descriptorBytes)
	if err != nil {
		t.Fatal(err)
	}
	if d.Encoding != v2part.DenseEncoding || d.Source.Incarnation != source.Incarnation || d.Source.Stream != source.Stream {
		t.Fatalf("live seal did not use the shared dense contract: %+v", d)
	}
	livePath := strings.TrimSuffix(descriptors[0], ".descriptor")
	liveBytes, err := os.ReadFile(livePath)
	if err != nil {
		t.Fatal(err)
	}
	nativeSnapshot, err := native.Snapshot(ctx, nativeMeta.Name)
	if err != nil {
		t.Fatal(err)
	}
	baseline := v2convert.NativeBaseline{Snapshot: sha256.Sum256([]byte("single-batch-native-fixture")), Source: source, Group: d.Scope.Group, Part: d.Scope.Part, Order: 1}
	nativeRecords, err := v2convert.NativeRecords(nativeSnapshot, baseline)
	if err != nil {
		t.Fatal(err)
	}
	nativeRoot := t.TempDir()
	nativePaths, err := (v2convert.CoreWriter{Source: source}).Write(ctx, nativeRoot, nativeRecords)
	if err != nil || len(nativePaths) != 2 {
		t.Fatalf("native convert: paths=%v error=%v", nativePaths, err)
	}
	sameAsLiveSeal(t, "native conversion", nativeRoot, nativePaths, livePath, d)
	copyRoot := filepath.Join(t.TempDir(), "copied")
	sealedSnapshot := engine.DBs()[0].NewSnapshot()
	copied, err := v2core.LinkPartSnapshot(ctx, copyRoot, v2core.SealedSnapshotReader{Snapshot: sealedSnapshot, Directory: filepath.Join(root, "owner-000")})
	_ = sealedSnapshot.Close()
	if err != nil || len(copied.Parts) != 2 {
		t.Fatalf("live part copy: %+v %v", copied, err)
	}
	for name, want := range map[string][]byte{fmt.Sprintf("%x.part", d.SHA256): liveBytes, fmt.Sprintf("%x.part", sha256.Sum256(descriptorBytes)): descriptorBytes} {
		got, err := os.ReadFile(filepath.Join(copyRoot, name))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("copied live part %s changed bytes: %v", name, err)
		}
	}
	var materialized []v2convert.Record
	if err := v2part.ReadExpanded(ctx, filepath.Join(copyRoot, fmt.Sprintf("%x.part", d.SHA256)), &d, func(key v2part.Key, value []byte) error {
		encoded, err := v2part.Encode(key)
		if err != nil {
			return err
		}
		materialized = append(materialized, v2convert.Record{Key: encoded, Value: bytes.Clone(value)})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	convertedRoot := t.TempDir()
	writer := v2convert.CoreWriter{Source: source}
	paths, err := writer.Write(ctx, convertedRoot, materialized)
	if err != nil || len(paths) != 2 {
		t.Fatalf("convert: paths=%v error=%v", paths, err)
	}
	sameAsLiveSeal(t, "converter", convertedRoot, paths, livePath, d)
	var converted []v2convert.Record
	for _, path := range paths {
		if err := writer.Read(ctx, filepath.Join(convertedRoot, path), func(record v2convert.Record) error {
			converted = append(converted, v2convert.Record{Key: bytes.Clone(record.Key), Value: bytes.Clone(record.Value)})
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if len(converted) != len(materialized) {
		t.Fatalf("converted readback %d/%d", len(converted), len(materialized))
	}
	for i := range converted {
		if !bytes.Equal(converted[i].Key, materialized[i].Key) || !bytes.Equal(converted[i].Value, materialized[i].Value) {
			t.Fatalf("converted reader changed record %d", i)
		}
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	engine = nil
	engine, err = v2core.Open(root, cfg, hooks)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := engine.DBs()[0].NewSnapshot()
	defer snapshot.Close()
	names := 0
	if err := v2core.ScanRollups(snapshot, func(_ v2core.RollupPolicy, _ v2core.RollupMetadata, records []sealrollup.Record) error {
		names++
		found := map[int64]uint64{}
		for _, record := range records {
			if record.Archive == 0 {
				found[record.Sample.Time] = record.Sample.Bits
			}
		}
		for _, row := range rows {
			if value, ok := found[row.Time]; !ok || value != row.Bits {
				t.Fatalf("recovery changed timestamp %d bits %x/%x present=%v", row.Time, value, row.Bits, ok)
			}
		}
		return nil
	}); err != nil || names != 1 {
		t.Fatalf("recovered %d names: %v", names, err)
	}
}

// sameAsLiveSeal checks a converted part against a live seal. Live seals use
// the fast Zstd level and conversions the rest level, so their physical bytes
// differ; descriptors match apart from size and hash, and records are equal.
func sameAsLiveSeal(t *testing.T, what, root string, paths []string, livePath string, live v2part.Descriptor) {
	t.Helper()
	var partPath string
	var d v2part.Descriptor
	for _, path := range paths {
		if !strings.HasSuffix(path, ".descriptor") {
			partPath = filepath.Join(root, path)
			continue
		}
		b, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		if d, err = v2part.ParseDescriptor(b); err != nil {
			t.Fatal(err)
		}
	}
	normalized := d
	// Conversions write baseline parts without Seq: they sort before every seal.
	normalized.SHA256, normalized.Bytes, normalized.Source.Seq = live.SHA256, live.Bytes, live.Source.Seq
	if partPath == "" || !reflect.DeepEqual(normalized, live) {
		t.Fatalf("%s changed the live seal descriptor", what)
	}
	read := func(path string, d v2part.Descriptor) (out []v2convert.Record) {
		if err := v2part.ReadExpanded(context.Background(), path, &d, func(key v2part.Key, value []byte) error {
			encoded, err := v2part.Encode(key)
			out = append(out, v2convert.Record{Key: encoded, Value: bytes.Clone(value)})
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return out
	}
	if got, want := read(partPath, d), read(livePath, live); len(want) == 0 || !reflect.DeepEqual(got, want) {
		t.Fatalf("%s changed live seal records", what)
	}
}
