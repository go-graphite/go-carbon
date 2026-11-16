package v2convert

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2part"

	"github.com/cockroachdb/pebble/sstable"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

func testWriter() CoreWriter {
	return CoreWriter{Source: v2part.Provenance{Incarnation: v2part.PartID{9}, Stream: "test-source"}}
}

func fixture(t *testing.T) (string, []Record) {
	t.Helper()
	root := t.TempDir()
	rows := []Record{}
	// Daily samples and IEEE edge bits exercise the format that previously
	// ballooned through singleton per-day partitions. No density assertion here.
	for i, b := range []uint64{0, 1 << 63, 0x7ff8000000000042, 0x7ff0000000000000, 1} {
		key, e := partsformat.Encode(partsformat.Key{Group: "test", VShard: partsformat.Route("test.daily"), Metric: "test.daily", Part: partsformat.PartID{1}, BlockStart: int64(i)})
		if e != nil {
			t.Fatal(e)
		}
		value := sealrollup.EncodeRecord(sealrollup.Record{Delta: sealrollup.Delta{Archive: 0, Slot: int64(i), Order: 1, Sample: sealrollup.Sample{Time: 1700000000 + int64(i)*86400, Bits: b}}})
		rows = append(rows, Record{key, value})
	}
	config := sealrollup.Config{Archives: []sealrollup.Archive{{Step: 86400, Count: 4096}}, Method: sealrollup.Last}
	policy, e := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: "test.daily", Group: "test", Config: config})
	if e != nil {
		t.Fatal(e)
	}
	digest, e := sealrollup.PolicyDigest(config)
	if e != nil {
		t.Fatal(e)
	}
	metadata, e := json.Marshal(v2core.RollupMetadata{Version: 1, ID: 1, Policy: policy, Frontier: sealrollup.Frontier{Policy: digest, Order: 1, MaxTime: 1700000000 + 4*86400}})
	if e != nil {
		t.Fatal(e)
	}
	key, e := partsformat.Encode(partsformat.Key{Group: "test", VShard: partsformat.Route("test.daily"), Metric: "test.daily", Part: partsformat.PartID{1}, Archive: v2part.MetadataArchive})
	if e != nil {
		t.Fatal(e)
	}
	rows = append(rows, Record{key, metadata})
	path := filepath.Join(root, "input.sst")
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	w := sstable.NewWriter(&partFile{f}, sstable.WriterOptions{})
	for _, r := range rows {
		if e = w.Set(r.Key, r.Value); e != nil {
			t.Fatal(e)
		}
	}
	if e = w.Close(); e != nil {
		t.Fatal(e)
	}
	return path, rows
}
func TestConvertResumeAndReadback(t *testing.T) {
	source, rows := fixture(t)
	ctx := context.Background()
	o := Options{Directory: t.TempDir(), PartBytes: 1 << 20, Adapter: CoreRecords{}, Writer: testWriter()}
	r, e := ConvertSST(ctx, source, o)
	if e != nil {
		t.Fatal(e)
	}
	if r.Entries != uint64(len(rows)) || len(r.Files) != 2 {
		t.Fatalf("bad receipt %+v", r)
	}
	o.Checkpoint = func(string) error { return errors.New("completed part must not rebuild") }
	second, e := ConvertSST(ctx, source, o)
	if e != nil {
		t.Fatal(e)
	}
	if second.LogicalDigest != r.LogicalDigest {
		t.Fatal("resume changed digest")
	}
	var dir string
	entries, _ := os.ReadDir(o.Directory)
	for _, entry := range entries {
		if entry.IsDir() {
			dir = filepath.Join(o.Directory, entry.Name())
		}
	}
	p := filepath.Join(dir, r.Files[0].Name)
	f, e := os.OpenFile(p, os.O_WRONLY, 0)
	if e != nil {
		t.Fatal(e)
	}
	_, e = f.WriteAt([]byte("bad"), 0)
	f.Close()
	if e != nil {
		t.Fatal(e)
	}
	if _, e = ConvertSST(ctx, source, o); e == nil {
		t.Fatal("corruption accepted on resume")
	}
}
func TestConversionCrashBeforePublish(t *testing.T) {
	for _, point := range []string{"files", "receipt"} {
		t.Run(point, func(t *testing.T) {
			source, _ := fixture(t)
			o := Options{Directory: t.TempDir(), PartBytes: 1 << 20, Adapter: CoreRecords{}, Writer: testWriter(), Checkpoint: func(p string) error {
				if p == point {
					return errors.New("crash")
				}
				return nil
			}}
			if _, e := ConvertSST(context.Background(), source, o); e == nil {
				t.Fatal("missing injected failure")
			}
			dirs, e := os.ReadDir(o.Directory)
			if e != nil || len(dirs) != 0 {
				t.Fatalf("partial publication: %v %v", dirs, e)
			}
			o.Checkpoint = nil
			if _, e := ConvertSST(context.Background(), source, o); e != nil {
				t.Fatal(e)
			}
		})
	}
}

type droppingWriter struct{ CoreWriter }

func (droppingWriter) Format() string { return "deliberately-lossy" }
func (w droppingWriter) Write(ctx context.Context, dir string, r []Record) ([]string, error) {
	return w.CoreWriter.Write(ctx, dir, r[:len(r)-1])
}
func TestRejectLossyWriterAndMemoryOverflow(t *testing.T) {
	source, _ := fixture(t)
	for _, o := range []Options{
		{Directory: t.TempDir(), PartBytes: 1 << 20, Adapter: CoreRecords{}, Writer: droppingWriter{}},
		{Directory: t.TempDir(), PartBytes: 8, Adapter: CoreRecords{}, Writer: testWriter()},
	} {
		if _, e := ConvertSST(context.Background(), source, o); e == nil {
			t.Fatal("invalid output accepted")
		}
	}
}
func TestRejectNativeAndUnresolvedInput(t *testing.T) {
	if _, e := (CoreRecords{}).Convert(Entry{Key: []byte("m/native"), Value: []byte("native")}); e == nil {
		t.Fatal("native accepted as GCP3")
	}
	source, rows := fixture(t)
	dir := t.TempDir()
	f, e := os.Create(filepath.Join(dir, "merge.sst"))
	if e != nil {
		t.Fatal(e)
	}
	w := sstable.NewWriter(&partFile{f}, sstable.WriterOptions{})
	if e = w.Add(sstable.InternalKey{UserKey: rows[0].Key, Trailer: uint64(sstable.InternalKeyKindMerge)}, rows[0].Value); e != nil {
		t.Fatal(e)
	}
	if e = w.Close(); e != nil {
		t.Fatal(e)
	}
	if _, e = ConvertSST(context.Background(), filepath.Join(dir, "merge.sst"), Options{Directory: t.TempDir(), PartBytes: 1 << 20, Adapter: CoreRecords{}, Writer: testWriter()}); e == nil {
		t.Fatal("merge accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e = ScanSST(ctx, source, nil); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
}

func TestScanNativeMergeAndRangeInventory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "native.sst")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := sstable.NewWriter(&partFile{f}, sstable.WriterOptions{MergerName: "go-carbon.chunks.exact.v1"})
	if err = w.Add(sstable.InternalKey{UserKey: []byte("m/native"), Trailer: 7<<8 | uint64(sstable.InternalKeyKindMerge)}, []byte("opaque")); err != nil {
		t.Fatal(err)
	}
	if err = w.DeleteRange([]byte("a"), []byte("z")); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	stats, err := ScanSST(context.Background(), path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Entries != 1 || stats.RangeDeletes != 1 || stats.Kinds["MERGE"] != 1 {
		t.Fatalf("lost native inventory: %+v", stats)
	}
	if _, err = ConvertSST(context.Background(), path, Options{Directory: t.TempDir(), PartBytes: 1 << 20, Adapter: CoreRecords{}, Writer: testWriter()}); err == nil {
		t.Fatal("unresolved native range accepted")
	}
}

func TestConvertSharedDenseInput(t *testing.T) {
	source, _ := fixture(t)
	options := Options{Directory: t.TempDir(), PartBytes: 1 << 20, Adapter: CoreRecords{}, Writer: testWriter()}
	first, e := ConvertSST(context.Background(), source, options)
	if e != nil {
		t.Fatal(e)
	}
	entries, _ := os.ReadDir(options.Directory)
	dense := filepath.Join(options.Directory, entries[0].Name(), first.Files[0].Name)
	options.Directory = t.TempDir()
	second, e := ConvertSST(context.Background(), dense, options)
	if e != nil {
		t.Fatal(e)
	}
	if first.LogicalDigest != second.LogicalDigest || first.Entries != second.Entries {
		t.Fatal("dense source changed logical records")
	}
}

func TestConvertGroupsVShardsAndRetainsOriginalParts(t *testing.T) {
	_, template := fixture(t)
	var rows []Record
	for n, name := range []string{"test.daily", "test.other", "test.third", "test.fourth"} {
		for _, row := range template {
			k, _ := v2part.Decode(row.Key)
			k.Metric = v2part.ID(name)
			k.VShard = v2part.Route(k.Metric)
			value := bytes.Clone(row.Value)
			if k.Archive == v2part.MetadataArchive {
				var m v2core.RollupMetadata
				var p v2core.RollupPolicy
				if e := json.Unmarshal(value, &m); e != nil {
					t.Fatal(e)
				}
				if e := json.Unmarshal(m.Policy, &p); e != nil {
					t.Fatal(e)
				}
				p.Name = k.Metric
				m.ID = uint64(n + 1)
				m.Policy, _ = v2core.EncodeRollupPolicy(p)
				value, _ = json.Marshal(m)
			}
			key, _ := v2part.Encode(k)
			rows = append(rows, Record{key, value})
			if n == 1 {
				k.Part = v2part.PartID{2}
				key, _ = v2part.Encode(k)
				rows = append(rows, Record{key, bytes.Clone(value)})
			}
		}
	}
	slices.SortFunc(rows, func(a, b Record) int { return bytes.Compare(a.Key, b.Key) })
	source := filepath.Join(t.TempDir(), "source.sst")
	f, e := os.Create(source)
	if e != nil {
		t.Fatal(e)
	}
	w := sstable.NewWriter(&partFile{f}, sstable.WriterOptions{})
	for _, row := range rows {
		if e = w.Set(row.Key, row.Value); e != nil {
			t.Fatal(e)
		}
	}
	if e = w.Close(); e != nil {
		t.Fatal(e)
	}
	options := Options{Directory: t.TempDir(), PartBytes: 1 << 20, Adapter: CoreRecords{}, Writer: testWriter()}
	r, e := ConvertSST(context.Background(), source, options)
	if e != nil {
		t.Fatal(e)
	}
	if len(r.Files) != 4 || r.Entries != uint64(len(rows)) {
		t.Fatalf("did not group vshards into the two original parts: %+v", r)
	}
}

func TestConvertResumeBindsProvenance(t *testing.T) {
	source, _ := fixture(t)
	dir := t.TempDir()
	writer := testWriter()
	options := Options{Directory: dir, PartBytes: 1 << 20, Adapter: CoreRecords{}, Writer: writer}
	first, e := ConvertSST(context.Background(), source, options)
	if e != nil {
		t.Fatal(e)
	}
	writer.Source.Incarnation = v2part.PartID{8}
	options.Writer = writer
	second, e := ConvertSST(context.Background(), source, options)
	if e != nil {
		t.Fatal(e)
	}
	if first.Writer == second.Writer {
		t.Fatal("producer provenance missing from resume identity")
	}
	entries, e := os.ReadDir(dir)
	if e != nil || len(entries) != 2 {
		t.Fatalf("wrong resume authority: %d %v", len(entries), e)
	}
}
