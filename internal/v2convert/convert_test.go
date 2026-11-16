package v2convert

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/sstable"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

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
	paths, e := (CoreWriter{}).Write(context.Background(), root, rows)
	if e != nil {
		t.Fatal(e)
	}
	return filepath.Join(root, paths[0]), rows
}
func TestConvertResumeAndReadback(t *testing.T) {
	source, rows := fixture(t)
	ctx := context.Background()
	o := Options{Directory: t.TempDir(), PartBytes: 1 << 20, Adapter: CoreRecords{}, Writer: CoreWriter{}}
	r, e := ConvertSST(ctx, source, o)
	if e != nil {
		t.Fatal(e)
	}
	if r.Entries != uint64(len(rows)) || len(r.Files) != 1 {
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
			o := Options{Directory: t.TempDir(), PartBytes: 1 << 20, Adapter: CoreRecords{}, Writer: CoreWriter{}, Checkpoint: func(p string) error {
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
		{Directory: t.TempDir(), PartBytes: 8, Adapter: CoreRecords{}, Writer: CoreWriter{}},
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
	if _, e = ConvertSST(context.Background(), filepath.Join(dir, "merge.sst"), Options{Directory: t.TempDir(), PartBytes: 1 << 20, Adapter: CoreRecords{}, Writer: CoreWriter{}}); e == nil {
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
	if _, err = ConvertSST(context.Background(), path, Options{Directory: t.TempDir(), PartBytes: 1 << 20, Adapter: CoreRecords{}, Writer: CoreWriter{}}); err == nil {
		t.Fatal("unresolved native range accepted")
	}
}
