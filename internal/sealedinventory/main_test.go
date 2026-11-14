package sealedinventory

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

type fixtureCodec struct{ chunkstore.ContinuationCodec }

func fixtureKey(kind byte, block int) []byte {
	k := make([]byte, 16)
	k[0] = 'f'
	binary.BigEndian.PutUint64(k[1:9], 1)
	k[9] = kind
	binary.BigEndian.PutUint32(k[12:], uint32(block))
	if kind != 'b' {
		k = append(k, 0, 0, 0, 0)
	}
	return k
}
func (fixtureCodec) Create(_ context.Context, kv chunkstore.ContinuationIO, _ chunkstore.Metadata, _ func() time.Time) error {
	for i := 0; i < 128; i++ {
		n := 4096
		if i%4 == 0 {
			n = 257
		}
		points := make([]chunkstore.Point, n)
		state := uint64(19)
		for j := range points {
			state ^= state << 13
			state ^= state >> 7
			state ^= state << 17
			points[j] = chunkstore.Point{Timestamp: 1700000000 + int64(j*60), Value: float64(state%16) / 3}
		}
		if i%2 == 1 {
			for j := range points {
				points[j].Value = math.Trunc(points[j].Value)
			}
		}
		value, err := chunkstore.EncodeSealedPoints(points, 60)
		if err != nil {
			return err
		}
		key := fixtureKey('c', i)
		if i%3 != 0 {
			key = chunkstore.ContinuationBucketKey(23, key)
		}
		if err = kv.Set(key, value); err != nil {
			return err
		}
		if err = kv.Set(fixtureKey('b', i), binary.BigEndian.AppendUint32(nil, uint32(n))); err != nil {
			return err
		}
	}
	for i := 0; i < 3; i++ {
		var b chunkstore.PointBlock
		b.Present[0] = 1
		b.Points[0] = chunkstore.Point{Timestamp: 1700000000, Value: float64(i) + .5}
		if err := kv.Set(fixtureKey('p', i), chunkstore.EncodePointBlock(&b)); err != nil {
			return err
		}
	}
	return kv.Set([]byte("n/main.wsp"), []byte("fixture"))
}
func fixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	store, err := chunkstore.Open(filepath.Join(root, "store"), chunkstore.Options{ExactBlocks: true, TimeBuckets: true, Continuation: fixtureCodec{}, MemTableSize: 1 << 20, CacheSize: 1 << 20, BloomFilterBits: 10})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	_, err = store.Create(context.Background(), chunkstore.MetricConfig{Name: "fixture.metric", Retentions: []chunkstore.Retention{{Step: 60, Count: 10000}}, AggregationMethod: chunkstore.Average})
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Flush(); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(root, "checkpoint")
	if err = store.Checkpoint(dest); err != nil {
		t.Fatal(err)
	}
	return dest
}
func loadReport(t *testing.T, path string) report {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out report
	if err = json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return out
}
func TestInventory(t *testing.T) {
	dir := fixture(t)
	root := t.TempDir()
	inv := filepath.Join(root, "inventory.json")
	_, total, err := listFiles(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = run([]string{"-sst-dir", dir, "-read-mb-s", "0", "-out", inv}); err != nil {
		t.Fatal(err)
	}
	report := loadReport(t, inv)
	if report.Classes["catalog/metadata"].Entries != 5 {
		t.Fatalf("metadata count: %d", report.Classes["catalog/metadata"].Entries)
	}
	if report.FileBytes != total {
		t.Fatalf("file bytes %d != %d", report.FileBytes, total)
	}
	var sealed uint64
	for class, n := range report.Classes {
		if strings.HasPrefix(class, "sealed/") {
			sealed += n.Entries
		}
	}
	if sealed != 128 || report.Classes["ordinary"].Entries != 3 || report.Classes["descriptor"].Entries != 128 {
		t.Fatalf("wrong fixture classes: %+v", report.Classes)
	}
	for _, f := range report.Files {
		var physical float64
		for _, c := range f.Classes {
			physical += c.PhysicalBytes
		}
		if math.Abs(physical-float64(f.DataBytes)) > 1e-6 || f.DataBytes+f.OverheadBytes != f.Bytes {
			t.Fatalf("physical accounting failed: %+v", f)
		}
		raw, err := sstable.NewSimpleReadable(mustOpen(t, filepath.Join(dir, f.Name)))
		if err != nil {
			t.Fatal(err)
		}
		reader, err := sstable.NewReader(raw, sstable.ReaderOptions{}, sstable.Mergers{"go-carbon.chunks.exact.v1": nil})
		if err != nil {
			t.Fatal(err)
		}
		var logical, entries uint64
		for _, c := range f.Classes {
			logical += c.LogicalBytes
			entries += c.Entries
		}
		if logical+8*entries != reader.Properties.RawKeySize+reader.Properties.RawValueSize {
			t.Fatal("logical bytes disagree with Pebble properties")
		}
		layout, err := reader.Layout()
		if err != nil {
			t.Fatal(err)
		}
		var bytes uint64
		for _, b := range layout.Data {
			bytes += b.Length + 5
		}
		if bytes != f.DataBytes {
			t.Fatal("Pebble layout mismatch")
		}
		_ = reader.Close()
	}
}
func mustOpen(t *testing.T, path string) *os.File {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	return f
}
func TestSafetyAndRangeDeletes(t *testing.T) {
	root := t.TempDir()
	input := filepath.Join(root, "input")
	if err := os.Mkdir(input, 0700); err != nil {
		t.Fatal(err)
	}
	if err := outside(input, filepath.Join(input, "temp")); err == nil {
		t.Fatal("accepted nested output")
	}
	link := filepath.Join(root, "alias")
	if err := os.Symlink(input, link); err != nil {
		t.Fatal(err)
	}
	if err := outside(input, filepath.Join(link, "temp")); err == nil {
		t.Fatal("accepted symlinked nested output")
	}
	if err := os.Symlink(filepath.Join(root, "out.sst"), filepath.Join(input, "link.sst")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := listFiles(input, 0); err == nil {
		t.Fatal("accepted symlink SST")
	}
	if err := os.Remove(filepath.Join(input, "link.sst")); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(input, "000001.sst")
	f, err := vfs.Default.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{})
	k := fixtureKey('c', 0)
	points := []chunkstore.Point{{Timestamp: 1700000000, Value: .5}}
	v, err := chunkstore.EncodeSealedPoints(points, 60)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Add(sstable.InternalKey{UserKey: k, Trailer: 1<<8 | uint64(pebble.InternalKeyKindSet)}, v); err != nil {
		t.Fatal(err)
	}
	if err = w.Add(sstable.InternalKey{UserKey: k, Trailer: 2<<8 | uint64(pebble.InternalKeyKindRangeDelete)}, append(append([]byte(nil), k...), 0)); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	r := runner{ctx: context.Background(), cfg: config{dir: input, mode: "inventory"}, levels: map[string]int{}}
	got, err := r.scan(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.RangeDelKeys != 1 || got.Classes["sealed/float-planes/raw"].TombstonedEntries != 1 {
		t.Fatalf("range deletes not counted: %+v", got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r.ctx = ctx
	if _, err = r.scan(path); err == nil {
		t.Fatal("ignored cancellation")
	}
}

func TestResearchCLIRejected(t *testing.T) {
	for _, args := range [][]string{{"transcode"}, {"-policy", "current"}, {"-keep"}} {
		if err := Main(args); err == nil {
			t.Fatalf("accepted research flags: %v", args)
		}
	}
}
