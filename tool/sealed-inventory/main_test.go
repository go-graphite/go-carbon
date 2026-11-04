package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/floatresearch"
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
func TestInventoryAndTranscode(t *testing.T) {
	dir := fixture(t)
	root := t.TempDir()
	inv := filepath.Join(root, "inventory.json")
	_, total, err := listFiles(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = run([]string{"inventory", "-sst-dir", dir, "-read-mb-s", "0", "-out", inv}); err != nil {
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
	// Keep one paired candidate so the test verifies every emitted key/value.
	temp := filepath.Join(root, "outputs")
	if err = os.Mkdir(temp, 0700); err != nil {
		t.Fatal(err)
	}
	trans := filepath.Join(root, "transcode.json")
	if err = run([]string{"transcode", "-sst-dir", dir, "-tmp", temp, "-out", trans, "-policy", "prefix256", "-read-mb-s", "0", "-keep"}); err != nil {
		t.Fatal(err)
	}
	result := loadReport(t, trans)
	var changed uint64
	for _, f := range result.Files {
		if !f.BaselineWithin3Pct {
			t.Fatalf("baseline delta %.3f%% (%s)", f.BaselineDeltaPct, f.WriterOptions)
		}
		p := f.Policies["prefix256"]
		if p.OracleFailures != 0 {
			t.Fatal("oracle failures")
		}
		changed += p.ChangedGroups
		outputs, err := filepath.Glob(filepath.Join(temp, "sealed-inventory-*", f.Name+".prefix256.sst"))
		if err != nil || len(outputs) != 1 {
			t.Fatal("missing kept output", err)
		}
		compareTables(t, filepath.Join(dir, f.Name), outputs[0])
	}
	if changed == 0 {
		t.Fatal("fixture did not exercise a changed float frame")
	}
	// Run all policies with deletion, then verify cleanup.
	all := filepath.Join(root, "all.json")
	if err = run([]string{"transcode", "-sst-dir", dir, "-tmp", temp, "-out", all, "-read-mb-s", "0"}); err != nil {
		t.Fatal(err)
	}
	for _, f := range loadReport(t, all).Files {
		if p := f.Policies["current"]; p.Bytes != f.BaselineBytes || p.ChangedGroups != 0 {
			t.Fatal("current rewrite changed source values")
		}
		for _, p := range f.Policies {
			if p.OracleFailures > 0 {
				t.Fatal("oracle failures")
			}
		}
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
func table(t *testing.T, path string) (*sstable.Reader, sstable.Iterator) {
	t.Helper()
	r, err := sstable.NewSimpleReadable(mustOpen(t, path))
	if err != nil {
		t.Fatal(err)
	}
	reader, err := sstable.NewReader(r, sstable.ReaderOptions{}, sstable.Mergers{"go-carbon.chunks.exact.v1": nil})
	if err != nil {
		t.Fatal(err)
	}
	i, err := reader.NewIter(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return reader, i
}
func compareTables(t *testing.T, a, b string) {
	t.Helper()
	ra, ia := table(t, a)
	defer ra.Close()
	defer ia.Close()
	rb, ib := table(t, b)
	defer rb.Close()
	defer ib.Close()
	kb, vb := ib.First()
	for ka, va := ia.First(); ka != nil; ka, va = ia.Next() {
		if kb == nil || !bytes.Equal(ka.UserKey, kb.UserKey) || ka.Trailer != kb.Trailer {
			t.Fatal("keys changed")
		}
		av, _, e := va.Value(nil)
		if e != nil {
			t.Fatal(e)
		}
		bv, _, e := vb.Value(nil)
		if e != nil {
			t.Fatal(e)
		}
		if !bytes.Equal(av, bv) {
			class, _, e := classify(ka.UserKey, av)
			if e != nil || !strings.HasPrefix(class, "sealed/float-planes/") {
				t.Fatal("non-float value changed")
			}
			ap, e := chunkstore.DecodeSealedPoints(av)
			if e != nil {
				t.Fatal(e)
			}
			bp, e := decodeCandidate(bv)
			if e != nil || !samePoints(ap, bp) {
				t.Fatal("rewritten group mismatch", e)
			}
		}
		kb, vb = ib.Next()
	}
	if kb != nil {
		t.Fatal("extra keys")
	}
	if err := ia.Error(); err != nil {
		t.Fatal(err)
	}
	if err := ib.Error(); err != nil {
		t.Fatal(err)
	}
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
	r.outputLimit = 2 * got.Bytes
	out := filepath.Join(root, "copy.sst")
	reader, _, err := r.open(path)
	if err != nil {
		t.Fatal(err)
	}
	layout, err := reader.Layout()
	if err != nil {
		t.Fatal(err)
	}
	opts, err := r.writerOptions(reader, layout, got)
	_ = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = r.rewrite(path, out, "baseline", opts); err != nil {
		t.Fatal(err)
	}
	r.cfg.dir = root
	copy, err := r.scan(out)
	if err != nil || copy.RangeDelKeys != 1 {
		t.Fatal("range delete lost", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r.ctx = ctx
	if _, err = r.scan(path); err == nil {
		t.Fatal("ignored cancellation")
	}
}
func TestPolicyOracleSpecials(t *testing.T) {
	points := make([]chunkstore.Point, 4096)
	words := []uint64{0, 1, 1 << 63, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000001234, math.Float64bits(.3), math.Float64bits(1.0 / 3)}
	for i := range points {
		points[i] = chunkstore.Point{Timestamp: 1700000000 + int64(i*60), Value: math.Float64frombits(words[i%len(words)])}
	}
	original, err := chunkstore.EncodeSealedPoints(points, 60)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"current", "prefix256", "minfour", "alp", "chimp"} {
		t.Run(p, func(t *testing.T) {
			_, b, e := safeCandidate(p, floatresearch.Group{Points: points, Step: 60}, original)
			if e != nil {
				t.Fatal(e)
			}
			got, e := decodeCandidate(b)
			if e != nil || !samePoints(points, got) {
				t.Fatal(fmt.Sprint("oracle: ", e))
			}
			if len(b) > len(original) {
				t.Fatal("size fallback failed")
			}
		})
	}
}

// Execute the actual signal-aware run function in a child so SIGTERM does not
// affect the test runner. Only immutable local fixture tables are read.
func TestSignalHelper(t *testing.T) {
	t.Helper()
	raw := os.Getenv("SEALED_INVENTORY_CHILD_ARGS")
	if raw == "" {
		return
	}
	var args []string
	if err := json.Unmarshal([]byte(raw), &args); err != nil {
		os.Exit(3)
	}
	if err := run(args); err != nil {
		os.Exit(2)
	}
	os.Exit(0)
}
func TestSIGTERMCleansOutputs(t *testing.T) {
	root := t.TempDir()
	input := filepath.Join(root, "input")
	output := filepath.Join(root, "output")
	for _, dir := range []string{input, output} {
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	f, err := vfs.Default.Create(filepath.Join(input, "000001.sst"))
	if err != nil {
		t.Fatal(err)
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{})
	random := rand.New(rand.NewPCG(19, 23))
	value := make([]byte, 4096)
	for i := 0; i < 512; i++ {
		for j := range value {
			value[j] = byte(random.Uint64())
		}
		if err = w.Set([]byte(fmt.Sprintf("z/%08d", i)), value); err != nil {
			t.Fatal(err)
		}
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	args, _ := json.Marshal([]string{"transcode", "-sst-dir", input, "-tmp", output, "-out", filepath.Join(root, "report.json"), "-policy", "current", "-read-mb-s", "8", "-keep"})
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestSignalHelper$")
	cmd.Env = append(os.Environ(), "SEALED_INVENTORY_CHILD_ARGS="+string(args))
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	signaled := false
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for !signaled {
		select {
		case <-deadline.C:
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Fatal("child did not start writing")
		case <-tick.C:
			paths, e := filepath.Glob(filepath.Join(output, "sealed-inventory-*", "*.sst"))
			if e != nil {
				t.Fatal(e)
			}
			if len(paths) > 0 {
				if err = cmd.Process.Signal(syscall.SIGTERM); err != nil {
					t.Fatal(err)
				}
				signaled = true
			}
		}
	}
	if err = cmd.Wait(); err == nil {
		t.Fatal("SIGTERM reported success")
	}
	dirs, err := os.ReadDir(output)
	if err != nil {
		t.Fatal(err)
	}
	if len(dirs) != 0 {
		t.Fatalf("left temporary outputs after SIGTERM: %v", dirs)
	}
}
