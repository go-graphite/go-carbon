package whisperio

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// Frozen pre-optimization byte reader: compares the complete encoded output,
// including NaN payloads, signed zeros and arbitrary timestamp truncation.
func (f *continuationFile) readAtAllocatingOracle(data []byte, offset int64) (int, error) {
	if f.closed {
		return 0, os.ErrClosed
	}
	if offset < 0 {
		return 0, errors.New("negative continuation read")
	}
	if len(data) == 0 {
		return 0, nil
	}
	if offset >= int64(f.data.layout.Size) {
		return 0, io.EOF
	}
	end := min(int64(f.data.layout.Size), offset+int64(len(data)))
	if end < offset {
		return 0, errors.New("continuation read overflow")
	}
	position, written := int(offset), 0
	var points []whisper.TimeSeriesPoint
	for int64(position) < end {
		if position < len(f.data.header) {
			count := min(int(end)-position, len(f.data.header)-position)
			copy(data[written:written+count], f.data.header[position:position+count])
			position, written = position+count, written+count
			continue
		}
		archive, a, err := f.archiveAt(position)
		if err != nil {
			return written, err
		}
		if f.data.layout.Compressed {
			index := (position - a.Offset) / a.BlockSize
			block, err := f.readRawBlock(archive, index, a, &points)
			if err != nil {
				return written, err
			}
			start := (position - a.Offset) % a.BlockSize
			count := min(int(end)-position, a.BlockSize-start)
			if len(block) == 0 {
				clear(data[written : written+count])
			} else {
				copy(data[written:written+count], block[start:start+count])
			}
			position, written = position+count, written+count
		} else {
			groupIndex := (position - a.Offset) / (128 * 12)
			group, err := f.classicGroup(archive, a, groupIndex, false)
			if err != nil {
				return written, err
			}
			limit := min(int(end), a.Offset+min(a.Count, (groupIndex+1)*128)*12)
			for position < limit {
				slot := ((position - a.Offset) / 12) % 128
				within := (position - a.Offset) % 12
				var record [12]byte
				if group.Present[slot/64]&(uint64(1)<<uint(slot%64)) != 0 {
					point := group.Points[slot]
					binary.BigEndian.PutUint32(record[:4], uint32(point.Timestamp))
					binary.BigEndian.PutUint64(record[4:], math.Float64bits(point.Value))
				}
				count := min(limit-position, 12-within)
				copy(data[written:written+count], record[within:within+count])
				position, written = position+count, written+count
			}
		}
	}
	if written != len(data) {
		return written, io.EOF
	}
	return written, nil
}

func scratchClassicFixture(t testing.TB) (*continuationFile, mapContinuationIO, store.Metadata) {
	t.Helper()
	rng := rand.New(rand.NewSource(41))
	retentions := []store.Retention{{Step: 60, Count: 20160}, {Step: 1800, Count: 17521}}
	archives := make([]map[int]store.Point, len(retentions))
	bits := []uint64{0, 1 << 63, 1, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000042, 0x7ff0000000000001}
	for ai, r := range retentions {
		archives[ai] = map[int]store.Point{}
		for slot := 0; slot < r.Count; slot++ {
			if slot%7 == 0 || (slot/128)%9 == 0 {
				continue
			}
			ts := int64(1790000000 + slot*r.Step)
			if slot%97 == 0 {
				ts = int64(rng.Uint64())
			}
			v := rng.Uint64()
			if slot%3 == 0 {
				v = bits[slot%len(bits)]
			}
			archives[ai][slot] = store.Point{Timestamp: ts, Value: math.Float64frombits(v)}
		}
	}
	kv, m := placedClassicImage(t, retentions, archives)
	fs := newContinuationFiles(kv, true)
	in, err := fs.OpenFile(fs.main(), os.O_RDONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	return in.(*continuationFile), kv, m
}

func TestClassicReadScratchDifferential(t *testing.T) {
	f, _, _ := scratchClassicFixture(t)
	rng := rand.New(rand.NewSource(73))
	for i := 0; i < 600; i++ {
		offset := int64(rng.Intn(f.data.layout.Size+20)) - 5
		size := rng.Intn(65537)
		if i == 0 {
			offset = 0
			size = f.data.layout.Size + 1
		}
		want, got := make([]byte, size), make([]byte, size)
		wn, we := f.readAtAllocatingOracle(want, offset)
		gn, ge := f.ReadAt(got, offset)
		if wn != gn || fmt.Sprint(we) != fmt.Sprint(ge) || !bytes.Equal(want, got) {
			t.Fatalf("case %d offset %d len %d: %d/%v != %d/%v", i, offset, size, wn, we, gn, ge)
		}
	}
}

func TestClassicReadScratchSnapshotDifferential(t *testing.T) {
	f, kv, m := scratchClassicFixture(t)
	raw := make([]byte, f.data.layout.Size)
	if _, err := f.readAtAllocatingOracle(raw, 0); err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/oracle.wsp"
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	w, err := whisper.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	want, err := continuationArchiveSnapshot(context.Background(), w, m)
	if err != nil {
		t.Fatal(err)
	}
	got, err := (&ContinuationCodec{}).Snapshot(context.Background(), kv, m, classicTestClock)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EqualSnapshots(want, got); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkClassicReadScratch(b *testing.B) {
	f, _, _ := scratchClassicFixture(b)
	data := make([]byte, f.data.layout.Size)
	for _, old := range []bool{true, false} {
		b.Run(fmt.Sprintf("oracle=%t", old), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				var err error
				if old {
					_, err = f.readAtAllocatingOracle(data, 0)
				} else {
					_, err = f.ReadAt(data, 0)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Preserve every other filesystem capability (layout and extents included),
// substituting only the frozen ReadAt implementation.
type allocatingOracleFS struct{ *continuationFiles }
type allocatingOracleFile struct{ *continuationFile }

func (fs allocatingOracleFS) OpenFile(name string, flag int, mode os.FileMode) (whisper.StorageFile, error) {
	f, err := fs.continuationFiles.OpenFile(name, flag, mode)
	if err != nil {
		return nil, err
	}
	return allocatingOracleFile{f.(*continuationFile)}, nil
}
func (f allocatingOracleFile) ReadAt(p []byte, off int64) (int, error) {
	return f.readAtAllocatingOracle(p, off)
}

func snapshotAllocatingOracle(ctx context.Context, kv store.ContinuationIO, m store.Metadata, clock func() time.Time) (store.Snapshot, error) {
	fs := newContinuationFiles(newContinuationOverlay(kv), false).withClock(clock)
	options := continuationOptions(fs, func() time.Time { return time.Unix(0, 0) })
	options.FileSystem = allocatingOracleFS{fs}
	w, err := whisper.OpenWithOptions(fs.main(), options)
	if err != nil {
		return store.Snapshot{}, err
	}
	defer w.Close()
	if w.IsCompressed() {
		if err := w.MergeOutOfOrder(); err != nil {
			return store.Snapshot{}, err
		}
	}
	return continuationArchiveSnapshot(ctx, w, m)
}

func BenchmarkClassicScratchSnapshot(b *testing.B) {
	_, kv, m := scratchClassicFixture(b)
	for _, old := range []bool{true, false} {
		b.Run(fmt.Sprintf("oracle=%t", old), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var err error
				if old {
					_, err = snapshotAllocatingOracle(context.Background(), kv, m, classicTestClock)
				} else {
					_, err = (&ContinuationCodec{}).Snapshot(context.Background(), kv, m, classicTestClock)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Optional private transfer fixtures are never part of the repository.
func TestClassicScratchCapturedSnapshots(t *testing.T) {
	root := os.Getenv("IMPORT_SCRATCH_FIXTURES")
	if root == "" {
		t.Skip("optional transfer fixtures")
	}
	paths, err := filepath.Glob(filepath.Join(root, "*.wire"))
	if err != nil || len(paths) == 0 {
		t.Fatal("no fixtures", err)
	}
	n := 0
	for _, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		records, err := store.ReadTransfer(f, 64<<20)
		f.Close()
		if err != nil {
			t.Fatal(err)
		}
		for _, record := range records {
			if record.Version.ID == 0 {
				continue
			}
			kv := mapContinuationIO{}
			for _, item := range record.Keys {
				kv[string(item.Key)] = item.Value
			}
			m := store.Metadata{MetricConfig: record.Config, Continuation: true}
			want, err := snapshotAllocatingOracle(context.Background(), kv, m, classicTestClock)
			if err != nil {
				t.Fatal(err)
			}
			got, err := (&ContinuationCodec{}).Snapshot(context.Background(), kv, m, classicTestClock)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.EqualSnapshots(want, got); err != nil {
				t.Fatal(err)
			}
			n++
		}
	}
	t.Logf("%d captured snapshots bit-exact", n)
	if n == 0 {
		t.Fatal("no snapshots compared")
	}
}

func TestClassicReadScratchReferencedAndBuffered(t *testing.T) {
	ctx := context.Background()
	m := store.Metadata{MetricConfig: store.MetricConfig{Name: "scratch.references", Retentions: []store.Retention{{Step: 60, Count: 512}}, AggregationMethod: store.Sum}}
	points := make([]store.Point, 512)
	for i := range points {
		points[i] = store.Point{Timestamp: 1793969280 + int64(i)*60, Value: float64(i)}
	}
	data, err := store.EncodeSealedPoints(points, 60)
	if err != nil {
		t.Fatal(err)
	}
	part := splicePart{fragment: spliceFragment{first: points[0].Timestamp, count: 512, payload: &splicePayload{data: data, sealed: true}}, slot: int(points[0].Timestamp / 60 % 512), count: 512}
	keys, err := (&ContinuationCodec{}).writeReferencedArchives(ctx, m, [][]splicePart{{part}}, []int64{points[0].Timestamp}, &store.NativeCombineClass{}, classicTestClock)
	if err != nil {
		t.Fatal(err)
	}
	kv := mapContinuationIO{}
	for _, item := range keys {
		kv[string(item.Key)] = item.Value
	}
	fs := newContinuationFiles(kv, false)
	d, err := fs.load("main.wsp")
	if err != nil {
		t.Fatal(err)
	}
	f := &continuationFile{fs: fs, data: d}
	check := func() {
		want, got := make([]byte, d.layout.Size), make([]byte, d.layout.Size)
		wn, we := f.readAtAllocatingOracle(want, 0)
		gn, ge := f.ReadAt(got, 0)
		if wn != gn || fmt.Sprint(we) != fmt.Sprint(ge) || !bytes.Equal(want, got) {
			t.Fatal("reference/buffer byte mismatch")
		}
	}
	check()
	fs.kv = &continuationPointBuffer{ContinuationIO: kv}
	var changed store.PointBlock
	changed.Present[0] = 1
	changed.Points[0] = store.Point{Timestamp: points[0].Timestamp, Value: math.Float64frombits(0x7ff8000000000042)}
	if err := f.setPointBlock(continuationBodyKey(d.id, 'p', 0, 0, 1), &changed); err != nil {
		t.Fatal(err)
	}
	check()
	if err := f.setPointBlock(continuationBodyKey(d.id, 'p', 0, 0, 0), &store.PointBlock{}); err != nil {
		t.Fatal(err)
	}
	check()
	// A buffered ordinary classic image follows the same owned-block path.
	ordinary, ordinaryKV, _ := scratchClassicFixture(t)
	ordinary.fs.kv = &continuationPointBuffer{ContinuationIO: ordinaryKV}
	want, got := make([]byte, ordinary.data.layout.Size), make([]byte, ordinary.data.layout.Size)
	wn, we := ordinary.readAtAllocatingOracle(want, 0)
	gn, ge := ordinary.ReadAt(got, 0)
	if wn != gn || fmt.Sprint(we) != fmt.Sprint(ge) || !bytes.Equal(want, got) {
		t.Fatal("buffer byte mismatch")
	}
}
