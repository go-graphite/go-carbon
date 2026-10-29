package recovery

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

func randomSeries(r *rand.Rand, n int) []points.Point {
	data := make([]points.Point, n)
	ts := int64(1_700_000_000 + r.IntN(1000))
	v := float64(r.IntN(1000))
	for i := range data {
		switch r.IntN(4) {
		case 0:
			v += float64(r.IntN(100))
		case 1:
			v = r.Float64() * 1000
		case 2:
			v = math.Float64frombits(r.Uint64())
		}
		step := int64(10)
		if r.IntN(10) == 0 {
			step = int64(r.IntN(100)) - 20
		}
		ts += step
		data[i] = points.Point{Value: v, Timestamp: ts}
	}
	return data
}

func TestCompactCodecRoundTripAndRejectsCorruption(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for range 2000 {
		data := randomSeries(r, 1+r.IntN(50))
		enc := AppendCompactPoints(nil, data)
		got, st, err := decodeCompact(nil, enc, len(data), false)
		if err != nil || !samePoints(got, data) {
			t.Fatalf("round trip: %v", err)
		}
		last := data[len(data)-1]
		if st.Timestamp != last.Timestamp || st.Bits != math.Float64bits(last.Value) {
			t.Fatal("final state", st, last)
		}
		if _, _, err = decodeCompact(nil, enc, len(data), true); err != nil {
			t.Fatal(err)
		}
		// Truncation, extra bytes and wrong counts must fail, not panic.
		for cut := range len(enc) {
			if _, _, err = decodeCompact(got[:0], enc[:cut], len(data), false); err == nil {
				t.Fatal("truncated encoding accepted")
			}
		}
		if _, _, err = decodeCompact(got[:0], append(enc, 0), len(data), false); err == nil {
			t.Fatal("trailing byte accepted")
		}
		if _, _, err = decodeCompact(got[:0], enc, len(data)+1, false); err == nil {
			t.Fatal("short encoding accepted")
		}
	}
	// A header that addresses more than eight value bytes is invalid.
	if _, _, err := decodeCompact(nil, []byte{1 + (7<<3 | 7), 1, 2, 3, 4, 5, 6, 7, 8}, 1, false); err == nil {
		t.Fatal("invalid value code accepted")
	}
}

// compactFixture writes a compact dump and a legacy WAL and publishes them.
func compactFixture(t *testing.T, dir, root string, metrics int) (map[string][]points.Point, File, File) {
	t.Helper()
	r := rand.New(rand.NewPCG(3, 4))
	builder := NewBuilder(func(name string) bool { return name != "metric.3" })
	dump, err := NewWriterFormat(filepath.Join(dir, CacheName(FormatCompact, "1.2.bin")), 0, 1<<20, builder, FormatCompact)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = NewWriterFormat(filepath.Join(dir, "input.x.bin"), 1, 4096, nil, FormatCompact); err == nil {
		t.Fatal("compact input file accepted")
	}
	wal, err := NewWriter(filepath.Join(dir, "input.1.2.bin"), 1, 4096, builder)
	if err != nil {
		t.Fatal(err)
	}
	want := make(map[string][]points.Point)
	err = dump.WriteSegments(8, 4, func(seg int, out *Segment) error {
		for i := seg; i < metrics; i += 8 {
			name := fmt.Sprintf("metric.%d", i)
			data := randomSeries(rand.New(rand.NewPCG(uint64(i), 5)), 1+i%40)
			if i%3 == 0 {
				// An in-flight batch, then the queued points.
				if err := out.WritePoints(&points.Points{Metric: name, Data: data[:1]}); err != nil {
					return err
				}
				data = data[1:]
				if len(data) == 0 {
					continue
				}
			}
			if err := out.WriteCompact(name, AppendCompactPoints(nil, data), len(data)); err != nil {
				return err
			}
			if err := out.WriteRaw(name, []byte{1}, 1); err == nil {
				return fmt.Errorf("legacy raw record accepted in a compact dump")
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := range metrics {
		name := fmt.Sprintf("metric.%d", i)
		want[name] = randomSeries(rand.New(rand.NewPCG(uint64(i), 5)), 1+i%40)
		if r.IntN(2) == 0 {
			p := points.OnePoint(name, float64(i), int64(2_000_000_000+i))
			if err := wal.WritePoints(p); err != nil {
				t.Fatal(err)
			}
			want[name] = append(want[name], p.Data...)
		}
	}
	cacheFile, err := dump.Close()
	if err != nil {
		t.Fatal(err)
	}
	walFile, err := wal.Close()
	if err != nil {
		t.Fatal(err)
	}
	index, err := WriteIndex(dir, builder)
	if err != nil {
		t.Fatal(err)
	}
	if err = Publish(dir, root, cacheFile, walFile, index, "read-id"); err != nil {
		t.Fatal(err)
	}
	return want, cacheFile, walFile
}

func TestCompactDumpServesCheckpointAndOrderedRestore(t *testing.T) {
	dir, root := t.TempDir(), t.TempDir()
	want, cacheFile, _ := compactFixture(t, dir, root, 200)
	raw, err := os.ReadFile(filepath.Join(dir, manifestName))
	if err != nil {
		t.Fatal(err)
	}
	var m Manifest
	if err = json.Unmarshal(raw, &m); err != nil || m.Version != 2 {
		t.Fatal("compact checkpoint manifest version", m.Version, err)
	}
	// A release that knows only version 1 rejects it and restores in order,
	// where it ignores the compact dump by its name.
	m.Version = 1
	if validManifest(&m) {
		t.Fatal("a compact dump passes as a version 1 checkpoint")
	}

	bundle, err := OpenBundle(dir, root)
	if err != nil {
		t.Fatal(err)
	}
	defer bundle.Close()
	if bundle.CacheFormat() != FormatCompact || bundle.Metrics() != uint64(len(want)) {
		t.Fatal("bundle format or metrics", bundle.CacheFormat(), bundle.Metrics())
	}
	for name, data := range want {
		slot, ok, err := bundle.Find(name)
		if err != nil || !ok {
			t.Fatal(name, ok, err)
		}
		p, err := bundle.Read(slot)
		if err != nil || !samePoints(p.Data, data) {
			t.Fatal("read overlay differs", name, err)
		}
		var head CompactRecord
		saved, tail, err := bundle.ReadCompact(slot, nil, func(rec CompactRecord) { head = rec })
		if err != nil || string(saved) != name || head.N == 0 {
			t.Fatal("compact read", name, head.N, err)
		}
		got, _, err := decodeCompact(nil, head.Data, head.N, false)
		if err != nil || !samePoints(append(got, tail...), data) {
			t.Fatal("compact head and tail differ", name, err)
		}
		if err = bundle.RawRecords(slot, func([]byte, int) error { return nil }); err == nil {
			t.Fatal("raw records of a compact checkpoint")
		}
	}

	restored := make(map[string][]points.Point)
	if err = ReadCompactFile(filepath.Join(dir, cacheFile.Name), func(p *points.Points) {
		restored[p.Metric] = append(restored[p.Metric], p.Data...)
	}); err != nil {
		t.Fatal(err)
	}
	if err = points.ReadFromFile(filepath.Join(dir, "input.1.2.bin"), func(p *points.Points) {
		restored[p.Metric] = append(restored[p.Metric], p.Data...)
	}); err != nil {
		t.Fatal(err)
	}
	if len(restored) != len(want) {
		t.Fatal("ordered restore metrics differ")
	}
	for name, data := range want {
		if !samePoints(restored[name], data) {
			t.Fatal("ordered restore differs", name)
		}
	}
	data, err := os.ReadFile(filepath.Join(dir, cacheFile.Name))
	if err != nil {
		t.Fatal(err)
	}
	for _, cut := range []int{1, len(data) / 3, len(data) - 1} {
		if err = ReadCompact(bytes.NewReader(data[:cut]), func(*points.Points) {}); err == nil {
			t.Fatal("truncated compact dump accepted", cut)
		}
	}
}

func TestCompactCheckpointRejectsCorruptPoints(t *testing.T) {
	dir, root := t.TempDir(), t.TempDir()
	want, cacheFile, walFile := compactFixture(t, dir, root, 20)
	path := filepath.Join(dir, cacheFile.Name)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Corrupt one point's header past the first record, keeping lengths, and
	// republish with matching checksums: the checkpoint opens, and reading
	// that metric fails instead of returning wrong points.
	nameLength, k := binary.Varint(data)
	name := data[k : k+int(nameLength)]
	_, m := binary.Varint(data[k+int(nameLength):])
	_, l := binary.Uvarint(data[k+int(nameLength)+m:])
	body := data[k+int(nameLength)+m+l:]
	body[0] = 0x7f
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if cacheFile, err = Describe(path); err != nil {
		t.Fatal(err)
	}
	var index File
	for _, e := range mustReadDir(t, dir) {
		if filepath.Ext(e) == ".bin" && len(e) > 15 && e[:15] == ".pending-index-" {
			index, _ = Describe(filepath.Join(dir, e))
		}
	}
	if err = Publish(dir, root, cacheFile, walFile, index, "read-id"); err != nil {
		t.Fatal(err)
	}
	bundle, err := OpenBundle(dir, root)
	if err != nil {
		t.Fatal(err)
	}
	defer bundle.Close()
	slot, ok, err := bundle.Find(string(name))
	if err != nil || !ok {
		t.Fatal(err)
	}
	if _, err = bundle.Read(slot); err == nil {
		t.Fatal("corrupt compact points read", len(want))
	}
	if _, _, err = bundle.ReadCompact(slot, nil, func(CompactRecord) {}); err == nil {
		t.Fatal("corrupt compact points claimed")
	}
}

// samePoints compares values by their bits, so NaN equals itself.
func samePoints(a, b []points.Point) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Timestamp != b[i].Timestamp || math.Float64bits(a[i].Value) != math.Float64bits(b[i].Value) {
			return false
		}
	}
	return true
}

func mustReadDir(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}
