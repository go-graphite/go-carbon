package recovery

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/klauspost/compress/zstd"

	"github.com/go-graphite/go-carbon/points"
)

// smallBlocks lowers the block limits so segments write several blocks.
func smallBlocks(t *testing.T, records, names int) {
	t.Helper()
	r, n := blockRecordBytes, blockNameBytes
	blockRecordBytes, blockNameBytes = records, names
	t.Cleanup(func() { blockRecordBytes, blockNameBytes = r, n })
}

// randomName returns a canonical metric name sharing a random prefix with
// earlier ones, from a few to more than a kilobyte long.
func randomName(r *rand.Rand, earlier []string) string {
	var b strings.Builder
	if len(earlier) > 0 && r.IntN(4) != 0 {
		prev := earlier[r.IntN(len(earlier))]
		parts := strings.Split(prev, ".")
		b.WriteString(strings.Join(parts[:1+r.IntN(len(parts))], "."))
	} else {
		b.WriteString(fmt.Sprintf("r%d", r.IntN(30)))
	}
	for range 1 + r.IntN(4) {
		b.WriteByte('.')
		n := 1 + r.IntN(12)
		if r.IntN(40) == 0 {
			n = 300 + r.IntN(600) // past the lookup's stack buffer, together
		}
		for range n {
			b.WriteByte("abcxyz019_-:"[r.IntN(12)])
		}
	}
	return b.String()
}

type blocksCase struct {
	names   []string
	records map[string][][]points.Point // per metric, its records in order
	segment map[string]int
}

func randomBlocksCase(r *rand.Rand, metrics, segments int) *blocksCase {
	c := &blocksCase{records: map[string][][]points.Point{}, segment: map[string]int{}}
	for len(c.names) < metrics {
		name := randomName(r, c.names)
		if _, ok := c.records[name]; ok {
			continue
		}
		c.names = append(c.names, name)
		c.segment[name] = r.IntN(segments)
		for range 1 + r.IntN(3)*r.IntN(2) { // in-flight batches, then queued points
			c.records[name] = append(c.records[name], randomSeries(r, 1+r.IntN(30)))
		}
	}
	return c
}

// fill writes segment seg's records in a scrambled order that keeps each
// metric's records in order, as the cache's map order does.
func (c *blocksCase) fill(seg int, out *Segment) error {
	var order []string
	for _, name := range c.names {
		if c.segment[name] == seg {
			order = append(order, name)
		}
	}
	r := rand.New(rand.NewPCG(uint64(seg), 1))
	r.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
	next := map[string]int{}
	for left := len(order); left > 0; {
		name := order[r.IntN(len(order))]
		i := next[name]
		if i == len(c.records[name]) {
			continue
		}
		next[name]++
		if i+1 == len(c.records[name]) {
			left--
		}
		data := c.records[name][i]
		var err error
		if r.IntN(2) == 0 {
			err = out.WritePoints(&points.Points{Metric: name, Data: data})
		} else {
			err = out.WriteCompact(name, AppendCompactPoints(nil, data), len(data))
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *blocksCase) want(name string) []points.Point {
	var all []points.Point
	for _, rec := range c.records[name] {
		all = append(all, rec...)
	}
	return all
}

// writeBlocksCheckpoint writes c as a stop does with a compact input file
// holding wal, and publishes it with a GCPOINT3 index. It returns the dump's
// descriptor.
func writeBlocksCheckpoint(t *testing.T, dir, root string, c *blocksCase, segments int, wal map[string][]points.Point) File {
	return writeBlocksCheckpointIndex(t, dir, root, c, segments, wal, WriteBlocksIndex)
}

// writeBlocksCheckpointIndex is writeBlocksCheckpoint with the index writer
// writeIndex: WriteBlocksIndex or WriteCompactIndex, which blocks dumps also
// accept.
func writeBlocksCheckpointIndex(t *testing.T, dir, root string, c *blocksCase, segments int, wal map[string][]points.Point,
	writeIndex func(string, *Builder) (File, error)) File {
	t.Helper()
	builder := NewBuilder(func(name string) bool { return !strings.HasPrefix(name, "r1") })
	for _, name := range c.names[:len(c.names)/2] { // warmed with input active
		if _, err := builder.ID(name); err != nil {
			t.Fatal(err)
		}
	}
	dump, err := NewWriterFormat(filepath.Join(dir, CacheName(FormatBlocks, "1.2.bin")), 0, 1<<20, builder, FormatBlocks)
	if err != nil {
		t.Fatal(err)
	}
	input, err := NewWriterFormat(filepath.Join(dir, InputName(FormatCompact, "1.2.bin")), 1, InputFileBuffer, builder, FormatCompact)
	if err != nil {
		t.Fatal(err)
	}
	if err = dump.WriteSegments(segments, 3, c.fill); err != nil {
		t.Fatal(err)
	}
	cacheFile, err := dump.Close()
	if err != nil {
		t.Fatal(err)
	}
	index, err := writeIndex(dir, builder)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range slices.Sorted(func(yield func(string) bool) {
		for n := range wal {
			if !yield(n) {
				return
			}
		}
	}) {
		if err = input.WritePoints(&points.Points{Metric: name, Data: wal[name]}); err != nil {
			t.Fatal(err)
		}
	}
	walFile, err := input.Close()
	if err != nil {
		t.Fatal(err)
	}
	if err = Publish(dir, root, cacheFile, walFile, index, "read-id"); err != nil {
		t.Fatal(err)
	}
	return cacheFile
}

// Random names and records, several blocks per segment: the ordered restore
// and every checkpoint lookup return exactly what was written, and near
// misses of every name are not found.
func TestBlocksRoundTripProperty(t *testing.T) {
	for seed := range uint64(8) {
		index, writeIndex := "v3", WriteBlocksIndex
		if seed%4 == 2 {
			index, writeIndex = "v2", WriteCompactIndex
		}
		t.Run(fmt.Sprint(seed, index), func(t *testing.T) {
			r := rand.New(rand.NewPCG(seed, 99))
			if seed%2 == 1 {
				smallBlocks(t, 1+r.IntN(4096), 1+r.IntN(2048))
			}
			const segments = 7
			c := randomBlocksCase(r, 300+r.IntN(1500), segments)
			wal := map[string][]points.Point{}
			for i, name := range c.names {
				if i%5 == 0 {
					wal[name] = randomSeries(r, 1+r.IntN(3))
				}
			}
			wal["late.metric"] = randomSeries(r, 2)
			dir, root := t.TempDir(), t.TempDir()
			cacheFile := writeBlocksCheckpointIndex(t, dir, root, c, segments, wal, writeIndex)

			restored := map[string][]points.Point{}
			records := map[string]int{}
			err := ReadBlocksFile(filepath.Join(dir, cacheFile.Name), func(p *points.Points) {
				restored[p.Metric] = append(restored[p.Metric], p.Data...)
				records[p.Metric]++
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(restored) != len(c.names) {
				t.Fatal("ordered restore metrics", len(restored), len(c.names))
			}
			for _, name := range c.names {
				if !samePoints(restored[name], c.want(name)) || records[name] != len(c.records[name]) {
					t.Fatal("ordered restore differs", name)
				}
			}

			bundle, err := OpenBundle(dir, root)
			if err != nil {
				t.Fatal(err)
			}
			defer bundle.Close()
			if bundle.CacheFormat() != FormatBlocks || bundle.Metrics() != uint64(len(c.names)+1) {
				t.Fatal("bundle", bundle.CacheFormat(), bundle.Metrics())
			}
			check := func(name string, want []points.Point) {
				slot, ok, err := bundle.Find(name)
				if err != nil || !ok {
					t.Fatal("not found", name, err)
				}
				if got, found, err := bundle.Name(slot); err != nil || !found || got != name {
					t.Fatal("name", name, got, err)
				}
				p, err := bundle.Read(slot)
				if err != nil || p.Metric != name || !samePoints(p.Data, want) {
					t.Fatal("read", name, err)
				}
				var head CompactRecord
				saved, tail, err := bundle.ReadCompact(slot, nil, func(rec CompactRecord) { head = rec })
				if err != nil || string(saved) != name {
					t.Fatal("compact read", name, err)
				}
				got := tail
				if head.N > 0 {
					got, _, _ = decodeCompact(nil, head.Data, head.N, false)
					got = append(got, tail...)
				}
				if !samePoints(got, want) {
					t.Fatal("compact head and tail", name)
				}
				if tail, err = bundle.ReadCompactAs(slot, name, nil, func(CompactRecord) {}); err != nil {
					t.Fatal("compact read as", name, err)
				}
				// Near misses share all but the end, or extend the name.
				for _, miss := range []string{name[:len(name)-1], name + "x", name[:len(name)-1] + "!", "x" + name} {
					if _, ok := c.records[miss]; ok || wal[miss] != nil {
						continue
					}
					if _, ok, err := bundle.Find(miss); ok || err != nil {
						t.Fatal("near miss found", miss, err)
					}
				}
			}
			for _, name := range c.names {
				check(name, append(c.want(name), wal[name]...))
			}
			check("late.metric", wal["late.metric"])
			// Every metric is visited once, under its slot's name.
			visited := map[uint64]bool{}
			err = bundle.VisitMetrics(func(slot uint64, name string) error {
				if got, found, err := bundle.Name(slot); err != nil || !found || got != name || visited[slot] {
					return fmt.Errorf("visit %d %q: %q %v %v", slot, name, got, found, err)
				}
				visited[slot] = true
				return nil
			})
			if err != nil || len(visited) != len(c.names)+1 {
				t.Fatal("visited metrics", len(visited), err)
			}
			var newNames []string
			if err = bundle.NewNames(func(name string) error { newNames = append(newNames, name); return nil }); err != nil {
				t.Fatal(err)
			}
			for _, name := range newNames {
				if !strings.HasPrefix(name, "r1") && name != "late.metric" {
					t.Fatal("known name listed as new", name)
				}
			}
		})
	}
}

// The name table answers lookups for every record, whatever its place in a
// group, and compares without copying exactly as a copied name would.
func TestBlockNameTableLookups(t *testing.T) {
	r := rand.New(rand.NewPCG(5, 6))
	var names []string
	for len(names) < 2000 {
		names = append(names, randomName(r, names))
	}
	slices.Sort(names)
	names = slices.Compact(names)
	// Repeats, as for a metric with several records.
	for i := 0; i < len(names); i += 37 {
		names = slices.Insert(names, i, names[i])
	}
	var b blockScratch
	for _, n := range names {
		b.add(n, []byte{1}, 1)
	}
	enc := newBlockEncoder(1)
	defer enc.Close()
	var blocks [][]byte
	smallBlocks(t, 1<<20, 4000)
	err := b.appendBlocks(enc, func(head []byte, records []blockRef) error {
		block := slices.Clone(head)
		for _, rec := range records {
			block = rec.appendTo(block)
		}
		binary.LittleEndian.PutUint32(block[24:], crc32.Checksum(block[blockHeaderSize:], castagnoli))
		binary.LittleEndian.PutUint32(block[28:], crc32.Checksum(block[:28], castagnoli))
		blocks = append(blocks, block)
		return nil
	})
	if err != nil || len(blocks) < 3 {
		t.Fatal("blocks", len(blocks), err)
	}
	// An index of the records, as the builder would write it.
	cache := bytes.Join(blocks, nil)
	in := &Index{recordCounts: [2]uint64{uint64(len(names))}, recordOffsets: [2]uint64{0}}
	var table []byte
	for at := 0; at < len(cache); {
		h, err := parseBlockHeader(cache[at:])
		if err != nil {
			t.Fatal(err)
		}
		pos := at + blockHeaderSize + h.namesLen
		for range h.records {
			count, k := binary.Uvarint(cache[pos:])
			size, m := binary.Uvarint(cache[pos+k:])
			_ = count
			length := k + m + int(size)
			table = binary.LittleEndian.AppendUint64(table, uint64(pos))
			table = binary.LittleEndian.AppendUint64(table, uint64(length))
			table = binary.LittleEndian.AppendUint64(table, 0)
			pos += length
		}
		at = pos
	}
	in.data = table
	if err := in.loadBlocks(cache); err != nil {
		t.Fatal(err)
	}
	for i, name := range names {
		id := uint64(i + 1)
		if got := string(in.names.appendName([]byte("prefix"), id)); got != "prefix"+name {
			t.Fatal("name", i, got, name)
		}
		if !in.names.is(id, name) {
			t.Fatal("is", i, name)
		}
		for _, other := range []string{names[(i+1)%len(names)], name[:len(name)-1], name + ".a", ""} {
			if in.names.is(id, other) != (other == name) {
				t.Fatal("is other", i, name, other)
			}
		}
	}
}

// A torn or corrupt block is never partly applied: the ordered restore
// visits the records of the blocks before it, then reports it.
func TestBlocksTornOrCorruptFile(t *testing.T) {
	smallBlocks(t, 2000, 1500)
	r := rand.New(rand.NewPCG(7, 8))
	const segments = 3
	c := randomBlocksCase(r, 400, segments)
	dir := t.TempDir()
	dump, err := NewWriterFormat(filepath.Join(dir, CacheName(FormatBlocks, "1.2.bin")), 0, 1<<20, nil, FormatBlocks)
	if err != nil {
		t.Fatal(err)
	}
	if err = dump.WriteSegments(segments, 2, c.fill); err != nil {
		t.Fatal(err)
	}
	file, err := dump.Close()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, file.Name))
	if err != nil {
		t.Fatal(err)
	}
	// Block boundaries and the records each block holds.
	var ends []int
	var counts []int
	for at := 0; at < len(data); {
		h, err := parseBlockHeader(data[at:])
		if err != nil {
			t.Fatal(err)
		}
		at += blockHeaderSize + h.namesLen + h.recordsLen
		ends = append(ends, at)
		counts = append(counts, h.records)
	}
	if len(ends) < 4 || ends[len(ends)-1] != len(data) {
		t.Fatal("blocks", len(ends))
	}
	complete := func(cut int) int {
		n := 0
		for i, end := range ends {
			if end <= cut {
				n += counts[i]
			}
		}
		return n
	}
	for cut := 0; cut < len(data); cut += 1 + r.IntN(97) {
		visited := 0
		err := ReadBlocks(bytes.NewReader(data[:cut]), func(*points.Points) { visited++ })
		atBoundary := cut == 0 || slices.Contains(ends, cut)
		if (err == nil) != atBoundary || visited != complete(cut) {
			t.Fatal("torn at", cut, err, visited, complete(cut))
		}
	}
	for i, end := range ends {
		start := 0
		if i > 0 {
			start = ends[i-1]
		}
		for _, at := range []int{start + 3, start + blockHeaderSize + 1, end - 1} {
			bad := slices.Clone(data)
			bad[at] ^= 0x10
			visited := 0
			if err := ReadBlocks(bytes.NewReader(bad), func(*points.Points) { visited++ }); err == nil || visited != complete(start) {
				t.Fatal("corrupt byte", at, err, visited, complete(start))
			}
		}
	}
}

func TestBlocksHeaderRejectsOverflowingExtents(t *testing.T) {
	for _, field := range []int{8, 12, 16, 20} {
		header := (blockHeader{records: 1, namesRaw: 3, namesLen: 4, recordsLen: 3}).append(nil)
		binary.LittleEndian.PutUint32(header[field:], ^uint32(0))
		binary.LittleEndian.PutUint32(header[28:], crc32.Checksum(header[:28], castagnoli))
		if _, err := parseBlockHeader(header); err == nil {
			t.Fatal("accepted overflowing extent", field)
		}
	}
}

// A checkpoint whose dump was altered and republished with matching
// checksums fails to open rather than serve wrong names; one with a
// non-canonical name never opens.
func TestBlocksCheckpointRejectsAlteredNames(t *testing.T) {
	r := rand.New(rand.NewPCG(9, 10))
	c := randomBlocksCase(r, 200, 2)
	for _, alter := range []string{"names", "canonical", "records"} {
		t.Run(alter, func(t *testing.T) {
			dir, root := t.TempDir(), t.TempDir()
			cc := c
			if alter == "canonical" {
				bad := c.names[0] + "..x"
				cc = &blocksCase{records: map[string][][]points.Point{}, segment: map[string]int{}}
				for k, v := range c.records {
					cc.records[k] = v
				}
				for k, v := range c.segment {
					cc.segment[k] = v
				}
				cc.names = append(slices.Clone(c.names), bad)
				cc.records[bad] = [][]points.Point{randomSeries(r, 2)}
				cc.segment[bad] = 0
			}
			cacheFile := writeBlocksCheckpoint(t, dir, root, cc, 2, nil)
			path := filepath.Join(dir, cacheFile.Name)
			if alter != "canonical" {
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				h, err := parseBlockHeader(data)
				if err != nil {
					t.Fatal(err)
				}
				if alter == "names" {
					// Recompress other names of the same length.
					names, _ := zstdDecode(data[blockHeaderSize : blockHeaderSize+h.namesLen])
					names[len(names)-1] ^= 1
					enc := newBlockEncoder(1)
					z := enc.EncodeAll(names, nil)
					enc.Close()
					data = slices.Concat(data[:blockHeaderSize], z, data[blockHeaderSize+h.namesLen:])
					hh := blockHeader{records: h.records, namesRaw: len(names), namesLen: len(z), recordsLen: h.recordsLen}
					copy(data, hh.append(nil))
				} else {
					// One record's length grows, the next one's shrinks.
					at := blockHeaderSize + h.namesLen
					_, k := binary.Uvarint(data[at:])
					data[at+k]++
				}
				if err = os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
				republish(t, dir, root, path)
			}
			bundle, err := OpenBundle(dir, root)
			if alter == "names" && err == nil {
				// The altered name may still be canonical and sorted: then
				// it opens, and the original name is gone.
				defer bundle.Close()
				missing := 0
				for _, name := range c.names {
					if _, ok, _ := bundle.Find(name); !ok {
						missing++
					}
				}
				if missing != 1 {
					t.Fatal("altered names", missing)
				}
				return
			}
			if err == nil {
				bundle.Close()
				t.Fatal("altered checkpoint opened", alter)
			}
		})
	}
}

func zstdDecode(z []byte) ([]byte, error) {
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	defer dec.Close()
	return dec.DecodeAll(z, nil)
}

// republish describes path again and republishes the checkpoint in dir.
func republish(t *testing.T, dir, root, path string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, manifestName))
	if err != nil {
		t.Fatal(err)
	}
	var m Manifest
	if err = json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if m.Cache, err = Describe(path); err != nil {
		t.Fatal(err)
	}
	if err = Publish(dir, root, m.Cache, m.WAL, m.Index, m.ReadIndexID); err != nil {
		t.Fatal(err)
	}
}

// Version 4 checkpoints go with blocks dumps only; a release that reads up
// to version 3 rejects them, and ignores the dump by its name.
func TestBlocksManifestVersion(t *testing.T) {
	dir, root := t.TempDir(), t.TempDir()
	c := randomBlocksCase(rand.New(rand.NewPCG(1, 1)), 50, 2)
	writeBlocksCheckpoint(t, dir, root, c, 2, nil)
	raw, err := os.ReadFile(filepath.Join(dir, manifestName))
	if err != nil {
		t.Fatal(err)
	}
	var m Manifest
	if err = json.Unmarshal(raw, &m); err != nil || m.Version != 4 {
		t.Fatal("manifest version", m.Version, err)
	}
	for _, v := range []int{1, 2, 3} {
		m.Version = v
		if validManifest(&m) {
			t.Fatal("a blocks dump passes as version", v)
		}
	}
	if CacheFormat(m.Cache.Name) != FormatBlocks || strings.HasPrefix(m.Cache.Name, CompactPrefix) || strings.HasPrefix(m.Cache.Name, "cache.") {
		t.Fatal("blocks dump name", m.Cache.Name)
	}
	if !IsDumpFile(m.Cache.Name) {
		t.Fatal("blocks dump is not a generation file")
	}
	// An unpublished blocks dump of another generation forces the ordered
	// restore, which reads it without any index.
	other := filepath.Join(dir, CacheName(FormatBlocks, "9.9.bin"))
	if err = os.WriteFile(other, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = OpenBundle(dir, root); err == nil || !strings.Contains(err.Error(), "unindexed recovery generation") {
		t.Fatal("extra blocks generation", err)
	}
}

// Dump writers run concurrently with diverted input; the blocks encoder
// shares one zstd encoder between segment workers.
func TestBlocksWriteSegmentsConcurrent(t *testing.T) {
	smallBlocks(t, 4096, 1024)
	r := rand.New(rand.NewPCG(11, 12))
	c := randomBlocksCase(r, 3000, 64)
	dir := t.TempDir()
	dump, err := NewWriterFormat(filepath.Join(dir, CacheName(FormatBlocks, "1.2.bin")), 0, 1<<20, NewConcurrentBuilder(nil, 8), FormatBlocks)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	err = dump.WriteSegments(64, 16, func(seg int, out *Segment) error {
		mu.Lock()
		defer mu.Unlock() // c.fill reads shared maps
		return c.fill(seg, out)
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = dump.Close(); err != nil {
		t.Fatal(err)
	}
	n := 0
	if err = ReadBlocksFile(filepath.Join(dir, CacheName(FormatBlocks, "1.2.bin")), func(*points.Points) { n++ }); err != nil {
		t.Fatal(err)
	}
	want := 0
	for _, recs := range c.records {
		want += len(recs)
	}
	if n != want {
		t.Fatal("records", n, want)
	}
	if err = dump.WritePoints(points.OnePoint("m", 1, 1)); err == nil {
		t.Fatal("serial write to a blocks dump")
	}
	if _, err = NewWriterFormat(filepath.Join(dir, "input.x.bin"), 1, 4096, NewBuilder(nil), FormatBlocks); err == nil {
		t.Fatal("blocks input file")
	}
}

// Borrowing immutable queued points changes neither bytes nor ordering.
// The ordinary API still owns a copy when a caller reuses its scratch.
func TestBlocksBorrowedMatchesOwnedBytes(t *testing.T) {
	smallBlocks(t, 4000, 1500)
	c := randomBlocksCase(rand.New(rand.NewPCG(12, 13)), 400, 7)
	for _, format := range []Format{FormatCompact, FormatBlocks} {
		t.Run(format.String(), func(t *testing.T) {
			write := func(mode int) []byte {
				path := filepath.Join(t.TempDir(), CacheName(format, "1.2.bin"))
				w, err := NewWriterFormat(path, 0, 4096, nil, format)
				if err != nil {
					t.Fatal(err)
				}
				err = w.WriteSegments(7, 3, func(seg int, out *Segment) error {
					for _, name := range c.names {
						if c.segment[name] != seg {
							continue
						}
						for i, data := range c.records[name] {
							raw := AppendCompactPoints(nil, data)
							if mode == 1 || (mode == 2 && i%2 == 0) {
								if err := out.WriteCompactBorrowed(name, raw, len(data)); err != nil {
									return err
								}
							} else {
								if err := out.WriteCompact(name, raw, len(data)); err != nil {
									return err
								}
								clear(raw) // transient encoding scratch is reused immediately
							}
						}
					}
					return nil
				})
				if err == nil {
					_, err = w.Close()
				}
				if err != nil {
					t.Fatal(err)
				}
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				return data
			}
			owned := write(0)
			for _, mode := range []int{1, 2} {
				if got := write(mode); !bytes.Equal(got, owned) {
					t.Fatal("borrowed bytes differ", mode)
				}
			}
			restored := map[string][]points.Point{}
			visit := func(p *points.Points) { restored[p.Metric] = append(restored[p.Metric], p.Data...) }
			var err error
			if format == FormatBlocks {
				err = ReadBlocks(bytes.NewReader(owned), visit)
			} else {
				err = ReadCompact(bytes.NewReader(owned), visit)
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range c.names {
				if !samePoints(restored[name], c.want(name)) {
					t.Fatal("owned scratch was retained", name)
				}
			}
		})
	}
}

// A GCPOINT3 index lists no records: a metric's records are the run named
// alike that ends at its slot's head, even when the run crosses blocks.
func TestBlocksIndexRunsAcrossBlocks(t *testing.T) {
	smallBlocks(t, 64, 64)
	c := &blocksCase{records: map[string][][]points.Point{}, segment: map[string]int{}}
	r := rand.New(rand.NewPCG(3, 3))
	for i, name := range []string{"a.long", "a.many", "b.one", "b.two"} {
		c.names = append(c.names, name)
		n := 1
		if i == 1 {
			n = 40 // in-flight batches of one metric, then its queued points
		}
		for range n {
			c.records[name] = append(c.records[name], randomSeries(r, 1+r.IntN(5)))
		}
	}
	dir, root := t.TempDir(), t.TempDir()
	writeBlocksCheckpoint(t, dir, root, c, 1, nil)
	m := readManifest(t, dir)
	index, err := os.ReadFile(filepath.Join(dir, m.Index.Name))
	if err != nil || string(index[:8]) != magicV3 {
		t.Fatal("index magic", err)
	}
	bundle, err := OpenBundle(dir, root)
	if err != nil {
		t.Fatal(err)
	}
	defer bundle.Close()
	if len(bundle.index.names.blocks) < 5 || bundle.index.names.runs != 4 {
		t.Fatal("blocks and runs", len(bundle.index.names.blocks), bundle.index.names.runs)
	}
	for _, name := range c.names {
		slot, ok, err := bundle.Find(name)
		if err != nil || !ok {
			t.Fatal(name, err)
		}
		p, err := bundle.Read(slot)
		if err != nil || !samePoints(p.Data, c.want(name)) {
			t.Fatal("read", name, err)
		}
	}
}

func TestBlocksIndexHashCollisionChecksMetric(t *testing.T) {
	const slots = 3 // two mapped ids at the GCPOINT3 table's load factor
	names := []string{"target"}
	for i := 0; ; i++ {
		name := fmt.Sprintf("other%d", i)
		if homeV3(xxhash.Sum64String(name), slots) == homeV3(xxhash.Sum64String(names[0]), slots) {
			names = append(names, name)
			break
		}
	}
	c := &blocksCase{names: names, records: map[string][][]points.Point{}, segment: map[string]int{}}
	for i, name := range names {
		c.records[name] = [][]points.Point{{{Timestamp: 1, Value: float64(i)}}}
	}
	dir, root := t.TempDir(), t.TempDir()
	writeBlocksCheckpoint(t, dir, root, c, 1, nil)
	m := readManifest(t, dir)
	index, _ := os.ReadFile(filepath.Join(dir, m.Index.Name))
	cache, _ := os.ReadFile(filepath.Join(dir, m.Cache.Name))
	wal, _ := os.ReadFile(filepath.Join(dir, m.WAL.Name))
	in, err := OpenFormat(index, cache, wal, FormatBlocks)
	if err != nil {
		t.Fatal(err)
	}
	slot := homeV3(xxhash.Sum64String(names[0]), in.slots)
	first, _, _ := in.Name(slot)
	other := names[0]
	if first == other {
		other = names[1]
	}
	// Even equal stored 32-bit hashes must compare the complete metric name.
	binary.LittleEndian.PutUint32(index[headerSize+slot*slotSizeV3:], uint32(xxhash.Sum64String(other)))
	found, ok, err := in.Find(other)
	if err != nil || !ok {
		t.Fatal("collision lookup", ok, err)
	}
	got, err := in.Read(found)
	if err != nil || got.Metric != other || !samePoints(got.Data, c.want(other)) {
		t.Fatal("hash collision merged metrics", got, err)
	}
}

func readManifest(t *testing.T, dir string) Manifest {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, manifestName))
	if err != nil {
		t.Fatal(err)
	}
	var m Manifest
	if err = json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// A GCPOINT3 index altered and republished with matching checksums does
// not open: heads that end no run or end one twice, points that do not add
// up, an id map that does not name its slots.
func TestBlocksIndexRejectsAlteredSlots(t *testing.T) {
	c := &blocksCase{records: map[string][][]points.Point{}, segment: map[string]int{}}
	r := rand.New(rand.NewPCG(4, 4))
	for i := range 60 {
		name := fmt.Sprintf("m.%02d", i)
		c.names = append(c.names, name)
		for range 1 + i%3 {
			c.records[name] = append(c.records[name], randomSeries(r, 2))
		}
	}
	alter := map[string]func(index []byte, slots [][]byte){
		"overflowing shards": func(index []byte, _ [][]byte) {
			offset := uint64(headerSize) + get64(index, 16)*slotSizeV3
			clear(index[offset : offset+idShards*8])
			put64(index[offset:], ^uint64(0), get64(index, 40)+1)
		},
		"head inside its run": func(_ []byte, slots [][]byte) {
			for _, s := range slots {
				if head := binary.LittleEndian.Uint32(s[4:]); head > 1 {
					binary.LittleEndian.PutUint32(s[4:], head-1)
					return
				}
			}
		},
		"head of another slot": func(_ []byte, slots [][]byte) {
			var heads [][]byte
			for _, s := range slots {
				if binary.LittleEndian.Uint32(s[4:]) != 0 {
					heads = append(heads, s)
				}
			}
			copy(heads[0][4:8], heads[1][4:8])
		},
		"points": func(_ []byte, slots [][]byte) {
			for _, s := range slots {
				if n := binary.LittleEndian.Uint32(s[8:]); n != 0 {
					binary.LittleEndian.PutUint32(s[8:], n+1)
					return
				}
			}
		},
		"id": func(_ []byte, slots [][]byte) {
			for _, s := range slots {
				if id := binary.LittleEndian.Uint32(s[12:]); id > 1 {
					binary.LittleEndian.PutUint32(s[12:], id+1000)
					return
				}
			}
		},
	}
	for what, change := range alter {
		t.Run(what, func(t *testing.T) {
			dir, root := t.TempDir(), t.TempDir()
			writeBlocksCheckpoint(t, dir, root, c, 2, nil)
			m := readManifest(t, dir)
			path := filepath.Join(dir, m.Index.Name)
			index, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			n := get64(index, 16)
			slots := make([][]byte, n)
			for i := range slots {
				slots[i] = index[headerSize+uint64(i)*slotSizeV3 : headerSize+uint64(i+1)*slotSizeV3]
			}
			change(index, slots)
			if err = os.WriteFile(path, index, 0600); err != nil {
				t.Fatal(err)
			}
			if m.Index, err = Describe(path); err != nil {
				t.Fatal(err)
			}
			if err = Publish(dir, root, m.Cache, m.WAL, m.Index, m.ReadIndexID); err != nil {
				t.Fatal(err)
			}
			b, err := OpenBundle(dir, root)
			if err == nil {
				// Points that do not add up surface on reading the metric.
				defer b.Close()
				if what != "points" {
					t.Fatal("altered index opened")
				}
				failed := false
				for _, name := range c.names {
					if slot, ok, _ := b.Find(name); ok {
						if _, err := b.Read(slot); err != nil {
							failed = true
						}
					}
				}
				if !failed {
					t.Fatal("altered points read")
				}
			}
		})
	}
}

// Checking only a name's bytes past those it shares with a canonical name
// decides exactly as checking all of them.
func TestCanonicalAfterMatchesCanonical(t *testing.T) {
	r := rand.New(rand.NewPCG(2, 3))
	random := func(n int) string {
		b := make([]byte, n)
		for i := range b {
			b[i] = "ab./\x00"[r.IntN(5)]
		}
		return string(b)
	}
	for range 200000 {
		prev := random(1 + r.IntN(8))
		if !canonical([]byte(prev)) {
			continue
		}
		shared := r.IntN(len(prev) + 1)
		name := prev[:shared] + random(r.IntN(6))
		if name != "" && canonicalAfter([]byte(name), shared) != canonical([]byte(name)) {
			t.Fatalf("%q after %q sharing %d", name, prev, shared)
		}
	}
}
