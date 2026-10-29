package recovery

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

// compactCheckpoint writes a compact checkpoint as a stop does: the cache
// dump, input diverted before and after the index is written, then the
// input file closed and the checkpoint published. It returns the expected
// points by metric and the names absent from the read index.
func compactCheckpoint(t *testing.T, dir, root string) (map[string][]points.Point, map[string]bool) {
	t.Helper()
	known := func(name string) bool { return !bytes.HasPrefix([]byte(name), []byte("new.")) }
	builder := NewBuilder(known)
	want := map[string][]points.Point{}
	// Warmed names: some leave the cache before the dump.
	for i := range 300 {
		if _, err := builder.ID(fmt.Sprintf("metric.%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	dump, err := NewWriterFormat(filepath.Join(dir, CacheName(FormatCompact, "1.2.bin")), 0, 1<<20, builder, FormatCompact)
	if err != nil {
		t.Fatal(err)
	}
	wal, err := NewWriterFormat(filepath.Join(dir, InputName(FormatCompact, "1.2.bin")), 1, InputFileBuffer, builder, FormatCompact)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = NewWriterFormat(filepath.Join(dir, "input-compact.x.bin"), 1, 4096, nil, FormatCompact); err == nil {
		t.Fatal("a compact input file without a builder")
	}
	var mu sync.Mutex
	divert := func(workers, from, to int, prefix string, ts int64) {
		var wg sync.WaitGroup
		for w := range workers {
			wg.Go(func() {
				for i := from + w; i < to; i += workers {
					name := fmt.Sprintf("%s.%d", prefix, i)
					p := points.OnePoint(name, float64(i)+0.5, ts).Add(float64(i)*3, ts+1)
					mu.Lock() // one metric's points keep the order of this lock
					if err := wal.WritePoints(p); err != nil {
						t.Error(err)
					}
					want[name] = append(want[name], p.Data...)
					mu.Unlock()
				}
			})
		}
		wg.Wait()
	}
	divert(4, 0, 250, "metric", 2_000_000_000) // during the dump
	err = dump.WriteSegments(4, 2, func(seg int, out *Segment) error {
		for i := seg; i < 200; i += 4 { // metric.200-299 left the cache
			name := fmt.Sprintf("metric.%d", i)
			data := randomSeries(rand.New(rand.NewPCG(uint64(i), 9)), 1+i%20)
			if err := out.WriteCompact(name, AppendCompactPoints(nil, data), len(data)); err != nil {
				return err
			}
			mu.Lock()
			want[name] = append(slices.Clone(data), want[name]...)
			mu.Unlock()
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	cacheFile, err := dump.Close()
	if err != nil {
		t.Fatal(err)
	}
	divert(3, 0, 50, "new", 2_000_000_010) // new metrics before the index
	index, err := WriteCompactIndex(dir, builder)
	if err != nil {
		t.Fatal(err)
	}
	divert(4, 100, 280, "metric", 2_000_000_020) // after the index: no new work
	divert(2, 40, 90, "new", 2_000_000_030)      // late metrics, unknown to it
	walFile, err := wal.Close()
	if err != nil {
		t.Fatal(err)
	}
	if err = wal.WritePoints(points.OnePoint("metric.1", 1, 1)); err == nil {
		t.Fatal("write after close")
	}
	if err = Publish(dir, root, cacheFile, walFile, index, "read-id"); err != nil {
		t.Fatal(err)
	}
	isNew := map[string]bool{}
	for name := range want {
		if !known(name) {
			isNew[name] = true
		}
	}
	return want, isNew
}

func TestCompactInputCheckpoint(t *testing.T) {
	dir, root := t.TempDir(), t.TempDir()
	want, isNew := compactCheckpoint(t, dir, root)
	bundle, err := OpenBundle(dir, root)
	if err != nil {
		t.Fatal(err)
	}
	defer bundle.Close()
	var total uint64
	for name, data := range want {
		total += uint64(len(data))
		slot, ok, err := bundle.Find(name)
		if err != nil || !ok {
			t.Fatal("not found", name, err)
		}
		if got, found, err := bundle.Name(slot); err != nil || !found || got != name {
			t.Fatal("name", name, got, err)
		}
		p, err := bundle.Read(slot)
		if err != nil || !samePoints(p.Data, data) || bundle.Count(slot) != uint64(len(data)) {
			t.Fatal("read overlay", name, err, len(p.Data), len(data))
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
		if !samePoints(got, data) {
			t.Fatal("claim history", name)
		}
	}
	// Warmed names that left the cache and got no input are not metrics.
	if _, ok, _ := bundle.Find("metric.299"); ok {
		t.Fatal("a name without points found")
	}
	if bundle.Metrics() != uint64(len(want)) || bundle.Points() != total {
		t.Fatal("totals", bundle.Metrics(), len(want), bundle.Points(), total)
	}
	var newNames []string
	if err = bundle.NewNames(func(name string) error { newNames = append(newNames, name); return nil }); err != nil {
		t.Fatal(err)
	}
	slices.Sort(newNames)
	var wantNew []string
	for name := range isNew {
		wantNew = append(wantNew, name)
	}
	slices.Sort(wantNew)
	if !slices.Equal(newNames, wantNew) {
		t.Fatal("new names", len(newNames), len(wantNew))
	}
	// Every slot visits a metric once.
	seen := 0
	for slot := range bundle.Slots() {
		if name, found, err := bundle.Name(slot); err != nil {
			t.Fatal(err)
		} else if found && bundle.Count(slot) > 0 {
			if _, ok := want[name]; !ok {
				t.Fatal("unexpected metric", name)
			}
			seen++
		}
	}
	if seen != len(want) {
		t.Fatal("slots", seen, len(want))
	}

	// The input file restores without the index, in order per metric.
	restored := map[string][]points.Point{}
	visit := func(p *points.Points) { restored[p.Metric] = append(restored[p.Metric], p.Data...) }
	if err = ReadCompactFile(filepath.Join(dir, CacheName(FormatCompact, "1.2.bin")), visit); err != nil {
		t.Fatal(err)
	}
	input := filepath.Join(dir, InputName(FormatCompact, "1.2.bin"))
	if err = ReadCompactInputFile(input, visit); err != nil {
		t.Fatal(err)
	}
	if len(restored) != len(want) {
		t.Fatal("ordered restore metrics", len(restored), len(want))
	}
	for name, data := range want {
		if !samePoints(restored[name], data) {
			t.Fatal("ordered restore", name)
		}
	}
	// A torn file restores its complete frames and reports the tear.
	data, err := os.ReadFile(input)
	if err != nil {
		t.Fatal(err)
	}
	for _, cut := range []int{len(data) - 1, len(data) / 2, 10} {
		if err = ReadCompactInput(bytes.NewReader(data[:cut]), func(*points.Points) {}); err == nil {
			t.Fatal("torn input file accepted", cut)
		}
	}
	if TailName(filepath.Base(input)) != "input.1.2.tail.bin" || !IsTail(TailName(filepath.Base(input))) {
		t.Fatal("tail name", TailName(filepath.Base(input)))
	}
}

// The radix sort's scratch must never be the rows the caller hands back to
// its partition, whatever the number of passes, or a later frame sorts rows
// into themselves.
func TestSortWalRowsKeepsScratchApart(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	var tmp []walRow
	for _, maxID := range []uint32{100, 1 << 20, 1 << 30} { // one, two and three passes
		for range 3 {
			rows := make([]walRow, 5000)
			for i := range rows {
				rows[i] = walRow{id: r.Uint32N(maxID), ts: int64(i)}
			}
			want := slices.Clone(rows)
			slices.SortStableFunc(want, func(a, b walRow) int { return cmp.Compare(a.id, b.id) })
			var sorted []walRow
			sorted, tmp = sortWalRows(rows, tmp)
			if &tmp[0] == &rows[0] {
				t.Fatal("scratch is the caller's buffer", maxID)
			}
			if !slices.Equal(sorted, want) {
				t.Fatal("not stably sorted", maxID)
			}
		}
	}
}

// Writers race on one partition over many frames, with ids out of order so
// every frame is sorted: each metric still restores all its points in order.
func TestCompactInputManyFramesOnePartition(t *testing.T) {
	dir := t.TempDir()
	builder := NewBuilder(func(string) bool { return true })
	var names []string
	for i := 0; len(names) < 32; i++ {
		name := fmt.Sprintf("metric.%d", i)
		id, err := builder.ID(name)
		if err != nil {
			t.Fatal(err)
		}
		if id%inputPartitions == 0 {
			names = append(names, name)
		}
	}
	slices.Reverse(names) // descending ids: every frame needs sorting
	path := filepath.Join(dir, InputName(FormatCompact, "1.2.bin"))
	wal, err := NewWriterFormat(path, 1, InputFileBuffer, builder, FormatCompact)
	if err != nil {
		t.Fatal(err)
	}
	const writers, rounds = 4, 1100 // about 4 frames
	want := map[string][]points.Point{}
	var wg sync.WaitGroup
	var mu sync.Mutex
	for w := range writers {
		mine := names[w*len(names)/writers : (w+1)*len(names)/writers]
		wg.Go(func() {
			got := map[string][]points.Point{}
			for round := range rounds {
				for i, name := range mine {
					p := points.OnePoint(name, float64(round*1000+i), int64(round)).Add(float64(round), int64(round)+1<<20)
					if err := wal.WritePoints(p); err != nil {
						t.Error(err)
						return
					}
					got[name] = append(got[name], p.Data...)
				}
			}
			mu.Lock()
			maps.Copy(want, got)
			mu.Unlock()
		})
	}
	wg.Wait()
	if _, err = wal.Close(); err != nil {
		t.Fatal(err)
	}
	restored := map[string][]points.Point{}
	if err = ReadCompactInputFile(path, func(p *points.Points) {
		restored[p.Metric] = append(restored[p.Metric], p.Data...)
	}); err != nil {
		t.Fatal(err)
	}
	if len(restored) != len(want) {
		t.Fatal("metrics", len(restored), len(want))
	}
	for name, data := range want {
		if !samePoints(restored[name], data) {
			t.Fatal("restore", name, len(restored[name]), len(data))
		}
	}
}

// Slot and id-map validation runs in parallel ranges; every inconsistency,
// wherever in the table it is, must still reject the index.
func TestCompactIndexRejectsInconsistentSlotsInAnyRange(t *testing.T) {
	dir, root := t.TempDir(), t.TempDir()
	compactCheckpoint(t, dir, root)
	raw, err := os.ReadFile(filepath.Join(dir, manifestName))
	if err != nil {
		t.Fatal(err)
	}
	var m Manifest
	if err = json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	read := func(f File) []byte {
		data, err := os.ReadFile(filepath.Join(dir, f.Name))
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	index, cache, wal := read(m.Index), read(m.Cache), read(m.WAL)
	if _, err = OpenFormat(index, cache, wal, FormatCompact); err != nil {
		t.Fatal(err)
	}
	slots := get64(index, 16)
	slotAt := func(slot uint64) []byte { return index[headerSize+slot*slotSize:][:slotSize] }
	// Occupied slots with and without cache records, from the end of the
	// table, so the last workers' ranges are covered; and the first empty one.
	var withCache, occupied []uint64
	empty := ^uint64(0)
	for slot := slots; slot > 0; slot-- {
		s := slotAt(slot - 1)
		switch {
		case get64(s, 24) == 0:
			empty = slot - 1
		case get64(s, 8) != 0:
			withCache = append(withCache, slot-1)
			occupied = append(occupied, slot-1)
		default:
			occupied = append(occupied, slot-1)
		}
	}
	if len(withCache) < 2 || len(occupied) < 2 || empty == ^uint64(0) || slots < 64 {
		t.Fatal("fixture lacks the slots this test corrupts", slots, len(withCache), len(occupied))
	}
	put := func(dst []byte, offset int, v uint64) { binary.LittleEndian.PutUint64(dst[offset:], v) }
	for name, corrupt := range map[string]func(index []byte){
		"swapped ids": func(index []byte) {
			a, b := slotAt(occupied[0]), slotAt(occupied[1])
			ida, idb := get64(a, 24), get64(b, 24)
			put(a, 24, idb)
			put(b, 24, ida)
		},
		"id map entry": func(index []byte) {
			// The id map lists each id's slot + 1; point the last slot's id at
			// another occupied slot.
			id := uint32(get64(slotAt(occupied[0]), 24) - 1)
			idOffset := uint64(headerSize) + slots*slotSize + get64(index, 8)*recordSize + idShards*8
			var shardIDs [idShards + 1]uint64
			for s := range idShards {
				shardIDs[s+1] = shardIDs[s] + get64(index, idOffset-idShards*8+uint64(s)*8)
			}
			at := idOffset + 4*(shardIDs[id&(idShards-1)]+uint64(id>>shardBits))
			binary.LittleEndian.PutUint32(index[at:], uint32(occupied[1]+1))
		},
		"extra point": func([]byte) {
			s := slotAt(withCache[0])
			put(s, 16, get64(s, 16)+1)
		},
		"overflowing points": func([]byte) {
			a, b := slotAt(withCache[0]), slotAt(withCache[1])
			put(a, 16, get64(a, 16)+(1<<63))
			put(b, 16, get64(b, 16)+(1<<63))
		},
		"empty slot with points": func([]byte) {
			put(slotAt(empty), 16, 1)
		},
		"cache head past records": func([]byte) {
			put(slotAt(withCache[0]), 8, get64(index, 8)+1)
		},
		"overflowing shard counts": func(index []byte) {
			// Keep the wrapped total equal to the number of ids. Individual
			// extents must still be rejected before parallel workers use them.
			at := uint64(headerSize) + slots*slotSize + get64(index, 8)*recordSize
			for shard := range idShards {
				put(index, int(at)+shard*8, 0)
			}
			put(index, int(at), ^uint64(0))
			put(index, int(at)+8, get64(index, 40)+1)
		},
	} {
		t.Run(name, func(t *testing.T) {
			saved := slices.Clone(index)
			defer copy(index, saved)
			corrupt(index)
			if _, err := OpenFormat(index, cache, wal, FormatCompact); err == nil {
				t.Fatal("inconsistent index accepted")
			}
		})
	}
}
