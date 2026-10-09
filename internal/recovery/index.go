// Package recovery indexes existing binary cache/WAL records without changing
// their legacy format. The index is an optional accelerator; the source files
// remain the recovery authority.
package recovery

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"runtime"
	"slices"
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/go-graphite/go-carbon/points"
)

const (
	headerSize = 72
	slotSize   = 32
	recordSize = 24
	magic      = "GCPOINT1"
)

type heads struct {
	cache, wal, count uint64
}
type record struct{ offset, length, previous uint64 }

// Builder records offsets while the ordinary binary dump and input WAL are
// written. Separate chains preserve cache-before-input replay order even when
// the two writers run concurrently and their records interleave.
//
// Metrics are numbered in first-seen order. Prepare hashes and classifies a
// prefix of them without holding the lock, so that work can run while input is
// still being written; Write then handles only the remainder.
type Builder struct {
	mu         sync.Mutex
	ids        map[string]uint32
	names      []string
	heads      []heads
	records    [2][]record
	sizes      [2]uint64
	points     uint64
	newKnown   func() func(string) bool
	workers    int
	prepareMu  sync.Mutex
	hashes     []uint64
	isNew      []bool
	classified int
}

// NewBuilder classifies names serially with known, which need not be safe for
// concurrent use.
func NewBuilder(known func(string) bool) *Builder {
	b := &Builder{ids: make(map[string]uint32), workers: 1}
	if known != nil {
		b.newKnown = func() func(string) bool { return known }
	}
	return b
}

// NewConcurrentBuilder hashes and classifies names on up to workers goroutines.
// newKnown is called once per worker; each returned function is used serially.
func NewConcurrentBuilder(newKnown func() func(string) bool, workers int) *Builder {
	return &Builder{ids: make(map[string]uint32), newKnown: newKnown, workers: max(workers, 1)}
}

// Reserve sizes the tables for an expected number of metrics before writers
// start, so growing them cannot stall the cache dump. It is only a hint.
func (b *Builder) Reserve(metrics int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.names) == 0 && metrics > 0 {
		b.ids = make(map[string]uint32, metrics)
		b.names = make([]string, 0, metrics)
		b.heads = make([]heads, 0, metrics)
		b.records[0] = make([]record, 0, metrics)
	}
}

// Add must be called only after a complete record was successfully written.
func (b *Builder) Add(file int, p *points.Points, size int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if file < 0 || file > 1 || size <= 0 || len(p.Data) == 0 {
		return fmt.Errorf("invalid recovery record")
	}
	id, ok := b.ids[p.Metric]
	if !ok {
		if len(b.names) >= math.MaxUint32 {
			return fmt.Errorf("too many recovery metrics")
		}
		id = uint32(len(b.names))
		b.ids[p.Metric] = id
		b.names = append(b.names, p.Metric)
		b.heads = append(b.heads, heads{})
	}
	h := &b.heads[id]
	previous := h.cache
	if file == 1 {
		previous = h.wal
	}
	b.records[file] = append(b.records[file], record{b.sizes[file], uint64(size), previous})
	b.sizes[file] += uint64(size)
	if file == 0 {
		h.cache = uint64(len(b.records[0]))
	} else {
		h.wal = uint64(len(b.records[1]))
	}
	h.count += uint64(len(p.Data))
	b.points += uint64(len(p.Data))
	return nil
}

// BatchEntry describes one complete record already written by a segment.
type BatchEntry struct {
	Metric string
	Size   int
	Count  int
}

// AddBatch registers consecutive records of one source under a single lock.
func (b *Builder) AddBatch(file int, entries []BatchEntry) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, e := range entries {
		if file < 0 || file > 1 || e.Size <= 0 || e.Count <= 0 {
			return fmt.Errorf("invalid recovery record")
		}
		id, ok := b.ids[e.Metric]
		if !ok {
			if len(b.names) >= math.MaxUint32 {
				return fmt.Errorf("too many recovery metrics")
			}
			id = uint32(len(b.names))
			b.ids[e.Metric] = id
			b.names = append(b.names, e.Metric)
			b.heads = append(b.heads, heads{})
		}
		h := &b.heads[id]
		previous := h.cache
		if file == 1 {
			previous = h.wal
		}
		b.records[file] = append(b.records[file], record{b.sizes[file], uint64(e.Size), previous})
		b.sizes[file] += uint64(e.Size)
		if file == 0 {
			h.cache = uint64(len(b.records[0]))
		} else {
			h.wal = uint64(len(b.records[1]))
		}
		h.count += uint64(e.Count)
		b.points += uint64(e.Count)
	}
	return nil
}

// Prepare hashes and classifies every metric seen so far. Writers may keep
// adding records meanwhile; Write classifies only metrics first seen later.
func (b *Builder) Prepare() {
	b.prepareMu.Lock()
	defer b.prepareMu.Unlock()
	b.mu.Lock()
	start, names := b.classified, b.names[b.classified:]
	b.mu.Unlock()
	if len(names) == 0 {
		return
	}
	hashes, isNew := b.classify(names)
	b.mu.Lock()
	b.hashes = append(b.hashes, hashes...)
	if isNew != nil {
		b.isNew = append(b.isNew, isNew...)
	}
	b.classified = start + len(names)
	b.mu.Unlock()
}

func put64(dst []byte, values ...uint64) {
	for i, value := range values {
		binary.LittleEndian.PutUint64(dst[i*8:], value)
	}
}
func get64(data []byte, offset uint64) uint64 { return binary.LittleEndian.Uint64(data[offset:]) }

// Write serializes a pointer-free open-addressed hash table and record chains.
// Hashes choose slots only: lookup always compares the complete metric bytes.
func (b *Builder) Write(w io.Writer) error {
	write := func(data []byte) error {
		n, err := w.Write(data)
		if err == nil && n != len(data) {
			err = io.ErrShortWrite
		}
		return err
	}
	// Classifying names is optional checkpoint work. Callers Prepare after the
	// authoritative dump is complete, so catalogue lookups never delay it.
	b.Prepare()
	b.prepareMu.Lock()
	defer b.prepareMu.Unlock()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.classified != len(b.names) {
		return fmt.Errorf("recovery metrics added during index write")
	}
	slots := uint64(2)
	for uint64(len(b.names))*4 > slots*3 {
		slots *= 2
	}
	table, newSlots := buildTable(slots, b.hashes, b.heads, b.isNew, b.workers)
	header := make([]byte, headerSize)
	copy(header, magic)
	put64(header[8:], uint64(len(b.records[0])), uint64(len(b.records[1])), slots, uint64(len(b.names)), b.points, b.sizes[0], b.sizes[1], uint64(len(newSlots)))
	if err := write(header); err != nil {
		return err
	}
	if err := write(table); err != nil {
		return err
	}
	batch := make([]byte, 0, 1<<20)
	flush := func(force bool) error {
		if len(batch) == 0 || (!force && len(batch)+recordSize <= cap(batch)) {
			return nil
		}
		err := write(batch)
		batch = batch[:0]
		return err
	}
	for _, records := range b.records {
		for _, r := range records {
			if err := flush(false); err != nil {
				return err
			}
			batch = binary.LittleEndian.AppendUint64(batch, r.offset)
			batch = binary.LittleEndian.AppendUint64(batch, r.length)
			batch = binary.LittleEndian.AppendUint64(batch, r.previous)
		}
	}
	slices.Sort(newSlots)
	for _, slot := range newSlots {
		if err := flush(false); err != nil {
			return err
		}
		batch = binary.LittleEndian.AppendUint64(batch, slot)
	}
	return flush(true)
}

// buildTable places every metric by linear probing. Workers own disjoint slot
// regions and probe only within them; entries that would cross a region end are
// placed afterwards by a serial pass. Any insertion order yields valid probe
// chains, so the result answers lookups exactly like a serial build.
func buildTable(slots uint64, hashes []uint64, hs []heads, isNew []bool, workers int) ([]byte, []uint64) {
	table := make([]byte, slots*slotSize)
	regions := uint64(1)
	for int(regions) < workers && regions*2 <= slots/4096 {
		regions *= 2
	}
	width := slots / regions
	byRegion := make([][]uint32, regions)
	for id, h := range hashes {
		r := (h & (slots - 1)) / width
		byRegion[r] = append(byRegion[r], uint32(id))
	}
	empty := func(slot uint64) bool {
		return get64(table, slot*slotSize+8) == 0 && get64(table, slot*slotSize+16) == 0
	}
	place := func(id uint32, slot uint64) uint64 {
		put64(table[slot*slotSize:], hashes[id], hs[id].cache, hs[id].wal, hs[id].count)
		return slot
	}
	deferred := make([][]uint32, regions)
	found := make([][]uint64, regions)
	var wg sync.WaitGroup
	for r := uint64(0); r < regions; r++ {
		wg.Go(func() {
			hi := (r + 1) * width
			for _, id := range byRegion[r] {
				slot := hashes[id] & (slots - 1)
				for slot < hi && !empty(slot) {
					slot++
				}
				if slot == hi {
					deferred[r] = append(deferred[r], id)
					continue
				}
				if isNew != nil && isNew[id] {
					found[r] = append(found[r], place(id, slot))
				} else {
					place(id, slot)
				}
			}
		})
	}
	wg.Wait()
	var newSlots []uint64
	for r := range found {
		newSlots = append(newSlots, found[r]...)
	}
	for _, ids := range deferred {
		for _, id := range ids {
			slot := hashes[id] & (slots - 1)
			for !empty(slot) {
				slot = (slot + 1) & (slots - 1)
			}
			place(id, slot)
			if isNew != nil && isNew[id] {
				newSlots = append(newSlots, slot)
			}
		}
	}
	return table, newSlots
}

// classify hashes every name and, with a catalogue, marks names it lacks.
// Contiguous chunks keep each worker's lookups independent of the others.
func (b *Builder) classify(names []string) ([]uint64, []bool) {
	hashes := make([]uint64, len(names))
	var isNew []bool
	if b.newKnown != nil {
		isNew = make([]bool, len(names))
	}
	workers := min(b.workers, max(len(names)/4096, 1))
	chunk := (len(names) + workers - 1) / workers
	var wg sync.WaitGroup
	for start := 0; start < len(names); start += chunk {
		end := min(start+chunk, len(names))
		wg.Go(func() {
			var known func(string) bool
			if isNew != nil {
				known = b.newKnown()
			}
			for i := start; i < end; i++ {
				hashes[i] = xxhash.Sum64String(names[i])
				if known != nil {
					isNew[i] = !known(names[i])
				}
			}
		})
	}
	wg.Wait()
	return hashes, isNew
}

// Index borrows immutable, checksum-validated source/index buffers. Its owner
// must keep their mappings alive for every method call.
type Index struct {
	data                        []byte
	source                      [2][]byte
	slots, metrics, points      uint64
	recordCounts, recordOffsets [2]uint64
	newCount, newOffset         uint64
}

func Open(index, cache, wal []byte) (*Index, error) {
	if len(index) < headerSize || string(index[:8]) != magic {
		return nil, fmt.Errorf("invalid recovery index header")
	}
	in := &Index{data: index, source: [2][]byte{cache, wal}}
	in.recordCounts = [2]uint64{get64(index, 8), get64(index, 16)}
	in.slots, in.metrics, in.points = get64(index, 24), get64(index, 32), get64(index, 40)
	if in.slots < 2 || in.slots&(in.slots-1) != 0 || in.slots > uint64((len(index)-headerSize)/slotSize) || in.metrics > in.slots*3/4 {
		return nil, fmt.Errorf("invalid recovery hash table size")
	}
	if get64(index, 48) != uint64(len(cache)) || get64(index, 56) != uint64(len(wal)) || in.points > uint64(len(cache))/2+uint64(len(wal))/2 {
		return nil, fmt.Errorf("recovery source lengths differ")
	}
	offset := uint64(headerSize) + in.slots*slotSize
	for file, count := range in.recordCounts {
		if count > uint64(len(index)-int(offset))/recordSize {
			return nil, fmt.Errorf("invalid recovery record extent")
		}
		in.recordOffsets[file] = offset
		offset += count * recordSize
	}
	in.newCount, in.newOffset = get64(index, 64), offset
	if in.newCount > in.metrics || in.newCount > uint64(len(index)-int(offset))/8 {
		return nil, fmt.Errorf("invalid recovery new-metric extent")
	}
	offset += in.newCount * 8
	if offset != uint64(len(index)) {
		return nil, fmt.Errorf("trailing recovery index bytes")
	}
	if err := in.validateRecords(); err != nil {
		return nil, err
	}

	var metrics, total uint64
	for slot := uint64(0); slot < in.slots; slot++ {
		_, h := in.slot(slot)
		if h.cache == 0 && h.wal == 0 {
			if h.count != 0 {
				return nil, fmt.Errorf("empty recovery slot has points")
			}
			continue
		}
		if h.cache > in.recordCounts[0] || h.wal > in.recordCounts[1] || h.count == 0 || h.count > in.points-total {
			return nil, fmt.Errorf("invalid recovery slot")
		}
		metrics++
		total += h.count
	}
	if metrics != in.metrics || total != in.points {
		return nil, fmt.Errorf("recovery index totals differ")
	}
	var previous uint64
	for i := uint64(0); i < in.newCount; i++ {
		slot := get64(index, in.newOffset+i*8)
		if slot >= in.slots || (i > 0 && slot <= previous) {
			return nil, fmt.Errorf("invalid new-metric slot")
		}
		_, h := in.slot(slot)
		if h.count == 0 {
			return nil, fmt.Errorf("empty new-metric slot")
		}
		previous = slot
	}
	return in, nil
}

// validateRecords checks independent bounded ranges of the immutable source
// tables. Every range verifies its boundary against the preceding record, so
// parallelism does not weaken the complete contiguous-source requirement.
func (in *Index) validateRecords() error {
	const recordsPerRange = 256 * 1024
	type recordRange struct {
		file        int
		first, last uint64
	}
	var ranges []recordRange
	for file, count := range in.recordCounts {
		if count == 0 && len(in.source[file]) != 0 {
			return fmt.Errorf("unindexed recovery source bytes")
		}
		for first := uint64(1); first <= count; first += recordsPerRange {
			ranges = append(ranges, recordRange{file, first, min(count, first+recordsPerRange-1)})
		}
	}
	if len(ranges) == 0 {
		return nil
	}
	workers := min(8, runtime.GOMAXPROCS(0), len(ranges))
	if workers == 1 {
		for _, r := range ranges {
			if err := in.validateRecordRange(r.file, r.first, r.last); err != nil {
				return err
			}
		}
		return nil
	}
	checks := make([]error, workers)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Go(func() {
			for i := worker; i < len(ranges); i += workers {
				r := ranges[i]
				if err := in.validateRecordRange(r.file, r.first, r.last); err != nil {
					checks[worker] = err
					return
				}
			}
		})
	}
	wg.Wait()
	return errors.Join(checks...)
}

func (in *Index) validateRecordRange(file int, first, last uint64) error {
	sourceSize := uint64(len(in.source[file]))
	end := uint64(0)
	if first > 1 {
		previous := in.record(file, first-1)
		if previous.offset > sourceSize || previous.length > sourceSize-previous.offset {
			return fmt.Errorf("invalid recovery record chain")
		}
		end = previous.offset + previous.length
	}
	for id := first; id <= last; id++ {
		r := in.record(file, id)
		if r.offset != end || r.length == 0 || r.length > sourceSize-end || r.previous >= id {
			return fmt.Errorf("invalid recovery record chain")
		}
		name, err := recordMetric(in.source[file][r.offset : r.offset+r.length])
		if err != nil {
			return err
		}
		// Canonical names must match filesystem spelling before pending reads open.
		if len(name) == 0 || name[0] == '.' || name[len(name)-1] == '.' || bytes.Contains(name, []byte("..")) || bytes.IndexByte(name, '/') >= 0 || bytes.IndexByte(name, 0) >= 0 {
			return fmt.Errorf("pending reads require canonical metric names")
		}
		end += r.length
	}
	if last == in.recordCounts[file] && end != sourceSize {
		return fmt.Errorf("unindexed recovery source bytes")
	}
	return nil
}

func (in *Index) record(file int, id uint64) record {
	offset := in.recordOffsets[file] + (id-1)*recordSize
	return record{get64(in.data, offset), get64(in.data, offset+8), get64(in.data, offset+16)}
}
func (in *Index) slot(slot uint64) (uint64, heads) {
	offset := headerSize + slot*slotSize
	return get64(in.data, offset), heads{cache: get64(in.data, offset+8), wal: get64(in.data, offset+16), count: get64(in.data, offset+24)}
}
func recordMetric(data []byte) ([]byte, error) {
	size, n := binary.Varint(data)
	if n <= 0 || size < 0 || size > points.MB || size > int64(len(data)-n) {
		return nil, fmt.Errorf("invalid recovery metric extent")
	}
	return data[n : n+int(size)], nil
}
func (in *Index) name(h heads) ([]byte, error) {
	file, id := 0, h.cache
	if id == 0 {
		file, id = 1, h.wal
	}
	if id == 0 {
		return nil, nil
	}
	r := in.record(file, id)
	return recordMetric(in.source[file][r.offset : r.offset+r.length])
}

func (in *Index) Find(metric string) (uint64, bool, error) {
	hash := xxhash.Sum64String(metric)
	slot := hash & (in.slots - 1)
	for attempts := uint64(0); attempts < in.slots; attempts++ {
		stored, h := in.slot(slot)
		if h.cache == 0 && h.wal == 0 {
			return 0, false, nil
		}
		if stored == hash {
			name, err := in.name(h)
			if err != nil {
				return 0, false, err
			}
			if string(name) == metric {
				return slot, true, nil
			}
		}
		slot = (slot + 1) & (in.slots - 1)
	}
	return 0, false, fmt.Errorf("recovery hash table has no empty slot")
}

func (in *Index) Read(slot uint64) (*points.Points, error) {
	if slot >= in.slots {
		return nil, fmt.Errorf("invalid recovery slot number")
	}
	_, h := in.slot(slot)
	if h.count == 0 {
		return nil, nil
	}
	if h.count > uint64(math.MaxInt)/16 {
		return nil, fmt.Errorf("recovery metric is too large")
	}
	name, err := in.name(h)
	if err != nil {
		return nil, err
	}
	result := &points.Points{Metric: string(name), Data: make([]points.Point, 0, int(h.count))}
	for file, head := range []uint64{h.cache, h.wal} {
		var chain []uint64
		for id := head; id != 0; id = in.record(file, id).previous {
			chain = append(chain, id)
		}
		for i := len(chain) - 1; i >= 0; i-- {
			r := in.record(file, chain[i])
			raw := in.source[file][r.offset : r.offset+r.length]
			metric, err := recordMetric(raw)
			if err != nil || !bytes.Equal(metric, name) {
				return nil, fmt.Errorf("recovery chain contains another metric")
			}
			var batch points.Points
			if err := batch.UnmarshalBinary(raw); err != nil {
				return nil, err
			}
			result.Data = append(result.Data, batch.Data...)
		}
	}
	if uint64(len(result.Data)) != h.count {
		return nil, fmt.Errorf("recovery metric point count differs")
	}
	return result, nil
}

func (in *Index) Metrics() uint64 { return in.metrics }
func (in *Index) Points() uint64  { return in.points }
func (in *Index) Slots() uint64   { return in.slots }
func (in *Index) Name(slot uint64) (string, bool, error) {
	if slot >= in.slots {
		return "", false, fmt.Errorf("invalid recovery slot number")
	}
	_, h := in.slot(slot)
	if h.count == 0 {
		return "", false, nil
	}
	name, err := in.name(h)
	return string(name), err == nil, err
}

// NewNames enumerates only metrics absent from the saved read index when this
// dump was written. Startup need not search the entire pending metric set.
func (in *Index) NewNames(visit func(string) error) error {
	for i := uint64(0); i < in.newCount; i++ {
		name, _, err := in.Name(get64(in.data, in.newOffset+i*8))
		if err != nil {
			return err
		}
		if err = visit(name); err != nil {
			return err
		}
	}
	return nil
}

func (in *Index) Count(slot uint64) uint64 {
	if slot >= in.slots {
		return 0
	}
	_, h := in.slot(slot)
	return h.count
}
