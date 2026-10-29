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
	"sync/atomic"

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
// Metric ids are allocated per shard of a 256-way map, with entries in pages
// owned by that shard, so concurrent dump segments neither contend on one
// counter nor write neighbouring entries in shared cache lines. Each source
// then appends its records under its own lock. Prepare hashes and classifies
// the metrics seen so far without blocking writers, so that work can overlap
// the dump; Write handles the remainder.
type Builder struct {
	shards     [idShards]idShard
	hint       int
	src        [2]source
	newKnown   func() func(string) bool
	workers    int
	prepareMu  sync.Mutex
	order      []uint32 // classified ids in table order
	hashes     []uint64
	isNew      []bool
	classified [idShards]uint32
}

const (
	idShards   = 256
	shardBits  = 8
	pageBits   = 12
	shardPages = 1 << (32 - shardBits - pageBits)
)

type idShard struct {
	mu    sync.Mutex
	ids   map[string]uint32
	count uint32
	pages [shardPages]atomic.Pointer[entryPage]
}

// entry fields of one source are written only under that source's lock.
type entry struct {
	name                 string
	cache, wal           uint64
	cacheCount, walCount uint64
}
type entryPage [1 << pageBits]entry

type source struct {
	mu      sync.Mutex
	records []record
	size    uint64
	points  uint64
}

// NewBuilder classifies names serially with known, which need not be safe for
// concurrent use.
func NewBuilder(known func(string) bool) *Builder {
	b := &Builder{workers: 1}
	if known != nil {
		b.newKnown = func() func(string) bool { return known }
	}
	return b
}

// NewConcurrentBuilder hashes and classifies names on up to workers goroutines.
// newKnown is called once per worker; each returned function is used serially.
func NewConcurrentBuilder(newKnown func() func(string) bool, workers int) *Builder {
	return &Builder{newKnown: newKnown, workers: max(workers, 1)}
}

// Reserve sizes the tables for an expected number of metrics before writers
// start, so growing them cannot stall the cache dump. It is only a hint.
func (b *Builder) Reserve(metrics int) {
	if metrics <= 0 {
		return
	}
	b.hint = metrics
	b.src[0].mu.Lock()
	if len(b.src[0].records) == 0 {
		b.src[0].records = make([]record, 0, metrics)
	}
	b.src[0].mu.Unlock()
}

// SetWorkers changes how many goroutines later Prepare and Write calls use,
// e.g. once the dump has released its cores.
func (b *Builder) SetWorkers(n int) {
	b.prepareMu.Lock()
	b.workers = max(n, 1)
	b.prepareMu.Unlock()
}

// entry returns the entry of an id returned by ID.
func (b *Builder) entry(id uint32) *entry {
	local := id >> shardBits
	return &b.shards[id&(idShards-1)].pages[local>>pageBits].Load()[local&(1<<pageBits-1)]
}

// ID returns the id of metric, allocating one on first use. It is safe for
// concurrent use. Names can be warmed before any records exist; serialization
// omits identifiers that never acquire source records.
func (b *Builder) ID(metric string) (uint32, error) {
	shard := uint32(xxhash.Sum64String(metric) & (idShards - 1))
	s := &b.shards[shard]
	s.mu.Lock()
	defer s.mu.Unlock()
	if local, ok := s.ids[metric]; ok {
		return local<<shardBits | shard, nil
	}
	if s.ids == nil {
		s.ids = make(map[string]uint32, b.hint/idShards)
	}
	local := s.count
	if local >= 1<<(32-shardBits) {
		return 0, fmt.Errorf("too many recovery metrics")
	}
	page := &s.pages[local>>pageBits]
	if page.Load() == nil {
		page.Store(new(entryPage))
	}
	page.Load()[local&(1<<pageBits-1)].name = metric
	s.ids[metric] = local
	s.count++
	return local<<shardBits | shard, nil
}

// BatchEntry describes one complete record already written by a segment.
type BatchEntry struct {
	ID    uint32
	Size  int
	Count int
	gap   bool // Size bytes that are no record, such as a block's header
}

// Add must be called only after a complete record was successfully written.
func (b *Builder) Add(file int, p *points.Points, size int) error {
	if file < 0 || file > 1 || size <= 0 || len(p.Data) == 0 {
		return fmt.Errorf("invalid recovery record")
	}
	id, err := b.ID(p.Metric)
	if err != nil {
		return err
	}
	return b.AddBatch(file, []BatchEntry{{ID: id, Size: size, Count: len(p.Data)}})
}

// AddBatch registers consecutive records of one source under a single lock.
func (b *Builder) AddBatch(file int, entries []BatchEntry) error {
	if file < 0 || file > 1 {
		return fmt.Errorf("invalid recovery record")
	}
	src := &b.src[file]
	src.mu.Lock()
	defer src.mu.Unlock()
	for _, e := range entries {
		if e.gap && e.Size > 0 {
			src.size += uint64(e.Size)
			continue
		}
		if e.Size <= 0 || e.Count <= 0 {
			return fmt.Errorf("invalid recovery record")
		}
		m := b.entry(e.ID)
		head, count := &m.cache, &m.cacheCount
		if file == 1 {
			head, count = &m.wal, &m.walCount
		}
		src.records = append(src.records, record{src.size, uint64(e.Size), *head})
		src.size += uint64(e.Size)
		*head = uint64(len(src.records))
		*count += uint64(e.Count)
		src.points += uint64(e.Count)
	}
	return nil
}

// Prepare hashes and classifies every metric seen so far. Writers may keep
// adding records meanwhile; Write classifies only metrics first seen later.
func (b *Builder) Prepare() {
	b.prepareMu.Lock()
	defer b.prepareMu.Unlock()
	var counts [idShards]uint32
	for i := range b.shards {
		b.shards[i].mu.Lock()
		counts[i] = b.shards[i].count
		b.shards[i].mu.Unlock()
	}
	var ids []uint32
	for shard := range b.shards {
		for local := b.classified[shard]; local < counts[shard]; local++ {
			ids = append(ids, local<<shardBits|uint32(shard))
		}
	}
	if len(ids) == 0 {
		return
	}
	names := make([]string, len(ids))
	for i, id := range ids {
		names[i] = b.entry(id).name
	}
	hashes, isNew := b.classify(names)
	b.order = append(b.order, ids...)
	b.hashes = append(b.hashes, hashes...)
	if isNew != nil {
		b.isNew = append(b.isNew, isNew...)
	}
	b.classified = counts
}

func put64(dst []byte, values ...uint64) {
	for i, value := range values {
		binary.LittleEndian.PutUint64(dst[i*8:], value)
	}
}
func get64(data []byte, offset uint64) uint64 { return binary.LittleEndian.Uint64(data[offset:]) }

// Write serializes a pointer-free open-addressed hash table and record chains.
// Hashes choose slots only: lookup always compares the complete metric bytes.
// Writers must have finished.
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
	for i := range b.src {
		b.src[i].mu.Lock()
		defer b.src[i].mu.Unlock()
	}
	getHeads := func(k uint32) heads {
		m := b.entry(b.order[k])
		return heads{cache: m.cache, wal: m.wal, count: m.cacheCount + m.walCount}
	}
	// Warmed names may have left cache before the dump. Only actual source
	// records define membership, point counts and pending-name discovery.
	metrics := uint64(0)
	for k := range b.order {
		if getHeads(uint32(k)).count != 0 {
			metrics++
		}
	}
	for i := range b.shards {
		if b.shards[i].count != b.classified[i] {
			return fmt.Errorf("recovery metrics added during index write")
		}
	}
	slots := uint64(2)
	for metrics*4 > slots*3 {
		slots *= 2
	}
	table, newSlots := buildTable(slots, b.hashes, b.isNew, b.workers, tableLayout{
		include: func(k uint32) bool { return getHeads(k).count != 0 },
		fill: func(dst []byte, k uint32) {
			h := getHeads(k)
			put64(dst, b.hashes[k], h.cache, h.wal, h.count)
		},
		occupied: func(slot []byte) bool { return get64(slot, 8) != 0 || get64(slot, 16) != 0 },
	})
	header := make([]byte, headerSize)
	copy(header, magic)
	put64(header[8:], uint64(len(b.src[0].records)), uint64(len(b.src[1].records)), slots, metrics, b.src[0].points+b.src[1].points, b.src[0].size, b.src[1].size, uint64(len(newSlots)))
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
	for i := range b.src {
		for _, r := range b.src[i].records {
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

// buildTable places recorded metrics by linear probing. Workers own disjoint slot
// regions and probe only within them; entries that would cross a region end are
// placed afterwards by a serial pass. Any insertion order yields valid probe
// chains, so the result answers lookups exactly like a serial build.
// tableLayout says which classified metrics a table holds and how a slot
// is written; occupied tells a written slot from an empty one. placed, if
// set, receives each placed metric's slot plus one.
type tableLayout struct {
	include  func(k uint32) bool
	fill     func(dst []byte, k uint32)
	occupied func(slot []byte) bool
	placed   []uint32
}

func buildTable(slots uint64, hashes []uint64, isNew []bool, workers int, layout tableLayout) ([]byte, []uint64) {
	table := make([]byte, slots*slotSize)
	regions := uint64(1)
	for int(regions) < workers && regions*2 <= slots/4096 {
		regions *= 2
	}
	width := slots / regions
	byRegion := make([][]uint32, regions)
	for id, h := range hashes {
		if !layout.include(uint32(id)) {
			continue
		}
		r := (h & (slots - 1)) / width
		byRegion[r] = append(byRegion[r], uint32(id))
	}
	empty := func(slot uint64) bool {
		return !layout.occupied(table[slot*slotSize : (slot+1)*slotSize])
	}
	place := func(id uint32, slot uint64) uint64 {
		layout.fill(table[slot*slotSize:(slot+1)*slotSize], id)
		if layout.placed != nil {
			layout.placed[id] = uint32(slot) + 1
		}
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
	format                      Format     // of the cache source
	names                       *nameTable // of a FormatBlocks cache source
	v2                          bool       // GCPOINT2 or GCPOINT3: compact input in wal
	v3                          bool       // GCPOINT3: no record table, 16-byte slots
	cachePoints                 uint64
	shardIDs                    [idShards + 1]uint64
	idOffset                    uint64
	wal                         *walOverlay
	newList                     []uint64
	slots, metrics, points      uint64
	recordCounts, recordOffsets [2]uint64
	newCount, newOffset         uint64
}

func Open(index, cache, wal []byte) (*Index, error) {
	return OpenFormat(index, cache, wal, FormatLegacy)
}

// OpenFormat opens an index over a cache source of format f. GCPOINT2 and
// GCPOINT3 indexes go with a compact input file in wal; GCPOINT3 requires
// a blocks cache source.
func OpenFormat(index, cache, wal []byte, f Format) (*Index, error) {
	if len(index) >= headerSize && string(index[:8]) == magicV2 {
		return openV2(index, cache, wal, f)
	}
	if len(index) >= headerSize && string(index[:8]) == magicV3 {
		return openV3(index, cache, wal, f)
	}
	if len(index) < headerSize || string(index[:8]) != magic {
		return nil, fmt.Errorf("invalid recovery index header")
	}
	in := &Index{data: index, source: [2][]byte{cache, wal}, format: f}
	in.recordCounts = [2]uint64{get64(index, 8), get64(index, 16)}
	in.slots, in.metrics, in.points = get64(index, 24), get64(index, 32), get64(index, 40)
	if in.slots < 2 || in.slots&(in.slots-1) != 0 || in.slots > uint64((len(index)-headerSize)/slotSize) || in.metrics > in.slots*3/4 {
		return nil, fmt.Errorf("invalid recovery hash table size")
	}
	// A legacy point takes at least two bytes, a compact one at least one.
	cachePoints := uint64(len(cache)) / 2
	if f.Compact() {
		cachePoints = uint64(len(cache))
	}
	if get64(index, 48) != uint64(len(cache)) || get64(index, 56) != uint64(len(wal)) || in.points > cachePoints+uint64(len(wal))/2 {
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
	if f == FormatBlocks {
		if err := in.loadBlocks(cache); err != nil {
			return nil, err
		}
	}
	if err := in.validateRecords(); err != nil {
		return nil, err
	}

	if err := in.validateSlots(); err != nil {
		return nil, err
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
		if file == 0 && in.names != nil {
			continue // checked with the blocks' names
		}
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
	workers := min(32, runtime.GOMAXPROCS(0), len(ranges))
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

// validateSlots reduces independent table ranges, checking the final sum before
// adding it so a corrupt table cannot hide overflow between workers.
func (in *Index) validateSlots() error {
	const slotsPerWorker = 1 << 20
	workers := min(32, runtime.GOMAXPROCS(0), int((in.slots+slotsPerWorker-1)/slotsPerWorker))
	type totals struct {
		metrics, points uint64
		err             error
	}
	parts := make([]totals, workers)
	check := func(worker int) {
		part := &parts[worker]
		first := in.slots * uint64(worker) / uint64(workers)
		last := in.slots * uint64(worker+1) / uint64(workers)
		for slot := first; slot < last; slot++ {
			_, h := in.slot(slot)
			if h.cache == 0 && h.wal == 0 {
				if h.count != 0 {
					part.err = fmt.Errorf("empty recovery slot has points")
					return
				}
				continue
			}
			if h.cache > in.recordCounts[0] || h.wal > in.recordCounts[1] || h.count == 0 || h.count > in.points-part.points {
				part.err = fmt.Errorf("invalid recovery slot")
				return
			}
			part.metrics++
			part.points += h.count
		}
	}
	if workers == 1 {
		check(0)
	} else {
		var wg sync.WaitGroup
		for worker := range workers {
			wg.Go(func() { check(worker) })
		}
		wg.Wait()
	}
	var metrics, total uint64
	for _, part := range parts {
		if part.err != nil {
			return part.err
		}
		if part.points > in.points-total {
			return fmt.Errorf("recovery index totals differ")
		}
		metrics += part.metrics
		total += part.points
	}
	if metrics != in.metrics || total != in.points {
		return fmt.Errorf("recovery index totals differ")
	}
	return nil
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
	if in.v3 {
		return in.names.record(id)
	}
	offset := in.recordOffsets[file] + (id-1)*recordSize
	return record{get64(in.data, offset), get64(in.data, offset+8), get64(in.data, offset+16)}
}
func (in *Index) slot(slot uint64) (uint64, heads) {
	if in.v2 {
		if slot >= in.slots {
			return 0, heads{count: in.wal.count(slot)}
		}
		if in.v3 {
			at := headerSize + slot*slotSizeV3
			return uint64(binary.LittleEndian.Uint32(in.data[at:])), heads{cache: uint64(binary.LittleEndian.Uint32(in.data[at+4:])),
				count: uint64(binary.LittleEndian.Uint32(in.data[at+8:])) + in.wal.count(slot)}
		}
		offset := headerSize + slot*slotSize
		return get64(in.data, offset), heads{cache: get64(in.data, offset+8), count: get64(in.data, offset+16) + in.wal.count(slot)}
	}
	offset := headerSize + slot*slotSize
	return get64(in.data, offset), heads{cache: get64(in.data, offset+8), wal: get64(in.data, offset+16), count: get64(in.data, offset+24)}
}

// occupied reports whether a table slot ends no probe chain.
func (in *Index) occupied(slot uint64) bool {
	if in.v3 {
		return binary.LittleEndian.Uint32(in.data[headerSize+slot*slotSizeV3+12:]) != 0
	}
	offset := headerSize + slot*slotSize
	if in.v2 {
		return get64(in.data, offset+24) != 0
	}
	return get64(in.data, offset+8) != 0 || get64(in.data, offset+16) != 0
}
func recordMetric(data []byte) ([]byte, error) {
	size, n := binary.Varint(data)
	if n <= 0 || size < 0 || size > points.MB || size > int64(len(data)-n) {
		return nil, fmt.Errorf("invalid recovery metric extent")
	}
	return data[n : n+int(size)], nil
}
func (in *Index) name(slot uint64, h heads) ([]byte, error) {
	return in.appendName(nil, slot, h)
}

// decodes reports whether a slot's name is decoded from a FormatBlocks
// cache dump's names, rather than borrowed from a source.
func (in *Index) decodes(h heads) bool { return in.names != nil && h.cache != 0 }

// appendName appends slot's name to dst if it is decoded (see decodes);
// otherwise it returns the name borrowed from its source.
func (in *Index) appendName(dst []byte, slot uint64, h heads) ([]byte, error) {
	if in.decodes(h) {
		return in.names.appendName(dst, h.cache), nil
	}
	if in.v2 && h.cache == 0 {
		if slot >= in.slots {
			return []byte(in.wal.late[slot-in.slots]), nil
		}
		if name, ok := in.wal.names[slot]; ok {
			return []byte(name), nil
		}
		return nil, nil
	}
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
	slot, stored := hash&(in.slots-1), hash
	if in.v3 {
		slot, stored = homeV3(hash, in.slots), uint64(uint32(hash))
	}
	for attempts := uint64(0); attempts < in.slots; attempts++ {
		if !in.occupied(slot) {
			if in.v2 {
				if late, ok := in.wal.lateAt[metric]; ok {
					return late, true, nil
				}
			}
			return 0, false, nil
		}
		// A compact index also holds names without points: not found.
		if got, h := in.slot(slot); got == stored && h.count != 0 {
			same, err := in.nameIs(slot, h, metric)
			if err != nil {
				return 0, false, err
			}
			if same {
				return slot, true, nil
			}
		}
		if slot++; slot == in.slots {
			slot = 0
		}
	}
	return 0, false, fmt.Errorf("recovery hash table has no empty slot")
}

// RawRecords visits slot's records in replay order (cache chain, then WAL
// chain) as their exact encoded bytes, each with its point count. The bytes
// borrow the mapped source.
func (in *Index) RawRecords(slot uint64, visit func(raw []byte, count int) error) error {
	if slot >= in.Slots() {
		return fmt.Errorf("invalid recovery slot number")
	}
	if in.format != FormatLegacy || in.v2 {
		return fmt.Errorf("raw records of a %v recovery source", in.format)
	}
	_, h := in.slot(slot)
	if h.count == 0 {
		return nil
	}
	var chain []uint64
	for file, head := range []uint64{h.cache, h.wal} {
		chain = chain[:0]
		for id := head; id != 0; id = in.record(file, id).previous {
			chain = append(chain, id)
		}
		for i := len(chain) - 1; i >= 0; i-- {
			r := in.record(file, chain[i])
			raw := in.source[file][r.offset : r.offset+r.length]
			name, err := recordMetric(raw)
			if err != nil {
				return err
			}
			_, n := binary.Varint(raw)
			count, m := binary.Varint(raw[n+len(name):])
			if m <= 0 || count <= 0 {
				return fmt.Errorf("invalid recovery record point count")
			}
			if err = visit(raw, int(count)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (in *Index) Read(slot uint64) (*points.Points, error) {
	return in.read(slot, 0)
}

// ReadForWrite returns detached history with room for the incoming points and
// bounded spare capacity for following writes. A short tail burst must not
// repeatedly copy the entire saved history.
func (in *Index) ReadForWrite(slot uint64, extra int) (*points.Points, error) {
	return in.read(slot, extra)
}

func (in *Index) read(slot uint64, extra int) (*points.Points, error) {
	if slot >= in.Slots() {
		return nil, fmt.Errorf("invalid recovery slot number")
	}
	_, h := in.slot(slot)
	if h.count == 0 {
		return nil, nil
	}
	if h.count > uint64(math.MaxInt)/16 {
		return nil, fmt.Errorf("recovery metric is too large")
	}
	if extra < 0 || uint64(extra) > uint64(math.MaxInt)/16-h.count {
		return nil, fmt.Errorf("invalid recovery append capacity")
	}
	var buf [1024]byte
	name, err := in.appendName(buf[:0], slot, h)
	if err != nil {
		return nil, err
	}
	capacity := int(h.count) + extra
	if extra > 0 {
		capacity += min(int(h.count)/8, 64, math.MaxInt/16-capacity)
	}
	result := &points.Points{Metric: string(name), Data: make([]points.Point, 0, capacity)}
	if result.Data, err = in.appendRecords(result.Data, result.Metric, slot, h, nil); err != nil {
		return nil, err
	}
	return result, nil
}

// appendRecords appends the points of h's records in replay order. With head
// set, a first record in compact format is passed to head instead, still
// encoded, and its points are not appended. Every record must be named
// name; the slot's name is, which the caller checked or read.
func (in *Index) appendRecords(dst []points.Point, name string, slot uint64, h heads, head func(CompactRecord)) ([]points.Point, error) {
	start, total := len(dst), uint64(0)
	var chain []uint64
	for file, first := range []uint64{h.cache, h.wal} {
		chain = chain[:0]
		for id := first; id != 0; id = in.record(file, id).previous {
			chain = append(chain, id)
		}
		for i := len(chain) - 1; i >= 0; i-- {
			r := in.record(file, chain[i])
			raw := in.source[file][r.offset : r.offset+r.length]
			var metric []byte
			var err error
			if file == 0 && in.format.Compact() {
				var n int
				var data []byte
				if in.names != nil {
					// Blocks name records apart from their bytes; the
					// slot is named after its head record.
					if n, data, err = blockBody(raw); err == nil && chain[i] != h.cache && !in.names.is(chain[i], name) {
						err = fmt.Errorf("recovery chain contains another metric")
					}
				} else {
					metric, n, data, err = compactBody(raw)
				}
				if err == nil {
					if head != nil && total == 0 {
						var st CompactState
						if _, st, err = decodeCompact(nil, data, n, true); err == nil {
							head(CompactRecord{Data: data, N: n, State: st})
							total += uint64(n)
						}
					} else {
						before := len(dst)
						dst, _, err = decodeCompact(dst, data, n, false)
						total += uint64(len(dst) - before)
					}
				}
			} else {
				before := len(dst)
				metric, dst, err = points.AppendBinaryPoints(dst, raw)
				total += uint64(len(dst) - before)
			}
			if err != nil {
				return dst[:start], err
			}
			if metric != nil && string(metric) != name {
				return dst[:start], fmt.Errorf("recovery chain contains another metric")
			}
		}
	}
	if in.v2 {
		wal := in.wal.of(slot)
		dst = append(dst, wal...)
		total += uint64(len(wal))
	}
	if total != h.count {
		return dst[:start], fmt.Errorf("recovery metric point count differs")
	}
	return dst, nil
}

// ReadCompact reads slot's saved points for an encoder of the compact format.
// If its first record is compact, head receives it still encoded, borrowing
// the mapped source; the points of every other record are appended to tail.
// It returns the metric name, borrowed too, and the extended tail.
func (in *Index) ReadCompact(slot uint64, tail []points.Point, head func(CompactRecord)) ([]byte, []points.Point, error) {
	if slot >= in.Slots() {
		return nil, tail, fmt.Errorf("invalid recovery slot number")
	}
	_, h := in.slot(slot)
	if h.count == 0 {
		return nil, tail, nil
	}
	if h.count > uint64(math.MaxInt)/16 {
		return nil, tail, fmt.Errorf("recovery metric is too large")
	}
	name, err := in.name(slot, h)
	if err != nil {
		return nil, tail, err
	}
	tail, err = in.appendRecords(tail, string(name), slot, h, head)
	return name, tail, err
}

// ReadCompactAs is ReadCompact of a slot known to hold metric name: found
// by it, or named by Name. It does not read the slot's name again.
func (in *Index) ReadCompactAs(slot uint64, name string, tail []points.Point, head func(CompactRecord)) ([]points.Point, error) {
	if slot >= in.Slots() {
		return tail, fmt.Errorf("invalid recovery slot number")
	}
	_, h := in.slot(slot)
	if h.count == 0 {
		return tail, fmt.Errorf("pending recovery metric has no points")
	}
	if h.count > uint64(math.MaxInt)/16 {
		return tail, fmt.Errorf("recovery metric is too large")
	}
	return in.appendRecords(tail, name, slot, h, head)
}

// nameIs reports whether slot is named metric.
func (in *Index) nameIs(slot uint64, h heads, metric string) (bool, error) {
	if in.decodes(h) {
		return in.names.is(h.cache, metric), nil
	}
	name, err := in.name(slot, h)
	return err == nil && string(name) == metric, err
}

// CacheFormat reports the record format of the cache source.
func (in *Index) CacheFormat() Format { return in.format }

func (in *Index) Metrics() uint64 { return in.metrics }
func (in *Index) Points() uint64  { return in.points }

// Slots bounds slot numbers: the table's, then a compact checkpoint's late
// metrics.
func (in *Index) Slots() uint64 {
	if in.v2 {
		return uint64(len(in.wal.start) - 1)
	}
	return in.slots
}
func (in *Index) Name(slot uint64) (string, bool, error) {
	if slot >= in.Slots() {
		return "", false, fmt.Errorf("invalid recovery slot number")
	}
	_, h := in.slot(slot)
	if h.count == 0 {
		return "", false, nil
	}
	var buf [1024]byte
	name, err := in.appendName(buf[:0], slot, h)
	return string(name), err == nil, err
}

// NewNames enumerates only metrics absent from the saved read index when this
// dump was written. Startup need not search the entire pending metric set.
func (in *Index) NewNames(visit func(string) error) error {
	return in.VisitNewNames(func(name []byte) error { return visit(string(name)) })
}

// VisitNewNames borrows names for the duration of each callback. A visitor can
// reject an unstorable name without copying it; retained names must be copied.
// The visitor must not modify the immutable bytes.
func (in *Index) VisitNewNames(visit func([]byte) error) error {
	return in.visitNewNamesRange(0, in.newCount, visit)
}

func (in *Index) visitNewNamesRange(first, last uint64, visit func([]byte) error) error {
	var buf []byte
	for i := first; i < last; i++ {
		slot := in.newSlot(i)
		_, h := in.slot(slot)
		name, err := in.appendName(buf[:0], slot, h)
		if err != nil {
			return err
		}
		if in.decodes(h) {
			buf = name // decoded into buf, which may have grown
		}
		if err = visit(name); err != nil {
			return err
		}
	}
	return nil
}

// FilterNewNames evaluates a pure, concurrency-safe predicate over borrowed
// names. It must not mutate or retain the bytes. Admitted names are copied,
// then visited serially in the same
// order as NewNames. Bounded waves keep at most 32 batches of names in flight;
// each wave joins every worker before visiting results or returning an error.
func (in *Index) FilterNewNames(keep func([]byte) bool, visit func(string) error) error {
	const batchSize = 512
	workers := min(32, runtime.GOMAXPROCS(0), int((in.newCount+batchSize-1)/batchSize))
	if workers <= 1 {
		return in.VisitNewNames(func(name []byte) error {
			if keep(name) {
				return visit(string(name))
			}
			return nil
		})
	}
	type batch struct {
		names []string
		err   error
	}
	results := make([]batch, workers)
	for first := uint64(0); first < in.newCount; first += uint64(workers * batchSize) {
		var wg sync.WaitGroup
		count := min(workers, int((in.newCount-first+batchSize-1)/batchSize))
		for worker := range count {
			wg.Go(func() {
				r := &results[worker]
				r.names = r.names[:0]
				lo := first + uint64(worker*batchSize)
				r.err = in.visitNewNamesRange(lo, min(in.newCount, lo+batchSize), func(name []byte) error {
					if keep(name) {
						r.names = append(r.names, string(name))
					}
					return nil
				})
			})
		}
		wg.Wait()
		for _, r := range results[:count] {
			for _, name := range r.names {
				if err := visit(name); err != nil {
					return err
				}
			}
			if r.err != nil {
				return r.err
			}
		}
	}
	return nil
}

func (in *Index) NewNameCount() uint64 { return in.newCount }

// VisitNewSlots calls visit with the slot of every metric absent from the
// saved read index when the dump was written.
func (in *Index) VisitNewSlots(visit func(slot uint64)) {
	for i := uint64(0); i < in.newCount; i++ {
		visit(in.newSlot(i))
	}
}

func (in *Index) newSlot(i uint64) uint64 {
	if in.v2 {
		return in.newList[i]
	}
	return get64(in.data, in.newOffset+i*8)
}

func (in *Index) Count(slot uint64) uint64 {
	if slot >= in.Slots() {
		return 0
	}
	_, h := in.slot(slot)
	return h.count
}
