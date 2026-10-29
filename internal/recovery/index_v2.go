package recovery

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"

	"github.com/klauspost/compress/zstd"

	"github.com/go-graphite/go-carbon/points"
)

// A compact checkpoint index (magic GCPOINT2) goes with a compact cache dump
// and a compact input file. The stop writes it while input still flows into
// the input file: it covers the cache dump and every builder id allocated so
// far, and maps those ids to slots. The input file's rows refer to builder
// ids, so the successor maps them to slots when it opens the checkpoint;
// ids allocated after the index was written are new metrics, which the
// input file names. Nothing in the index depends on input that arrives
// later, so input closes only to complete the input file.
//
//	header      magic, cache records, slots, cache points, cache bytes,
//	            builder ids mapped, new-name slots (72 bytes)
//	slots       hash, cache chain head, cache points, builder id + 1
//	records     cache records, as in GCPOINT1
//	shard ids   ids mapped per builder shard (256 x 8 bytes)
//	id map      slot + 1 of each mapped id, by shard then local id (4 bytes)
//	new slots   slots of names absent from the read index
const magicV2 = "GCPOINT2"

// WriteCompactIndex writes the index of a compact checkpoint while input may
// still be diverted into its compact input file; see magicV2. The cache dump
// must be complete. Like WriteIndex it is not synchronized.
func WriteCompactIndex(dir string, builder *Builder) (File, error) {
	file, err := os.CreateTemp(dir, ".pending-index-*.bin")
	if err != nil {
		return File{}, err
	}
	keep := false
	defer func() {
		_ = file.Close()
		if !keep {
			_ = os.Remove(file.Name())
		}
	}()
	buffer := newSink(file, 1<<20)
	if err = builder.writeCompact(buffer); err != nil {
		return File{}, err
	}
	if err = buffer.Flush(); err != nil {
		return File{}, err
	}
	info, err := file.Stat()
	if err != nil {
		return File{}, err
	}
	result := buffer.descriptor(filepath.Base(file.Name()), info.Size())
	if err = file.Close(); err != nil {
		return File{}, err
	}
	keep = true
	return result, nil
}

func (b *Builder) writeCompact(w io.Writer) error {
	write := func(data []byte) error {
		n, err := w.Write(data)
		if err == nil && n != len(data) {
			err = io.ErrShortWrite
		}
		return err
	}
	// Ids allocated from here on are new to the checkpoint; the input file
	// defines their names.
	b.Prepare()
	b.prepareMu.Lock()
	defer b.prepareMu.Unlock()
	cache := &b.src[0]
	cache.mu.Lock()
	defer cache.mu.Unlock()
	ids := uint64(len(b.order))
	slots := uint64(2)
	for ids*4 > slots*3 {
		slots *= 2
	}
	placed := make([]uint32, len(b.order))
	table, newSlots := buildTable(slots, b.hashes, b.isNew, b.workers, tableLayout{
		include: func(uint32) bool { return true },
		fill: func(dst []byte, k uint32) {
			m := b.entry(b.order[k])
			put64(dst, b.hashes[k], m.cache, m.cacheCount, uint64(b.order[k])+1)
		},
		occupied: func(slot []byte) bool { return get64(slot, 24) != 0 },
		placed:   placed,
	})
	header := make([]byte, headerSize)
	copy(header, magicV2)
	put64(header[8:], uint64(len(cache.records)), slots, cache.points, cache.size, ids, uint64(len(newSlots)))
	if err := write(header); err != nil {
		return err
	}
	if err := write(table); err != nil {
		return err
	}
	batch := make([]byte, 0, 1<<20)
	flush := func(force bool) error {
		if len(batch) == 0 || (!force && len(batch) < cap(batch)-recordSize) {
			return nil
		}
		err := write(batch)
		batch = batch[:0]
		return err
	}
	for _, r := range cache.records {
		if err := flush(false); err != nil {
			return err
		}
		batch = binary.LittleEndian.AppendUint64(batch, r.offset)
		batch = binary.LittleEndian.AppendUint64(batch, r.length)
		batch = binary.LittleEndian.AppendUint64(batch, r.previous)
	}
	if err := flush(true); err != nil {
		return err
	}
	var offsets [idShards + 1]uint64
	for s := range idShards {
		batch = binary.LittleEndian.AppendUint64(batch, uint64(b.classified[s]))
		offsets[s+1] = offsets[s] + uint64(b.classified[s])
	}
	idmap := make([]byte, 4*ids)
	for k, id := range b.order {
		at := offsets[id&(idShards-1)] + uint64(id>>shardBits)
		binary.LittleEndian.PutUint32(idmap[4*at:], placed[k])
	}
	batch = append(batch, idmap...)
	if ids%2 == 1 {
		batch = append(batch, 0, 0, 0, 0)
	}
	slices.Sort(newSlots)
	for _, slot := range newSlots {
		batch = binary.LittleEndian.AppendUint64(batch, slot)
	}
	return write(batch)
}

// walOverlay holds a compact input file's points by slot. Slots from the
// index's table size on are late: metrics the index does not know, named
// by the input file.
type walOverlay struct {
	start  []uint32 // points of slot s are points[start[s]:start[s+1]]
	points []points.Point
	late   []string
	lateAt map[string]uint64
	// names of known slots without cache records, defined by the input
	names map[uint64]string
}

func (o *walOverlay) count(slot uint64) uint64 {
	if o == nil || slot+1 >= uint64(len(o.start)) {
		return 0
	}
	return uint64(o.start[slot+1] - o.start[slot])
}

func (o *walOverlay) of(slot uint64) []points.Point {
	if o == nil || slot+1 >= uint64(len(o.start)) {
		return nil
	}
	return o.points[o.start[slot]:o.start[slot+1]]
}

// openV2 opens a GCPOINT2 index over its compact cache dump and input file.
func openV2(index, cache, wal []byte, f Format) (*Index, error) {
	if !f.Compact() {
		return nil, fmt.Errorf("a compact checkpoint index needs a compact cache dump")
	}
	in := &Index{data: index, source: [2][]byte{cache, nil}, format: f, v2: true}
	in.recordCounts[0] = get64(index, 8)
	in.slots, in.cachePoints = get64(index, 16), get64(index, 24)
	ids, newCount := get64(index, 40), get64(index, 48)
	if in.slots < 2 || in.slots&(in.slots-1) != 0 || in.slots > uint64((len(index)-headerSize)/slotSize) || ids > in.slots {
		return nil, fmt.Errorf("invalid recovery hash table size")
	}
	if get64(index, 32) != uint64(len(cache)) || in.cachePoints > uint64(len(cache)) {
		return nil, fmt.Errorf("recovery source lengths differ")
	}
	offset := uint64(headerSize) + in.slots*slotSize
	if in.recordCounts[0] > uint64(len(index)-int(offset))/recordSize {
		return nil, fmt.Errorf("invalid recovery record extent")
	}
	in.recordOffsets[0] = offset
	offset += in.recordCounts[0] * recordSize
	if offset+idShards*8 > uint64(len(index)) {
		return nil, fmt.Errorf("invalid recovery id extent")
	}
	var total uint64
	for s := range idShards {
		count := get64(index, offset+uint64(s)*8)
		if count > ids-in.shardIDs[s] {
			return nil, fmt.Errorf("invalid recovery id extent")
		}
		in.shardIDs[s+1] = in.shardIDs[s] + count
		total = in.shardIDs[s+1]
	}
	offset += idShards * 8
	in.idOffset = offset
	if total != ids || ids > uint64(len(index)-int(offset))/4 {
		return nil, fmt.Errorf("invalid recovery id map")
	}
	offset += (ids*4 + 7) &^ 7
	in.newOffset = offset
	if newCount > uint64(len(index)-int(offset))/8 || offset+newCount*8 != uint64(len(index)) {
		return nil, fmt.Errorf("invalid recovery new-metric extent")
	}
	if f == FormatBlocks {
		if err := in.loadBlocks(cache); err != nil {
			return nil, err
		}
	}
	if err := in.validateRecords(); err != nil {
		return nil, err
	}
	if err := in.validateSlotsV2(); err != nil {
		return nil, err
	}
	if err := in.loadWAL(wal); err != nil {
		return nil, err
	}
	in.finishV2(newCount)
	return in, nil
}

// validateSlotsV2 checks every slot and the id map against each other. The
// successor waits on it before it serves, so both passes run on all cores:
// each worker checks a range, and the ranges' point totals are combined so
// that a corrupt table cannot overflow them.
func (in *Index) validateSlotsV2() error {
	workers := min(32, runtime.GOMAXPROCS(0))
	type part struct {
		points uint64
		err    error
	}
	slots := make([]part, workers)
	ids := make([]error, workers)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			slots[w].points, slots[w].err = in.validateSlotRangeV2(in.slots*uint64(w)/uint64(workers), in.slots*uint64(w+1)/uint64(workers))
		})
		wg.Go(func() {
			ids[w] = in.validateIDRangeV2(in.shardIDs[idShards]*uint64(w)/uint64(workers), in.shardIDs[idShards]*uint64(w+1)/uint64(workers))
		})
	}
	wg.Wait()
	var cachePoints uint64
	for _, p := range slots {
		if p.err != nil {
			return p.err
		}
		if p.points > in.cachePoints-cachePoints {
			return fmt.Errorf("recovery index totals differ")
		}
		cachePoints += p.points
	}
	if cachePoints != in.cachePoints {
		return fmt.Errorf("recovery index totals differ")
	}
	return errors.Join(ids...)
}

// validateSlotRangeV2 checks slots [first, last) and returns their points.
func (in *Index) validateSlotRangeV2(first, last uint64) (uint64, error) {
	var cachePoints uint64
	for slot := first; slot < last; slot++ {
		at := headerSize + slot*slotSize
		cacheHead, cacheCount, id := get64(in.data, at+8), get64(in.data, at+16), get64(in.data, at+24)
		if id == 0 {
			if cacheHead != 0 || cacheCount != 0 {
				return 0, fmt.Errorf("empty recovery slot has points")
			}
			continue
		}
		if cacheHead > in.recordCounts[0] || (cacheHead == 0) != (cacheCount == 0) || cacheCount > in.cachePoints-cachePoints {
			return 0, fmt.Errorf("invalid recovery slot")
		}
		if got, ok := in.idSlot(uint32(id - 1)); !ok || got != slot {
			return 0, fmt.Errorf("recovery slot and id map differ")
		}
		cachePoints += cacheCount
	}
	return cachePoints, nil
}

// validateIDRangeV2 checks that entries [first, last) of the id map, which
// lists ids by shard, are each placed in a slot that names them back: no two
// ids share a slot, so input partitions never share one either.
func (in *Index) validateIDRangeV2(first, last uint64) error {
	shard := uint64(0)
	for at := first; at < last; at++ {
		for at >= in.shardIDs[shard+1] {
			shard++
		}
		slot := binary.LittleEndian.Uint32(in.data[in.idOffset+4*at:])
		id := (at-in.shardIDs[shard])<<shardBits | shard
		if slot == 0 || uint64(slot) > in.slots || get64(in.data, headerSize+uint64(slot-1)*slotSize+24) != id+1 {
			return fmt.Errorf("invalid recovery id map")
		}
	}
	return nil
}

// idSlot returns the slot the index placed builder id in.
func (in *Index) idSlot(id uint32) (uint64, bool) {
	shard, local := uint64(id&(idShards-1)), uint64(id>>shardBits)
	if local >= in.shardIDs[shard+1]-in.shardIDs[shard] {
		return 0, false
	}
	slot := binary.LittleEndian.Uint32(in.data[in.idOffset+4*(in.shardIDs[shard]+local):])
	if slot == 0 {
		return 0, false
	}
	return uint64(slot - 1), true
}

// canonical reports whether name is spelled as on the filesystem.
func canonical(name []byte) bool {
	return len(name) > 0 && name[0] != '.' && name[len(name)-1] != '.' && !bytes.Contains(name, []byte("..")) &&
		bytes.IndexByte(name, '/') < 0 && bytes.IndexByte(name, 0) < 0
}

// loadWAL reads the compact input file into the overlay: one worker per
// partition decodes its frames in order, first counting each slot's points,
// then placing them.
func (in *Index) loadWAL(wal []byte) error {
	var frames [inputPartitions][]inputFrameHeader
	for data := wal; len(data) > 0; {
		h, n, err := parseInputFrame(data)
		if err != nil {
			return err
		}
		frames[h.partition] = append(frames[h.partition], h)
		data = data[n:]
	}
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(inputPartitions), zstd.IgnoreChecksum(true))
	if err != nil {
		return err
	}
	defer dec.Close()
	o := &walOverlay{names: make(map[uint64]string)}
	counts := make([]uint32, in.slots)
	type partState struct {
		late      map[uint32]int // id -> local late index
		lateNames []string
		lateCount []uint32
		names     map[uint64]string
		err       error
	}
	var parts [inputPartitions]partState
	// resolve maps a row's id to a known slot, or to a local late index.
	resolve := func(ps *partState, id uint32) (uint64, int, error) {
		if slot, ok := in.idSlot(id); ok {
			return slot, -1, nil
		}
		if i, ok := ps.late[id]; ok {
			return 0, i, nil
		}
		return 0, 0, fmt.Errorf("compact input id %d is neither indexed nor defined", id)
	}
	parallel := func(work func(p int, ps *partState, raw []byte) []byte) error {
		var wg sync.WaitGroup
		for p := range parts {
			wg.Go(func() { _ = work(p, &parts[p], nil) })
		}
		wg.Wait()
		var errs error
		for p := range parts {
			errs = errors.Join(errs, parts[p].err)
		}
		return errs
	}
	err = parallel(func(p int, ps *partState, raw []byte) []byte {
		ps.late, ps.names = make(map[uint32]int), make(map[uint64]string)
		for _, h := range frames[p] {
			raw, ps.err = decodeInputFrame(dec, h, raw, func(id uint32, name []byte) error {
				if !canonical(name) {
					return fmt.Errorf("pending reads require canonical metric names")
				}
				if slot, ok := in.idSlot(id); ok {
					if _, h := in.slot(slot); h.cache == 0 {
						ps.names[slot] = string(name)
					}
					return nil
				}
				if _, ok := ps.late[id]; ok {
					return fmt.Errorf("compact input id %d defined twice", id)
				}
				ps.late[id] = len(ps.lateNames)
				ps.lateNames = append(ps.lateNames, string(name))
				ps.lateCount = append(ps.lateCount, 0)
				return nil
			}, func(id uint32, _ int64, _ uint64) error {
				slot, late, err := resolve(ps, id)
				switch {
				case err != nil:
					return err
				case late >= 0:
					ps.lateCount[late]++
				default:
					counts[slot]++ // a slot's id belongs to this partition only
				}
				return nil
			})
			if ps.err != nil {
				return raw
			}
		}
		return raw
	})
	if err != nil {
		return err
	}
	// Late slots follow the table, partition by partition.
	o.lateAt = make(map[string]uint64)
	lateBase := [inputPartitions]uint64{}
	next := in.slots
	for p := range parts {
		lateBase[p] = next
		for i, name := range parts[p].lateNames {
			if _, ok := o.lateAt[name]; ok {
				return fmt.Errorf("compact input defines %q twice", name)
			}
			o.lateAt[name] = next + uint64(i)
		}
		o.late = append(o.late, parts[p].lateNames...)
		next += uint64(len(parts[p].lateNames))
		for slot, name := range parts[p].names {
			o.names[slot] = name
		}
	}
	o.start = make([]uint32, next+1)
	var sum uint64
	for slot := range next {
		c := uint64(0)
		if slot < in.slots {
			c = uint64(counts[slot])
		} else {
			p := lateOwner(lateBase[:], slot)
			c = uint64(parts[p].lateCount[slot-lateBase[p]])
		}
		o.start[slot] = uint32(sum)
		sum += c
	}
	if sum > 1<<32-1 {
		return fmt.Errorf("compact input file holds too many points")
	}
	o.start[next] = uint32(sum)
	o.points = make([]points.Point, sum)
	cursor := slices.Clone(o.start[:next])
	err = parallel(func(p int, ps *partState, raw []byte) []byte {
		for _, h := range frames[p] {
			raw, ps.err = decodeInputFrame(dec, h, raw, func(uint32, []byte) error { return nil }, func(id uint32, ts int64, bits uint64) error {
				slot, late, err := resolve(ps, id)
				if err != nil {
					return err
				}
				if late >= 0 {
					slot = lateBase[p] + uint64(late)
				}
				at := cursor[slot]
				if at >= o.start[slot+1] {
					return errInputFrame
				}
				o.points[at] = points.Point{Value: math.Float64frombits(bits), Timestamp: ts}
				cursor[slot]++
				return nil
			})
			if ps.err != nil {
				return raw
			}
		}
		return raw
	})
	if err != nil {
		return err
	}
	in.wal = o
	return nil
}

// lateOwner returns the partition whose late slots include slot.
func lateOwner(base []uint64, slot uint64) int {
	p := 0
	for i := range base {
		if base[i] <= slot {
			p = i
		}
	}
	return p
}

// finishV2 counts metrics and points, and lists new names: indexed new
// slots that hold points, then every late slot.
func (in *Index) finishV2(newCount uint64) {
	total := in.Slots()
	workers := min(32, runtime.GOMAXPROCS(0))
	metrics := make([]uint64, workers)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for slot := total * uint64(w) / uint64(workers); slot < total*uint64(w+1)/uint64(workers); slot++ {
				if _, h := in.slot(slot); h.count != 0 {
					metrics[w]++
				}
			}
		})
	}
	wg.Wait()
	for _, m := range metrics {
		in.metrics += m
	}
	in.points = in.cachePoints + uint64(len(in.wal.points))
	for i := range newCount {
		var slot uint64
		if in.v3 {
			slot = uint64(binary.LittleEndian.Uint32(in.data[in.newOffset+i*4:]))
		} else {
			slot = get64(in.data, in.newOffset+i*8)
		}
		if slot < in.slots {
			if _, h := in.slot(slot); h.count != 0 {
				in.newList = append(in.newList, slot)
			}
		}
	}
	for slot := in.slots; slot < total; slot++ {
		if in.wal.count(slot) != 0 {
			in.newList = append(in.newList, slot)
		}
	}
	in.newCount = uint64(len(in.newList))
}
