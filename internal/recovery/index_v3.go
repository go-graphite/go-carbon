package recovery

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
)

// A blocks checkpoint index (magic GCPOINT3) goes with a FormatBlocks cache
// dump and a compact input file, as GCPOINT2 goes with a compact one, in
// about a quarter of its bytes. The dump's blocks give every record's offset
// and length, so the index lists no records; a metric's records are adjacent
// in its block, so its chain is the run of records named alike that ends at
// its head. Slots take 16 bytes, and the table is sized for a load of 3/4
// instead of to a power of two:
//
//	header      magic, cache records, slots, cache points, cache bytes,
//	            builder ids mapped, new-name slots (72 bytes)
//	slots       low 32 bits of the name's hash, cache head record, cache
//	            points, builder id + 1 (4 bytes each)
//	shard ids   ids mapped per builder shard (256 x 8 bytes)
//	id map      slot + 1 of each mapped id, by shard then local id (4 bytes)
//	new slots   slots of names absent from the read index (4 bytes)
//
// A name's probe starts at slot (hash * slots) >> 64 and wraps around.
const (
	magicV3    = "GCPOINT3"
	slotSizeV3 = 16
)

// homeV3 is the first slot a hash probes in a table of n slots.
func homeV3(hash, n uint64) uint64 {
	hi, _ := bits.Mul64(hash, n)
	return hi
}

// WriteBlocksIndex writes the index of a checkpoint whose cache dump is of
// FormatBlocks; see magicV3. It is written like WriteCompactIndex: while
// input may still be diverted into the compact input file, after the dump.
func WriteBlocksIndex(dir string, builder *Builder) (File, error) {
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
	if err = builder.writeBlocks(buffer); err != nil {
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

func (b *Builder) writeBlocks(w io.Writer) error {
	write := func(data []byte) error {
		n, err := w.Write(data)
		if err == nil && n != len(data) {
			err = io.ErrShortWrite
		}
		return err
	}
	b.Prepare()
	b.prepareMu.Lock()
	defer b.prepareMu.Unlock()
	cache := &b.src[0]
	cache.mu.Lock()
	defer cache.mu.Unlock()
	// The index lists no records: chains must be runs of adjacent records.
	for i, r := range cache.records {
		if r.previous != 0 && r.previous != uint64(i) {
			return fmt.Errorf("recovery records of a metric are not adjacent")
		}
	}
	ids := uint64(len(b.order))
	if uint64(len(cache.records)) > math.MaxUint32 || ids >= math.MaxUint32/2 {
		return fmt.Errorf("too many recovery records for a blocks index")
	}
	for _, id := range b.order {
		if b.entry(id).cacheCount > math.MaxUint32 || id == math.MaxUint32 {
			return fmt.Errorf("recovery metric too large for a blocks index")
		}
	}
	slots := max(ids+ids/3+1, 2)
	placed := make([]uint32, len(b.order))
	table, newSlots := buildTableV3(slots, b.hashes, b.isNew, b.workers, func(dst []byte, k uint32) {
		m := b.entry(b.order[k])
		binary.LittleEndian.PutUint32(dst, uint32(b.hashes[k]))
		binary.LittleEndian.PutUint32(dst[4:], uint32(m.cache))
		binary.LittleEndian.PutUint32(dst[8:], uint32(m.cacheCount))
		binary.LittleEndian.PutUint32(dst[12:], b.order[k]+1)
	}, placed)
	header := make([]byte, headerSize)
	copy(header, magicV3)
	put64(header[8:], uint64(len(cache.records)), slots, cache.points, cache.size, ids, uint64(len(newSlots)))
	if err := write(header); err != nil {
		return err
	}
	if err := write(table); err != nil {
		return err
	}
	var offsets [idShards + 1]uint64
	batch := make([]byte, 0, idShards*8+4*int(ids)+4+4*len(newSlots))
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
		batch = binary.LittleEndian.AppendUint32(batch, uint32(slot))
	}
	return write(batch)
}

// buildTableV3 places every classified metric in a table of n slots by
// linear probing from homeV3, as buildTable does: workers own disjoint slot
// regions, and entries that would leave their region are placed afterwards
// by a serial pass that wraps around. fill writes a placed metric's slot,
// whose last 4 bytes are nonzero.
func buildTableV3(n uint64, hashes []uint64, isNew []bool, workers int, fill func(dst []byte, k uint32), placed []uint32) ([]byte, []uint64) {
	table := make([]byte, n*slotSizeV3)
	regions := uint64(1)
	for int(regions) < workers && regions*2 <= n/4096 {
		regions *= 2
	}
	bound := func(r uint64) uint64 { return n * r / regions }
	region := func(home uint64) uint64 {
		r := home * regions / n
		for r > 0 && home < bound(r) {
			r--
		}
		for home >= bound(r+1) {
			r++
		}
		return r
	}
	byRegion := make([][]uint32, regions)
	for k, h := range hashes {
		r := region(homeV3(h, n))
		byRegion[r] = append(byRegion[r], uint32(k))
	}
	empty := func(slot uint64) bool { return binary.LittleEndian.Uint32(table[slot*slotSizeV3+12:]) == 0 }
	place := func(k uint32, slot uint64) {
		fill(table[slot*slotSizeV3:(slot+1)*slotSizeV3], k)
		placed[k] = uint32(slot) + 1
	}
	deferred := make([][]uint32, regions)
	var wg sync.WaitGroup
	for r := range regions {
		wg.Go(func() {
			hi := bound(r + 1)
			for _, k := range byRegion[r] {
				slot := homeV3(hashes[k], n)
				for slot < hi && !empty(slot) {
					slot++
				}
				if slot == hi {
					deferred[r] = append(deferred[r], k)
					continue
				}
				place(k, slot)
			}
		})
	}
	wg.Wait()
	for _, ks := range deferred {
		for _, k := range ks {
			slot := homeV3(hashes[k], n)
			for !empty(slot) {
				if slot++; slot == n {
					slot = 0
				}
			}
			place(k, slot)
		}
	}
	var newSlots []uint64
	if isNew != nil {
		for k, p := range placed {
			if isNew[k] {
				newSlots = append(newSlots, uint64(p-1))
			}
		}
	}
	return table, newSlots
}

// openV3 opens a GCPOINT3 index over its blocks cache dump and compact
// input file.
func openV3(index, cache, wal []byte, f Format) (*Index, error) {
	if f != FormatBlocks {
		return nil, fmt.Errorf("a blocks checkpoint index needs a blocks cache dump")
	}
	in := &Index{data: index, source: [2][]byte{cache, nil}, format: f, v2: true, v3: true}
	in.recordCounts[0] = get64(index, 8)
	in.slots, in.cachePoints = get64(index, 16), get64(index, 24)
	ids, newCount := get64(index, 40), get64(index, 48)
	if in.slots < 2 || in.slots > math.MaxUint32 || in.slots > uint64(len(index)-headerSize)/slotSizeV3 || ids >= in.slots {
		return nil, fmt.Errorf("invalid recovery hash table size")
	}
	if get64(index, 32) != uint64(len(cache)) || in.cachePoints > uint64(len(cache)) || in.recordCounts[0] > math.MaxUint32 {
		return nil, fmt.Errorf("recovery source lengths differ")
	}
	offset := uint64(headerSize) + in.slots*slotSizeV3
	if offset+idShards*8 > uint64(len(index)) {
		return nil, fmt.Errorf("invalid recovery id extent")
	}
	var total uint64
	for s := range idShards {
		count := get64(index, offset+uint64(s)*8)
		if count > ids-in.shardIDs[s] {
			return nil, fmt.Errorf("invalid recovery id map")
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
	if offset > uint64(len(index)) || newCount > uint64(len(index)-int(offset))/4 || offset+newCount*4 != uint64(len(index)) {
		return nil, fmt.Errorf("invalid recovery new-metric extent")
	}
	if err := in.loadBlocks(cache); err != nil {
		return nil, err
	}
	if err := in.validateSlotsV3(); err != nil {
		return nil, err
	}
	if err := in.loadWAL(wal); err != nil {
		return nil, err
	}
	in.finishV2(newCount)
	return in, nil
}

// validateSlotsV3 checks every slot and the id map against each other, and
// that the slots' heads end every run of records named alike exactly once.
func (in *Index) validateSlotsV3() error {
	runs := in.names.runs
	covered := make([]atomic.Uint64, (in.recordCounts[0]+64)/64)
	const slotsPerWorker = 1 << 18
	workers := min(32, runtime.GOMAXPROCS(0), int((in.slots+slotsPerWorker-1)/slotsPerWorker))
	type part struct {
		points, heads uint64
		err           error
	}
	parts := make([]part, workers)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			p := &parts[w]
			for slot := in.slots * uint64(w) / uint64(workers); slot < in.slots*uint64(w+1)/uint64(workers); slot++ {
				at := headerSize + slot*slotSizeV3
				head := uint64(binary.LittleEndian.Uint32(in.data[at+4:]))
				count := uint64(binary.LittleEndian.Uint32(in.data[at+8:]))
				id := binary.LittleEndian.Uint32(in.data[at+12:])
				if id == 0 {
					if head != 0 || count != 0 {
						p.err = fmt.Errorf("empty recovery slot has points")
						return
					}
					continue
				}
				if head > in.recordCounts[0] || (head == 0) != (count == 0) {
					p.err = fmt.Errorf("invalid recovery slot")
					return
				}
				if got, ok := in.idSlot(id - 1); !ok || got != slot {
					p.err = fmt.Errorf("recovery slot and id map differ")
					return
				}
				if head == 0 {
					continue
				}
				// A head ends its metric's run, which no other slot ends.
				if head < in.recordCounts[0] && in.names.repeats(head+1) {
					p.err = fmt.Errorf("recovery slot head is not its metric's last record")
					return
				}
				if covered[head/64].Or(1<<(head%64))&(1<<(head%64)) != 0 {
					p.err = fmt.Errorf("recovery records in two slots")
					return
				}
				p.heads++
				p.points += count // each below 2^32: no overflow before 2^32 slots
			}
		})
	}
	wg.Wait()
	var points, heads uint64
	var errs error
	for _, p := range parts {
		errs = errors.Join(errs, p.err)
		points += p.points
		heads += p.heads
	}
	if errs != nil {
		return errs
	}
	if points != in.cachePoints || heads != runs {
		return fmt.Errorf("recovery index totals differ")
	}
	// Every id is placed, in a slot that names it back.
	for shard := range uint64(idShards) {
		for local := range in.shardIDs[shard+1] - in.shardIDs[shard] {
			slot := binary.LittleEndian.Uint32(in.data[in.idOffset+4*(in.shardIDs[shard]+local):])
			id := local<<shardBits | shard
			if slot == 0 || uint64(slot) > in.slots || binary.LittleEndian.Uint32(in.data[headerSize+uint64(slot-1)*slotSizeV3+12:]) != uint32(id+1) {
				return fmt.Errorf("invalid recovery id map")
			}
		}
	}
	return nil
}
