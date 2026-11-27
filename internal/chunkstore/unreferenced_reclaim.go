package chunkstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"slices"

	"github.com/cockroachdb/pebble"
)

// UnreferencedOptions selects the ID window ReclaimUnreferencedIDs sweeps.
type UnreferencedOptions struct {
	// From and Below bound the swept IDs: From <= id < Below. Below is the
	// fence; it must not exceed the allocator watermark minus Margin, so IDs a
	// recent reservation may still publish are never considered.
	From, Below, Margin uint64
	// MinRunIDs skips runs of fewer unreferenced IDs; they are only counted.
	MinRunIDs uint64
	// CountKeys counts every key in each deleted range (a full scan of it).
	CountKeys bool
	// Apply writes one range deletion per run and key space; otherwise
	// nothing is written.
	Apply bool
}

// UnreferencedSpace reports one per-ID key space.
type UnreferencedSpace struct {
	Space  string
	Ranges int    // non-empty unreferenced ranges
	Keys   int64  `json:",omitempty"` // with CountKeys
	Bytes  uint64 // EstimateDiskUsage of the ranges
}

// UnreferencedReclaim counts what ReclaimUnreferencedIDs found and deleted.
type UnreferencedReclaim struct {
	Watermark, From, Below uint64
	References             map[string]uint64 // referenced IDs inside the window, by reference kind
	ReferencedIDs          uint64            // distinct referenced IDs inside the window
	Runs                   uint64            // maximal unreferenced runs inside the window
	RunIDs                 uint64
	ShortRuns              uint64 // runs below MinRunIDs, left alone
	ShortRunIDs            uint64
	Buckets                int // time buckets present
	Spaces                 []UnreferencedSpace
	Ranges                 int
	Keys                   int64 `json:",omitempty"`
	Bytes                  uint64
	RangeDeletes           int         // written with Apply
	Samples                [][2]uint64 `json:",omitempty"` // first deleted ranges, [first, end)
}

// DefaultUnreferencedMargin keeps the sweep this far below the allocator
// watermark. Bulk reservations allocate whole blocks at the top.
const DefaultUnreferencedMargin = 1 << 24

// maxUnreferencedFence bounds the live-ID bitmap at 1 GiB.
const maxUnreferencedFence = 1 << 33

var unreferencedDeleteBatch = 4096

// ReclaimUnreferencedIDs deletes the keys of metric IDs that nothing in the
// store references. Every metric generation keeps its data under per-ID key
// spaces (chunks, continuation keys in every time bucket, revisions,
// transfer receipts and mutation histories); deleting a catalog record
// without dropping these, or a reservation that never published, leaves them
// unreachable. The live set is the union of every durable reference: catalog
// records, native reservations, receipts, combine pieces and origins, staged
// transfer deletes and pending file cleanups. Each maximal run of other IDs
// in [From, Below) becomes one range deletion per non-empty key space.
//
// Run it only while no daemon has the store open: in-memory staged imports
// are not durable references, and the store LOCK is the guard.
func (s *Store) ReclaimUnreferencedIDs(ctx context.Context, opt UnreferencedOptions) (UnreferencedReclaim, error) {
	result := UnreferencedReclaim{From: opt.From, Below: opt.Below, References: map[string]uint64{}}
	if opt.Apply && s.readOnly {
		return result, ErrReadOnly
	}
	ctx, finish, err := s.beginRead(ctx)
	if err != nil {
		return result, err
	}
	defer finish()
	if result.From == 0 {
		result.From = 1
	}
	snap := s.db.NewSnapshot()
	defer snap.Close()
	watermark, err := readWatermark(snap)
	if err != nil {
		return result, err
	}
	result.Watermark = watermark
	switch {
	case opt.Below == 0:
		return result, errors.New("set the ID fence")
	case watermark < opt.Margin || opt.Below > watermark-opt.Margin:
		return result, fmt.Errorf("fence %d is above the allocator watermark %d minus margin %d", opt.Below, watermark, opt.Margin)
	case opt.Below > maxUnreferencedFence:
		return result, fmt.Errorf("fence %d exceeds %d", opt.Below, uint64(maxUnreferencedFence))
	case result.From >= opt.Below:
		return result, fmt.Errorf("window [%d, %d) is empty", result.From, opt.Below)
	}
	live := make(idBits, (opt.Below+63)/64)
	if err := collectReferences(ctx, snap, live, result.From, opt.Below, result.References); err != nil {
		return result, err
	}
	live.countRuns(result.From, opt.Below, opt.MinRunIDs, &result)

	it, err := snap.NewIter(nil)
	if err != nil {
		return result, err
	}
	defer it.Close()
	buckets, err := timeBuckets(it)
	if err != nil {
		return result, err
	}
	result.Buckets = len(buckets)
	spaces := unreferencedSpaces(buckets)
	sweep := unreferencedSweep{s: s, it: it, live: live, opt: opt, from: result.From, result: &result}
	defer sweep.close()
	for _, space := range spaces {
		report := UnreferencedSpace{Space: space.name}
		if err := sweep.space(ctx, space.prefix, &report); err != nil {
			return result, err
		}
		if report.Ranges == 0 {
			continue
		}
		result.Spaces = append(result.Spaces, report)
		result.Ranges += report.Ranges
		result.Keys += report.Keys
		result.Bytes += report.Bytes
	}
	if err := sweep.flush(); err != nil {
		return result, err
	}
	if result.RangeDeletes > 0 {
		s.invalidateReadCache()
	}
	return result, nil
}

type unreferencedKeySpace struct {
	name   string
	prefix []byte // followed by the big-endian metric ID
}

// unreferencedSpaces lists every key space that stores data under a metric
// ID: chunks, metric-prefixed continuation keys, revisions, transfer
// receipts, mutation histories and continuation keys in each time bucket.
func unreferencedSpaces(buckets []uint32) []unreferencedKeySpace {
	spaces := []unreferencedKeySpace{
		{"c", []byte("c")}, {"u", []byte("u")}, {"r", []byte("r")}, {"a", []byte("a")},
		{"proof/ranges/", []byte("proof/ranges/")},
	}
	for _, bucket := range buckets {
		prefix := binary.BigEndian.AppendUint32([]byte{continuationBucketKeyPrefix}, bucket)
		spaces = append(spaces, unreferencedKeySpace{fmt.Sprintf("t/%d/u", bucket), append(prefix, 'u')})
	}
	return spaces
}

func readWatermark(r pebble.Reader) (uint64, error) {
	data, closer, err := r.Get(sequenceKey())
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer closer.Close()
	if len(data) != 8 {
		return 0, errors.New("invalid catalog sequence")
	}
	return binary.BigEndian.Uint64(data), nil
}

// unreferencedRefs decodes only the ID-bearing fields of native intents,
// receipts and combine pieces. Unknown fields are ignored.
type unreferencedRefs struct {
	First       uint64 // NATIVE/intent
	Count       int
	FirstID     uint64 // NATIVE/receipt
	ReservedIDs int
	Mappings    []struct{ TargetID uint64 }
	Receipt     *struct { // COMBINE/intent
		FirstID     uint64
		ReservedIDs int
		Mappings    []struct{ TargetID uint64 }
	}
	Origins []struct{ ID uint64 }
	Meta    struct{ ID uint64 } // COMBINE/piece
	Old     struct{ ID uint64 }
}

// collectReferences marks every referenced ID in [from, below).
func collectReferences(ctx context.Context, r pebble.Reader, live idBits, from, below uint64, counts map[string]uint64) error {
	mark := func(kind string, id uint64) {
		if id >= from && id < below && !live.has(id) {
			live.set(id)
			counts[kind]++
		}
	}
	markRange := func(kind string, first uint64, count int) error {
		if count < 0 || first > math.MaxUint64-uint64(count) {
			return fmt.Errorf("%w: %s reservation %d+%d", ErrFormat, kind, first, count)
		}
		lo, hi := max(first, from), min(first+uint64(count), below)
		for id := lo; id < hi; id++ {
			mark(kind, id)
		}
		return nil
	}
	scan := func(lower, upper []byte, visit func(key, value []byte) error) error {
		it, err := r.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
		if err != nil {
			return err
		}
		for valid := it.First(); valid; valid = it.Next() {
			if err := ctx.Err(); err != nil {
				return errors.Join(err, it.Close())
			}
			if err := visit(it.Key(), it.Value()); err != nil {
				return errors.Join(fmt.Errorf("%q: %w", it.Key(), err), it.Close())
			}
		}
		return errors.Join(it.Error(), it.Close())
	}
	decode := func(value []byte) (unreferencedRefs, error) {
		var refs unreferencedRefs
		return refs, json.Unmarshal(value, &refs)
	}
	perID := func(kind string, at int) func(key, _ []byte) error {
		return func(key, _ []byte) error {
			if len(key) < at+8 {
				return ErrFormat
			}
			mark(kind, binary.BigEndian.Uint64(key[at:]))
			return nil
		}
	}
	steps := []struct {
		kind         string
		lower, upper []byte
		visit        func(key, value []byte) error
	}{
		{"catalog", []byte("m/"), []byte("m0"), func(key, value []byte) error {
			m, err := decodeCatalog(string(key[2:]), value)
			if err != nil {
				return err
			}
			mark("catalog", m.ID)
			return nil
		}},
		{"native-intent", []byte("NATIVE/intent/"), prefixEnd([]byte("NATIVE/intent/")), func(_, value []byte) error {
			refs, err := decode(value)
			if err != nil {
				return err
			}
			return markRange("native-intent", refs.First, refs.Count)
		}},
		{"native-receipt", []byte("NATIVE/receipt/"), prefixEnd([]byte("NATIVE/receipt/")), func(_, value []byte) error {
			refs, err := decode(value)
			if err != nil {
				return err
			}
			for _, m := range refs.Mappings {
				mark("native-receipt", m.TargetID)
			}
			return nil
		}},
		{"combine-intent", []byte("COMBINE/intent/"), prefixEnd([]byte("COMBINE/intent/")), func(_, value []byte) error {
			refs, err := decode(value)
			if err != nil {
				return err
			}
			if refs.Receipt != nil {
				if err := markRange("combine-intent", refs.Receipt.FirstID, refs.Receipt.ReservedIDs); err != nil {
					return err
				}
				for _, m := range refs.Receipt.Mappings {
					mark("combine-intent", m.TargetID)
				}
			}
			for _, o := range refs.Origins {
				mark("combine-intent", o.ID)
			}
			return nil
		}},
		{"combine-piece", []byte("COMBINE/piece/"), prefixEnd([]byte("COMBINE/piece/")), func(_, value []byte) error {
			refs, err := decode(value)
			if err != nil {
				return err
			}
			mark("combine-piece", refs.Meta.ID)
			mark("combine-piece", refs.Old.ID)
			return nil
		}},
		{"transfer-delete", []byte("d"), []byte("e"), perID("transfer-delete", 1)},
		{"file-cleanup", []byte("f/cleanup/"), prefixEnd([]byte("f/cleanup/")), perID("file-cleanup", len("f/cleanup/"))},
	}
	for _, step := range steps {
		if err := scan(step.lower, step.upper, step.visit); err != nil {
			return fmt.Errorf("%s references: %w", step.kind, err)
		}
	}
	return nil
}

// timeBuckets lists the buckets present in the time-partitioned space.
func timeBuckets(it *pebble.Iterator) ([]uint32, error) {
	var buckets []uint32
	lower := []byte{continuationBucketKeyPrefix}
	for it.SeekGE(lower) {
		key := it.Key()
		if key[0] != continuationBucketKeyPrefix {
			break
		}
		if len(key) < 5 {
			return nil, fmt.Errorf("%w: time-partitioned key %q", ErrFormat, key)
		}
		bucket := binary.BigEndian.Uint32(key[1:5])
		buckets = append(buckets, bucket)
		if bucket == math.MaxUint32 {
			break
		}
		lower = binary.BigEndian.AppendUint32([]byte{continuationBucketKeyPrefix}, bucket+1)
	}
	return buckets, it.Error()
}

type unreferencedSweep struct {
	s      *Store
	it     *pebble.Iterator
	live   idBits
	opt    UnreferencedOptions
	from   uint64
	result *UnreferencedReclaim
	batch  *pebble.Batch
	queued int
}

// space finds the non-empty unreferenced ranges under prefix. Each seek
// lands on an ID with keys: an unreferenced one opens a range up to the next
// referenced ID, a referenced one skips to the next unreferenced ID.
func (w *unreferencedSweep) space(ctx context.Context, prefix []byte, report *UnreferencedSpace) error {
	below := w.opt.Below
	key := func(id uint64) []byte { return binary.BigEndian.AppendUint64(bytes.Clone(prefix), id) }
	for id := w.from; id < below; {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !w.it.SeekGE(key(id)) {
			break
		}
		k := w.it.Key()
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		if len(k) < len(prefix)+8 {
			return fmt.Errorf("%w: per-ID key %q", ErrFormat, k)
		}
		found := binary.BigEndian.Uint64(k[len(prefix):])
		if found >= below {
			break
		}
		if w.live.has(found) {
			id = w.live.nextClear(found+1, below)
			continue
		}
		end := w.live.nextSet(found+1, below)
		id = end
		if start := w.live.prevSet(found, w.from); end-start < w.opt.MinRunIDs {
			continue
		}
		if err := w.add(key(found), key(end), report); err != nil {
			return err
		}
		w.sample(found, end)
	}
	return w.it.Error()
}

// sample keeps the first distinct ranges for the report.
func (w *unreferencedSweep) sample(first, end uint64) {
	r := [2]uint64{first, end}
	if len(w.result.Samples) >= 10 || slices.Contains(w.result.Samples, r) {
		return
	}
	w.result.Samples = append(w.result.Samples, r)
}

func (w *unreferencedSweep) add(lower, upper []byte, report *UnreferencedSpace) error {
	report.Ranges++
	size, err := w.s.db.EstimateDiskUsage(lower, upper)
	if err != nil {
		return err
	}
	report.Bytes += size
	if w.opt.CountKeys {
		n, err := countRange(w.s.db, lower, upper)
		if err != nil {
			return err
		}
		report.Keys += n
	}
	if !w.opt.Apply {
		return nil
	}
	if w.batch == nil {
		w.batch = w.s.db.NewBatch()
	}
	if err := w.batch.DeleteRange(lower, upper, nil); err != nil {
		return err
	}
	w.queued++
	if w.queued >= unreferencedDeleteBatch {
		return w.flush()
	}
	return nil
}

func (w *unreferencedSweep) flush() error {
	if w.batch == nil || w.queued == 0 {
		return nil
	}
	if err := w.batch.Commit(pebble.Sync); err != nil {
		return err
	}
	w.result.RangeDeletes += w.queued
	w.queued = 0
	w.batch.Reset()
	return nil
}

func (w *unreferencedSweep) close() {
	if w.batch != nil {
		_ = w.batch.Close()
	}
}

func countRange(r pebble.Reader, lower, upper []byte) (int64, error) {
	it, err := r.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return 0, err
	}
	var n int64
	for valid := it.First(); valid; valid = it.Next() {
		n++
	}
	return n, errors.Join(it.Error(), it.Close())
}

// idBits is a dense bitmap of IDs below a fence.
type idBits []uint64

func (b idBits) set(id uint64)      { b[id/64] |= 1 << (id % 64) }
func (b idBits) has(id uint64) bool { return b[id/64]&(1<<(id%64)) != 0 }

// nextSet returns the first set ID in [id, limit), or limit.
func (b idBits) nextSet(id, limit uint64) uint64 { return b.next(id, limit, 0) }

// nextClear returns the first clear ID in [id, limit), or limit.
func (b idBits) nextClear(id, limit uint64) uint64 { return b.next(id, limit, math.MaxUint64) }

func (b idBits) next(id, limit, flip uint64) uint64 {
	for id < limit {
		word := (b[id/64] ^ flip) >> (id % 64)
		if word != 0 {
			return min(id+uint64(bits.TrailingZeros64(word)), limit)
		}
		id = (id/64 + 1) * 64
	}
	return limit
}

// prevSet returns one past the last set ID in [floor, id), or floor: the
// start of the clear run that contains id.
func (b idBits) prevSet(id, floor uint64) uint64 {
	for id > floor {
		i := id - 1
		word := b[i/64] << (63 - i%64)
		if word != 0 {
			return max(i-uint64(bits.LeadingZeros64(word))+1, floor)
		}
		id = i / 64 * 64
	}
	return floor
}

func (b idBits) countRuns(from, below, minRun uint64, result *UnreferencedReclaim) {
	result.ReferencedIDs = 0
	for id := from; id < below; {
		start := b.nextClear(id, below)
		result.ReferencedIDs += start - id
		if start == below {
			break
		}
		end := b.nextSet(start+1, below)
		result.Runs++
		result.RunIDs += end - start
		if end-start < minRun {
			result.ShortRuns++
			result.ShortRunIDs += end - start
		}
		id = end
	}
}
