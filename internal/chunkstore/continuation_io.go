package chunkstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
)

// PointBlock exposes the same bounded, exact integer/float encoding used by
// archive chunks to storage adapters. Slots are physical positions here; the
// adapter owns their interpretation. Encoded values always contain zero deltas.
type PointBlock struct {
	Present [2]uint64
	Points  [chunkSlots]Point
}

func EncodePointBlock(block *PointBlock) []byte {
	c := chunk{Present: block.Present, Points: block.Points}
	return encodeExactChunk(&c)
}

func DecodePointBlock(data []byte, block *PointBlock) error {
	if len(data) < 2 || data[0] != exactChunkVersion {
		return errInvalidChunk
	}
	var c chunk
	if err := decodeChunk(data, &c); err != nil {
		return err
	}
	if c.Deltas != 0 {
		return errInvalidChunk
	}
	block.Present, block.Points = c.Present, c.Points
	return nil
}

// ContinuationIO scopes an adapter's file headers, physical blocks and OOO slots
// to one metric generation. Reads own their returned bytes. The writer uses the
// same indexed batch for all main-file and sidecar mutations; its caller owns
// catalog publication, revision/activity changes and the final atomic commit.
type ContinuationIO interface {
	Get([]byte) ([]byte, error)
	Set([]byte, []byte) error
	Delete([]byte) error
	DeletePrefix([]byte) error
	Visit([]byte, func([]byte, []byte) error) error
}

// ContinuationProber reads keys that are usually absent or long unchanged,
// where an io reading through one shared iterator saves opening a table per
// read in every level (see continuationIO.Probe). Results equal Get's.
type ContinuationProber interface {
	Probe([]byte) ([]byte, error)
}

// ContinuationTimeBuckets reports whether new physical blocks go to the
// store's time-partitioned key space (format v7). Blocks already written keep
// the layout their descriptors record.
type ContinuationTimeBuckets interface {
	TimeBuckets() bool
}

// ContinuationDropper removes a metric generation's time-partitioned keys.
// Only the codec knows which buckets they occupy; the metric-prefixed range
// that the store deletes cannot reach them.
type ContinuationDropper interface {
	Drop(context.Context, ContinuationIO) error
}

// ContinuationRelayouter plans moving a generation's v6 blocks that no longer
// receive appends into the history bucket of the time-partitioned space.
type ContinuationRelayouter interface {
	RelayoutPlan(context.Context, ContinuationIO) (RelayoutPlan, error)
}

// RelayoutPlan lists blocks to move, each applied only if its descriptor still
// holds Old: copies are ingested first, then the descriptor flips and the old
// groups are deleted under the metric lock.
type RelayoutPlan struct {
	Blocks []RelayoutBlock
	Bytes  int
}

type RelayoutBlock struct {
	Descriptor, Old, New []byte
	// Copies hold bucketed suffixes in ascending order, with their values.
	Copies  []RelayoutKV
	Removed [][]byte
}

type RelayoutKV struct{ Key, Value []byte }

// A suffix starting with continuationBucketMark and a 4-byte bucket is stored
// as 't' + bucket + the generation prefix + the rest. Keys written in the same
// period then sit together instead of beside each metric's whole history, so
// compactions of recent writes stop rewriting cold history. Visit returns
// such keys in suffix form. Committed bucketed keys are reached only through a
// prefix that names their bucket: Visit and DeletePrefix of a shorter prefix
// cover the metric-prefixed keys alone (ContinuationDropper removes the rest).
// A fresh stage's batch view still includes them, so it can be ingested whole.
const continuationBucketMark = 'T'

// ContinuationBucketKey returns suffix in bucket of the time-partitioned space.
func ContinuationBucketKey(bucket uint32, suffix []byte) []byte {
	key := make([]byte, 5, 5+len(suffix))
	key[0] = continuationBucketMark
	binary.BigEndian.PutUint32(key[1:], bucket)
	return append(key, suffix...)
}

func isContinuationBucketSuffix(suffix []byte) bool {
	return len(suffix) >= 5 && suffix[0] == continuationBucketMark
}

type continuationIO struct {
	sidecar *SidecarSummary
	ctx     context.Context
	reader  pebble.Reader
	batch   *pebble.Batch
	prefix  [17]byte
	changed bool
	buckets bool
	// Non-nil only for a new ID/generation whose publication still has a
	// catalog conflict check. Its visible state is entirely in this batch.
	pendingKeys map[string]struct{}
	// iter serves the io's metric-prefixed walks and probes through one
	// iterator (see withIterator); iterBusy sends a read made from inside a
	// Visit callback to a fresh lookup, since it would reposition iter.
	iter             *pebble.Iterator
	iterOptions      pebble.IterOptions
	iterOn, iterBusy bool
	// iterBorrowed marks an iter the io's caller owns (see
	// withBorrowedIterator): Close leaves it open.
	iterBorrowed bool
	// iterBatch is the batch length iter last observed: a writable io's
	// iterator is refreshed when the batch has grown since.
	iterBatch int
	// state, when the store caches metric state, serves the generation's
	// state keys and records what the io reads and writes of them.
	state *stateView
}

// stateView serves an io's reads of continuation state keys from a cached
// metricState valid for the io's reader, and records the io's reads and
// writes of them, so the state they leave can be cached (see stateCache).
type stateView struct {
	keys ContinuationStateKeys
	// base is the cached state the io started from, or nil; it is immutable.
	// cur is base plus what the io read and wrote, created on the first change.
	base, cur *metricState
	meta      Metadata
}

func (v *stateView) view() *metricState {
	if v.cur != nil {
		return v.cur
	}
	return v.base
}

func (v *stateView) mutable() *metricState {
	if v.cur == nil {
		v.cur = v.base.clone(v.meta)
	}
	return v.cur
}

// lookup reports whether key's value is known to the view.
func (v *stateView) lookup(key []byte) ([]byte, bool) {
	if st := v.view(); st != nil {
		return st.lookup(key)
	}
	return nil, false
}

// final returns the state the io leaves, with meta as its metadata.
func (v *stateView) final(meta Metadata) *metricState {
	if v.cur == nil {
		return v.base.clone(meta).seal()
	}
	meta.MetricConfig = cloneConfig(meta.MetricConfig)
	v.cur.meta = meta
	return v.cur.seal()
}

// withIterator makes the io serve its metric-prefixed Visits and Probes
// through one iterator over the generation's keys instead of a fresh LSM
// lookup or iterator per read. Reading a metric takes many small reads under
// one prefix (names, header pages, sidecar groups, block groups), and each
// fresh one positions every level and L0 sublevel again, opening a table
// iterator in each. Get keeps fresh lookups, which stop at a key's newest
// version. A writable io qualifies when it reads its own indexed batch: the
// iterator then observes the batch's writes (see sharedIter). A fresh io
// keeps its pending-key view. The caller must Close the io.
func (io *continuationIO) withIterator() *continuationIO {
	if io.pendingKeys == nil && (io.batch == nil || io.readsOwnBatch()) {
		io.iterOn = true
	}
	return io
}

// withBorrowedIterator makes a read-only io serve its metric-prefixed Visits
// and Probes through iter, an iterator over its reader's continuation keys
// that the caller owns and reuses for the ios of many metrics, reading them in
// key order: consecutive metrics then share each level's open table and
// position (see Store.PrewarmMetadata). iter must have no upper bound below
// the io's keys. Close leaves it open.
func (io *continuationIO) withBorrowedIterator(iter *pebble.Iterator) *continuationIO {
	if io.batch == nil && io.pendingKeys == nil {
		io.iter, io.iterOn, io.iterBorrowed = iter, true, true
	}
	return io
}

// readsOwnBatch reports whether the io reads through the indexed batch it
// writes to, and so observes its own writes.
func (io *continuationIO) readsOwnBatch() bool {
	reader, indexed := io.reader.(*pebble.Batch)
	return indexed && reader == io.batch && reader.Indexed()
}

// Close releases the read iterator, if any.
func (io *continuationIO) Close() error {
	if io.iter == nil {
		return nil
	}
	if io.iterBorrowed {
		io.iter = nil
		return nil
	}
	err := io.iter.Close()
	io.iter = nil
	return err
}

// sharedIter returns the read iterator for suffix, or nil when suffix must use
// a fresh lookup: no iterator requested, a nested read, or a time-bucketed key
// outside the generation's metric prefix.
func (io *continuationIO) sharedIter(suffix []byte) (*pebble.Iterator, error) {
	if !io.iterOn || io.iterBusy || isContinuationBucketSuffix(suffix) {
		return nil, nil
	}
	if io.iter == nil {
		io.iterOptions = pebble.IterOptions{LowerBound: io.prefix[:], UpperBound: prefixEnd(io.prefix[:])}
		iter, err := io.reader.NewIter(&io.iterOptions)
		if err != nil {
			return nil, err
		}
		io.iter, io.iterBatch = iter, io.batchLen()
	} else if n := io.batchLen(); n != io.iterBatch {
		// An iterator over an indexed batch observes the writes made before it
		// was created or last refreshed. Unchanged options refresh just that
		// view and keep the tables each level has open.
		io.iter.SetOptions(&io.iterOptions)
		io.iterBatch = n
	}
	return io.iter, nil
}

// batchLen grows with every write to the io's batch.
func (io *continuationIO) batchLen() int {
	if io.batch == nil {
		return 0
	}
	return io.batch.Len()
}

func newContinuationIO(ctx context.Context, reader pebble.Reader, batch *pebble.Batch, metric Metadata) *continuationIO {
	io := &continuationIO{ctx: ctx, reader: reader, batch: batch}
	copy(io.prefix[:], continuationPrefix(metric.ID, metric.Generation))
	return io
}

func newFreshContinuationIO(ctx context.Context, batch *pebble.Batch, metric Metadata) *continuationIO {
	io := newContinuationIO(ctx, batch, batch, metric)
	io.pendingKeys = make(map[string]struct{})
	return io
}

func (s *Store) newContinuationIO(ctx context.Context, reader pebble.Reader, batch *pebble.Batch, metric Metadata) *continuationIO {
	io := newContinuationIO(ctx, reader, batch, metric)
	io.buckets = s.timeBuckets
	return io
}

func (s *Store) newFreshContinuationIO(ctx context.Context, batch *pebble.Batch, metric Metadata) *continuationIO {
	io := newFreshContinuationIO(ctx, batch, metric)
	io.buckets = s.timeBuckets
	return io
}

func (io *continuationIO) TimeBuckets() bool { return io.buckets }

// dropContinuation deletes the generation's time-partitioned keys into batch;
// the caller deletes its metric-prefixed range. reader must observe the
// generation's current descriptors.
func (s *Store) dropContinuation(ctx context.Context, reader pebble.Reader, batch *pebble.Batch, m Metadata) error {
	if !s.timeBuckets {
		return nil
	}
	dropper, ok := s.continuation.(ContinuationDropper)
	if !ok {
		return fmt.Errorf("%w: continuation codec cannot drop time-partitioned keys", ErrFormat)
	}
	return dropper.Drop(ctx, s.newContinuationIO(ctx, reader, batch, m))
}

func continuationPrefix(id, generation uint64) []byte {
	key := make([]byte, 17)
	key[0] = 'u'
	binary.BigEndian.PutUint64(key[1:9], id)
	binary.BigEndian.PutUint64(key[9:17], generation)
	return key
}

func (s *Store) newStateBatch() *pebble.Batch {
	if s.continuation != nil {
		return s.db.NewIndexedBatch()
	}
	return s.db.NewBatch()
}

func (io *continuationIO) key(suffix []byte) []byte {
	return appendContinuationKey(make([]byte, 0, len(io.prefix)+len(suffix)), io.prefix[:], suffix)
}

// appendContinuationKey appends the store key of a generation's suffix.
func appendContinuationKey(dst, prefix, suffix []byte) []byte {
	if isContinuationBucketSuffix(suffix) {
		dst = append(append(dst, continuationBucketKeyPrefix), suffix[1:5]...)
		return append(append(dst, prefix...), suffix[5:]...)
	}
	return append(append(dst, prefix...), suffix...)
}

// suffix inverts key for keys under this generation.
func (io *continuationIO) suffix(key []byte) []byte {
	if key[0] != continuationBucketKeyPrefix {
		return append([]byte(nil), key[len(io.prefix):]...)
	}
	suffix := make([]byte, len(key)-len(io.prefix))
	suffix[0] = continuationBucketMark
	copy(suffix[1:5], key[1:5])
	copy(suffix[5:], key[5+len(io.prefix):])
	return suffix
}

// continuationBucketKeyPrefix leads every time-partitioned key in the store.
const continuationBucketKeyPrefix = 't'

// Get returns the newest version of key through a fresh lookup, which stops
// at the first level holding the key. That is the cheapest read of a key
// recent updates rewrote, whose older versions fill the levels below it.
func (io *continuationIO) Get(key []byte) ([]byte, error) { return io.read(key, false) }

// Probe reads key like Get, through the shared iterator when the io has one
// (see withIterator). It suits a key that is usually absent or long
// unchanged: the iterator consults each level's bloom filter without opening
// a table again, but unlike a fresh lookup it also reads every older version
// of a key below the newest.
func (io *continuationIO) Probe(key []byte) ([]byte, error) { return io.read(key, true) }

func (io *continuationIO) read(key []byte, probe bool) ([]byte, error) {
	if err := io.ctx.Err(); err != nil {
		return nil, err
	}
	if io.pendingKeys != nil {
		if _, exists := io.pendingKeys[string(key)]; !exists {
			return nil, ErrNotFound
		}
	}
	if v := io.state; v != nil && v.keys.StateKey(key) {
		if value, known := v.lookup(key); known {
			if value == nil {
				return nil, ErrNotFound
			}
			return append([]byte(nil), value...), nil
		}
		value, err := io.readStore(key, probe)
		if err == nil {
			v.mutable().set(key, append([]byte(nil), value...))
		} else if errors.Is(err, ErrNotFound) {
			v.mutable().set(key, nil)
		}
		return value, err
	}
	if slot := io.stateSlot(key); slot != nil {
		if value, known := io.slotValue(slot, key); known {
			return value, nil
		}
		value, err := io.readStore(key, probe)
		if err == nil {
			io.setSlot(slot, key, value)
		}
		return value, err
	}
	return io.readStore(key, probe)
}

// readStore reads key from the io's reader (see read).
func (io *continuationIO) readStore(key []byte, probe bool) ([]byte, error) {
	if !probe {
		return io.lookup(key)
	}
	iter, err := io.sharedIter(key)
	if err != nil || iter == nil {
		return io.lookup(key)
	}
	countStoreRead()
	// The comparer's Split is the whole key, so a prefix seek is a point
	// lookup: it consults each table's bloom filter as a fresh lookup does,
	// where SeekGE reads index and data blocks in every level.
	full := io.key(key)
	if iter.SeekPrefixGE(full) && bytes.Equal(iter.Key(), full) {
		return append([]byte(nil), iter.Value()...), nil
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return nil, ErrNotFound
}

// lookup reads key with a fresh LSM lookup.
func (io *continuationIO) lookup(key []byte) ([]byte, error) {
	countStoreRead()
	value, closer, err := io.reader.Get(io.key(key))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	owned := append([]byte(nil), value...)
	return owned, closer.Close()
}

func (io *continuationIO) writable() error {
	if err := io.ctx.Err(); err != nil {
		return err
	}
	if io.batch == nil {
		return errors.New("continuation snapshot is read-only")
	}
	return nil
}

func (io *continuationIO) Set(key, value []byte) error {
	if err := io.writable(); err != nil {
		return err
	}
	if bytes.Equal(key, SidecarSummaryKey) {
		summary, err := DecodeSidecarSummary(value)
		if err != nil {
			return err
		}
		io.sidecar = &summary
	}
	err := io.batch.Set(io.key(key), value, nil)
	if err == nil && io.pendingKeys != nil {
		io.pendingKeys[string(key)] = struct{}{}
	}
	if v := io.state; err == nil && v != nil && v.keys.StateKey(key) {
		v.mutable().set(key, append([]byte(nil), value...))
	}
	if err == nil {
		if slot := io.stateSlot(key); slot != nil {
			io.setSlot(slot, key, value)
		}
	}
	io.changed = io.changed || err == nil
	return err
}

func (io *continuationIO) Delete(key []byte) error {
	io.clearSlots()
	if err := io.writable(); err != nil {
		return err
	}
	if io.pendingKeys != nil {
		if _, exists := io.pendingKeys[string(key)]; !exists {
			return nil
		}
	}
	err := io.batch.Delete(io.key(key), nil)
	if err == nil && io.pendingKeys != nil {
		delete(io.pendingKeys, string(key))
	}
	if v := io.state; err == nil && v != nil && v.keys.StateKey(key) {
		v.mutable().set(key, nil)
	}
	io.changed = io.changed || err == nil
	return err
}

func (io *continuationIO) DeletePrefix(prefix []byte) error {
	io.clearSlots()
	if err := io.writable(); err != nil {
		return err
	}
	if io.pendingKeys != nil {
		// No older data exists in this generation. Point deletes of this
		// batch's own writes are sufficient; empty range tombstones make
		// subsequent imports repeatedly fragment the whole memtable.
		for key := range io.pendingKeys {
			if strings.HasPrefix(key, string(prefix)) {
				if err := io.Delete([]byte(key)); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if v := io.state; v != nil {
		// Recorded first: should a delete below fail, the update fails and
		// nothing of this view is cached.
		v.mutable().deletePrefix(prefix)
	}
	start := io.key(prefix)
	if keys, ok, err := io.visibleKeysUpTo(start, continuationPointDeleteLimit); err != nil {
		return err
	} else if ok {
		for _, key := range keys {
			if err := io.batch.Delete(key, nil); err != nil {
				return err
			}
		}
		io.changed = true
		return nil
	}
	continuationRangeDeletes.Add(1)
	err := io.batch.DeleteRange(start, prefixEnd(start), nil)
	io.changed = io.changed || err == nil
	return err
}

// Every new memtable range tombstone invalidates the fragment cache that all
// reads of that memtable share: the next reader re-fragments every range
// tombstone in the memtable while the others wait. With a 1024-key limit,
// go-whisper's out-of-order rewrites deleted whole virtual files by range
// (most after a restart's cache restore), and on a production store 40-80 of 96
// persister workers queued on that fragmentation. Prefixes are deleted key
// by key up to a limit no virtual file reaches; the counter shows any
// remaining range tombstones.
const continuationPointDeleteLimit = 1 << 20

// continuationRangeDeletes counts DeletePrefix calls that fell back to a range
// tombstone.
var continuationRangeDeletes atomic.Uint64

// visibleKeysUpTo lists keys under start only when the reader observes this
// batch's own writes; otherwise a key set earlier in the batch would survive.
// Store callers serialize mutations for this metric generation through commit.
func (io *continuationIO) visibleKeysUpTo(start []byte, limit int) ([][]byte, bool, error) {
	if !io.readsOwnBatch() {
		return nil, false, nil
	}
	iter, err := io.batch.NewIter(&pebble.IterOptions{LowerBound: start, UpperBound: prefixEnd(start)})
	if err != nil {
		return nil, false, err
	}
	var keys [][]byte
	for iter.First(); iter.Valid(); iter.Next() {
		if len(keys) == limit {
			return nil, false, iter.Close()
		}
		keys = append(keys, append([]byte(nil), iter.Key()...))
	}
	return keys, true, errors.Join(iter.Error(), iter.Close())
}

func (io *continuationIO) Visit(prefix []byte, visit func([]byte, []byte) error) error {
	if err := io.ctx.Err(); err != nil {
		return err
	}
	if io.pendingKeys != nil {
		var keys []string
		for key := range io.pendingKeys {
			if strings.HasPrefix(key, string(prefix)) {
				keys = append(keys, key)
			}
		}
		sort.Strings(keys)
		// Freeze the batch view before invoking callbacks, and return owned
		// bytes just as the ordinary iterator does.
		values := make([][]byte, len(keys))
		for i, key := range keys {
			value, err := io.Get([]byte(key))
			if err != nil {
				return err
			}
			values[i] = value
		}
		for i, key := range keys {
			if err := io.ctx.Err(); err != nil {
				return err
			}
			if err := visit([]byte(key), values[i]); err != nil {
				return err
			}
		}
		return nil
	}
	if v := io.state; v != nil && v.keys.StatePrefix(prefix) {
		return io.visitState(v, prefix, visit)
	}
	return io.visitStore(prefix, visit)
}

// visitState walks a cacheable prefix from the view when it holds all of it,
// else from the store, recording the result.
func (io *continuationIO) visitState(v *stateView, prefix []byte, visit func([]byte, []byte) error) error {
	if st := v.view(); st != nil && st.isComplete(prefix) {
		// Freeze the view before invoking callbacks, as an iterator does.
		keys, values := st.walk(prefix)
		for i, key := range keys {
			if err := io.ctx.Err(); err != nil {
				return err
			}
			if err := visit(key, values[i]); err != nil {
				return err
			}
		}
		return nil
	}
	found := make(map[string][]byte)
	err := io.visitStore(prefix, func(key, value []byte) error {
		found[string(key)] = append([]byte(nil), value...)
		return visit(key, value)
	})
	if err == nil {
		v.mutable().replacePrefix(prefix, found)
	}
	return err
}

// visitStore walks prefix in the io's reader (see Visit).
func (io *continuationIO) visitStore(prefix []byte, visit func([]byte, []byte) error) error {
	countStoreRead()
	start := io.key(prefix)
	if shared, err := io.sharedIter(prefix); err != nil {
		return err
	} else if shared != nil {
		io.iterBusy = true
		defer func() { io.iterBusy = false }()
		for valid := shared.SeekGE(start); valid && bytes.HasPrefix(shared.Key(), start); valid = shared.Next() {
			if err := io.ctx.Err(); err != nil {
				return err
			}
			if err := visit(io.suffix(shared.Key()), append([]byte(nil), shared.Value()...)); err != nil {
				return err
			}
		}
		return shared.Error()
	}
	iter, err := io.reader.NewIter(&pebble.IterOptions{LowerBound: start, UpperBound: prefixEnd(start)})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := io.ctx.Err(); err != nil {
			return err
		}
		if err := visit(io.suffix(iter.Key()), append([]byte(nil), iter.Value()...)); err != nil {
			return err
		}
	}
	return iter.Error()
}
