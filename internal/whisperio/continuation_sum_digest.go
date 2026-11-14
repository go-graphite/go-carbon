package whisperio

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// The identity is logical time, never a physical block or pack-group index.
// Dense complete buckets keep the original eight-byte sum. Other entries also
// store their canonical finite count and exact covered slot bounds. Invalidation
// follows logical time buckets, including partial ranges and sparse histories.
const sumBucketSlots = 64 * 128

var sumRevisionKey = []byte(store.ContinuationDigestPrefix)
var sumBucketPrefix = []byte(store.ContinuationDigestPrefix + "/")

type sumCacheWriter interface {
	CacheDigest([]byte, []byte)
	ClearDigestCache()
}

func sumBucketKey(archive, bucket int) []byte {
	key := append([]byte(nil), sumBucketPrefix...)
	key = binary.BigEndian.AppendUint16(key, uint16(archive))
	return binary.BigEndian.AppendUint32(key, uint32(bucket))
}

func sumCacheCurrent(kv store.ContinuationIO, revision uint64) (bool, error) {
	data, err := kv.Get(sumRevisionKey)
	if errors.Is(err, store.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return len(data) == 8 && binary.BigEndian.Uint64(data) == revision, nil
}

// SumDigest reads the canonical archive view on a single immutable snapshot.
// Every visited bucket range is lazily cached, including sparse and partial
// ranges. A hit requires the exact slot bounds and a current revision fence.
// The sum is probabilistic, not a cryptographic integrity proof.
func (c *ContinuationCodec) SumDigest(ctx context.Context, kv store.ContinuationIO, m store.Metadata, from, until int, clock func() time.Time, steps ...int) ([]store.SumArchive, error) {
	// A cache hit must not open or reconstruct the virtual Whisper files.
	var w *whisper.Whisper
	defer func() {
		if w != nil {
			_ = w.Close()
		}
	}()
	now := int(clock().Unix())
	current, err := sumCacheCurrent(kv, m.Revision)
	if err != nil {
		return nil, err
	}
	cache, writable := kv.(sumCacheWriter)
	filled := false
	out := make([]store.SumArchive, 0, len(m.Retentions))
	for ai, r := range m.Retentions {
		if len(steps) > 0 && steps[0] > 0 && r.Step != steps[0] {
			continue
		}
		lo, hi := store.DigestBounds(r, now, from, until)
		var sparse []store.PointSum
		sparseChecked := false
		sparseOK := false
		var total store.PointSum
		span := r.Step * sumBucketSlots
		for start := lo; start < hi; {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			bucket := start / span
			end := min(hi, (bucket+1)*span)
			key := sumBucketKey(ai, bucket)
			firstSlot := (start - bucket*span) / r.Step
			endSlot := (end - bucket*span) / r.Step
			if current {
				value, err := kv.Get(key)
				if err != nil && !errors.Is(err, store.ErrNotFound) {
					return nil, err
				}
				if d, ok := decodeSumBucket(value, firstSlot, endSlot); err == nil && ok {
					total.Merge(d)
					start = end
					continue
				}
			}
			if w == nil {
				fs := newContinuationFiles(kv, true).withClock(clock)
				fs.rangeReads = true
				w, err = whisper.OpenWithOptions(fs.main(), continuationOptions(fs, clock))
				if err != nil {
					return nil, err
				}
			}
			var d store.PointSum
			if !w.IsCompressed() {
				err = visitClassicWindow(ctx, w.StorageFile().(*continuationFile), ai, start, end, func(ts int, v float64) { d.Add(int64(ts), v) })
			}
			if w.IsCompressed() && !sparseChecked {
				sparseChecked = true
				if !c.keepDenseSumReads && hi-lo >= r.MaxRetention()/2 {
					sparse, sparseOK, err = compressedArchiveSums(ctx, w, ai, r, lo, hi)
					if err != nil {
						return nil, err
					}
				}
			}
			if sparseOK {
				d = sparse[bucket-lo/span]
			} else if w.IsCompressed() || errors.Is(err, errClassicUnalignedBase) {
				var series *whisper.TimeSeries
				series, err = w.FetchArchiveWindow(whisper.ArchiveWindow{Archive: ai, From: start, Until: end})
				if err == nil {
					for i, v := range series.Values() {
						d.Add(int64(series.FromTime()+i*r.Step), v)
					}
				}
			}
			if err != nil {
				return nil, err
			}
			if writable {
				if !filled && !current {
					cache.ClearDigestCache()
				}
				cache.CacheDigest(key, encodeSumBucket(d, firstSlot, endSlot))
				filled = true
			}
			total.Merge(d)
			start = end
		}
		out = append(out, store.SumArchive{Archive: ai, Step: r.Step, Points: r.Count, From: lo, Until: hi, Sum: fmt.Sprintf("%016x", total.Sum), Count: total.Count})
	}
	if writable && filled {
		cache.CacheDigest(sumRevisionKey, binary.BigEndian.AppendUint64(nil, m.Revision))
	}
	return out, nil
}

// The extended value remains disposable and backward compatible: older readers
// ignore its length. Slot bounds distinguish a clipped request from a full bucket.
func encodeSumBucket(d store.PointSum, first, end int) []byte {
	value := binary.BigEndian.AppendUint64(nil, d.Sum)
	if first == 0 && end == sumBucketSlots && d.Count == sumBucketSlots {
		return value
	}
	value = binary.BigEndian.AppendUint16(value, uint16(d.Count))
	value = binary.BigEndian.AppendUint16(value, uint16(first))
	return binary.BigEndian.AppendUint16(value, uint16(end))
}

func decodeSumBucket(value []byte, first, end int) (store.PointSum, bool) {
	if len(value) == 8 && first == 0 && end == sumBucketSlots {
		return store.PointSum{Sum: binary.BigEndian.Uint64(value), Count: sumBucketSlots}, true
	}
	if len(value) != 14 || int(binary.BigEndian.Uint16(value[10:12])) != first || int(binary.BigEndian.Uint16(value[12:14])) != end {
		return store.PointSum{}, false
	}
	count := uint64(binary.BigEndian.Uint16(value[8:10]))
	if count > uint64(end-first) {
		return store.PointSum{}, false
	}
	return store.PointSum{Sum: binary.BigEndian.Uint64(value), Count: count}, true
}

// sumWriteView collects only header metadata, never historical point payloads.
// Changed/retired block ranges and old/new buffered-rollup windows conservatively
// invalidate time buckets. A file replacement or classic slot repair clears the
// cache. Mutations outside Update do not advance the cache's revision fence and
// therefore miss safely on the next digest (cold packing, patch, copy import).
type sumWriteView struct {
	file       uint64
	compressed bool
	sidecar    bool
	blocks     []whisper.ContinuationBlock
	buffers    []whisper.ArchiveWindow
}

func readSumWriteView(kv store.ContinuationIO, clock func() time.Time) (sumWriteView, error) {
	fs := newContinuationFiles(kv, true).withClock(clock)
	w, err := whisper.OpenWithOptions(fs.main(), continuationOptions(fs, clock))
	if err != nil {
		return sumWriteView{}, err
	}
	defer w.Close()
	result := sumWriteView{file: w.StorageFile().(*continuationFile).data.id, compressed: w.IsCompressed(), sidecar: w.OutOfOrderPath() != "", buffers: w.BufferedRollupWindows()}
	if result.compressed {
		layout, err := w.ContinuationLayout()
		if err != nil {
			return result, err
		}
		result.blocks = layout.Blocks
	}
	return result, nil
}

func (c *ContinuationCodec) updateWithSumCache(ctx context.Context, kv store.ContinuationIO, m store.Metadata, input []store.Point, retention int, clock func() time.Time, maintenance *store.ContinuationMaintenance) error {
	if len(input) == 0 {
		return c.updateReanchoring(ctx, kv, m, input, retention, clock, maintenance)
	}
	current, err := sumCacheCurrent(kv, m.Revision)
	if err != nil {
		return err
	}
	if !current {
		return c.updateReanchoring(ctx, kv, m, input, retention, clock, maintenance)
	}
	if retention >= 0 {
		// Archive-preserving imports select by retention seconds and can write
		// directly into circular slots. Keep their broader repair semantics out
		// of normal ingestion's selective invalidation path.
		if err := c.updateReanchoring(ctx, kv, m, input, retention, clock, maintenance); err != nil {
			return err
		}
		return kv.DeletePrefix(sumRevisionKey)
	}
	before, err := readSumWriteView(kv, clock)
	if err != nil {
		return err
	}
	maintenanceChanged := false
	if maintenance != nil {
		// Writers pass a maintenance policy even when no merge is due. Only
		// actual maintenance must discard all historical cache entries.
		policy := *maintenance
		report := policy.Result
		policy.Result = func(merged bool, err error) {
			maintenanceChanged = maintenanceChanged || merged
			if report != nil {
				report(merged, err)
			}
		}
		maintenance = &policy
	}
	if err := c.updateReanchoring(ctx, kv, m, input, retention, clock, maintenance); err != nil {
		return err
	}
	after, err := readSumWriteView(kv, clock)
	if err != nil {
		return err
	}
	if !before.compressed || before.file != after.file || before.sidecar && !after.sidecar || maintenanceChanged {
		return kv.DeletePrefix(sumRevisionKey)
	}
	// Coalesce dirty windows into time-bucket identities once. A large backfill
	// must not scan every input point again for every existing cache entry.
	now := int(clock().Unix())
	dirtyBuckets := make(map[string]struct{})
	mark := func(ai, from, until int) {
		from = max(from, now-m.Retentions[ai].MaxRetention()+1)
		until = min(until, now+1)
		if until <= from {
			return
		}
		span := m.Retentions[ai].Step * sumBucketSlots
		for b := from / span; b <= (until-1)/span; b++ {
			dirtyBuckets[string(sumBucketKey(ai, b))] = struct{}{}
		}
	}
	for _, window := range append(before.buffers, after.buffers...) {
		mark(window.Archive, window.From, window.Until)
	}
	type blockID struct{ archive, index int }
	old := make(map[blockID]whisper.ContinuationBlock, len(before.blocks))
	for _, b := range before.blocks {
		old[blockID{b.Archive, b.Index}] = b
	}
	addBlock := func(b whisper.ContinuationBlock) { mark(b.Archive, b.First, b.Last+b.Step) }
	for _, b := range after.blocks {
		id := blockID{b.Archive, b.Index}
		previous, ok := old[id]
		if ok && b.First == previous.First && b.Last > previous.Last && b.Count > previous.Count {
			// Extending an existing physical block leaves its older points
			// intact. In-place corrections are covered by input/buffer windows.
			mark(b.Archive, previous.Last+b.Step, b.Last+b.Step)
		} else if !ok || b != previous {
			addBlock(b)
			if ok {
				addBlock(previous)
			}
		}
		delete(old, id)
	}
	for _, b := range old {
		addBlock(b)
	}
	// In-place overwrites can leave block bounds/count unchanged. Input may also
	// propagate into any lower archive, so invalidate its aligned slot in each.
	for _, p := range input {
		if p.Timestamp <= 0 || p.Timestamp > int64(now) {
			continue
		}
		for ai, r := range m.Retentions {
			ts := int(p.Timestamp) / r.Step * r.Step
			mark(ai, ts, ts+r.Step)
		}
	}
	var remove [][]byte
	err = kv.Visit(sumBucketPrefix, func(key, _ []byte) error {
		if len(key) != 9 {
			remove = append(remove, key)
			return nil
		}
		ai := int(binary.BigEndian.Uint16(key[3:5]))
		if ai >= len(m.Retentions) {
			remove = append(remove, key)
			return nil
		}
		r := m.Retentions[ai]
		start := int(binary.BigEndian.Uint32(key[5:])) * r.Step * sumBucketSlots
		_, changed := dirtyBuckets[string(key)]
		dirty := changed || start < now-r.MaxRetention()+1
		if dirty {
			remove = append(remove, key)
		}
		return ctx.Err()
	})
	if err != nil {
		return err
	}
	for _, key := range remove {
		if err := kv.Delete(key); err != nil {
			return err
		}
	}
	return kv.Set(sumRevisionKey, binary.BigEndian.AppendUint64(nil, m.Revision+1))
}
