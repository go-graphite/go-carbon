package whisperio

import (
	"context"
	"errors"
	"math"
	"sort"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// MergeArchives merges a newer copy (target) into a staged compressed source
// without re-encoding the source's history. Per archive it appends the
// expected union's points newer than the source's blocks, except those the
// target's buffers produce, then installs the target's buffers. The result
// is built on a private overlay and read back. Only when every archive equals
// want are the changed keys written to dst and "" returned; otherwise it
// returns a store.TransferMergeFallback* reason and the caller uses
// RestoreArchives.
//
// A classic (uncompressed) source is merged in place instead, whatever the
// target's format: see mergeClassicArchives. A classic target can instead
// serve as the base for a compressed source. classic reports either slot merge.
func (c *ContinuationCodec) MergeArchives(ctx context.Context, source, target, dst store.ContinuationIO, want, sourceSnapshot, targetSnapshot store.Snapshot, clock func() time.Time) (fallback string, classic bool, err error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	epoch := func() time.Time { return time.Unix(0, 0) }
	overlay := newContinuationOverlay(source)
	fs := newContinuationFiles(overlay, false).withClock(clock)
	w, err := whisper.OpenWithOptions(fs.main(), continuationOptions(fs, epoch))
	if err != nil {
		return "", false, err
	}
	defer w.Close()
	if len(want.Archives) != len(targetSnapshot.Archives) || len(want.Archives) != len(sourceSnapshot.Archives) {
		return store.TransferMergeFallbackLayout, false, nil
	}
	if !w.IsCompressed() {
		// RestoreArchives writes the policy from want's metadata; an in-place
		// merge keeps the source header, so both must agree.
		if store.AggregationMethod(w.AggregationMethod()) != want.Metadata.AggregationMethod || w.XFilesFactor() != want.Metadata.XFilesFactor {
			return store.TransferMergeFallbackLayout, true, nil
		}
		if err := w.Close(); err != nil {
			return "", true, err
		}
		fallback, err := mergeClassicArchives(ctx, overlay, dst, want, sourceSnapshot, targetSnapshot, clock)
		return fallback, true, err
	}
	tfs := newContinuationFiles(newContinuationOverlay(target), false).withClock(clock)
	tw, err := whisper.OpenWithOptions(tfs.main(), continuationOptions(tfs, epoch))
	if err != nil {
		return "", false, err
	}
	defer tw.Close()
	if !tw.IsCompressed() {
		// A previous import may already have restored the target as classic.
		// Preserve that ring and apply the source's surviving points, including
		// timestamp overlap; want already gives equal timestamps to the target.
		if store.AggregationMethod(tw.AggregationMethod()) != want.Metadata.AggregationMethod || tw.XFilesFactor() != want.Metadata.XFilesFactor {
			return store.TransferMergeFallbackLayout, false, nil
		}
		if err := tw.Close(); err != nil {
			return "", false, err
		}
		return mergeIntoClassicTarget(ctx, source, target, dst, want, sourceSnapshot, targetSnapshot, clock)
	}
	// Fold sidecars as Snapshot does, so stored points match want.
	if err := w.MergeOutOfOrder(); err != nil {
		return "", false, err
	}
	if err := tw.MergeOutOfOrder(); err != nil {
		return "", false, err
	}
	// A target point at or below the source's blocks overlaps them and can
	// never be appended; fall back before doing any work.
	froms := make([]int, len(want.Archives))
	for i := range want.Archives {
		from, err := w.ArchiveBlockWatermark(i)
		if err != nil {
			return store.TransferMergeFallbackLayout, false, nil
		}
		if points := targetSnapshot.Archives[i].Points; len(points) > 0 && points[0].Timestamp <= int64(from) {
			return store.TransferMergeFallbackOverlap, false, nil
		}
		froms[i] = from
	}
	for i, archive := range want.Archives {
		from := froms[i]
		until, err := tw.ArchiveBlockWatermark(i)
		if err != nil {
			return store.TransferMergeFallbackLayout, false, nil
		}
		virtual := targetSnapshot.Archives[i].Points
		virtual = virtual[sort.Search(len(virtual), func(k int) bool { return virtual[k].Timestamp > int64(until) }):]
		var tail []whisper.TimeSeriesPoint
		for _, p := range archive.Points {
			if p.Timestamp <= int64(from) {
				continue
			}
			k := sort.Search(len(virtual), func(k int) bool { return virtual[k].Timestamp >= p.Timestamp })
			if k < len(virtual) && virtual[k].Timestamp == p.Timestamp {
				continue // produced by the target's buffers
			}
			tail = append(tail, whisper.TimeSeriesPoint{Time: int(p.Timestamp), Value: p.Value})
		}
		if err := w.AppendArchivePoints(i, tail); err != nil {
			if errors.Is(err, whisper.ErrArchiveAppendOrder) {
				return store.TransferMergeFallbackOrder, false, nil
			}
			return "", false, err
		}
	}
	if err := w.ReplaceBuffers(tw); err != nil {
		return store.TransferMergeFallbackLayout, false, nil
	}
	if err := w.Close(); err != nil {
		return "", false, err
	}
	if err := fs.flush(); err != nil {
		return "", false, err
	}
	rfs := newContinuationFiles(overlay, false).withClock(clock)
	r, err := whisper.OpenWithOptions(rfs.main(), continuationOptions(rfs, epoch))
	if err != nil {
		return "", false, err
	}
	defer r.Close()
	rfs.snapshotReads = !c.keepSnapshotReadAllocations
	match, err := continuationArchivesMatch(ctx, r, want)
	if err != nil {
		return "", false, err
	}
	if !match {
		return store.TransferMergeFallbackVerify, false, nil
	}
	return "", false, overlay.applyTo(dst)
}

// Verify each independently authoritative archive as it is decoded. The
// decoder's arrays need not be copied into a second, whole-metric snapshot.
// Every stored timestamp and float bit is still checked before publication.
func continuationArchivesMatch(ctx context.Context, w *whisper.Whisper, want store.Snapshot) (bool, error) {
	for i, archive := range want.Archives {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		points, err := w.ArchivePointsWithRollups(i)
		if err != nil {
			return false, err
		}
		if len(points) != len(archive.Points) {
			return false, nil
		}
		for j, p := range points {
			q := archive.Points[j]
			if int64(p.Time) != q.Timestamp || math.Float64bits(p.Value) != math.Float64bits(q.Value) {
				return false, nil
			}
		}
	}
	return true, nil
}

// applyTo writes the overlay's changes to dst: prefix deletes first, then
// point deletes and values. DeletePrefix already dropped overlay values under
// its prefix, so later values survive.
func (o *continuationOverlay) applyTo(dst store.ContinuationIO) error {
	for _, p := range o.prefixes {
		if err := dst.DeletePrefix(p); err != nil {
			return err
		}
	}
	for k := range o.deleted {
		if err := dst.Delete([]byte(k)); err != nil {
			return err
		}
	}
	for k, v := range o.values {
		if err := dst.Set([]byte(k), v); err != nil {
			return err
		}
	}
	return nil
}

var _ store.ContinuationArchiveMerger = (*ContinuationCodec)(nil)
