package whisperio

import (
	"context"
	"errors"
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
// want are the changed keys written to dst; otherwise it returns false and
// the caller uses RestoreArchives.
//
// WIP: the read-back is a full decode; a ranged verify of the appended blocks
// is the next step towards the per-name target.
func (c *ContinuationCodec) MergeArchives(ctx context.Context, source, target, dst store.ContinuationIO, want, targetSnapshot store.Snapshot, clock func() time.Time) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	epoch := func() time.Time { return time.Unix(0, 0) }
	overlay := newContinuationOverlay(source)
	fs := newContinuationFiles(overlay, false).withClock(clock)
	w, err := whisper.OpenWithOptions(fs.main(), continuationOptions(fs, epoch))
	if err != nil {
		return false, err
	}
	defer w.Close()
	tfs := newContinuationFiles(newContinuationOverlay(target), false).withClock(clock)
	tw, err := whisper.OpenWithOptions(tfs.main(), continuationOptions(tfs, epoch))
	if err != nil {
		return false, err
	}
	defer tw.Close()
	if !w.IsCompressed() || !tw.IsCompressed() || len(want.Archives) != len(targetSnapshot.Archives) {
		return false, nil
	}
	// Fold sidecars as Snapshot does, so stored points match want.
	if err := w.MergeOutOfOrder(); err != nil {
		return false, err
	}
	if err := tw.MergeOutOfOrder(); err != nil {
		return false, err
	}
	// A target point at or below the source's blocks overlaps them and can
	// never be appended; fall back before doing any work.
	froms := make([]int, len(want.Archives))
	for i := range want.Archives {
		from, err := w.ArchiveBlockWatermark(i)
		if err != nil {
			return false, nil
		}
		if points := targetSnapshot.Archives[i].Points; len(points) > 0 && points[0].Timestamp <= int64(from) {
			return false, nil
		}
		froms[i] = from
	}
	for i, archive := range want.Archives {
		from := froms[i]
		until, err := tw.ArchiveBlockWatermark(i)
		if err != nil {
			return false, nil
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
				return false, nil
			}
			return false, err
		}
	}
	if err := w.ReplaceBuffers(tw); err != nil {
		return false, nil
	}
	if err := w.Close(); err != nil {
		return false, err
	}
	if err := fs.flush(); err != nil {
		return false, err
	}
	rfs := newContinuationFiles(overlay, false).withClock(clock)
	r, err := whisper.OpenWithOptions(rfs.main(), continuationOptions(rfs, epoch))
	if err != nil {
		return false, err
	}
	defer r.Close()
	got, err := continuationArchiveSnapshot(ctx, r, want.Metadata)
	if err != nil {
		return false, err
	}
	if store.EqualSnapshots(want, got) != nil {
		return false, nil
	}
	return true, overlay.applyTo(dst)
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
