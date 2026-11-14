package whisperio

import (
	"context"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// RestoreArchives writes a canonical continuation with independently
// authoritative archives. A live resize's target-wins union cannot be replayed
// through Update: that propagates base points over authoritative coarse values.
// Disjoint transfers retain their original compressed writer state verbatim.
func (c *ContinuationCodec) RestoreArchives(ctx context.Context, kv store.ContinuationIO, snapshot store.Snapshot, clock func() time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// The importer canonicalizes timestamps and ring aliases before calling
	// us. Fail closed if a different caller cannot fit its exact image in a
	// classic ring; truncating an off-grid distance silently moves points.
	if len(snapshot.Archives) != len(snapshot.Metadata.Retentions) {
		return fmt.Errorf("restore archive count differs from metadata")
	}
	for ai, archive := range snapshot.Archives {
		r := archive.Retention
		if r != snapshot.Metadata.Retentions[ai] || r.Step <= 0 || r.Count <= 0 {
			return fmt.Errorf("restore archive %d invalid retention", ai)
		}
		if err := validateRestoreSlots(ai, archive); err != nil {
			return err
		}
	}
	fs := newContinuationFiles(kv, false).withClock(clock)
	options := continuationOptions(fs, clock)
	options.Compressed = false
	retentions := make([]whisper.Retention, len(snapshot.Metadata.Retentions))
	for i, r := range snapshot.Metadata.Retentions {
		retentions[i] = whisper.NewRetention(r.Step, r.Count)
	}
	w, err := whisper.CreateWithOptions(fs.main(), whisper.NewRetentionsNoPointer(retentions), whisper.AggregationMethod(snapshot.Metadata.AggregationMethod), snapshot.Metadata.XFilesFactor, options)
	if err != nil {
		return err
	}
	defer w.Close()
	file, err := fs.OpenFile(fs.main(), os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	f := file.(*continuationFile)
	f.data.windowIndex = &classicWindowIndex{archives: make([]classicWindowArchive, len(snapshot.Archives)), revision: snapshot.Metadata.Revision}
	f.data.windowLoaded, f.data.windowDirty = true, true
	for ai, archive := range snapshot.Archives {
		for _, p := range archive.Points {
			a := &f.data.windowIndex.archives[ai]
			if a.base == 0 || p.Timestamp < a.base {
				a.base = p.Timestamp
			}
		}
	}
	layout := w.StorageLayout()
	for ai, archive := range snapshot.Archives {
		// Classic Fetch uses physical slot zero as the ring's timestamp anchor.
		// An absolute epoch slot can leave that slot empty in a sparse archive.
		var base int64
		if len(archive.Points) > 0 {
			base = archive.Points[0].Timestamp
			for _, p := range archive.Points[1:] {
				base = min(base, p.Timestamp)
			}
		}
		groups := make(map[int]*store.PointBlock)
		for _, p := range archive.Points {
			if err := ctx.Err(); err != nil {
				return err
			}
			slot := int((p.Timestamp-base)/int64(archive.Retention.Step)) % archive.Retention.Count
			block := groups[slot/128]
			if block == nil {
				block = &store.PointBlock{}
				groups[slot/128] = block
			}
			offset := slot % 128
			block.Present[offset/64] |= uint64(1) << uint(offset%64)
			block.Points[offset] = p
			f.allocateClassicPages(layout.Archives[ai].Offset+slot*12, layout.Archives[ai].Offset+slot*12+12)
		}
		for index, block := range groups {
			if err := f.setPointBlock(continuationBodyKey(f.data.id, 'p', ai, 0, index), block); err != nil {
				return err
			}
		}
	}
	if err := w.Close(); err != nil {
		return err
	}
	return fs.flush()
}

var restoreSlotPool = sync.Pool{New: func() any { return make(map[int64]bool) }}

func validateRestoreSlots(ai int, archive store.Archive) error {
	r := archive.Retention
	seen := restoreSlotPool.Get().(map[int64]bool)
	defer func() {
		if len(seen) <= 1<<18 {
			clear(seen)
			restoreSlotPool.Put(seen)
		}
	}()
	for _, p := range archive.Points {
		if p.Timestamp <= 0 || p.Timestamp > math.MaxUint32 || p.Timestamp%int64(r.Step) != 0 {
			return fmt.Errorf("restore archive %d timestamp %d is not on its grid", ai, p.Timestamp)
		}
		slot := p.Timestamp / int64(r.Step) % int64(r.Count)
		if seen[slot] {
			return fmt.Errorf("restore archive %d has aliased ring slots", ai)
		}
		seen[slot] = true
	}
	return nil
}
