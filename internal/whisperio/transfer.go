package whisperio

import (
	"context"
	"os"
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
	layout := w.StorageLayout()
	for ai, archive := range snapshot.Archives {
		groups := make(map[int]*store.PointBlock)
		for _, p := range archive.Points {
			if err := ctx.Err(); err != nil {
				return err
			}
			slot := int(p.Timestamp/int64(archive.Retention.Step)) % archive.Retention.Count
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
