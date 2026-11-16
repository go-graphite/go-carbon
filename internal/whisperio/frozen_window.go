package whisperio

import (
	"context"
	"errors"
	"math"
	"os"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// FetchWindowInto preserves the source engine's archive/OOO/buffer semantics
// while decoding only uncovered runs. Compressed groups intersecting those
// runs retain the existing CRC and range-read behavior. Covered payloads are
// intentionally not verified by this query, just as with ordinary range reads.
func (c *ContinuationCodec) FetchWindowInto(ctx context.Context, kv store.ContinuationIO, m store.Metadata, archive, from, until int, dst []float64, covered []bool, clock func() time.Time) (err error) {
	defer func() {
		if value := recover(); value != nil {
			cause := ctx.Err()
			message, ok := value.(string)
			if cause != nil && ok && message == "Failed to read baseInterval: "+cause.Error() {
				err = cause
				return
			}
			panic(value)
		}
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	if archive < 0 || archive >= len(m.Retentions) {
		return store.ErrFormat
	}
	step := m.Retentions[archive].Step
	if step <= 0 || from < 0 || until <= from || from%step != 0 || until%step != 0 || (until-from)/step != len(dst) || len(dst) > m.Retentions[archive].Count || covered != nil && len(covered) != len(dst) {
		return store.ErrFormat
	}
	fs := newContinuationFiles(kv, true)
	fs.rangeReads = true
	workspace := borrowFetchBlock(fs)
	defer releaseFetchBlock(fs, workspace)
	options := continuationOptions(fs, clock)
	options.FileSystem = &windowFiles{fs}
	w, err := whisper.OpenWithOptions(fs.main(), options)
	if err != nil {
		return err
	}
	defer w.Close()
	f := w.StorageFile().(*windowFile).continuationFile
	for first := 0; first < len(dst); {
		if err := ctx.Err(); err != nil {
			return err
		}
		if covered != nil && covered[first] {
			first++
			continue
		}
		end := first + 1
		for end < len(dst) && (covered == nil || !covered[end]) {
			end++
		}
		lo, hi := from+first*step, from+end*step
		for i := first; i < end; i++ {
			dst[i] = math.NaN()
		}
		if !w.IsCompressed() {
			err = visitClassicWindow(ctx, f, archive, lo, hi, func(ts int, value float64) { dst[(ts-from)/step] = value })
			if err != nil && !errors.Is(err, errClassicUnalignedBase) {
				return err
			}
		}
		if w.IsCompressed() || errors.Is(err, errClassicUnalignedBase) {
			series, e := w.FetchArchiveWindow(whisper.ArchiveWindow{Archive: archive, From: lo, Until: hi})
			if e != nil {
				return e
			}
			if series == nil || series.FromTime() != lo || series.UntilTime() != hi || series.Step() != step || len(series.Values()) != end-first {
				return store.ErrFormat
			}
			copy(dst[first:end], series.Values())
		}
		first = end
	}
	return ctx.Err()
}

// Whisper copies a returned block before reading another one. Keep a single
// operation-private decode buffer even when range pruning is enabled. Unlike
// the full-block memo, this buffer is never reused as a cached range result.
type windowFiles struct{ *continuationFiles }

func (fs *windowFiles) OpenFile(path string, flags int, mode os.FileMode) (whisper.StorageFile, error) {
	f, err := fs.continuationFiles.OpenFile(path, flags, mode)
	if err != nil {
		return nil, err
	}
	return &windowFile{f.(*continuationFile)}, nil
}

type windowFile struct{ *continuationFile }

func (f *windowFile) ReadStorageBlockRange(block whisper.ContinuationBlock, from, until int) ([]whisper.TimeSeriesPoint, error) {
	points, err := f.readStorageBlock(block, f.fs.snapshotBlock[:0], from, until, true)
	f.fs.snapshotBlock = points
	return points, err
}
