package whisperio

import (
	"context"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

type frozenReadDecoder struct {
	w                    *whisper.Whisper
	fs                   *continuationFiles
	m                    store.Metadata
	clock                func() time.Time
	workspace            *fetchBlockWorkspace
	rangeReadsEnabled    func() bool
	keepBlockAllocations bool
	dense                *frozenDensePlan
}

func (c *ContinuationCodec) NewFrozenReadDecoder(kv store.ContinuationIO, m store.Metadata, clock func() time.Time) (store.FrozenReadDecoder, error) {
	fs := newContinuationFiles(kv, true)
	w, err := whisper.OpenWithOptions(fs.main(), continuationOptions(fs, clock))
	if err != nil {
		return nil, err
	}
	d := &frozenReadDecoder{w: w, fs: fs, m: m, clock: clock,
		rangeReadsEnabled: c.RangeReadsEnabled, keepBlockAllocations: c.keepFetchBlockAllocations}
	return d, nil
}

func (d *frozenReadDecoder) Close() error {
	if d.workspace != nil {
		releaseFetchBlock(d.fs, d.workspace)
		d.workspace = nil
	}
	return d.w.Close()
}

func (d *frozenReadDecoder) Fetch(ctx context.Context, from, until int) (result *store.Series, err error) {
	defer func() {
		// Keep the same idle scratch bound as the ordinary fetch block pool.
		if cap(d.fs.snapshotBlock) > maxFetchBlockPoints {
			d.fs.snapshotBlock = nil
			d.fs.snapshotBlockOK = false
		}
	}()
	defer func() {
		if value := recover(); value != nil {
			cause := ctx.Err()
			message, ok := value.(string)
			if cause != nil && ok && message == "Failed to read baseInterval: "+cause.Error() {
				result, err = nil, cause
				return
			}
			panic(value)
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.prepareFetch()
	if !d.w.IsCompressed() {
		return fetchClassic(ctx, d.w, d.m, int(d.clock().Unix()), from, until)
	}
	series, err := d.w.Fetch(from, until)
	if err != nil || series == nil {
		return nil, err
	}
	return &store.Series{Metadata: d.m, FromTime: series.FromTime(), UntilTime: series.UntilTime(), Step: series.Step(), Values: series.Values()}, nil
}

func (d *frozenReadDecoder) prepareFetch() {
	d.fs.rangeReads = d.rangeReadsEnabled != nil && d.rangeReadsEnabled()
	if d.w.IsCompressed() && !d.keepBlockAllocations {
		if !d.fs.rangeReads && d.workspace == nil {
			d.workspace = borrowFetchBlock(d.fs)
			d.workspace.points = nil // The file owns backing storage until release.
		} else if d.fs.rangeReads && d.workspace != nil {
			releaseFetchBlock(d.fs, d.workspace)
			d.workspace = nil
		}
	}
}
