package inputlog

import (
	"errors"
	"hash/crc32"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
	"go.uber.org/zap"

	"github.com/go-graphite/go-carbon/internal/phasecpu"
	"github.com/go-graphite/go-carbon/points"
)

type ReplayStats struct {
	Frames, Points, Bytes int64
	// Corrupt counts frames skipped because their payload did not verify,
	// or because an earlier frame of their stream did not.
	Corrupt int64
	// DictionaryFrames counts frames before the replay start that were read
	// for the names and points the replayed frames build on.
	DictionaryFrames int64
	Duration         time.Duration
}

type replayFrame struct {
	payload []byte
	h       frameHeader
	seq     uint64
	off     int64
	apply   bool
}

// Replay applies every point recorded after the last position known to be
// persisted. Points of one metric are applied in their recorded order; apply
// runs concurrently for different partitions, so for different metrics. A
// frame that fails verification is skipped and counted, with the rest of its
// stream in that segment; an I/O error stops replay.
func (l *Log) Replay(apply func(*points.Points)) (ReplayStats, error) {
	return l.ReplayStopped(apply, nil)
}

// ReplayCursor identifies the last protected record in one ordered partition.
// Segment/offset identify a frame; Records counts complete callback records.
type ReplayCursor struct {
	Segment uint64 `json:"segment"`
	Salt    uint64 `json:"salt"`
	Offset  int64  `json:"offset"`
	Records uint64 `json:"records"`
}

// ReplayStopped joins all partition workers before returning on cancellation.
func (l *Log) ReplayStopped(apply func(*points.Points), stop <-chan struct{}) (ReplayStats, error) {
	return l.ReplayStoppedFrom(func(p *points.Points) bool { apply(p); return true }, stop, nil, nil)
}

// ReplayStoppedFrom decodes dictionary prefixes but applies only records after
// each cursor. advance runs after apply accepts a complete record; callers must
// protect those records durably before publishing the resulting cursors.
func (l *Log) ReplayStoppedFrom(apply func(*points.Points) bool, stop <-chan struct{}, from map[uint16]ReplayCursor, advance func(uint16, ReplayCursor)) (ReplayStats, error) {
	phasecpu.Set(phasecpu.Restore)
	defer phasecpu.Set(phasecpu.Other)
	started := time.Now()
	var stats ReplayStats
	if len(l.pending) == 0 {
		return stats, nil
	}
	// Sequence numbers can be reused after a complete journal retirement.
	// A cursor belongs only to the original immutable segment incarnation.
	salts := make(map[uint64]uint64)
	for _, f := range l.pending {
		salts[f.seg.seq] = f.h.salt
	}
	active := make(map[uint16]ReplayCursor)
	for part, cursor := range from {
		if cursor.Salt != 0 && salts[cursor.Segment] == cursor.Salt {
			active[part] = cursor
		}
	}
	from = active
	workers := min(runtime.GOMAXPROCS(0), partitions)
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(workers), zstd.IgnoreChecksum(true))
	if err != nil {
		return stats, err
	}
	defer dec.Close()
	var pointCount, corrupt atomic.Int64
	buffers := sync.Pool{New: func() any { return new([]byte) }}
	// Each partition's frames go to one worker, which alone uses its stream.
	var streams [partitions]streamDecoder
	var blocked [partitions]bool
	queues := make([]chan replayFrame, workers)
	var wg sync.WaitGroup
	for i := range queues {
		queues[i] = make(chan replayFrame, 4)
		wg.Go(func() {
			var raw []byte
			for f := range queues[i] {
				var frameErr error
				var records uint64
				var accepted int
				cursor := from[f.h.partition]
				raw, _, frameErr = decodeFrame(dec, f, raw[:0], &streams[f.h.partition], func(p *points.Points) {
					records++
					if blocked[f.h.partition] {
						return
					}
					if f.seq < cursor.Segment || (f.seq == cursor.Segment && (f.off < cursor.Offset || (f.off == cursor.Offset && records <= cursor.Records))) {
						return
					}
					if apply(p) {
						accepted += len(p.Data)
						if advance != nil {
							advance(f.h.partition, ReplayCursor{Segment: f.seq, Salt: f.h.salt, Offset: f.off, Records: records})
						}
					} else {
						blocked[f.h.partition] = true
					}
				})
				pointCount.Add(int64(accepted))
				if f.apply {
					l.replayPendingBytes.Add(-int64(frameHeaderSize) - int64(f.h.length))
				}
				if frameErr != nil {
					corrupt.Add(1)
					l.logger.Warn("skipping corrupt input log frame", zap.Uint64("segment", f.seq),
						zap.Uint16("partition", f.h.partition), zap.Error(frameErr))
				}
				payload := f.payload[:0]
				buffers.Put(&payload)
			}
		})
	}
	var readErr error
frames:
	for i, f := range slices.Concat(l.prefix, l.pending) {
		select {
		case <-stop:
			readErr = errors.New("input log replay stopped")
			break frames
		default:
		}
		replayed := i >= len(l.prefix)
		if !replayed && f.h.codec != codecStream {
			continue
		}
		buf := buffers.Get().(*[]byte)
		payload := slices.Grow((*buf)[:0], int(f.h.length))[:f.h.length]
		if _, readErr = f.seg.file.ReadAt(payload, f.off+frameHeaderSize); readErr != nil {
			break
		}
		if replayed {
			stats.Frames++
			stats.Bytes += int64(len(payload))
		} else {
			stats.DictionaryFrames++
		}
		select {
		case queues[int(f.h.partition)%workers] <- replayFrame{payload: payload, h: f.h, seq: f.seg.seq, off: f.off, apply: replayed}:
		case <-stop:
			readErr = errors.New("input log replay stopped")
			break frames
		}
	}
	for _, q := range queues {
		close(q)
	}
	wg.Wait()
	stats.Points, stats.Corrupt, stats.Duration = pointCount.Load(), corrupt.Load(), time.Since(started)
	l.total.replayed.Add(stats.Points)
	l.total.replayNanos.Store(int64(stats.Duration))
	return stats, readErr
}

var errFrame = errors.New("input log frame does not verify")

// decodeFrame verifies and decodes a frame, applying its points unless it
// precedes the replay start. It returns the number of points applied.
func decodeFrame(dec *zstd.Decoder, f replayFrame, raw []byte, stream *streamDecoder, apply func(*points.Points)) ([]byte, int, error) {
	if f.h.codec == codecRecords {
		raw, err := decompress(dec, f, raw)
		if err != nil {
			return raw, 0, err
		}
		n, err := decodeRecords(raw, apply)
		return raw, n, err
	}
	stream.begin(f.seq)
	if stream.broken {
		return raw, 0, errStream
	}
	visit := apply
	if !f.apply {
		visit = nil
	}
	raw, err := decompress(dec, f, raw)
	ages, ageErr := arrivalFrame(f.payload)
	if err == nil {
		err = ageErr
	}
	stream.arrivals = ages

	n := 0
	if err == nil {
		n, err = stream.decode(raw, visit)
	}
	if err != nil {
		stream.broken = true
		return raw, 0, err
	}
	if !f.apply {
		n = 0
	}
	return raw, n, nil
}

func decompress(dec *zstd.Decoder, f replayFrame, raw []byte) ([]byte, error) {
	if crc32.Checksum(f.payload, castagnoli) != f.h.crc {
		return raw, errFrame
	}
	raw, err := dec.DecodeAll(f.payload, raw)
	if err == nil && len(raw) != int(f.h.rawLength) {
		err = errFrame
	}
	return raw, err
}
