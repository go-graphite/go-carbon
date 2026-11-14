package inputlog

import (
	"errors"
	"hash/crc32"
	"runtime"
	"sync"
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

// ReplayCursors holds one cursor per on-disk partition. Each partition has a
// single writer; callers may snapshot the set after replay workers have joined.
type ReplayCursors [partitions]ReplayCursor

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
	// Inventory metadata once. Each reader owns its payload buffer, decoder,
	// stream state and ordered frame list. A blocked partition must not stall
	// disk reads or decoding for other partitions behind a producer queue.
	type work struct {
		frame *frameRef
		apply bool
	}
	jobs := make([][]work, workers)
	for _, list := range []struct {
		frames []frameRef
		apply  bool
	}{{l.prefix, false}, {l.pending, true}} {
		for i := range list.frames {
			f := &list.frames[i]
			if !list.apply && f.h.codec != codecStream {
				continue
			}
			worker := int(f.h.partition) % workers
			jobs[worker] = append(jobs[worker], work{f, list.apply})
		}
	}
	type result struct {
		stats ReplayStats
		err   error
	}
	results := make([]result, workers)
	var wg sync.WaitGroup
	for i := range jobs {
		wg.Go(func() {
			r := &results[i]
			dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.IgnoreChecksum(true))
			if err != nil {
				r.err = err
				return
			}
			defer dec.Close()
			var raw, payload []byte
			var streams [partitions]streamDecoder
			var blocked [partitions]bool
			for _, job := range jobs[i] {
				select {
				case <-stop:
					r.err = errors.New("input log replay stopped")
					return
				default:
				}
				ref := job.frame
				if cap(payload) < int(ref.h.length) {
					payload = make([]byte, ref.h.length)
				} else {
					payload = payload[:ref.h.length]
				}
				if _, r.err = ref.seg.file.ReadAt(payload, ref.off+frameHeaderSize); r.err != nil {
					return
				}
				f := replayFrame{payload: payload, h: ref.h, seq: ref.seg.seq, off: ref.off, apply: job.apply}
				var records uint64
				var frameErr error
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
						r.stats.Points += int64(len(p.Data))
						if advance != nil {
							advance(f.h.partition, ReplayCursor{Segment: f.seq, Salt: f.h.salt, Offset: f.off, Records: records})
						}
					} else {
						blocked[f.h.partition] = true
					}
				})
				if f.apply {
					r.stats.Frames++
					r.stats.Bytes += int64(len(payload))
					l.replayPendingBytes.Add(-int64(frameHeaderSize) - int64(f.h.length))
				} else {
					r.stats.DictionaryFrames++
				}
				if frameErr != nil {
					r.stats.Corrupt++
					l.logger.Warn("skipping corrupt input log frame", zap.Uint64("segment", f.seq),
						zap.Uint16("partition", f.h.partition), zap.Error(frameErr))
				}
			}
		})
	}
	wg.Wait()
	var readErr error
	for _, r := range results {
		stats.Frames += r.stats.Frames
		stats.Points += r.stats.Points
		stats.Bytes += r.stats.Bytes
		stats.Corrupt += r.stats.Corrupt
		stats.DictionaryFrames += r.stats.DictionaryFrames
		readErr = errors.Join(readErr, r.err)
	}
	stats.Duration = time.Since(started)
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
