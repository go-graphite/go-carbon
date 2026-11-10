package inputlog

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"

	"github.com/go-graphite/go-carbon/points"
)

// InspectLimits bounds an offline, exact-length journal prefix. Inspection never
// opens the live log, acquires its lock, advances replay or creates a Log.
type InspectLimits struct {
	Bytes, Points, DictionaryBytes int64
	Frames, Names                  int
}

type InspectStats struct {
	Bytes, Points, DictionaryBytes int64
	Frames, Names                  int
	Segment                        uint64
}

// Inspect decodes every frame, including points already persisted before the
// recovery watermark. The caller supplies a copied, immutable prefix, not an
// active preallocated file. Callbacks are provisional until the entire prefix
// verifies and Inspect returns nil; they must never write points to a store.
func Inspect(ctx context.Context, input io.Reader, limits InspectLimits, visit func(*points.Points) error) (InspectStats, error) {
	var stats InspectStats
	if limits.Bytes < segmentHeaderSize || limits.Points <= 0 || limits.DictionaryBytes <= 0 || limits.Frames <= 0 || limits.Names <= 0 {
		return stats, errors.New("positive inspection limits required")
	}
	header := make([]byte, segmentHeaderSize)
	if _, err := io.ReadFull(input, header); err != nil {
		return stats, err
	}
	segment, ok := parseSegmentHeader(header)
	if !ok || segment.seq == 0 {
		return stats, errors.New("invalid snapshot segment header")
	}
	stats.Bytes, stats.Segment = segmentHeaderSize, segment.seq
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(2*maxFrameRaw))
	if err != nil {
		return stats, err
	}
	defer decoder.Close()
	var streams [partitions]streamDecoder
	var payload, raw []byte
	for {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		header = header[:frameHeaderSize]
		n, err := io.ReadFull(input, header)
		if errors.Is(err, io.EOF) && n == 0 {
			if stats.Frames == 0 {
				return stats, errors.New("snapshot contains no frames")
			}
			return stats, nil
		}
		if err != nil {
			return stats, fmt.Errorf("torn frame header at %d: %w", stats.Bytes, err)
		}
		frame, ok := parseFrameHeader(header)
		if !ok || frame.salt != segment.salt || frame.partition >= partitions || (frame.codec != codecRecords && frame.codec != codecStream) || frame.length == 0 || frame.length > 2*maxFrameRaw || frame.rawLength == 0 || frame.rawLength > maxFrameRaw+maxRowBytes+256 {
			return stats, fmt.Errorf("invalid snapshot frame at %d", stats.Bytes)
		}
		if stats.Frames >= limits.Frames || stats.Bytes+frameHeaderSize+int64(frame.length) > limits.Bytes || stats.Points+int64(frame.points) > limits.Points {
			return stats, errors.New("snapshot inspection limit exceeded")
		}
		if cap(payload) < int(frame.length) {
			payload = make([]byte, frame.length)
		} else {
			payload = payload[:frame.length]
		}
		if _, err := io.ReadFull(input, payload); err != nil {
			return stats, fmt.Errorf("torn frame payload at %d: %w", stats.Bytes, err)
		}
		stream := &streams[frame.partition]
		before := len(stream.names)
		var count int
		var visitErr error
		raw, count, err = decodeFrame(decoder, replayFrame{payload: payload, h: frame, seq: segment.seq, apply: true}, raw[:0], stream, func(p *points.Points) {
			if visit != nil && visitErr == nil {
				visitErr = visit(p)
			}
		})
		if err != nil {
			return stats, fmt.Errorf("snapshot frame did not verify at %d: decoded=%d declared=%d: %w", stats.Bytes, count, frame.points, err)
		}
		if count != int(frame.points) {
			return stats, fmt.Errorf("snapshot frame count differs at %d: decoded=%d declared=%d", stats.Bytes, count, frame.points)
		}
		if visitErr != nil {
			return stats, visitErr
		}
		for _, name := range stream.names[before:] {
			stats.Names++
			stats.DictionaryBytes += int64(len(name))
		}
		if stats.Names > limits.Names || stats.DictionaryBytes > limits.DictionaryBytes {
			return stats, errors.New("snapshot dictionary limit exceeded")
		}
		stats.Frames++
		stats.Points += int64(count)
		stats.Bytes += frameHeaderSize + int64(frame.length)
	}
}
