package inputlog

import (
	"errors"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-graphite/go-carbon/points"
	"github.com/klauspost/compress/zstd"
	"go.uber.org/zap"
)

// SegmentWriter gives an external intake owner the journal's encoder and
// group-commit writer. The owner supplies an exclusively owned, empty file and
// controls its ordering/retirement boundary. Append acknowledges only after the
// ordinary journal write and data-sync barriers complete. It is single-owner.
type SegmentWriter struct {
	log     Log
	scratch workerBuffer
	ref     StreamRef
	failed  error
}

func NewSegmentWriter(file *os.File, sequence uint64) (*SegmentWriter, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() != 0 {
		return nil, errors.New("input log segment writer requires an empty file")
	}
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderCRC(false))
	if err != nil {
		return nil, err
	}
	seg := &segment{file: file, path: file.Name(), seq: sequence, salt: rand.Uint64(), end: segmentHeaderSize}
	w := &SegmentWriter{log: Log{enc: enc, logger: zap.NewNop(), active: seg, wseg: seg}}
	w.log.wbuf = segmentHeader{seq: sequence, salt: seg.salt, created: time.Now().UnixNano()}.append(nil)
	return w, nil
}
func (w *SegmentWriter) Close()           { w.log.enc.Close() }
func (w *SegmentWriter) Metrics() Metrics { return w.log.Metrics() }

// Append preserves each name's order. Tagged spellings may alias after cache
// normalization: frame barriers retain their interleaving across dictionary ids.
// reserve runs before disk mutation, with the exact bytes to be appended.
func (w *SegmentWriter) Append(records []*points.Points, reserve func(int64) error) (written int64, err error) {
	if w.failed != nil {
		return 0, w.failed
	}
	defer func() {
		if err != nil {
			w.failed = err
		}
	}()
	l := &w.log
	part := &l.parts[0]
	part.out, part.frames = part.out[:0], part.frames[:0]
	part.enc.begin(l.active.seq)
	flush := func() {
		w.scratch.raw = part.enc.flush(&w.scratch.streamScratch, w.scratch.raw, func(raw []byte, n int) []byte { l.compress(part, 0, raw, n); return raw })
	}
	for _, p := range records {
		tagged := strings.Contains(p.Metric, ";")
		if tagged {
			flush()
		}
		w.ref.Metric = p.Metric
		w.ref.state.Store(0)
		ref := &w.ref
		for _, v := range p.Data {
			part.enc.add(&w.scratch.streamScratch, ref.Entry(v.Value, v.Timestamp))
			if len(w.scratch.rows) >= maxSortRows || len(w.scratch.names) >= 2*maxFrameRaw {
				flush()
			}
		}
		if tagged {
			flush()
		}
	}
	flush()
	size := int64(len(l.wbuf) + len(part.out) + frameHeaderSize*len(part.frames))
	if reserve != nil {
		if err := reserve(size); err != nil {
			return 0, err
		}
	}
	before := l.woff
	err = l.write(1, l.active.seq)
	// External ownership pins the entire segment. Keep its first replay-from
	// position, without retaining an unbounded list of per-append ticks.
	if len(l.ticks) > 1 {
		l.ticks = l.ticks[:1]
	}
	if err != nil {
		return 0, err
	}
	return l.woff - before, nil
}

// IsSegment identifies the journal format without relying on a filename.
func IsSegment(r io.ReaderAt) bool {
	var b [segmentHeaderSize]byte
	n, _ := r.ReadAt(b[:], 0)
	_, ok := parseSegmentHeader(b[:])
	return n == len(b) && ok
}

// SegmentReplay uses the same verified frame decoder as Log.ReplayStoppedFrom.
// Keep one reader per segment, including while its published frontier grows.
// Resume always decodes the dictionary prefix; callers publish only complete
// frame ends after applying every returned metric group.
type SegmentReplay struct {
	reader       io.ReaderAt
	dec          *zstd.Decoder
	seg          segmentHeader
	offset       int64
	streams      [partitions]streamDecoder
	raw, payload []byte
	records      []*points.Points
}

func NewSegmentReplay(reader io.ReaderAt) (*SegmentReplay, error) {
	var b [segmentHeaderSize]byte
	if _, err := reader.ReadAt(b[:], 0); err != nil {
		return nil, err
	}
	seg, ok := parseSegmentHeader(b[:])
	if !ok {
		return nil, errFrame
	}
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(64<<20))
	if err != nil {
		return nil, err
	}
	return &SegmentReplay{reader: reader, seg: seg, dec: dec, offset: segmentHeaderSize}, nil
}
func (r *SegmentReplay) Close() { r.dec.Close() }
func (r *SegmentReplay) Next(frontier int64) ([]*points.Points, int64, int, error) {
	if r.offset == frontier {
		return nil, r.offset, 0, io.EOF
	}
	if frontier-r.offset < frameHeaderSize {
		return nil, r.offset, 0, io.ErrUnexpectedEOF
	}
	var b [frameHeaderSize]byte
	if _, err := r.reader.ReadAt(b[:], r.offset); err != nil {
		return nil, r.offset, 0, err
	}
	h, ok := parseFrameHeader(b[:])
	if !ok || h.salt != r.seg.salt || h.partition >= partitions || h.rawLength > 2*maxFrameRaw || (h.codec != codecStream && h.codec != codecRecords) {
		return nil, r.offset, 0, errFrame
	}
	end := r.offset + frameHeaderSize + int64(h.length)
	if end > frontier {
		return nil, r.offset, 0, io.ErrUnexpectedEOF
	}
	if cap(r.payload) < int(h.length) {
		r.payload = make([]byte, h.length)
	} else {
		r.payload = r.payload[:h.length]
	}
	if _, err := r.reader.ReadAt(r.payload, r.offset+frameHeaderSize); err != nil {
		return nil, r.offset, 0, err
	}
	clear(r.records)
	r.records = r.records[:0]
	var n int
	var err error
	r.raw, n, err = decodeFrame(r.dec, replayFrame{payload: r.payload, h: h, seq: r.seg.seq, off: r.offset, apply: true}, r.raw[:0], &r.streams[h.partition], func(p *points.Points) { r.records = append(r.records, p) })
	if err != nil {
		return nil, r.offset, 0, err
	}
	if n != int(h.points) {
		return nil, r.offset, 0, errFrame
	}
	r.offset = end
	return r.records, end, n, nil
}

// RetireSegment removes a durably protected segment and syncs its directory.
// It is retryable after an interrupted unlink or directory sync. Descriptor and
// lock ownership stays with the caller, which closes only after this barrier.
func RetireSegment(path string, remove func(string) error, syncDirectory func(string) error) error {
	if err := remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}
