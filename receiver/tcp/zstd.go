package tcp

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/klauspost/compress/zstd"
	"go.uber.org/zap"
)

const defaultZstdMaxFrameSize = 16 << 20

func validateZstdLimit(n int) error {
	if n != 0 && (n < 1024 || n > 1<<30) {
		return errors.New("zstd-max-frame-size must be between 1024 and 1073741824, or zero for the default")
	}
	return nil
}

// dispatchFirstBytes is the connection-start protocol boundary. A future
// protocol can join here without adding work to the plaintext line loop.
func (rcv *TCP) dispatchFirstBytes(wire *bufio.Reader) (*zstdFrames, error) {
	if rcv.compression != "zstd" {
		first, _ := wire.Peek(1)
		if len(first) == 0 || first[0] != 0x28 {
			return nil, nil
		}
		magic, _ := wire.Peek(4)
		if !bytes.Equal(magic, []byte{0x28, 0xb5, 0x2f, 0xfd}) {
			return nil, nil
		}
	}
	limit := rcv.zstdMaxFrameSize
	if limit == 0 {
		limit = defaultZstdMaxFrameSize
	}
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(uint64(limit)), zstd.WithDecoderMaxWindow(uint64(max(limit, 1024))))
	if err != nil {
		return nil, err
	}
	atomic.AddUint64(&rcv.zstdStats.connections, 1)
	return &zstdFrames{wire: wire, decoder: decoder, limit: limit, stats: &rcv.zstdStats}, nil
}

// zstdFrames keeps only one complete wire frame and its decoded output. A read
// interrupted inside a frame leaves the consumed prefix in frame for handoff.
// Frame/block layout: https://www.rfc-editor.org/rfc/rfc8878.html#section-3.1
// DecodeAll verifies the payload and optional checksum, with a hard output cap.
type zstdFrames struct {
	wire         *bufio.Reader
	decoder      *zstd.Decoder
	limit        int
	frame, plain []byte
	offset       int
	stats        *zstdStats
}

func (z *zstdFrames) take(n int) error {
	// This also bounds zero-length block attacks and skippable-frame payloads.
	if n < 0 || n > z.limit+z.limit/128+1024-len(z.frame) {
		return errors.New("zstd compressed frame exceeds limit")
	}
	start := len(z.frame)
	z.frame = append(z.frame, make([]byte, n)...)
	read, err := io.ReadFull(z.wire, z.frame[start:])
	z.frame = z.frame[:start+read]
	if err != nil && (start != 0 || read != 0) && errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}

func (z *zstdFrames) readFrame() (bool, error) {
	if err := z.take(4); err != nil {
		return false, err
	}
	var h zstd.Header
	for {
		err := h.Decode(z.frame)
		if err == nil {
			break
		}
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			return false, err
		}
		if len(z.frame) >= 18 {
			return false, errors.New("invalid zstd header")
		}
		if err = z.take(1); err != nil {
			return false, err
		}
	}
	if h.Skippable {
		return true, z.take(int(h.SkippableSize))
	}
	if h.WindowSize > uint64(z.limit) || (h.HasFCS && h.FrameContentSize > uint64(z.limit)) {
		return false, errors.New("zstd window or content size exceeds limit")
	}
	if h.DictionaryID != 0 {
		return false, errors.New("zstd dictionaries are unsupported")
	}
	for {
		start := len(z.frame)
		if err := z.take(3); err != nil {
			return false, err
		}
		b := z.frame[start:]
		header := uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16
		size, typ := int(header>>3), (header>>1)&3
		if typ == 3 || size > 128<<10 {
			return false, errors.New("invalid zstd block header")
		}
		if typ == 1 {
			size = 1
		}
		if err := z.take(size); err != nil {
			return false, err
		}
		if header&1 != 0 {
			break
		}
	}
	if h.HasCheckSum {
		if err := z.take(4); err != nil {
			return false, err
		}
	}
	return false, nil
}

func (z *zstdFrames) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for z.offset == len(z.plain) {
		z.plain, z.offset = z.plain[:0], 0
		z.frame = z.frame[:0]
		skip, err := z.readFrame()
		if err != nil {
			return 0, err
		}
		atomic.AddUint64(&z.stats.compressedBytes, uint64(len(z.frame)))
		if skip {
			continue
		}
		z.plain, err = z.decoder.DecodeAll(z.frame, z.plain[:0])
		if err != nil {
			z.plain = z.plain[:0]
			return 0, fmt.Errorf("decode zstd frame: %w", err)
		}
		atomic.AddUint64(&z.stats.frames, 1)
		atomic.AddUint64(&z.stats.decompressedBytes, uint64(len(z.plain)))
		z.offset = 0
		z.frame = z.frame[:0]
	}
	n := copy(p, z.plain[z.offset:])
	z.offset += n
	return n, nil
}

func (rcv *TCP) zstdFailure(err error) {
	atomic.AddUint64(&rcv.zstdStats.decodeErrors, 1)
	rcv.logger.Error("zstd input failed", zap.Error(err))
}

func (rcv *TCP) detachInput(c *handoffConn, reader *bufio.Reader, rest *bytes.Reader, partial []byte, z *zstdFrames) bool {
	if z == nil {
		return rcv.detach(c, reader, rest, partial)
	}
	tail := append([]byte(nil), partial...)
	buffered, _ := reader.Peek(reader.Buffered())
	tail = append(tail, buffered...)
	tail = append(tail, z.plain[z.offset:]...)
	// Raw independent frames avoid encoder state and accept arbitrarily long
	// lines by splitting the unparsed tail into bounded frames.
	var leftover []byte
	for len(tail) > 0 {
		n := min(len(tail), z.limit)
		leftover = appendRawZstdFrame(leftover, tail[:n])
		tail = tail[n:]
	}
	if len(leftover) == 0 {
		leftover = appendRawZstdFrame(leftover, nil)
	}
	leftover = append(leftover, z.frame...)
	buffered, _ = z.wire.Peek(z.wire.Buffered())
	leftover = append(leftover, buffered...)
	if rest != nil && rest.Len() > 0 {
		n := len(leftover)
		leftover = append(leftover, make([]byte, rest.Len())...)
		_, _ = rest.Read(leftover[n:])
	}
	return rcv.detachBytes(c, leftover)
}

// Raw zstd blocks are sufficient for the one-off handoff tail. A frame still
// has a standard content size and can be decoded independently by any peer.
func appendRawZstdFrame(dst, plain []byte) []byte {
	dst = append(dst, 0x28, 0xb5, 0x2f, 0xfd, 0xa0)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(plain)))
	for {
		n := min(len(plain), 128<<10)
		header := uint32(n) << 3
		if n == len(plain) {
			header |= 1
		}
		dst = append(dst, byte(header), byte(header>>8), byte(header>>16))
		dst = append(dst, plain[:n]...)
		plain = plain[n:]
		if len(plain) == 0 {
			return dst
		}
	}
}
