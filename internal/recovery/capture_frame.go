package recovery

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/go-graphite/go-carbon/points"
	"github.com/klauspost/compress/zstd"
)

// Capture frames are independent zstd blocks of the ordinary legacy records.
// A completed write publishes the entire frame; a torn final frame contributes
// no acknowledged submission. CRC detects corruption before any record applies.
const CaptureHeaderSize = 24
const captureMagic = 0x32534347 // GCS2
const maxCaptureRaw = 64 << 20

func EncodeCaptureFrame(enc *zstd.Encoder, raw, dst []byte, count int) []byte {
	dst = append(dst[:0], make([]byte, CaptureHeaderSize)...)
	dst = enc.EncodeAll(raw, dst)
	binary.LittleEndian.PutUint32(dst, captureMagic)
	binary.LittleEndian.PutUint32(dst[4:], uint32(len(dst)-CaptureHeaderSize))
	binary.LittleEndian.PutUint32(dst[8:], uint32(len(raw)))
	binary.LittleEndian.PutUint32(dst[12:], uint32(count))
	binary.LittleEndian.PutUint32(dst[16:], crc32.Update(crc32.Checksum(dst[:16], castagnoli), castagnoli, dst[CaptureHeaderSize:]))
	return dst
}
func CaptureFrameSize(header []byte) (int, error) {
	if len(header) != CaptureHeaderSize || binary.LittleEndian.Uint32(header) != captureMagic {
		return 0, errors.New("invalid capture frame header")
	}
	compressed, raw := binary.LittleEndian.Uint32(header[4:]), binary.LittleEndian.Uint32(header[8:])
	if compressed > maxCaptureRaw || raw > maxCaptureRaw || compressed == 0 {
		return 0, errors.New("capture frame exceeds record bounds")
	}
	return CaptureHeaderSize + int(compressed), nil
}
func DecodeCaptureFrame(dec *zstd.Decoder, frame, raw []byte) ([]byte, int, error) {
	if len(frame) < CaptureHeaderSize {
		return nil, 0, io.ErrUnexpectedEOF
	}
	size, err := CaptureFrameSize(frame[:CaptureHeaderSize])
	if err != nil {
		return nil, 0, err
	}
	if len(frame) != size {
		return nil, 0, io.ErrUnexpectedEOF
	}
	if crc32.Update(crc32.Checksum(frame[:16], castagnoli), castagnoli, frame[CaptureHeaderSize:]) != binary.LittleEndian.Uint32(frame[16:]) {
		return nil, 0, errors.New("capture frame checksum mismatch")
	}
	raw, err = dec.DecodeAll(frame[CaptureHeaderSize:], raw[:0])
	if err != nil {
		return nil, 0, err
	}
	if len(raw) != int(binary.LittleEndian.Uint32(frame[8:])) {
		return nil, 0, errors.New("capture frame decoded length mismatch")
	}
	return raw, int(binary.LittleEndian.Uint32(frame[12:])), nil
}
func ReadCaptureFile(path string, visit func(*points.Points)) (int, error) {
	return ReadCaptureFileBorrowed(path, func(name []byte, data []points.Point) {
		visit(&points.Points{Metric: string(name), Data: append([]points.Point(nil), data...)})
	})
}
func ReadCaptureFileBorrowed(path string, visit func([]byte, []points.Point)) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(maxCaptureRaw))
	if err != nil {
		return 0, err
	}
	defer dec.Close()
	var header [CaptureHeaderSize]byte
	var frame, raw []byte
	count := 0
	for {
		_, err := io.ReadFull(f, header[:])
		if err == io.EOF {
			return count, nil
		}
		if err != nil {
			return count, err
		}
		size, err := CaptureFrameSize(header[:])
		if err != nil {
			return count, err
		}
		if cap(frame) < size {
			frame = make([]byte, size)
		} else {
			frame = frame[:size]
		}
		copy(frame, header[:])
		if _, err = io.ReadFull(f, frame[CaptureHeaderSize:]); err != nil {
			return count, err
		}
		var n int
		raw, n, err = DecodeCaptureFrame(dec, frame, raw)
		if err != nil {
			return count, err
		}
		actual := 0
		err = points.ReadBinaryBytesBorrowed(raw, func(name []byte, data []points.Point) { actual += len(data); visit(name, data) })
		if err != nil {
			return count, err
		}
		if actual != n {
			return count, fmt.Errorf("capture frame point count: got %d want %d", actual, n)
		}
		count += n
	}
}
