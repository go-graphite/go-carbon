//go:build !cgo

package v2dict

import "github.com/klauspost/compress/zstd"

// EncodeAll and DecodeAll support concurrent calls. Frames from either vendored
// implementation are interchangeable; no decoded block survives a lookup.
var blockEncoder, _ = zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedDefault))
var blockDecoder, _ = zstd.NewReader(nil)

func compressBlock(dst, src []byte) ([]byte, error)   { return blockEncoder.EncodeAll(src, dst), nil }
func decompressBlock(dst, src []byte) ([]byte, error) { return blockDecoder.DecodeAll(src, dst[:0]) }

// The Go decoder already reuses codec contexts internally.
type scanDecoder struct{}

func (d *scanDecoder) decode(dst, src []byte) ([]byte, error) { return decompressBlock(dst, src) }
func (d *scanDecoder) close()                                 {}
