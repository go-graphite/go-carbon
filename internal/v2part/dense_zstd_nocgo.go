//go:build !cgo

package v2part

import "github.com/klauspost/compress/zstd"

// The pure-Go encoder fixes its level per instance; seal parts use a second
// one. Its physical bytes differ from the C encoder's, as before.
type denseEncoder struct{ rest, seal *zstd.Encoder }

func newDenseEncoder() (*denseEncoder, error) {
	rest, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(1<<20), zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
	if err != nil {
		return nil, err
	}
	seal, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(1<<20), zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return nil, err
	}
	return &denseEncoder{rest: rest, seal: seal}, nil
}

func (e *denseEncoder) append(src, dst []byte, level int) ([]byte, error) {
	if level < denseRestLevel {
		return e.seal.EncodeAll(src, dst), nil
	}
	return e.rest.EncodeAll(src, dst), nil
}
