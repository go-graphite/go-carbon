//go:build !cgo

package v2part

import "github.com/klauspost/compress/zstd"

type denseEncoder struct{ encoder *zstd.Encoder }

func newDenseEncoder() (*denseEncoder, error) {
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(1<<20), zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
	if err != nil {
		return nil, err
	}
	return &denseEncoder{encoder: encoder}, nil
}

func (e *denseEncoder) append(src, dst []byte) ([]byte, error) {
	return e.encoder.EncodeAll(src, dst), nil
}
