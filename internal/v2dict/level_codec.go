package v2dict

import (
	"github.com/pierrec/lz4/v4"
)

// The enclosing block records both lengths. Equal lengths mean raw bytes;
// compressed blocks are strictly smaller. No decoder context is needed.
func compressBlock(dst, src []byte) ([]byte, error) {
	bound := lz4.CompressBlockBound(len(src))
	if cap(dst) < bound {
		dst = make([]byte, bound)
	}
	dst = dst[:bound]
	n, err := lz4.CompressBlock(src, dst, nil)
	if err != nil {
		return nil, err
	}
	if n == 0 || n >= len(src) {
		copy(dst, src)
		return dst[:len(src)], nil
	}
	return dst[:n], nil
}

// Decode only into the caller's bounded workspace, and require the exact raw
// length from the enclosing block even when the codec accepts a larger buffer.
func decompressBlock(dst, src []byte) ([]byte, error) {
	if len(src) == len(dst) {
		copy(dst, src)
		return dst, nil
	}
	n, err := lz4.UncompressBlock(src, dst)
	if err != nil {
		return nil, err
	}
	if n != len(dst) {
		return nil, errCorrupt
	}
	return dst[:n], nil
}
