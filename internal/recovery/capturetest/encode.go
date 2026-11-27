// Package capturetest builds the retired capture encoding for compatibility
// tests and comparative benchmarks. Production capture uses inputlog.
package capturetest

import (
	"encoding/binary"
	"hash/crc32"

	"github.com/klauspost/compress/zstd"
)

const headerSize = 24
const captureMagic = 0x32534347

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func Encode(enc *zstd.Encoder, raw, dst []byte, count int) []byte {
	dst = append(dst[:0], make([]byte, headerSize)...)
	dst = enc.EncodeAll(raw, dst)
	binary.LittleEndian.PutUint32(dst, captureMagic)
	binary.LittleEndian.PutUint32(dst[4:], uint32(len(dst)-headerSize))
	binary.LittleEndian.PutUint32(dst[8:], uint32(len(raw)))
	binary.LittleEndian.PutUint32(dst[12:], uint32(count))
	binary.LittleEndian.PutUint32(dst[16:], crc32.Update(crc32.Checksum(dst[:16], castagnoli), castagnoli, dst[headerSize:]))
	return dst
}
