package v2part

// The dense writer needs a reusable compressor. Pebble's public SST writer
// allocates a streaming C encoder for every block and has no compressor hook.
// This deliberately small SET-only writer emits the same public RocksDBv2
// container: prefix/restart data blocks, a single index, CRC32c and a footer.
// GCP3 and dense values remain opaque. Readers use the ordinary Pebble reader.

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"math/bits"
	"sync"
)

// denseBlockSize bounds a block before compression. Seal parts use
// denseSealBlockSize: every render seeks each live part once per name, and a
// block-cache miss decompresses and allocates a whole block, so young parts
// keep small blocks while merges rewrite them into large ones. A miss costs
// about 46 us at 64 KiB and scales with the block size. Readers take any size.
const (
	denseBlockSize     = 256 << 10
	denseSealBlockSize = 32 << 10
)

var denseCRC = crc32.MakeTable(crc32.Castagnoli)
var denseSSTPool sync.Pool

type denseBlock struct {
	data, last []byte
	restarts   []uint32
	entries    int
}

func (b *denseBlock) reset() {
	b.data, b.last, b.restarts = b.data[:0], b.last[:0], b.restarts[:0]
	b.entries = 0
}

func (b *denseBlock) add(key, value []byte, interval int) {
	shared := 0
	if b.entries%interval == 0 {
		b.restarts = append(b.restarts, uint32(len(b.data)))
	} else {
		shared = commonPrefix(key, b.last)
	}
	b.data = binary.AppendUvarint(b.data, uint64(shared))
	b.data = binary.AppendUvarint(b.data, uint64(len(key)-shared))
	b.data = binary.AppendUvarint(b.data, uint64(len(value)))
	b.data = append(b.data, key[shared:]...)
	b.data = append(b.data, value...)
	b.last = append(b.last[:0], key...)
	b.entries++
}

// commonPrefix is the length of the longest common prefix of a and b. It
// compares eight bytes at a time: keys share most of their ~200 bytes.
func commonPrefix(a, b []byte) int {
	n := min(len(a), len(b))
	i := 0
	for ; i+8 <= n; i += 8 {
		if x := binary.LittleEndian.Uint64(a[i:]) ^ binary.LittleEndian.Uint64(b[i:]); x != 0 {
			return i + bits.TrailingZeros64(x)>>3
		}
	}
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

func (b *denseBlock) finish() []byte {
	if len(b.restarts) == 0 {
		b.restarts = append(b.restarts, 0)
	}
	for _, offset := range b.restarts {
		b.data = binary.LittleEndian.AppendUint32(b.data, offset)
	}
	b.data = binary.LittleEndian.AppendUint32(b.data, uint32(len(b.restarts)))
	return b.data
}

type denseSSTWorkspace struct {
	encoder                       *denseEncoder
	data, index, properties, meta denseBlock
	compressed, key               []byte
}

// Zstd levels for dense blocks. Merges and imports write parts that stay at
// rest, so they keep the stronger level. A seal part is rewritten by the next
// size-tier merge within minutes; it pays only the fast level. Each block
// records its own type byte and a Zstd frame carries no level, so readers and
// merges decode every level alike.
const (
	denseRestLevel = 3
	denseSealLevel = 1
)

type denseSSTWriter struct {
	out                         *hashedOutput
	work                        *denseSSTWorkspace
	entries, rawKeys, rawValues uint64
	blocks                      uint64
	level, blockSize            int
	err                         error
}

func newDenseSSTWriter(out *hashedOutput) (*denseSSTWriter, error) {
	work, _ := denseSSTPool.Get().(*denseSSTWorkspace)
	if work == nil {
		encoder, err := newDenseEncoder()
		if err != nil {
			return nil, err
		}
		work = &denseSSTWorkspace{encoder: encoder}
	}
	work.data.reset()
	work.index.reset()
	work.properties.reset()
	work.meta.reset()
	return &denseSSTWriter{out: out, work: work, level: denseRestLevel, blockSize: denseBlockSize}, nil
}

func (w *denseSSTWriter) Set(key, value []byte) error {
	if w.err != nil {
		return w.err
	}
	if w.work == nil {
		return io.ErrClosedPipe
	}
	b := &w.work.data
	if b.entries != 0 && len(b.data)+len(key)+len(value)+4*len(b.restarts)+32 > w.blockSize {
		if w.err = w.flush(); w.err != nil {
			return w.err
		}
	}
	w.work.key = append(w.work.key[:0], key...)
	// Zero sequence number, SET kind. The trailer is little endian.
	w.work.key = binary.LittleEndian.AppendUint64(w.work.key, 1)
	b.add(w.work.key, value, 64)
	w.entries++
	w.rawKeys += uint64(len(key) + 8)
	w.rawValues += uint64(len(value))
	return nil
}

func appendDenseHandle(b []byte, offset, length uint64) []byte {
	b = binary.AppendUvarint(b, offset)
	return binary.AppendUvarint(b, length)
}

func (w *denseSSTWriter) block(data []byte, compress bool) (offset, length uint64, err error) {
	stored, kind := data, byte(0)
	if compress {
		work := w.work
		work.compressed = binary.AppendUvarint(work.compressed[:0], uint64(len(data)))
		work.compressed, err = work.encoder.append(data, work.compressed, w.level)
		if err != nil {
			return 0, 0, err
		}
		if len(work.compressed) < len(data)-len(data)/8 {
			stored, kind = work.compressed, 7 // RocksDB Zstd block type.
		}
	}
	offset, length = w.out.n, uint64(len(stored))
	var trailer [5]byte
	trailer[0] = kind
	crc := crc32.Update(crc32.Checksum(stored, denseCRC), denseCRC, trailer[:1])
	binary.LittleEndian.PutUint32(trailer[1:], (crc>>15|crc<<17)+0xa282ead8)
	if err = w.out.Write(stored); err == nil {
		err = w.out.Write(trailer[:])
	}
	return offset, length, err
}

func (w *denseSSTWriter) flush() error {
	b := &w.work.data
	if b.entries == 0 {
		return nil
	}
	offset, length, err := w.block(b.finish(), true)
	if err != nil {
		return err
	}
	var handle [20]byte
	w.work.index.add(b.last, appendDenseHandle(handle[:0], offset, length), 1)
	w.blocks++
	b.reset()
	return nil
}

func (w *denseSSTWriter) Close() (err error) {
	if w.work == nil {
		return io.ErrClosedPipe
	}
	defer func() {
		// A part owns this complete workspace until Close/Abort. It is never
		// shared by concurrent writers and contains no output-owned objects.
		work := w.work
		w.work = nil
		denseSSTPool.Put(work)
		if err != nil {
			w.out.Abort()
		}
	}()
	if w.err != nil {
		return w.err
	}
	if err = w.flush(); err != nil {
		return err
	}
	dataSize := w.out.n
	indexOffset, indexLength, err := w.block(w.work.index.finish(), true)
	if err != nil {
		return err
	}
	// Property names are sorted bytewise. Integer statistics use RocksDB's
	// varint format, while its fixed-width index type uses four bytes.
	p := &w.work.properties
	p.add([]byte("rocksdb.block.based.table.index.type"), []byte{0, 0, 0, 0}, 1)
	p.add([]byte("rocksdb.comparator"), []byte("leveldb.BytewiseComparator"), 1)
	p.add([]byte("rocksdb.compression"), []byte("ZSTD"), 1)
	var scratch [20]byte
	for _, property := range []struct {
		name  string
		value uint64
	}{
		{"rocksdb.data.size", dataSize},
		{"rocksdb.index.size", indexLength + 5},
		{"rocksdb.num.data.blocks", w.blocks},
		{"rocksdb.num.entries", w.entries},
		{"rocksdb.raw.key.size", w.rawKeys},
		{"rocksdb.raw.value.size", w.rawValues},
	} {
		p.add([]byte(property.name), binary.AppendUvarint(scratch[:0], property.value), 1)
	}
	propertiesOffset, propertiesLength, err := w.block(p.finish(), false)
	if err != nil {
		return err
	}
	w.work.meta.add([]byte("rocksdb.properties"), appendDenseHandle(scratch[:0], propertiesOffset, propertiesLength), 1)
	metaOffset, metaLength, err := w.block(w.work.meta.finish(), false)
	if err != nil {
		return err
	}
	var footer [53]byte
	footer[0] = 1 // CRC32c
	handles := appendDenseHandle(footer[:1], metaOffset, metaLength)
	_ = appendDenseHandle(handles, indexOffset, indexLength)
	binary.LittleEndian.PutUint32(footer[41:], 2)
	copy(footer[45:], "\xf7\xcf\xf4\x85\xb7\x41\xe2\x88")
	if err = w.out.Write(footer[:]); err != nil {
		return err
	}
	return w.out.Finish()
}
