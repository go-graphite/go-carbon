package carbonserver

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/bits"
)

const (
	snapshotBlockRows     = 256
	snapshotBlockHeader   = 36 // four (int64 base, uint8 delta width) columns
	snapshotTableEntry    = 32 // block offset and three cumulative usage counters
	snapshotFooterSize    = 32
	snapshotMetadataMagic = "GCMETA01"
)

// snapshotMetadataWriter stores fixed fields in independently decodable blocks.
// Delta widths are chosen per column, so equal retention/size values cost no
// per-metric bytes. Prefix sums make namespace quota totals independent of the
// number of descendants. All arithmetic retains the existing int64 semantics.
type snapshotMetadataWriter struct {
	w             io.Writer
	rows          [snapshotBlockRows][4]int64
	used          int
	count, offset uint64
	sums          [3]uint64
	table         []byte
	buf           []byte
}

func (w *snapshotMetadataWriter) append(values [4]int64) error {
	w.rows[w.used] = values
	w.used++
	w.count++
	if w.used == snapshotBlockRows {
		return w.flush()
	}
	return nil
}

func (w *snapshotMetadataWriter) addTableEntry() {
	w.table = binary.LittleEndian.AppendUint64(w.table, w.offset)
	for _, sum := range w.sums {
		w.table = binary.LittleEndian.AppendUint64(w.table, sum)
	}
}

func (w *snapshotMetadataWriter) flush() error {
	if w.used == 0 {
		return nil
	}
	w.addTableEntry()
	w.buf = append(w.buf[:0], make([]byte, snapshotBlockHeader)...)
	for column := 0; column < 4; column++ {
		lo, hi := w.rows[0][column], w.rows[0][column]
		for row := 1; row < w.used; row++ {
			lo = min(lo, w.rows[row][column])
			hi = max(hi, w.rows[row][column])
		}
		width := (bits.Len64(uint64(hi)-uint64(lo)) + 7) / 8
		binary.LittleEndian.PutUint64(w.buf[column*9:], uint64(lo))
		w.buf[column*9+8] = byte(width)
		for row := 0; row < w.used; row++ {
			delta := uint64(w.rows[row][column]) - uint64(lo)
			for b := 0; b < width; b++ {
				w.buf = append(w.buf, byte(delta>>(b*8)))
			}
			if column < len(w.sums) {
				w.sums[column] += uint64(w.rows[row][column])
			}
		}
	}
	n, err := w.w.Write(w.buf)
	if err != nil {
		return err
	}
	if n != len(w.buf) {
		return io.ErrShortWrite
	}
	w.offset += uint64(n)
	w.used = 0
	return nil
}

func (w *snapshotMetadataWriter) finish() error {
	if err := w.flush(); err != nil {
		return err
	}
	w.addTableEntry()
	footer := binary.LittleEndian.AppendUint64(nil, w.offset)
	footer = binary.LittleEndian.AppendUint64(footer, w.count)
	footer = binary.LittleEndian.AppendUint64(footer, snapshotBlockRows)
	footer = append(footer, snapshotMetadataMagic...)
	for _, data := range [][]byte{w.table, footer} {
		n, err := w.w.Write(data)
		if err != nil {
			return err
		}
		if n != len(data) {
			return io.ErrShortWrite
		}
	}
	return nil
}

type snapshotMetadata struct {
	data         []byte
	count, table uint64
}

func openSnapshotMetadata(data []byte) (*snapshotMetadata, error) {
	if len(data) < snapshotFooterSize+snapshotTableEntry {
		return nil, fmt.Errorf("snapshot metadata: truncated footer")
	}
	footer := data[len(data)-snapshotFooterSize:]
	if string(footer[24:]) != snapshotMetadataMagic || binary.LittleEndian.Uint64(footer[16:24]) != snapshotBlockRows {
		return nil, fmt.Errorf("snapshot metadata: unsupported format")
	}
	m := &snapshotMetadata{data: data, table: binary.LittleEndian.Uint64(footer), count: binary.LittleEndian.Uint64(footer[8:])}
	blocks := m.count / snapshotBlockRows
	if m.count%snapshotBlockRows != 0 {
		blocks++
	}
	end := uint64(len(data) - snapshotFooterSize)
	if blocks > end/snapshotTableEntry-1 || m.table != end-(blocks+1)*snapshotTableEntry {
		return nil, fmt.Errorf("snapshot metadata: invalid table extent")
	}
	var offset uint64
	for block := uint64(0); block < blocks; block++ {
		start := m.blockOffset(block)
		next := m.blockOffset(block + 1)
		if start != offset || next < start || next > m.table || next-start < snapshotBlockHeader {
			return nil, fmt.Errorf("snapshot metadata: invalid block extent")
		}
		rows := min(uint64(snapshotBlockRows), m.count-block*snapshotBlockRows)
		size := uint64(snapshotBlockHeader)
		for column := uint64(0); column < 4; column++ {
			width := uint64(data[start+column*9+8])
			if width > 8 {
				return nil, fmt.Errorf("snapshot metadata: invalid column width")
			}
			size += rows * width
		}
		if next-start != size {
			return nil, fmt.Errorf("snapshot metadata: invalid column extent")
		}
		offset = next
	}
	if m.blockOffset(blocks) != m.table || offset != m.table {
		return nil, fmt.Errorf("snapshot metadata: trailing block data")
	}
	return m, nil
}

func (m *snapshotMetadata) blockOffset(block uint64) uint64 {
	return binary.LittleEndian.Uint64(m.data[m.table+block*snapshotTableEntry:])
}

func (m *snapshotMetadata) get(row uint64) ([4]int64, error) {
	var values [4]int64
	if row >= m.count {
		return values, fmt.Errorf("snapshot metadata: row out of range")
	}
	block := row / snapshotBlockRows
	start := m.blockOffset(block)
	rows := min(uint64(snapshotBlockRows), m.count-block*snapshotBlockRows)
	offset := start + snapshotBlockHeader
	for column := uint64(0); column < 4; column++ {
		base := binary.LittleEndian.Uint64(m.data[start+column*9:])
		width := uint64(m.data[start+column*9+8])
		position := offset + (row%snapshotBlockRows)*width
		var delta uint64
		for b := uint64(0); b < width; b++ {
			delta |= uint64(m.data[position+b]) << (b * 8)
		}
		values[column] = int64(base + delta)
		offset += rows * width
	}
	return values, nil
}

func (m *snapshotMetadata) prefixSum(end uint64) [3]uint64 {
	block := end / snapshotBlockRows
	position := m.table + block*snapshotTableEntry + 8
	var sums [3]uint64
	for column := range sums {
		sums[column] = binary.LittleEndian.Uint64(m.data[position+uint64(column)*8:])
	}
	for row := block * snapshotBlockRows; row < end; row++ {
		values, _ := m.get(row) // caller checked the range against the validated row count
		for column := range sums {
			sums[column] += uint64(values[column])
		}
	}
	return sums
}

func (m *snapshotMetadata) usage(start, end uint64) ([3]int64, error) {
	var sums [3]int64
	if start > end || end > m.count {
		return sums, fmt.Errorf("snapshot metadata: usage range out of bounds")
	}
	a, b := m.prefixSum(start), m.prefixSum(end)
	for column := range sums {
		sums[column] = int64(b[column] - a[column])
	}
	return sums, nil
}
