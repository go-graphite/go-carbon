package partlog

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"sort"

	"github.com/klauspost/compress/zstd"
)

// The length's high bit distinguishes grouped WAL records from legacy records.
// Each bounded group is independently checksummed and compressed. IDs remain in
// arrival order even though names are front-coded in lexical order on disk.
const catalogGroupFlag uint32 = 1 << 31
const catalogGroupLimit = 4 << 20

func (c *NameCatalog) flushGroup() error {
	if len(c.pending) == 0 {
		return nil
	}
	sort.Slice(c.pending, func(i, j int) bool { return c.entries[c.pending[i]].key < c.entries[c.pending[j]].key })
	raw := binary.AppendUvarint(make([]byte, 0, c.pendingBytes), uint64(len(c.pending)))
	previous := ""
	for _, id := range c.pending {
		e := c.entries[id]
		prefix := 0
		for prefix < len(previous) && prefix < len(e.key) && previous[prefix] == e.key[prefix] {
			prefix++
		}
		raw = binary.AppendUvarint(raw, id)
		raw = binary.AppendUvarint(raw, uint64(prefix))
		raw = binary.AppendUvarint(raw, uint64(len(e.key)-prefix))
		raw = append(raw, e.key[prefix:]...)
		raw = binary.AppendUvarint(raw, uint64(len(e.definition)))
		raw = append(raw, e.definition...)
		previous = e.key
	}
	if c.groupEncoder == nil {
		var err error
		c.groupEncoder, err = zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithWindowSize(1<<20), zstd.WithEncoderCRC(true))
		if err != nil {
			return err
		}
	}
	body := binary.LittleEndian.AppendUint32(nil, uint32(len(raw)))
	body = c.groupEncoder.EncodeAll(raw, body)
	h := binary.LittleEndian.AppendUint32(nil, uint32(len(body))|catalogGroupFlag)
	h = binary.LittleEndian.AppendUint32(h, crc32.Checksum(h, predictorV3CRC))
	crc := crc32.Update(crc32.Checksum(h, predictorV3CRC), predictorV3CRC, body)
	if _, err := c.buffer.Write(h); err != nil {
		return err
	}
	if _, err := c.buffer.Write(body); err != nil {
		return err
	}
	if _, err := c.buffer.Write(binary.LittleEndian.AppendUint32(nil, crc)); err != nil {
		return err
	}
	c.pending = c.pending[:0]
	c.pendingBytes = 0
	return nil
}

func (c *NameCatalog) readGroup(body []byte) error {
	if len(body) < 4 {
		return errCatalog
	}
	rawSize := binary.LittleEndian.Uint32(body)
	if rawSize == 0 || rawSize > catalogGroupLimit {
		return errCatalog
	}
	if c.groupDecoder == nil {
		var err error
		c.groupDecoder, err = zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(64<<20), zstd.WithDecodeAllCapLimit(true))
		if err != nil {
			return err
		}
	}
	raw, err := c.groupDecoder.DecodeAll(body[4:], make([]byte, 0, int(rawSize)))
	if err != nil {
		return err
	}
	if len(raw) != int(rawSize) {
		return errCatalog
	}
	d := dictionaryReader{b: raw}
	count := d.u()
	if count == 0 || count > uint64(len(raw)/4) {
		return errCatalog
	}
	type binding struct {
		id  uint64
		key string
		def []byte
	}
	bindings := make([]binding, 0, int(count))
	previous := ""
	expandedBytes := 0
	for range count {
		id, prefix := d.u(), d.u()
		suffix := d.bytes()
		def := d.bytes()
		if d.err != nil || prefix > uint64(len(previous)) {
			return errCatalog
		}
		// Front coding must not amplify bounded raw bytes into unbounded names.
		expandedBytes += int(prefix) + len(suffix) + len(def) + 64
		if expandedBytes > catalogGroupLimit {
			return errCatalog
		}
		key := previous[:int(prefix)] + string(suffix)
		if key == "" || (len(bindings) > 0 && key <= previous) {
			return errCatalog
		}
		bindings = append(bindings, binding{id, key, def})
		previous = key
	}
	if d.err != nil || len(d.b) != 0 {
		return errCatalog
	}
	sort.Slice(bindings, func(i, j int) bool { return bindings[i].id < bindings[j].id })
	for i, b := range bindings {
		if b.id == 0 || b.id > uint64(len(c.entries)) || (i > 0 && b.id == bindings[i-1].id) {
			return errCatalog
		}
		if b.id < uint64(len(c.entries)) {
			old := c.entries[b.id]
			if old.key != b.key || !bytes.Equal(old.definition, b.def) {
				return errCatalog
			}
		} else {
			hash := NameHash(b.key)
			if c.ids.lookup(b.key, hash, c.entries) != 0 {
				return errCatalog
			}
			c.ids.insert(hash, b.id)
			c.entries = append(c.entries, catalogEntry{key: b.key, definition: bytes.Clone(b.def)})
		}
	}
	return nil
}
