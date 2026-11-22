package partlog

import (
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
	sort.Slice(c.pending, func(i, j int) bool { return c.key(c.pending[i]) < c.key(c.pending[j]) })
	raw := binary.AppendUvarint(make([]byte, 0, c.pendingBytes), uint64(len(c.pending)))
	previous := ""
	for _, id := range c.pending {
		key := c.key(id)
		prefix := 0
		for prefix < len(previous) && prefix < len(key) && previous[prefix] == key[prefix] {
			prefix++
		}
		raw = binary.AppendUvarint(raw, id)
		raw = binary.AppendUvarint(raw, uint64(prefix))
		raw = binary.AppendUvarint(raw, uint64(len(key)-prefix))
		raw = append(raw, key[prefix:]...)
		definition := c.definition(id)
		raw = binary.AppendUvarint(raw, uint64(len(definition)))
		raw = append(raw, definition...)
		previous = key
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
	h = binary.LittleEndian.AppendUint32(h, crc32.Checksum(h, catalogCRC))
	crc := crc32.Update(crc32.Checksum(h, catalogCRC), catalogCRC, body)
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
		if b.id == 0 || b.id > c.Count()+1 || (i > 0 && b.id == bindings[i-1].id) {
			return errCatalog
		}
		if b.id <= c.Count() {
			if !c.Matches(b.id, b.key, b.def) {
				return errCatalog
			}
		} else {
			hash := NameHash(b.key)
			if c.ids.lookup(b.key, hash, c) != 0 {
				return errCatalog
			}
			c.ids.insert(hash, b.id)
			if err := c.bindEntry(b.id, b.key, b.def); err != nil {
				return err
			}
		}
	}
	return nil
}
