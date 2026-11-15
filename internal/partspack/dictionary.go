package partspack

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"slices"

	"github.com/go-graphite/go-carbon/internal/partsformat"
)

// MaxPartBytes bounds the complete authenticated dictionary and its dependents.
const MaxPartBytes = 64 << 20

// MaxPartRecords bounds index allocations before any record is decoded.
const MaxPartRecords = MaxPartBytes / 128

// Record retains the complete portable identity and the existing exact block
// bytes. A dictionary ordinal is meaningful only inside its containing part.
type Record struct {
	Key  partsformat.Key
	Data []byte
}

// DictionaryPart is an indivisible, self-contained packed value/index. Records
// returned by OpenPart borrow Data from its input; keep that input immutable.
// Copy, pin and retire the complete part, never a subset of its encoded bytes.
type DictionaryPart struct {
	Policy  Policy
	Records []Record
}

// EncodePart replaces repeated names and ownership prefixes with local ordinals
// inside this value. It does not change the shared GCP3 key ABI. Every record
// must belong to one existing ownership/time partition and immutable PartID.
// Input order is immaterial; output follows the original portable key order.
func EncodePart(policy Policy, records []Record) ([]byte, error) {
	if len(records) == 0 || len(records) > MaxPartRecords {
		return nil, errors.New("invalid packed record count")
	}
	pb, err := policy.MarshalBinary()
	if err != nil {
		return nil, err
	}
	type keyed struct {
		r Record
		k []byte
	}
	rows := make([]keyed, len(records))
	names := make([]partsformat.ID, 0)
	seen := make(map[partsformat.ID]bool)
	headerCounts := make(map[string]int)
	first := records[0].Key
	budget := len(pb) + 128
	expandedBudget := 0
	for i, r := range records {
		k := r.Key
		if k.Group != first.Group || k.VShard != first.VShard || k.Partition != first.Partition || k.Part != first.Part {
			return nil, errors.New("packed part crosses ownership, partition or part identity")
		}
		if int(k.Archive) >= len(policy.Archives) || len(r.Data) == 0 {
			return nil, errors.New("invalid packed archive or empty payload")
		}
		kb, err := partsformat.Encode(k)
		if err != nil {
			return nil, err
		}
		expandedBudget += len(kb) + len(r.Data) + 64
		if expandedBudget > 4*MaxPartBytes {
			return nil, errors.New("packed part exceeds expanded byte limit")
		}
		budget += len(r.Data) + 32
		if !seen[k.Metric] {
			budget += len(k.Metric) + 10
			seen[k.Metric] = true
			names = append(names, k.Metric)
		}
		if budget > MaxPartBytes {
			return nil, errors.New("packed part exceeds byte limit")
		}
		rows[i] = keyed{r, kb}
		if len(r.Data) >= 12 {
			headerCounts[string(r.Data[:12])]++
		}
	}
	slices.Sort(names)
	ordinals := make(map[partsformat.ID]uint64, len(names))
	for i, name := range names {
		ordinals[name] = uint64(i)
	}
	// Cold-pack's first 12 bytes (version/flags/count/first/step) often repeat
	// across many names. Factor exact byte prefixes, retaining each CRC and body.
	// This is reversible byte factoring, not a second numeric codec.
	var headers []string
	for header, count := range headerCounts {
		if count > 1 {
			headers = append(headers, header)
		}
	}
	slices.Sort(headers)
	headerIDs := make(map[string]uint64, len(headers))
	for i, h := range headers {
		headerIDs[h] = uint64(i + 1)
	}
	slices.SortFunc(rows, func(a, b keyed) int { return bytes.Compare(a.k, b.k) })
	prefix, err := partsformat.PartitionPrefixForGroup(first.Group, first.VShard, first.Partition)
	if err != nil {
		return nil, err
	}
	prefix = append(prefix, first.Part[:]...)
	b := make([]byte, 0, budget)
	b = append(b, "GCD1"...)
	b = binary.AppendUvarint(b, uint64(len(prefix)))
	b = append(b, prefix...)
	b = binary.AppendUvarint(b, uint64(len(pb)))
	b = append(b, pb...)
	b = binary.AppendUvarint(b, uint64(len(names)))
	var previousName partsformat.ID
	for _, name := range names {
		shared := 0
		for shared < len(previousName) && shared < len(name) && previousName[shared] == name[shared] {
			shared++
		}
		b = binary.AppendUvarint(b, uint64(shared))
		b = binary.AppendUvarint(b, uint64(len(name)-shared))
		b = append(b, name[shared:]...)
		previousName = name
	}
	b = binary.AppendUvarint(b, uint64(len(headers)))
	for _, h := range headers {
		b = append(b, h...)
	}
	b = binary.AppendUvarint(b, uint64(len(rows)))
	for i, row := range rows {
		if i > 0 && bytes.Equal(rows[i-1].k, row.k) {
			return nil, errors.New("duplicate packed portable key")
		}
		k := row.r.Key
		b = binary.AppendUvarint(b, ordinals[k.Metric])
		b = binary.AppendUvarint(b, uint64(k.Archive))
		b = binary.AppendVarint(b, k.BlockStart)
		b = binary.AppendUvarint(b, uint64(k.Ordinal))
		data := row.r.Data
		var headerID uint64
		if len(data) >= 12 {
			headerID = headerIDs[string(data[:12])]
		}
		tag := headerID << 1
		if headerID > 0 {
			// The containing SHA covers the complete reconstructed payload. A
			// valid cold-block CRC is redundant on disk and is reconstructed
			// exactly at open; arbitrary opaque payloads keep all their bytes.
			if len(data) >= 16 && data[0] == 3 && binary.LittleEndian.Uint32(data[12:16]) == crc32.Update(crc32.ChecksumIEEE(data[:12]), crc32.IEEETable, data[16:]) {
				tag |= 1
				data = data[16:]
			} else {
				data = data[12:]
			}
		}
		b = binary.AppendUvarint(b, tag)
		b = binary.AppendUvarint(b, uint64(len(data)))
		b = append(b, data...)
	}
	if len(b)+sha256.Size > MaxPartBytes {
		return nil, errors.New("packed part exceeds byte limit")
	}
	sum := sha256.Sum256(b)
	return append(b, sum[:]...), nil
}

type partCursor struct{ b []byte }

func (c *partCursor) uint() (uint64, error) {
	v, n := binary.Uvarint(c.b)
	if n <= 0 {
		return 0, errors.New("invalid packed integer")
	}
	c.b = c.b[n:]
	return v, nil
}
func (c *partCursor) blob() ([]byte, error) {
	n, err := c.uint()
	if err != nil || n > uint64(len(c.b)) {
		return nil, errors.New("invalid packed length")
	}
	b := c.b[:int(n)]
	c.b = c.b[int(n):]
	return b, nil
}

// OpenPart authenticates the complete dictionary and all dependent records
// before exposing any ordinal. Ordinals resolve to exact canonical bytes; keys
// are revalidated using the shared portable format, including routing checks.
func OpenPart(b []byte) (*DictionaryPart, error) {
	if len(b) < 4+sha256.Size || len(b) > MaxPartBytes || string(b[:4]) != "GCD1" {
		return nil, errors.New("invalid packed part header")
	}
	body := b[:len(b)-sha256.Size]
	sum := sha256.Sum256(body)
	if !bytes.Equal(sum[:], b[len(body):]) {
		return nil, errors.New("packed part digest mismatch")
	}
	c := partCursor{body[4:]}
	prefix, err := c.blob()
	if err != nil || len(prefix) < 34 {
		return nil, errors.New("invalid packed ownership prefix")
	}
	pb, err := c.blob()
	if err != nil {
		return nil, err
	}
	policy, err := ParsePolicy(pb)
	if err != nil {
		return nil, err
	}
	namesCount, err := c.uint()
	if err != nil || namesCount == 0 || namesCount > MaxPartRecords || namesCount > uint64(len(c.b))/2 {
		return nil, errors.New("invalid packed dictionary count")
	}
	names := make([]partsformat.ID, int(namesCount))
	nameBytes := 0
	for i := range names {
		shared, e1 := c.uint()
		suffix, e2 := c.blob()
		var prev partsformat.ID
		if i > 0 {
			prev = names[i-1]
		}
		if e1 != nil || e2 != nil || shared > uint64(len(prev)) || len(suffix) == 0 || shared+uint64(len(suffix)) > partsformat.MaxNameBytes {
			return nil, errors.New("invalid packed dictionary name")
		}
		names[i] = partsformat.ID(string(prev[:int(shared)]) + string(suffix))
		nameBytes += len(names[i])
		if nameBytes > MaxPartBytes {
			return nil, errors.New("expanded dictionary exceeds byte limit")
		}
		if i > 0 && names[i] <= names[i-1] {
			return nil, errors.New("packed dictionary is not strictly ordered")
		}
	}
	headerCount, err := c.uint()
	if err != nil || headerCount > uint64(len(c.b))/12 {
		return nil, errors.New("invalid packed header dictionary")
	}
	headers := c.b[:int(headerCount)*12]
	c.b = c.b[len(headers):]
	for i := 1; i < int(headerCount); i++ {
		if bytes.Compare(headers[(i-1)*12:i*12], headers[i*12:(i+1)*12]) >= 0 {
			return nil, errors.New("packed headers are not strictly ordered")
		}
	}
	count, err := c.uint()
	if err != nil || count == 0 || count > MaxPartRecords || count > uint64(len(c.b))/6 {
		return nil, errors.New("invalid packed record count")
	}
	p := &DictionaryPart{Policy: policy, Records: make([]Record, 0, int(count))}
	var previous []byte
	expandedBytes := 0
	for range count {
		name, e1 := c.uint()
		archive, e2 := c.uint()
		start, n := binary.Varint(c.b)
		if e1 != nil || e2 != nil || n <= 0 || name >= uint64(len(names)) || archive >= uint64(len(policy.Archives)) {
			return nil, errors.New("invalid packed record reference")
		}
		c.b = c.b[n:]
		ordinal, e3 := c.uint()
		tag, e5 := c.uint()
		headerID := tag >> 1
		stripCRC := tag&1 != 0
		data, e4 := c.blob()
		if e3 != nil || e4 != nil || e5 != nil || headerID > headerCount || ordinal > 1<<32-1 || (headerID == 0 && (len(data) == 0 || stripCRC)) {
			return nil, errors.New("invalid packed record payload")
		}
		if headerID > 0 {
			headerLen := 12
			if stripCRC {
				headerLen = 16
			}
			restored := make([]byte, headerLen+len(data))
			copy(restored, headers[(int(headerID)-1)*12:int(headerID)*12])
			copy(restored[headerLen:], data)
			if stripCRC {
				if restored[0] != 3 {
					return nil, errors.New("invalid packed cold CRC reconstruction")
				}
				binary.LittleEndian.PutUint32(restored[12:16], crc32.Update(crc32.ChecksumIEEE(restored[:12]), crc32.IEEETable, data))
			}
			data = restored
		}
		expandedBytes += len(prefix) + len(names[name]) + len(data) + 84
		if expandedBytes > 4*MaxPartBytes {
			return nil, errors.New("packed part exceeds expanded byte limit")
		}
		// Reconstruct the original GCP3 key rather than introducing a second
		// interpretation of placement-group, routing or partition bytes.
		key := append([]byte(nil), prefix...)
		key = binary.BigEndian.AppendUint32(key, uint32(len(names[name])))
		key = append(key, names[name]...)
		key = binary.BigEndian.AppendUint32(key, uint32(archive))
		key = binary.BigEndian.AppendUint64(key, uint64(start)^(1<<63))
		key = binary.BigEndian.AppendUint32(key, uint32(ordinal))
		k, err := partsformat.Decode(key)
		if err != nil {
			return nil, fmt.Errorf("packed portable key: %w", err)
		}
		if previous != nil && bytes.Compare(previous, key) >= 0 {
			return nil, errors.New("packed records are not strictly ordered")
		}
		previous = key
		p.Records = append(p.Records, Record{k, data})
	}
	if len(c.b) != 0 {
		return nil, errors.New("trailing packed part bytes")
	}
	return p, nil
}
