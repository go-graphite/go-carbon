package v2backend

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash/maphash"
	"slices"
	"sort"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/v2core"
)

// This single-origin physical format differs deliberately from portable GCP3.
// PG/partition/seal/origin/vshard/name makes each DB seal append-disjoint while
// retaining exact portable names, routing, apply order and value bits. Imports
// remain unavailable until they can fence the origin and allocate a new seal.
var appendLogPrefix = []byte("GCS\x04")

const appendCheckpointSeals = 64
const appendCheckpointBytes = 128 << 20
const appendFilterWords = 8192

var appendHashSeed = maphash.MakeSeed()

type appendPart struct {
	Prefix               []byte // PG/partition/Part, through the immutable part identity.
	Filter               []uint64
	MinVShard, MaxVShard uint16
}
type appendState struct {
	metadata           map[string][]byte // latest logical metadata, never point data/refs.
	parts              []*appendPart
	deltaBytes         int
	sequence           uint64
	checkpointSequence uint64
}

func dataVersion(o *owner) byte {
	if o.appendState != nil {
		return 4
	}
	return 3
}
func appendLogKey(seq uint64) []byte {
	return binary.BigEndian.AppendUint64(slices.Clone(appendLogPrefix), seq)
}

func encodeDataKey(old []byte, seq uint64, origin [8]byte) ([]byte, error) {
	k, err := partsformat.Decode(old)
	if err != nil {
		return nil, err
	}
	gn := len(k.Group)
	p := 8 + gn
	out := slices.Clone(old)
	out[3] = 4
	// Old: PG/vshard/partition/random-Part/name. New: PG/partition/Part/vshard/name.
	copy(out[p:p+8], old[p+2:p+10])
	binary.BigEndian.PutUint64(out[p+8:p+16], seq)
	copy(out[p+16:p+24], origin[:])
	copy(out[p+24:p+26], old[p:p+2])
	return out, nil
}
func decodeDataKey(b []byte) (partsformat.Key, error) {
	if len(b) < 4 || b[3] != 4 {
		return partsformat.Decode(b)
	}
	if len(b) < 54 {
		return partsformat.Key{}, errors.New("truncated append data key")
	}
	gn := int(binary.BigEndian.Uint32(b[4:8]))
	p := 8 + gn
	if gn > partsformat.MaxNameBytes || p > len(b)-46 {
		return partsformat.Key{}, errors.New("invalid append group")
	}
	old := slices.Clone(b)
	old[3] = 3
	copy(old[p:p+2], b[p+24:p+26])
	copy(old[p+2:p+10], b[p:p+8])
	copy(old[p+10:p+26], b[p+8:p+24])
	return partsformat.Decode(old)
}
func filterHash(name string) uint64 { return maphash.String(appendHashSeed, name) }
func (p *appendPart) add(name string) {
	h := filterHash(name)
	for i := 0; i < 3; i++ {
		bit := h & ((appendFilterWords * 64) - 1)
		p.Filter[bit/64] |= 1 << (bit % 64)
		h = (h >> 21) | (h << 43)
	}
}
func (p *appendPart) metricPrefix(name string) []byte {
	vs := partsformat.Route(partsformat.ID(name))
	if vs < p.MinVShard || vs > p.MaxVShard {
		return nil
	}
	h := filterHash(name)
	for i := 0; i < 3; i++ {
		bit := h & ((appendFilterWords * 64) - 1)
		if p.Filter[bit/64]&(1<<(bit%64)) == 0 {
			return nil
		}
		h = (h >> 21) | (h << 43)
	}
	b := binary.BigEndian.AppendUint16(slices.Clone(p.Prefix), vs)
	b = binary.BigEndian.AppendUint32(b, uint32(len(name)))
	return append(b, name...)
}

// Persist filters using a stable hash seed is unnecessary: restart rebuilds
// filters from exact names in data keys. Checkpoints persist part prefixes only.
func encodeAppendRecord(full bool, rows []v2core.KV, parts []*appendPart) []byte {
	size := 13
	for _, row := range rows {
		size += 8 + len(row.Key) + len(row.Value)
	}
	for _, p := range parts {
		size += 8 + len(p.Prefix)
	}
	b := make([]byte, 5, size)
	copy(b, []byte{'A', 'P', 'S', 4, 0})
	if full {
		b[4] = 1
	}
	b = binary.BigEndian.AppendUint32(b, uint32(len(rows)))
	for _, row := range rows {
		b = binary.BigEndian.AppendUint32(b, uint32(len(row.Key)))
		b = binary.BigEndian.AppendUint32(b, uint32(len(row.Value)))
		b = append(b, row.Key...)
		b = append(b, row.Value...)
	}
	b = binary.BigEndian.AppendUint32(b, uint32(len(parts)))
	for _, p := range parts {
		b = appendString(b, string(p.Prefix))
		b = binary.BigEndian.AppendUint16(b, p.MinVShard)
		b = binary.BigEndian.AppendUint16(b, p.MaxVShard)
	}
	return b
}
func decodeAppendRecord(b []byte) (bool, []v2core.KV, []*appendPart, error) {
	if len(b) < 9 || !bytes.Equal(b[:4], []byte{'A', 'P', 'S', 4}) || b[4] > 1 {
		return false, nil, nil, errors.New("invalid append seal record")
	}
	full := b[4] == 1
	d := checkpointDecoder{b: b[5:]}
	n := uint64(d.u32())
	if n > uint64(len(d.b))/8 {
		return false, nil, nil, errors.New("invalid seal metadata count")
	}
	rows := make([]v2core.KV, 0, n)
	for range n {
		kn, vn := uint64(d.u32()), uint64(d.u32())
		if kn+vn > uint64(len(d.b)) {
			return false, nil, nil, errors.New("invalid seal metadata length")
		}
		rows = append(rows, v2core.KV{Key: slices.Clone(d.take(int(kn))), Value: slices.Clone(d.take(int(vn)))})
	}
	n = uint64(d.u32())
	if n > uint64(len(d.b))/8 {
		return false, nil, nil, errors.New("invalid seal part count")
	}
	var parts []*appendPart
	for range n {
		p := &appendPart{Prefix: []byte(d.str()), Filter: make([]uint64, appendFilterWords)}
		p.MinVShard = binary.BigEndian.Uint16(d.take(2))
		p.MaxVShard = binary.BigEndian.Uint16(d.take(2))
		if len(p.Prefix) < 32 || !bytes.Equal(p.Prefix[:4], []byte{'G', 'C', 'P', 4}) || uint64(binary.BigEndian.Uint32(p.Prefix[4:8])) > partsformat.MaxNameBytes || uint64(len(p.Prefix)) != 32+uint64(binary.BigEndian.Uint32(p.Prefix[4:8])) || p.MinVShard > p.MaxVShard || p.MaxVShard >= partsformat.VirtualShards {
			return false, nil, nil, errors.New("invalid seal part")
		}
		parts = append(parts, p)
	}
	return full, rows, parts, d.end()
}
func (s *Store) appendSeal(id int, seq uint64, plan *v2core.SealPlan) error {
	state := s.owners[id].appendState
	if state == nil || seq <= state.sequence {
		return errors.New("non-monotonic append seal")
	}
	digest := sha256.Sum256(binary.BigEndian.AppendUint32([]byte(s.log.Identity()), uint32(id)))
	var origin [8]byte
	copy(origin[:], digest[:8])
	files := make(map[string][]v2core.KV)
	byPart := make(map[string]*appendPart)
	var metadata []v2core.KV
	for _, file := range plan.Files {
		for _, row := range file {
			switch {
			case bytes.HasPrefix(row.Key, []byte{'G', 'C', 'P', 3}):
				key, err := encodeDataKey(row.Key, seq, origin)
				if err != nil {
					return err
				}
				gn := int(binary.BigEndian.Uint32(key[4:8]))
				p := 8 + gn + 24
				prefix := string(key[:p])
				files[prefix] = append(files[prefix], v2core.KV{Key: key, Value: row.Value})
				part := byPart[prefix]
				if part == nil {
					part = &appendPart{Prefix: slices.Clone(key[:p]), Filter: make([]uint64, appendFilterWords), MinVShard: partsformat.VirtualShards - 1}
					byPart[prefix] = part
				}
				k, err := partsformat.Decode(row.Key)
				if err != nil {
					return err
				}
				part.MinVShard = min(part.MinVShard, k.VShard)
				part.MaxVShard = max(part.MaxVShard, k.VShard)
				part.add(string(k.Metric))
			case bytes.HasPrefix(row.Key, []byte{253, 'V', '2', 1}): // refs are represented by the part catalog.
			default:
				metadata = append(metadata, v2core.KV{Key: slices.Clone(row.Key), Value: slices.Clone(row.Value)})
			}
		}
	}
	var parts []*appendPart
	for _, part := range byPart {
		parts = append(parts, part)
	}
	sort.Slice(parts, func(i, j int) bool { return bytes.Compare(parts[i].Prefix, parts[j].Prefix) < 0 })
	bytesAdded := 0
	for _, row := range metadata {
		bytesAdded += len(row.Key) + len(row.Value)
	}
	full := seq-state.checkpointSequence >= appendCheckpointSeals || state.deltaBytes+bytesAdded >= appendCheckpointBytes
	recordRows, recordParts := metadata, parts
	if full {
		merged := make(map[string][]byte, len(state.metadata)+len(metadata))
		for k, v := range state.metadata {
			merged[k] = v
		}
		for _, row := range metadata {
			merged[string(row.Key)] = row.Value
		}
		recordRows = make([]v2core.KV, 0, len(merged))
		for k, v := range merged {
			recordRows = append(recordRows, v2core.KV{Key: []byte(k), Value: v})
		}
		sort.Slice(recordRows, func(i, j int) bool { return bytes.Compare(recordRows[i].Key, recordRows[j].Key) < 0 })
		recordParts = append(slices.Clone(state.parts), parts...)
	}
	plan.Files = nil
	for _, file := range files {
		slices.SortFunc(file, func(a, b v2core.KV) int { return bytes.Compare(a.Key, b.Key) })
		plan.Files = append(plan.Files, file)
	}
	plan.Files = append(plan.Files, []v2core.KV{{Key: appendLogKey(seq), Value: encodeAppendRecord(full, recordRows, recordParts)}})
	sort.Slice(plan.Files, func(i, j int) bool { return bytes.Compare(plan.Files[i][0].Key, plan.Files[j][0].Key) < 0 })
	if full {
		plan.RangeDeletes = append(plan.RangeDeletes, [2][]byte{slices.Clone(appendLogPrefix), appendLogKey(seq)})
	}
	commit := plan.Commit
	plan.Commit = func() {
		commit()
		for _, row := range metadata {
			state.metadata[string(row.Key)] = row.Value
		}
		state.parts = append(state.parts, parts...)
		state.sequence = seq
		state.deltaBytes += bytesAdded
		if full {
			state.deltaBytes = 0
			state.checkpointSequence = seq
		}
	}
	return nil
}
func (s *Store) recoverAppendStates() error {
	for id, db := range s.dbs {
		state := &appendState{metadata: make(map[string][]byte)}
		it, err := db.NewIter(&pebble.IterOptions{LowerBound: appendLogPrefix, UpperBound: partsformat.PrefixEnd(appendLogPrefix)})
		if err != nil {
			return err
		}
		for it.First(); it.Valid(); it.Next() {
			if len(it.Key()) != 12 {
				_ = it.Close()
				return errors.New("invalid append seal key")
			}
			seq := binary.BigEndian.Uint64(it.Key()[4:])
			if seq <= state.sequence {
				_ = it.Close()
				return errors.New("unordered append seal")
			}
			full, rows, parts, e := decodeAppendRecord(it.Value())
			if e != nil {
				_ = it.Close()
				return e
			}
			if full {
				clear(state.metadata)
				state.parts = nil
				state.checkpointSequence = seq
				state.deltaBytes = 0
			}
			for _, row := range rows {
				state.metadata[string(row.Key)] = row.Value
				if !full {
					state.deltaBytes += len(row.Key) + len(row.Value)
				}
			}
			state.parts = append(state.parts, parts...)
			state.sequence = seq
		}
		if err = errors.Join(it.Error(), it.Close()); err != nil {
			return err
		}
		// Rebuild membership filters from exact physical names with one sequential
		// restart scan; no read-before-write is performed during intake or sealing.
		byPrefix := make(map[string]*appendPart, len(state.parts))
		for _, p := range state.parts {
			if byPrefix[string(p.Prefix)] != nil {
				return errors.New("duplicate append part")
			}
			byPrefix[string(p.Prefix)] = p
		}
		it, err = db.NewIter(&pebble.IterOptions{LowerBound: []byte{'G', 'C', 'P', 4}, UpperBound: []byte{'G', 'C', 'P', 5}})
		if err != nil {
			return err
		}
		for it.First(); it.Valid(); it.Next() {
			k, e := decodeDataKey(it.Key())
			if e != nil {
				_ = it.Close()
				return e
			}
			p := 8 + len(k.Group) + 24
			part := byPrefix[string(it.Key()[:p])]
			if part == nil {
				_ = it.Close()
				return errors.New("data missing from append catalog")
			}
			if k.VShard < part.MinVShard || k.VShard > part.MaxVShard {
				_ = it.Close()
				return errors.New("data outside append catalog bounds")
			}
			part.add(string(k.Metric))
		}
		if err = errors.Join(it.Error(), it.Close()); err != nil {
			return err
		}
		for _, o := range s.owners {
			if o.id%s.opts.Shards == id {
				o.appendState = state
			}
		}
	}
	return nil
}
