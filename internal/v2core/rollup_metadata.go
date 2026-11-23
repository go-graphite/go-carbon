package v2core

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	partsformat "github.com/go-graphite/go-carbon/internal/v2part"
)

var rollupMetadataMagic = []byte{'R', 'U', 'M', 1}

// AppendRollupMetadata appends a fixed-width binary metadata row followed by
// the immutable canonical policy. No JSON serialization occurs at seal time.
// The caller owns dst through durable publication. Old JSON rows remain readable.
func AppendRollupMetadata(dst []byte, m RollupMetadata) []byte {
	dst = append(dst, rollupMetadataMagic...)
	if m.Version == 3 {
		dst[len(dst)-1] = 3
	} else if m.BaselineOrder > 0 {
		dst[len(dst)-1] = 2
	}
	dst = append(dst, m.Version)
	flags := byte(0)
	if m.Expired {
		flags = 1
	}
	dst = append(dst, flags)
	for _, n := range []uint64{m.ID, m.Frontier.Order, uint64(m.Frontier.MaxTime), uint64(m.LastUpdate), m.Revision, m.ResetOrder} {
		dst = binary.BigEndian.AppendUint64(dst, n)
	}
	if m.BaselineOrder > 0 || m.Version == 3 {
		dst = binary.BigEndian.AppendUint64(dst, m.BaselineOrder)
	}
	if m.Version == 3 {
		dst = binary.BigEndian.AppendUint64(dst, 0)
	}
	dst = append(dst, m.Frontier.Policy[:]...)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(m.Policy)))
	return append(dst, m.Policy...)
}

// rollupWriteDefinitions makes seal and merge writers emit the full encoding
// with the stored definition. Only the encoding differential test sets it.
var rollupWriteDefinitions atomic.Bool

// appendMetadata emits the slim seal encoding: the definition is not stored.
func (h *PolicyIdentity) appendMetadata(dst []byte, m RollupMetadata) []byte {
	if rollupWriteDefinitions.Load() {
		m.Policy = nil
		dst = AppendRollupMetadata(dst, m)
		start := len(dst)
		dst = h.appendDefinition(dst)
		binary.BigEndian.PutUint32(dst[start-4:start], uint32(len(dst)-start))
		return dst
	}
	return appendRollupMetadataSlim(dst, m)
}

// rollupMetadataSlim marks rows that store no definition. The header is the
// Version 3 layout; readers rebuild the definition bit-exactly from the key's
// name and group and the part policy table entry named by Frontier.Policy.
// Binaries that predate it fail on such rows instead of misreading them.
const rollupMetadataSlim = 4

// appendRollupMetadataSlim writes m without its definition. Decoding returns
// exactly what AppendRollupMetadata(m) decodes to, including the definition.
func appendRollupMetadataSlim(dst []byte, m RollupMetadata) []byte {
	flags := byte(0)
	if m.Expired {
		flags = 1
	}
	dst = append(dst, 'R', 'U', 'M', rollupMetadataSlim, m.Version, flags)
	for _, n := range []uint64{m.ID, m.Frontier.Order, uint64(m.Frontier.MaxTime), uint64(m.LastUpdate), m.Revision, m.ResetOrder, m.BaselineOrder} {
		dst = binary.BigEndian.AppendUint64(dst, n)
	}
	dst = binary.BigEndian.AppendUint64(dst, 0) // retired closure watermark
	dst = append(dst, m.Frontier.Policy[:]...)
	return binary.BigEndian.AppendUint32(dst, 0)
}

// DecodeRollupMetadata accepts binary rows and legacy JSON metadata stored
// under the metadata key of name in group. Returned policy bytes are detached
// from the iterator buffer and safe after Next/Close.
func DecodeRollupMetadata(name partsformat.ID, group partsformat.Group, data []byte) (RollupMetadata, error) {
	m, slim, err := decodeRollupMetadataHeader(data)
	if err != nil {
		return m, err
	}
	if slim {
		m.Policy, err = appendRollupDefinition(nil, name, group, m.Frontier.Policy)
	} else if len(data) >= 4 && bytes.Equal(data[:3], rollupMetadataMagic[:3]) {
		m.Policy = bytes.Clone(m.Policy)
	}
	return m, err
}

// The policy view is valid only until the iterator moves. Intern it before
// retaining metadata; the catalog owns the immutable definition returned.
func decodeRollupMetadataView(name partsformat.ID, group partsformat.Group, data []byte) (RollupMetadata, error) {
	m, slim, err := decodeRollupMetadataHeader(data)
	if err == nil && slim {
		m.Policy, err = appendRollupDefinition(nil, name, group, m.Frontier.Policy)
	}
	return m, err
}

// decodeRollupMetadataHeader decodes every field except a slim row's
// definition: slim rows return a nil Policy and slim true.
func decodeRollupMetadataHeader(data []byte) (m RollupMetadata, slim bool, err error) {
	if len(data) < 4 || !bytes.Equal(data[:3], rollupMetadataMagic[:3]) || (data[3] < 1 || data[3] > rollupMetadataSlim) {
		// A separate variable keeps m off the heap on the binary path.
		var j RollupMetadata
		err = json.Unmarshal(data, &j)
		return j, false, err
	}
	header := 90
	if data[3] == 2 {
		header = 98
	}
	if data[3] >= 3 {
		header = 106
	}
	slim = data[3] == rollupMetadataSlim
	if len(data) < header || data[5] > 1 || uint64(binary.BigEndian.Uint32(data[header-4:header])) != uint64(len(data)-header) || (slim && len(data) != header) {
		return m, false, errors.New("invalid binary rollup metadata")
	}
	m.Version = data[4]
	m.Expired = data[5] == 1
	m.ID = binary.BigEndian.Uint64(data[6:14])
	m.Frontier = sealrollup.Frontier{Order: binary.BigEndian.Uint64(data[14:22]), MaxTime: int64(binary.BigEndian.Uint64(data[22:30]))}
	m.LastUpdate = int64(binary.BigEndian.Uint64(data[30:38]))
	m.Revision = binary.BigEndian.Uint64(data[38:46])
	m.ResetOrder = binary.BigEndian.Uint64(data[46:54])
	offset := 54
	if data[3] >= 2 {
		m.BaselineOrder = binary.BigEndian.Uint64(data[54:62])
		offset = 62
	}
	if data[3] >= 3 {
		// Bytes 62:70 held the retired closure watermark.
		offset = 70
	}
	copy(m.Frontier.Policy[:], data[offset:offset+32])
	if !slim {
		m.Policy = data[header:]
	}
	return m, slim, nil
}

type rollupDefinitionKey struct {
	policy [32]byte
	group  partsformat.Group
}

// rollupDefinitionSuffixes caches `,"Group":...,"Config":...}` per policy
// and group; only the name varies between definitions.
var rollupDefinitionSuffixes sync.Map // rollupDefinitionKey -> []byte

// appendRollupDefinition appends EncodeRollupPolicy({name, group, config})
// where config is the part policy table entry whose PolicyDigest is policy.
// It fails closed when no registered part table carries that policy.
func appendRollupDefinition(dst []byte, name partsformat.ID, group partsformat.Group, policy [32]byte) ([]byte, error) {
	resolved, ok := partsformat.ResolvePolicy(policy)
	if !ok {
		return dst, errors.New("rollup metadata policy missing from part policy tables")
	}
	key := rollupDefinitionKey{policy, group}
	suffix, ok := rollupDefinitionSuffixes.Load(key)
	if !ok {
		g, err := json.Marshal(string(group))
		if err != nil {
			return dst, err
		}
		s := append([]byte(`,"Group":`), g...)
		s = append(s, `,"Config":`...)
		s = append(s, resolved.JSON...)
		s = append(s, '}')
		suffix, _ = rollupDefinitionSuffixes.LoadOrStore(key, s)
	}
	dst = append(dst, `{"Name":`...)
	if jsonPlainString(string(name)) {
		dst = append(dst, '"')
		dst = append(dst, name...)
		dst = append(dst, '"')
	} else {
		n, err := json.Marshal(string(name))
		if err != nil {
			return dst, err
		}
		dst = append(dst, n...)
	}
	return append(dst, suffix.([]byte)...), nil
}

// jsonPlainString reports whether encoding/json writes s unescaped.
func jsonPlainString(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < 0x20 || c >= 0x7f || c == '"' || c == '\\' || c == '<' || c == '>' || c == '&' {
			return false
		}
	}
	return true
}

// One serial owner exclusively holds these arenas until its ingest resolves.
func (r *coreRollups) release() {
	for i := range r.lanes {
		lane := &r.lanes[i]
		lane.output.release()
		lane.ranked = lane.ranked[:0]
		// One chunk for this seal's records, so the next one plans in place.
		if len(lane.chunks) != 0 {
			lane.records = make([]sealrollup.Record, 0, int(lane.base)+cap(lane.records))
		}
		clear(lane.chunks)
		lane.chunks, lane.bases, lane.base = lane.chunks[:0], lane.bases[:0], 0
	}
	for _, coverage := range r.coverage {
		clear(coverage.seen)
		coverage.spec.Policies = nil
	}
	r.stages = r.stages[:0]
	clear(r.stageMetrics)
	r.stageMetrics = r.stageMetrics[:0]
	clear(r.batches)
	r.batches = r.batches[:0]
	clear(r.seen)
	r.seen = r.seen[:0]
	r.pendingNames.Reset()
	for group, file := range r.files {
		clear(file)
		r.files[group] = file[:0]
	}
	r.samples = r.samples[:0]
	r.arena = r.arena[:0]
	r.returnScratch()
	r.pending = false
}
