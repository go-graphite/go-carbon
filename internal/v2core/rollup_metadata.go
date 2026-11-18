package v2core

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"

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
		dst = binary.BigEndian.AppendUint64(dst, uint64(m.ClosedThrough))
	}
	dst = append(dst, m.Frontier.Policy[:]...)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(m.Policy)))
	return append(dst, m.Policy...)
}

// appendMetadata emits exactly the public metadata encoding, with the policy
// appended directly from its immutable identity rather than a temporary copy.
func (h *PolicyIdentity) appendMetadata(dst []byte, m RollupMetadata) []byte {
	return appendPolicyMetadata(dst, m, h.policy.Name, h.plain, h.policy.policyDefinition)
}

// appendPolicyMetadata is appendMetadata from a seal stage's copied identity
// fields, so the encoder never dereferences the metric's PolicyIdentity.
func appendPolicyMetadata(dst []byte, m RollupMetadata, name partsformat.ID, plain bool, def *policyDefinition) []byte {
	m.Policy = nil
	dst = AppendRollupMetadata(dst, m)
	start := len(dst)
	dst = appendPolicyDefinition(dst, name, plain, def)
	binary.BigEndian.PutUint32(dst[start-4:start], uint32(len(dst)-start))
	return dst
}

// DecodeRollupMetadata accepts binary rows and legacy JSON metadata. Returned
// policy bytes are detached from the iterator buffer and safe after Next/Close.
func DecodeRollupMetadata(data []byte) (RollupMetadata, error) {
	m, err := decodeRollupMetadataView(data)
	if err == nil && len(data) >= 4 && bytes.Equal(data[:3], rollupMetadataMagic[:3]) && (data[3] >= 1 && data[3] <= 3) {
		m.Policy = bytes.Clone(m.Policy)
	}
	return m, err
}

// The policy view is valid only until the iterator moves. Intern it before
// retaining metadata; the catalog owns the immutable definition returned.
func decodeRollupMetadataView(data []byte) (RollupMetadata, error) {
	var m RollupMetadata
	if len(data) < 4 || !bytes.Equal(data[:3], rollupMetadataMagic[:3]) || (data[3] < 1 || data[3] > 3) {
		err := json.Unmarshal(data, &m)
		return m, err
	}
	header := 90
	if data[3] == 2 {
		header = 98
	}
	if data[3] == 3 {
		header = 106
	}
	if len(data) < header || data[5] > 1 || uint64(binary.BigEndian.Uint32(data[header-4:header])) != uint64(len(data)-header) {
		return m, errors.New("invalid binary rollup metadata")
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
	if data[3] == 3 {
		m.ClosedThrough = int64(binary.BigEndian.Uint64(data[62:70]))
		offset = 70
	}
	copy(m.Frontier.Policy[:], data[offset:offset+32])
	m.Policy = data[header:]
	return m, nil
}

// One serial owner exclusively holds these arenas until its ingest resolves.
func (r *coreRollups) release() {
	for i := range r.lanes {
		r.lanes[i].output.release()
		clear(r.lanes[i].ranked)
		r.lanes[i].ranked = r.lanes[i].ranked[:0]
	}
	for _, coverage := range r.coverage {
		clear(coverage.seen)
		coverage.spec.Policies = nil
	}
	clear(r.stages)
	r.stages = r.stages[:0]
	clear(r.batches)
	r.batches = r.batches[:0]
	clear(r.positions)
	clear(r.pendingNames)
	for group, file := range r.files {
		clear(file)
		r.files[group] = file[:0]
	}
	r.samples = r.samples[:0]
	r.arena = r.arena[:0]
	r.pending = false
}
