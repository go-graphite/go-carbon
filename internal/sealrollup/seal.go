package sealrollup

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
)

// Frontier belongs to one metric in one ordered input stream. Persist it in the
// same transaction as the original raw input and the archive replacements.
// Policy prevents a restart from silently applying a different stored schema.
type Frontier struct {
	Order   uint64
	MaxTime int64
	Policy  [32]byte
}

// Record is an authoritative physical-slot replacement. A correction has the
// same read semantics as an initial record: greatest Order wins before time
// filtering. The flag is diagnostic; it must never select a partial-sum path.
type Record struct {
	Delta
	Correction bool
	Kind       Kind
	// Upto is a Mat or None record's newest event order (a Tick) that its
	// window accounts for; 0 for Raw and Ghost records and for the seal's
	// fallback windows. Merges write it when they write windows down.
	Upto uint64
}

// Kind says what a stored record version is. Raw is a point at the archive
// its batch's admission clock routed it to, aligned to that archive's step.
// Mat is a window value written down: merges write windows down (WriteDown) before
// dropping superseded versions. Ghost is a point whose physical slot a later point of
// the same batch (another epoch) took: an event, never a value. None is a
// written-down absence: it bounds the event walk and never holds a value.
// Version readers resolve windows from raw events and merge write-downs.
type Kind uint8

const (
	KindRaw Kind = iota
	KindMat
	KindGhost
	KindNone
)

// SealPlan is detached from the immutable input view. Install Records and Next
// only after the transaction containing raw input, records and frontier commits.
//
// The last Versions entries of Records are superseded versions of this seal
// (RouteInto only): a Raw point of an earlier batch whose slot a later
// batch took, or a Ghost. They are sorted by (archive,
// slot), oldest first, and every one shares its (archive, slot) with exactly
// one of the leading len(Records)-Versions records, the slot's winner. Part
// writers store each slot's versions before its winner; state and per-slot
// readers use the winners only.
type SealPlan struct {
	Records  []Record
	Versions int
	Next     Frontier
}

// Winners returns the records that resolve each touched slot.
func (p SealPlan) Winners() []Record { return p.Records[:len(p.Records)-p.Versions] }

// PolicyDigest includes full archive counts and the exact float32 XFF bits.
func PolicyDigest(c Config) ([32]byte, error) {
	if err := c.Validate(); err != nil {
		return [32]byte{}, err
	}
	b := []byte("sealed-archive-policy-v1")
	b = binary.BigEndian.AppendUint32(b, uint32(c.Method))
	b = binary.BigEndian.AppendUint32(b, math.Float32bits(c.XFilesFactor))
	for _, a := range c.Archives {
		b = binary.BigEndian.AppendUint64(b, uint64(a.Step))
		b = binary.BigEndian.AppendUint64(b, uint64(a.Count))
	}
	return sha256.Sum256(b), nil
}

// Plan runs once per seal, preserving every original UpdateMany boundary and
// admission clock. It performs no database reads or publication. Replayed
// batches at or below prior.Order must be removed by the input-log owner before
// calling; accepting them here could apply an old correction twice.
func Plan(c Config, prior Frontier, view View, batches []Batch) (SealPlan, error) {
	policy, err := PolicyDigest(c)
	if err != nil {
		return SealPlan{}, err
	}
	p := Prepared{config: c, policy: policy}
	return p.Plan(prior, view, nil, batches)
}

// Prepared owns a validated, immutable policy. Reuse it across metrics/seals
// with that policy; configuration changes require a newly prepared value.
type Prepared struct {
	config Config
	policy [32]byte
}

func Prepare(c Config) (*Prepared, error) {
	policy, err := PolicyDigest(c)
	if err != nil {
		return nil, err
	}
	c.Archives = append([]Archive(nil), c.Archives...)
	return &Prepared{config: c, policy: policy}, nil
}
func (p *Prepared) Plan(prior Frontier, view View, ranges RangeView, batches []Batch) (SealPlan, error) {
	return p.PlanWithWorkspace(prior, view, ranges, batches, nil)
}

// PlanWithWorkspace is Plan with reusable owner-local scratch. Records remain
// detached and may be retained until the atomic ingest completes.
func (p *Prepared) PlanWithWorkspace(prior Frontier, view View, ranges RangeView, batches []Batch, w *Workspace) (SealPlan, error) {
	c, policy := p.config, p.policy
	if prior.Policy != ([32]byte{}) && prior.Policy != policy {
		return SealPlan{}, fmt.Errorf("stored rollup policy changed")
	}
	if prior.Order != 0 && prior.Policy == ([32]byte{}) {
		return SealPlan{}, fmt.Errorf("rollup frontier lacks policy")
	}
	next := prior
	next.Policy = policy
	for _, b := range batches {
		if b.Order <= next.Order {
			return SealPlan{}, fmt.Errorf("rollup batch %d is not after frontier %d", b.Order, next.Order)
		}
		next.Order = b.Order
		for _, p := range b.Points {
			next.MaxTime = max(next.MaxTime, p.Time)
		}
	}
	deltas, err := reduce(c, view, ranges, batches, w)
	if err != nil {
		return SealPlan{}, err
	}
	plan := SealPlan{Next: next, Records: make([]Record, 0, len(deltas))}
	for _, d := range deltas {
		d.Correction = d.Sample.Time <= prior.MaxTime
		if view != nil {
			p, ok := view(d.Archive, d.Slot)
			d.Correction = d.Correction || (ok && p.Time == d.Sample.Time)
		}
		plan.Records = append(plan.Records, d)
	}
	return plan, nil
}

// EncodeRecord preserves occupied NaNs, signed zero, source order and the actual
// epoch of a ring slot. Key identity supplies metric/archive/physical slot.
// Singleton values stay in their published form, which has no record kind:
// kinds and versions live in dense runs only (v2part).
func EncodeRecord(r Record) []byte {
	return AppendRecord(nil, r)
}

// AppendRecord allows a seal writer to own one contiguous key/value arena.
// Bytes are identical to EncodeRecord; the returned slice may alias dst.
func AppendRecord(dst []byte, r Record) []byte {
	at := len(dst)
	dst = append(dst, make([]byte, 26)...)
	b := dst[at:]
	b[0] = 1
	if r.Correction {
		b[1] = 1
	}
	binary.BigEndian.PutUint64(b[2:], uint64(r.Sample.Time))
	binary.BigEndian.PutUint64(b[10:], r.Sample.Bits)
	binary.BigEndian.PutUint64(b[18:], r.Order)
	return dst
}

func DecodeRecord(archive int, physicalSlot int64, b []byte) (Record, error) {
	if len(b) != 26 || b[0] != 1 || b[1] > 1 || archive < 0 || physicalSlot < 0 {
		return Record{}, fmt.Errorf("invalid sealed archive record")
	}
	order := binary.BigEndian.Uint64(b[18:])
	t := int64(binary.BigEndian.Uint64(b[2:]))
	if order == 0 || t <= 0 || t > math.MaxUint32 {
		return Record{}, fmt.Errorf("invalid sealed archive record clock/order")
	}
	return Record{Delta: Delta{Archive: archive, Slot: physicalSlot, Order: order,
		Sample: Sample{Time: t, Bits: binary.BigEndian.Uint64(b[10:]), Sequence: order}}, Correction: b[1] == 1}, nil
}

// Digest returns the immutable schema identity validated by Prepare.
func (p *Prepared) Digest() [32]byte { return p.policy }

// Config borrows this validated immutable schema; callers must not mutate Archives.
func (p *Prepared) Config() Config { return p.config }
