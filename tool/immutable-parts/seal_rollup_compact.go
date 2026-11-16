package main

// Compressed numeric experiment. Original apply receipts and physical archive
// replacements are immutable runs, rather than expanded per-point row copies.
import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"math/bits"
	"slices"
	"strconv"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/partsrun"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

type compactRollupSelection struct {
	head    *compactRollupHead
	batches []compactRollupBatch
}

// A workspace is leased until Commit. A second outstanding plan gets another
// workspace, so neither SST bytes nor unpublished history can be overwritten.
type compactPendingMetric struct {
	id       uint64
	config   sealrollup.Config
	frontier sealrollup.Frontier
	history  [][]byte
}
type compactSealWorkspace struct {
	outputBytes, outputData, outputMeta int
	configJSON                          [][]byte
	owner                               *atomicRollupOwner
	part                                uint64
	descriptors                         []compactRollupBatch
	pending                             []compactPendingMetric
	data, meta                          []coreKV
	files                               [2][]coreKV
	arena                               []byte
	scratch                             []sealrollup.Sample
	batches                             []sealrollup.Batch
	records                             []sealrollup.Record
	decoder                             compactRollupHead
	policyKeys                          []string
	configs                             []sealrollup.Config
	prepared                            []*sealrollup.Prepared
	commit                              func()
}

// Idle scratch is bounded per workspace. Large one-off heads must not retain
// their full materialized SST output indefinitely. Small steady heads retain
// their buffers and remain allocation-free; oversized heads regrow on reuse.
const compactIdleArenaBytes = 1 << 20
const compactIdleKVs = 4096

func compactOutputClass(n int) int {
	if n <= 1 {
		return n
	}
	return 1 << bits.Len(uint(n-1))
}

func (w *compactSealWorkspace) prepareOutput() {
	// Retain size hints, not large backing arrays. The next oversized seal
	// allocates once per output buffer instead of repeating geometric growth.
	if w.arena == nil && w.outputBytes > 0 {
		w.arena = make([]byte, 0, compactOutputClass(w.outputBytes))
	}
	if w.data == nil && w.outputData > 0 {
		w.data = make([]coreKV, 0, compactOutputClass(w.outputData))
	}
	if w.meta == nil && w.outputMeta > 0 {
		w.meta = make([]coreKV, 0, compactOutputClass(w.outputMeta))
	}
}

func (w *compactSealWorkspace) releaseOutput() {
	w.outputBytes, w.outputData, w.outputMeta = len(w.arena), len(w.data), len(w.meta)
	clear(w.data[:cap(w.data)])
	clear(w.meta[:cap(w.meta)])
	clear(w.batches[:cap(w.batches)])
	w.files = [2][]coreKV{}
	if cap(w.arena) > compactIdleArenaBytes {
		w.arena = nil
	}
	if cap(w.data) > compactIdleKVs {
		w.data = nil
	}
	if cap(w.meta) > compactIdleKVs {
		w.meta = nil
	}
}

func (w *compactSealWorkspace) install() {
	o := w.owner
	for i := range w.pending {
		m := &w.pending[i]
		s := o.state.get(m.id)
		if s == nil {
			s = &atomicRollupState{Config: m.config}
			o.state.set(m.id, s)
		}
		s.Frontier = m.frontier
		s.history, m.history = m.history, s.history
		s.Slots, s.index = nil, nil
	}
	o.part = w.part
	w.decoder.data, w.decoder.policies = nil, nil
	w.releaseOutput()
	// A serial owner needs one idle workspace. Concurrent outstanding plans
	// keep their own leases; returning many leases cannot grow the idle cache.
	if len(o.compactFree) == 0 {
		o.compactFree = append(o.compactFree, w)
	}
}
func appendCompactFrontier(dst []byte, f sealrollup.Frontier) []byte {
	dst = append(dst, `{"Order":`...)
	dst = strconv.AppendUint(dst, f.Order, 10)
	dst = append(dst, `,"MaxTime":`...)
	dst = strconv.AppendInt(dst, f.MaxTime, 10)
	dst = append(dst, `,"Policy":[`...)
	for i, b := range f.Policy {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = strconv.AppendUint(dst, uint64(b), 10)
	}
	return append(dst, ']', '}')
}

type compactRollupSubmission struct {
	selection compactRollupSelection
	packet    corePacket
	encoded   coreEncodedSeal
}

// submitCompactRollupGroup borrows the producer's complete frozen arena until
// every owner has ACKed. Independent owners get independent decoder scratch.
// Only batch descriptors cross the queues; points remain encoded throughout.
func (e *coreEngine) submitCompactRollupGroup(producer int, h *compactRollupHead, cursor uint64) error {
	if producer < 0 || producer >= e.cfg.Producers || cursor == 0 {
		return errors.New("invalid compact producer/cursor")
	}
	// Preserve disjoint groups in either direction. A multiple of the DB count
	// gives each DB several independent producer queues without all-to-all SST
	// publication. Non-divisible counts retain unrestricted routing.
	startOwner, stride := producer, e.cfg.Producers
	if e.cfg.Producers > len(e.owners) {
		startOwner, stride = 0, 1
		if e.cfg.Producers%len(e.owners) == 0 {
			startOwner, stride = producer%len(e.owners), len(e.owners)
		}
	}
	if h.submissionEngine != e || h.submissionProducer != producer || len(h.submission) != len(e.owners) {
		h.submissionEngine, h.submissionProducer = e, producer
		h.submission = make([]compactRollupSubmission, len(e.owners))
		for owner := startOwner; owner < len(e.owners); owner += stride {
			o := e.owners[owner]
			slot := &h.submission[owner]
			slot.packet.ack = make(chan error, 1)
			slot.packet.seal = true
			slot.packet.encoded = &slot.encoded
			slot.encoded.Plan = func(next coreFrontier) (coreSealPlan, error) {
				return o.plan(coreSealInput{Next: next, Compact: &slot.selection})
			}
		}
	}
	for owner := startOwner; owner < len(e.owners); owner += stride {
		slot := &h.submission[owner]
		slot.selection.head = h
		slot.selection.batches = slot.selection.batches[:0]
		slot.encoded.Points = 0
	}
	for _, m := range h.batches {
		owner := coreShard(m.id, len(e.owners))
		slot := &h.submission[owner]
		if (owner < startOwner || (owner-startOwner)%stride != 0) || m.count < 0 || m.count > e.cfg.MaxRows-slot.encoded.Points {
			return errors.New("compact metric outside producer group or head bound")
		}
		slot.selection.batches = append(slot.selection.batches, m)
		slot.encoded.Points += m.count
	}
	var first error
	submitted := startOwner
	for owner := startOwner; owner < len(e.owners); owner += stride {
		o := e.owners[owner]
		if o.plan == nil {
			first = errors.New("compact seal requires planner")
			break
		}
		slot := &h.submission[owner]
		slot.packet.cursor = cursor
		if err := o.queues[producer].push(&slot.packet, o.dead); err != nil {
			first = err
			break
		}
		submitted = owner + stride
	}
	// The complete producer arena remains borrowed until every submitted ACK.
	for owner := startOwner; owner < submitted; owner += stride {
		select {
		case err := <-h.submission[owner].packet.ack:
			first = errors.Join(first, err)
		case <-e.owners[owner].dead:
			first = errors.Join(first, errors.New("compact owner stopped"))
		}
	}

	return first
}

// Compact run value: version, compression flag, count, original clock, payload.
// Identity/order/archive live in the immutable key; policy is persisted once.
func compactRunValue(h *compactRollupHead, m compactRollupBatch) []byte {
	return appendCompactRunValue(make([]byte, 0, 18+m.end-m.start), h, m)
}

func appendCompactRunValue(out []byte, h *compactRollupHead, m compactRollupBatch) []byte {
	at := len(out)
	out = append(out, make([]byte, 18)...)
	out[at] = 1
	if m.compressed {
		out[at+1] = 1
	}
	binary.BigEndian.PutUint64(out[at+2:], uint64(m.count))
	binary.BigEndian.PutUint64(out[at+10:], uint64(m.clock))
	return append(out, h.data[m.start:m.end]...)
}

func decodeCompactRun(value []byte, id, order uint64, scratch []sealrollup.Sample) (sealrollup.Batch, error) {
	header, err := partsrun.Inspect(value, 1<<26)
	if err != nil {
		return sealrollup.Batch{}, err
	}
	if cap(scratch) < header.Count {
		scratch = make([]sealrollup.Sample, header.Count)
	}
	scratch = scratch[:header.Count]
	i := 0
	err = partsrun.Visit(value, 1<<26, func(p partsrun.Sample) error {
		scratch[i] = sealrollup.Sample{Time: p.Time, Bits: p.Bits, Sequence: p.Sequence}
		i++
		return nil
	})
	return sealrollup.Batch{Order: order, Now: header.Clock, Points: scratch}, err
}

func planCompactCoreRollups(o *atomicRollupOwner, in coreSealInput) (plan coreSealPlan, err error) {
	defer func() {
		if err != nil {
			o.Errors.Add(1)
		}
	}()
	g := in.Compact
	if g == nil || len(in.Rows) != 0 || len(in.Batches) != 0 {
		return plan, errors.New("invalid compact seal input")
	}
	if o.policies == nil {
		o.policies = map[string]*sealrollup.Prepared{}
	}
	if o.policyConfigs == nil {
		o.policyConfigs = make(map[string]sealrollup.Config)
	}
	var w *compactSealWorkspace
	if n := len(o.compactFree); n != 0 {
		w = o.compactFree[n-1]
		o.compactFree = o.compactFree[:n-1]
	} else {
		w = &compactSealWorkspace{owner: o}
		w.commit = w.install
	}
	defer func() {
		if err != nil {
			w.decoder.data, w.decoder.policies = nil, nil
			o.compactFree = append(o.compactFree, w)
		}
	}()
	w.part = in.Next.NextPart
	w.prepareOutput()
	w.descriptors = append(w.descriptors[:0], g.batches...)
	descriptors := w.descriptors
	slices.SortStableFunc(descriptors, func(a, b compactRollupBatch) int {
		if a.id < b.id {
			return -1
		}
		if a.id > b.id {
			return 1
		}
		return 0
	})
	pending := w.pending[:0]
	data, meta := w.data[:0], w.meta[:0]
	arena := w.arena[:0]
	scratch, batches := w.scratch, w.batches[:0]
	decoder := &w.decoder
	if g.head != nil {
		decoder.data, decoder.policies = g.head.data, g.head.policies
		for i, raw := range g.head.policies {
			if i == len(w.policyKeys) {
				w.policyKeys = append(w.policyKeys, "")
				w.configJSON = append(w.configJSON, nil)
				w.configs = append(w.configs, sealrollup.Config{})
				w.prepared = append(w.prepared, nil)
			}
			if w.policyKeys[i] != string(raw) {
				key := string(raw)
				cfg, ok := o.policyConfigs[key]
				if !ok {
					if err = json.Unmarshal(raw, &cfg); err != nil {
						return plan, err
					}
					o.policyConfigs[key] = cfg
				}
				prepared := o.policies[key]
				if prepared == nil {
					prepared, err = sealrollup.Prepare(cfg)
					if err != nil {
						return plan, err
					}
					o.policies[key] = prepared
				}
				w.policyKeys[i], w.configs[i], w.prepared[i] = key, cfg, prepared
				w.configJSON[i], err = json.Marshal(cfg)
				if err != nil {
					return plan, err
				}
			}
		}
	}
	// No expanded samples from an earlier metric survive into the next metric.
	for lo := 0; lo < len(descriptors); {
		hi := lo + 1
		for hi < len(descriptors) && descriptors[hi].id == descriptors[lo].id {
			hi++
		}
		id := descriptors[lo].id
		ms := descriptors[lo:hi]
		lo = hi
		if g.head == nil {
			return plan, errors.New("missing compact arena")
		}
		var cfg sealrollup.Config
		var prepared *sealrollup.Prepared
		var policy string
		total := 0
		for _, m := range ms {
			if int(m.policy) >= len(g.head.policies) || m.count < 0 {
				return plan, errors.New("invalid compact descriptor")
			}
			key := w.policyKeys[m.policy]
			if prepared == nil {
				cfg, prepared, policy = w.configs[m.policy], w.prepared[m.policy], key
			} else if key != policy {
				a, e := sealrollup.PolicyDigest(cfg)
				if e != nil {
					return plan, e
				}
				z, e := sealrollup.PolicyDigest(w.configs[m.policy])
				if e != nil {
					return plan, e
				}
				if a != z {
					return plan, errors.New("policy changed inside compact seal")
				}
			}
			total += m.count
		}
		if cap(scratch) < total {
			scratch = make([]sealrollup.Sample, total)
		}
		rows := scratch[:total]
		batches = batches[:0]
		for _, m := range ms {
			batch, e := decoder.decodeBatch(m, rows[:m.count])
			if e != nil {
				return plan, e
			}
			rows = rows[m.count:]
			for _, p := range batch.Points {
				if corePartition(p.Time, in.Next.Config.PartitionSeconds) < in.Next.ExpiredBefore {
					return plan, errors.New("late compact point targets expired partition")
				}
			}
			batches = append(batches, batch)
		}
		old := o.state.get(id)
		var prior sealrollup.Frontier
		if old != nil {
			prior = old.Frontier
		}
		if e := o.historyScratch.load(old, cfg); e != nil {
			return plan, e
		}
		p, e := prepared.PlanInto(w.records, prior, o.historyScratch.view, o.historyScratch.ranges, batches, &o.workspace)
		if e != nil {
			return plan, e
		}
		i := len(pending)
		if i == cap(pending) {
			pending = append(pending, compactPendingMetric{})
		} else {
			pending = pending[:i+1]
		}
		w.records = p.Records
		history, e := o.historyScratch.sealInto(p.Records, pending[i].history)
		if e != nil {
			return plan, e
		}
		pending[i] = compactPendingMetric{id, cfg, p.Next, history}
		// Records are ordered by archive/slot. Encode their timestamps, exact
		// IEEE bits and individual source orders directly, without row slices.
		for lo := 0; lo < len(p.Records); {
			hi := lo + 1
			for hi < len(p.Records) && p.Records[hi].Archive == p.Records[lo].Archive {
				hi++
			}
			records := p.Records[lo:hi]
			at := len(arena)
			arena = appendRollupDataKey(arena, in.Next.NextPart, id, 'b', uint32(records[0].Archive), 0)
			mid := len(arena)
			arena, e = partsrun.AppendWithWorkspace(arena, 0, len(records), func(i int) partsrun.Sample {
				r := records[i]
				return partsrun.Sample{Time: r.Sample.Time, Bits: r.Sample.Bits, Sequence: r.Order}
			}, &o.historyScratch.codec)
			if e != nil {
				return plan, e
			}
			data = append(data, coreKV{Key: arena[at:mid:mid], Value: arena[mid:len(arena):len(arena)]})
			lo = hi
		}
		for _, m := range ms {
			at := len(arena)
			arena = appendRollupDataKey(arena, in.Next.NextPart, id, 'c', 0, m.order)
			mid := len(arena)
			arena = appendCompactRunValue(arena, g.head, m)
			data = append(data, coreKV{Key: arena[at:mid:mid], Value: arena[mid:len(arena):len(arena)]})
		}
		at := len(arena)
		arena = append(arena, 'T', 's')
		arena = binary.BigEndian.AppendUint64(arena, id)
		mid := len(arena)
		arena = appendCompactFrontier(arena, p.Next)
		meta = append(meta, coreKV{arena[at:mid:mid], arena[mid:len(arena):len(arena)]})
		if old == nil {
			at := len(arena)
			arena = append(arena, 'T', 'p')
			arena = binary.BigEndian.AppendUint64(arena, id)
			// Policy values are immutable and identical for every metric of a schema.
			// Keep one canonical value per owner/policy instead of copying it per ID.
			meta = append(meta, coreKV{arena[at:len(arena):len(arena)], w.configJSON[ms[0].policy]})
		}
	}
	at := len(arena)
	arena = append(arena, 'T', 'r')
	mid := len(arena)
	arena = binary.BigEndian.AppendUint64(arena, in.Next.NextPart)
	meta = append(meta, coreKV{arena[at:mid:mid], arena[mid:len(arena):len(arena)]})
	slices.SortFunc(meta, func(a, b coreKV) int { return slices.Compare(a.Key, b.Key) })
	w.pending, w.data, w.meta, w.arena, w.scratch, w.batches = pending, data, meta, arena, scratch, batches
	w.files[0], w.files[1] = data, meta
	if len(data) > 0 {
		plan.Files = w.files[:]
	} else {
		plan.Files = w.files[1:]
	}
	plan.Commit = w.commit
	return plan, nil
}

// Numeric experiment read adapter. Both old raw blocks and newer compact
// receipts can coexist across reopen; raw sample identity/order is unchanged.
func appendCompactCoreRows(s *pebble.Snapshot, id uint64, out []row) ([]row, error) {
	it, err := s.NewIter(&pebble.IterOptions{LowerBound: []byte{'S'}, UpperBound: []byte{'T'}})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var scratch []sealrollup.Sample
	for it.First(); it.Valid(); it.Next() {
		k := it.Key()
		if len(k) != 31 || k[1] != 1 || k[18] != 'c' || binary.BigEndian.Uint64(k[10:]) != id {
			continue
		}
		b, e := decodeCompactRun(it.Value(), id, binary.BigEndian.Uint64(k[23:]), scratch)
		if e != nil {
			return nil, e
		}
		scratch = b.Points
		for _, p := range b.Points {
			out = append(out, row{ID: id, Time: p.Time, Bits: p.Bits, Seq: p.Sequence})
		}
	}
	return out, it.Error()
}
