package main

// Compressed numeric experiment. Original apply receipts and physical archive
// replacements are immutable runs, rather than expanded per-point row copies.
import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/partsrun"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

type compactRollupSelection struct {
	head    *compactRollupHead
	batches []compactRollupBatch
}

// submitCompactRollupGroup borrows the producer's complete frozen arena until
// every owner has ACKed. Independent owners get independent decoder scratch.
// Only batch descriptors cross the queues; points remain encoded throughout.
func (e *coreEngine) submitCompactRollupGroup(producer int, h *compactRollupHead, cursor uint64) error {
	if producer < 0 || producer >= e.cfg.Producers || cursor == 0 {
		return errors.New("invalid compact producer/cursor")
	}
	groups := make([]compactRollupSelection, len(e.owners))
	counts := make([]int, len(groups))
	for _, m := range h.batches {
		owner := coreShard(m.id, len(groups))
		if owner%e.cfg.Producers != producer || m.count < 0 || m.count > e.cfg.MaxRows-counts[owner] {
			return errors.New("compact metric outside producer group or head bound")
		}
		groups[owner].head = h
		groups[owner].batches = append(groups[owner].batches, m)
		counts[owner] += m.count
	}
	type wait struct {
		owner *coreOwner
		ack   chan error
	}
	var waits []wait
	var first error
	for owner := producer; owner < len(groups); owner += e.cfg.Producers {
		o := e.owners[owner]
		if o.plan == nil {
			first = errors.New("compact seal requires planner")
			break
		}
		g := groups[owner]
		ack := make(chan error, 1)
		encoded := &coreEncodedSeal{Points: counts[owner], Plan: func(next coreFrontier) (coreSealPlan, error) {
			// The per-owner planner wrapper recognizes the selection without any
			// shared lock or global producer registry.
			return o.plan(coreSealInput{Next: next, Compact: &g})
		}}
		if err := o.queues[producer].push(&corePacket{encoded: encoded, cursor: cursor, seal: true, ack: ack}, o.dead); err != nil {
			first = err
			break
		}
		waits = append(waits, wait{o, ack})
	}
	// Even on failure, wait for every submitted borrower before returning its arena.
	for _, w := range waits {
		select {
		case err := <-w.ack:
			first = errors.Join(first, err)
		case <-w.owner.dead:
			first = errors.Join(first, errors.New("compact owner stopped"))
		}
	}
	return first
}

// Compact run value: version, compression flag, count, original clock, payload.
// Identity/order/archive live in the immutable key; policy is persisted once.
func compactRunValue(h *compactRollupHead, m compactRollupBatch) []byte {
	out := make([]byte, 18, 18+m.end-m.start)
	out[0] = 1
	if m.compressed {
		out[1] = 1
	}
	binary.BigEndian.PutUint64(out[2:], uint64(m.count))
	binary.BigEndian.PutUint64(out[10:], uint64(m.clock))
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
	byID := make(map[uint64][]compactRollupBatch)
	for _, m := range g.batches {
		byID[m.id] = append(byID[m.id], m)
	}
	ids := make([]uint64, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	var data, meta []coreKV
	type plannedConfig struct {
		config  sealrollup.Config
		records int
	}
	configs := make(map[uint64]plannedConfig, len(ids))
	frontiers := make(map[uint64]sealrollup.Frontier, len(ids))
	var scratch []sealrollup.Sample
	var decoder compactRollupHead
	if g.head != nil {
		decoder.data, decoder.policies = g.head.data, g.head.policies
	}
	// No expanded samples from an earlier metric survive into the next metric.
	for _, id := range ids {
		ms := byID[id]
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
			key := string(g.head.policies[m.policy])
			if prepared == nil {
				if err = json.Unmarshal([]byte(key), &cfg); err != nil {
					return plan, err
				}
				prepared = o.policies[key]
				if prepared == nil {
					prepared, err = sealrollup.Prepare(cfg)
					if err != nil {
						return plan, err
					}
					o.policies[key] = prepared
				}
				policy = key
			} else if key != policy {
				var other sealrollup.Config
				if err = json.Unmarshal([]byte(key), &other); err != nil {
					return plan, err
				}
				a, e := sealrollup.PolicyDigest(cfg)
				if e != nil {
					return plan, e
				}
				z, e := sealrollup.PolicyDigest(other)
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
		batches := make([]sealrollup.Batch, 0, len(ms))
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
		old := o.state[id]
		var prior sealrollup.Frontier
		if old != nil {
			prior = old.Frontier
		}
		p, e := prepared.PlanWithWorkspace(prior, rollupView(old), old.rangeView(), batches, &o.workspace)
		if e != nil {
			return plan, e
		}
		configs[id], frontiers[id] = plannedConfig{cfg, len(p.Records)}, p.Next
		// Records are ordered by archive/slot. Encode their timestamps, exact
		// IEEE bits and individual source orders directly, without row slices.
		for lo := 0; lo < len(p.Records); {
			hi := lo + 1
			for hi < len(p.Records) && p.Records[hi].Archive == p.Records[lo].Archive {
				hi++
			}
			records := p.Records[lo:hi]
			value, e := partsrun.Append(nil, 0, len(records), func(i int) partsrun.Sample {
				r := records[i]
				return partsrun.Sample{Time: r.Sample.Time, Bits: r.Sample.Bits, Sequence: r.Order}
			})
			if e != nil {
				return plan, e
			}
			data = append(data, coreKV{Key: rollupDataKey(in.Next.NextPart, id, 'b', uint32(records[0].Archive), 0), Value: value})
			lo = hi
		}
		for _, m := range ms {
			data = append(data, coreKV{Key: rollupDataKey(in.Next.NextPart, id, 'c', 0, m.order), Value: compactRunValue(g.head, m)})
		}
		v, e := json.Marshal(p.Next)
		if e != nil {
			return plan, e
		}
		k := rollupMetaKey('s', id)
		k[0] = 'T'
		meta = append(meta, coreKV{k, v})
		if old == nil {
			v, e = json.Marshal(cfg)
			if e != nil {
				return plan, e
			}
			k = rollupMetaKey('p', id)
			k[0] = 'T'
			meta = append(meta, coreKV{k, v})
		}
	}
	meta = append(meta, coreKV{[]byte{'T', 'r'}, binary.BigEndian.AppendUint64(nil, in.Next.NextPart)})
	slices.SortFunc(meta, func(a, b coreKV) int { return slices.Compare(a.Key, b.Key) })
	for _, file := range [][]coreKV{data, meta} {
		if len(file) > 0 {
			plan.Files = append(plan.Files, file)
		}
	}
	plan.Commit = func() {
		for _, id := range ids {
			s := o.state[id]
			if s == nil {
				cfg := configs[id]
				s = &atomicRollupState{Config: cfg.config, Slots: make(map[[2]int64]sealrollup.Sample, cfg.records)}
				s.index = sealrollup.NewSlots(s.Config.Archives)
				o.state[id] = s
			}
			s.Frontier = frontiers[id]
		}
		for _, kv := range data {
			if kv.Key[18] == 'b' {
				id := binary.BigEndian.Uint64(kv.Key[10:])
				archive := int(binary.BigEndian.Uint32(kv.Key[19:]))
				if e := installCompactArchive(o.state[id], archive, kv.Value); e != nil {
					panic(fmt.Sprintf("validated compact archive failed at commit: %v", e))
				}
			}
		}
		o.part = in.Next.NextPart
	}
	return plan, nil
}

func installCompactArchive(s *atomicRollupState, archive int, value []byte) error {
	if archive < 0 || archive >= len(s.Config.Archives) {
		return errors.New("invalid compact archive")
	}
	b, err := decodeCompactRun(value, 0, 0, nil)
	if err != nil {
		return err
	}
	a := s.Config.Archives[archive]
	for _, p := range b.Points {
		s.install(sealrollup.Record{Delta: sealrollup.Delta{Archive: archive, Slot: (p.Time / a.Step) % a.Count, Order: p.Sequence, Sample: p}})
	}
	return nil
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
