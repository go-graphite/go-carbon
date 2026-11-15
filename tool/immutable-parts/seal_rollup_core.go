package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"slices"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// openCoreRollupsWithHeadPolicy activates memory/intake-derived head scheduling
// in the offline engine. HeadMaxAge is execution-only; explicit barriers and
// shutdown still seal immediately, and publication keeps its separate cadence.
func openCoreRollupsWithHeadPolicy(root string, cfg coreConfig, policy sealrollup.HeadPolicy) (*coreEngine, []*atomicRollupOwner, error) {
	if policy.MaxAge <= 0 || policy.MaxPoints == 0 || policy.MaxPoints > math.MaxInt {
		return nil, nil, errors.New("invalid rollup head policy")
	}
	cfg.HeadMaxAge = policy.MaxAge
	cfg.MaxRows = int(policy.MaxPoints)
	return openCoreRollups(root, cfg)
}

// openCoreRollups is the explicit original-batch entry point.
// This is a numeric offline engine, not the portable daemonstore backend.
func openCoreRollups(root string, cfg coreConfig) (*coreEngine, []*atomicRollupOwner, error) {
	states := make([]*atomicRollupOwner, cfg.Shards)
	for i := range states {
		dir := filepath.Join(root, fmt.Sprintf("owner-%03d", i))
		if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
			states[i] = &atomicRollupOwner{state: map[uint64]*atomicRollupState{}}
			continue
		} else if err != nil {
			return nil, nil, err
		}
		// Restore only before core owner goroutines exist. The hot seal path never
		// opens a disk iterator or reads a prior value before writing.
		db, err := pebble.Open(dir, &pebble.Options{})
		if err != nil {
			return nil, nil, err
		}
		states[i], err = openAtomicRollupOwner(db, dir)
		closeErr := db.Close()
		if err != nil {
			return nil, nil, err
		}
		if closeErr != nil {
			return nil, nil, closeErr
		}
	}
	e, err := openCoreWithHooks(root, cfg, coreHooks{PlanSeal: func(owner int, in coreSealInput) (coreSealPlan, error) {
		if in.Compact != nil {
			return planCompactCoreRollups(states[owner], in)
		}
		return planCoreRollups(states[owner], in)
	}})
	return e, states, err
}

func planCoreRollups(o *atomicRollupOwner, in coreSealInput) (plan coreSealPlan, err error) {
	defer func() {
		if err != nil {
			o.Errors.Add(1)
			log.Printf("ERROR immutable core rollup failed: %v", err)
		}
	}()
	if len(in.Rows) > 0 && len(in.Batches) == 0 {
		return plan, errors.New("rollup seal lacks original batches")
	}
	if o.policies == nil {
		o.policies = map[string]*sealrollup.Prepared{}
	}
	configs := map[string]sealrollup.Config{}
	inputs := map[uint64]*atomicRollupInput{}
	var ids []uint64
	count := 0
	for _, b := range in.Batches {
		key := string(b.Policy)
		c, exists := configs[key]
		if !exists {
			if err = json.Unmarshal(b.Policy, &c); err != nil {
				return plan, err
			}
			configs[key] = c
			if o.policies[key] == nil {
				o.policies[key], err = sealrollup.Prepare(c)
				if err != nil {
					return plan, err
				}
			}
		}
		input := inputs[b.ID]
		if input == nil {
			input = &atomicRollupInput{ID: b.ID, Config: c}
			inputs[b.ID] = input
			ids = append(ids, b.ID)
		} else {
			a, e := sealrollup.PolicyDigest(input.Config)
			if e != nil {
				return plan, e
			}
			z, e := sealrollup.PolicyDigest(c)
			if e != nil {
				return plan, e
			}
			if a != z {
				return plan, errors.New("policy changed inside seal")
			}
		}
		batch := sealrollup.Batch{Order: b.Order, Now: b.Clock, Points: make([]sealrollup.Sample, len(b.Points))}
		for j, r := range b.Points {
			if r.ID != b.ID {
				return plan, errors.New("mixed metric batch")
			}
			batch.Points[j] = sealrollup.Sample{Time: r.Time, Bits: r.Bits, Sequence: r.Seq}
		}
		input.Batches = append(input.Batches, batch)
		count += len(b.Points)
	}
	if count != len(in.Rows) {
		return plan, errors.New("rollup/raw row count mismatch")
	}
	// Numeric metric IDs precede kind/archive/slot in the private key. Plan's
	// records are already ordered by archive/slot, and batch orders increase.
	// Sorting only IDs permits a strictly ordered append without sorting every
	// point key again. Core still validates the complete resulting key order.
	slices.Sort(ids)
	data := make([]coreKV, 0, count+len(ids))
	meta := make([]coreKV, 0, 2*len(ids)+1)
	plans := make(map[uint64]sealrollup.SealPlan, len(ids))
	preparedByID := make(map[uint64]*sealrollup.Prepared, len(ids))
	for _, b := range in.Batches {
		preparedByID[b.ID] = o.policies[string(b.Policy)]
	}
	for _, id := range ids {
		input := inputs[id]
		old := o.state[id]
		var prior sealrollup.Frontier
		if old != nil {
			prior = old.Frontier
		}
		p, e := preparedByID[id].PlanWithWorkspace(prior, rollupView(old), old.rangeView(), input.Batches, &o.workspace)
		if e != nil {
			return plan, e
		}
		plans[id] = p
		bytes := 57*len(p.Records) + 47*len(input.Batches)
		for _, b := range input.Batches {
			bytes += 24 * len(b.Points)
		}
		arena := make([]byte, 0, bytes)
		for _, r := range p.Records {
			at := len(arena)
			arena = appendRollupDataKey(arena, in.Next.NextPart, id, 'a', uint32(r.Archive), uint64(r.Slot))
			mid := len(arena)
			arena = sealrollup.AppendRecord(arena, r)
			data = append(data, coreKV{Key: arena[at:mid:mid], Value: arena[mid:len(arena):len(arena)]})
		}
		for _, b := range input.Batches {
			at := len(arena)
			arena = appendRollupDataKey(arena, in.Next.NextPart, id, 'r', 0, b.Order)
			mid := len(arena)
			arena = appendRollupBatch(arena, b)
			data = append(data, coreKV{Key: arena[at:mid:mid], Value: arena[mid:len(arena):len(arena)]})
		}
		v, e := json.Marshal(p.Next)
		if e != nil {
			return plan, e
		}
		k := rollupMetaKey('s', id)
		k[0] = 'T'
		meta = append(meta, coreKV{Key: k, Value: v})
		if old == nil {
			v, e := json.Marshal(input.Config)
			if e != nil {
				return plan, e
			}
			k = rollupMetaKey('p', id)
			k[0] = 'T'
			meta = append(meta, coreKV{Key: k, Value: v})
		}
	}
	meta = append(meta, coreKV{Key: []byte{'T', 'r'}, Value: binary.BigEndian.AppendUint64(nil, in.Next.NextPart)})
	slices.SortFunc(meta, func(a, b coreKV) int { return bytes.Compare(a.Key, b.Key) })
	for _, file := range [][]coreKV{data, meta} {
		if len(file) > 0 {
			plan.Files = append(plan.Files, file)
		}
	}
	plan.Commit = func() {
		for _, id := range ids {
			input := inputs[id]
			p := plans[id]
			s := o.state[id]
			if s == nil {
				s = &atomicRollupState{Config: input.Config, Slots: map[[2]int64]sealrollup.Sample{}}
				s.Config.Archives = slices.Clone(input.Config.Archives)
				s.index = sealrollup.NewSlots(s.Config.Archives)
				o.state[id] = s
			}
			for _, r := range p.Records {
				s.install(r)
			}
			s.Frontier = p.Next
		}
		o.part = in.Next.NextPart
	}
	return plan, nil
}
