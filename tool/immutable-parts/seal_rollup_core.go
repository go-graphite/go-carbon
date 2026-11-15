package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

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
	e, err := openCoreWithHooks(root, cfg, coreHooks{PlanSeal: func(owner int, in coreSealInput) (coreSealPlan, error) { return planCoreRollups(states[owner], in) }})
	return e, states, err
}

func planCoreRollups(o *atomicRollupOwner, in coreSealInput) (plan coreSealPlan, err error) {
	defer func() {
		if err != nil {
			o.Errors++
			log.Printf("ERROR immutable core rollup failed: %v", err)
		}
	}()
	if len(in.Rows) > 0 && len(in.Batches) == 0 {
		return plan, errors.New("rollup seal lacks original batches")
	}
	inputs := map[uint64]*atomicRollupInput{}
	var ids []uint64
	count := 0
	for _, b := range in.Batches {
		var c sealrollup.Config
		if err = json.Unmarshal(b.Policy, &c); err != nil {
			return plan, err
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
	var data, meta []coreKV
	plans := make(map[uint64]sealrollup.SealPlan, len(ids))
	for _, id := range ids {
		input := inputs[id]
		old := o.state[id]
		var prior sealrollup.Frontier
		if old != nil {
			prior = old.Frontier
		}
		p, e := sealrollup.Plan(input.Config, prior, rollupView(old), input.Batches)
		if e != nil {
			return plan, e
		}
		plans[id] = p
		for _, b := range input.Batches {
			data = append(data, coreKV{Key: rollupDataKey(in.Next.NextPart, id, 'r', 0, b.Order), Value: encodeRollupBatch(b)})
		}
		for _, r := range p.Records {
			data = append(data, coreKV{Key: rollupDataKey(in.Next.NextPart, id, 'a', uint32(r.Archive), uint64(r.Slot)), Value: sealrollup.EncodeRecord(r)})
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
	for _, file := range [][]coreKV{data, meta} {
		if len(file) > 0 {
			slices.SortFunc(file, func(a, b coreKV) int { return bytes.Compare(a.Key, b.Key) })
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
				o.state[id] = s
			}
			for _, r := range p.Records {
				s.Slots[[2]int64{int64(r.Archive), r.Slot}] = r.Sample
			}
			s.Frontier = p.Next
		}
		o.part = in.Next.NextPart
	}
	return plan, nil
}
