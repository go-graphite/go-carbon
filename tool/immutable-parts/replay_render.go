//go:build sealrollup_integration

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/partlog"
	pr "github.com/go-graphite/go-carbon/internal/partsread"
	sr "github.com/go-graphite/go-carbon/internal/sealrollup"
)

// replayRenderPolicy is journaled before replay. The numeric diagnostic format
// cannot recover names, policy epochs, clocks or original apply boundaries from
// raw rows alone. This adapter refuses that lossy substitution.
type replayRenderPolicy struct {
	Name   string
	Config sr.Config
}

// replayRenderRecords derives archive replacements from persisted raw rows,
// using only the journal for identity, policy and original apply boundaries.
// It is an offline oracle adapter, not a production seal/publication path.
func replayRenderRecords(root string, owners int) ([]pr.Record, pr.Catalog, error) {
	log, err := partlog.Open(filepath.Join(root, "log"), partlog.Options{Streams: 1})
	if err != nil {
		return nil, nil, err
	}
	defer log.Close()
	var batches []coreBatch
	if err = log.Replay(0, 0, func(r partlog.Record) error {
		bs, e := decodeLogBatches(r.Payload)
		if e != nil {
			return fmt.Errorf("missing render dictionary/apply envelope: %w", e)
		}
		batches = append(batches, bs...)
		return nil
	}); err != nil {
		return nil, nil, err
	}
	if len(batches) == 0 {
		return nil, nil, errors.New("empty render journal")
	}
	dbs := make([]*pebble.DB, owners)
	defer func() {
		for _, db := range dbs {
			if db != nil {
				_ = db.Close()
			}
		}
	}()
	policies := map[uint64]replayRenderPolicy{}
	actual := map[uint64]map[uint64]row{}
	remaining := 0
	for _, b := range batches {
		var policy replayRenderPolicy
		if err = json.Unmarshal(b.Policy, &policy); err != nil {
			return nil, nil, err
		}
		if policy.Name == "" {
			return nil, nil, errors.New("missing canonical name")
		}
		if err = policy.Config.Validate(); err != nil {
			return nil, nil, err
		}
		if old, ok := policies[b.ID]; ok {
			if !reflect.DeepEqual(old, policy) {
				return nil, nil, errors.New("conflicting metric policy")
			}
			continue
		}
		policies[b.ID] = policy
		owner := coreShard(b.ID, owners)
		if dbs[owner] == nil {
			dbs[owner], err = pebble.Open(filepath.Join(root, "parts", fmt.Sprintf("owner-%03d", owner)), &pebble.Options{ReadOnly: true})
			if err != nil {
				return nil, nil, err
			}
		}
		snap := dbs[owner].NewSnapshot()
		rows, e := coreRead(snap, b.ID)
		_ = snap.Close()
		if e != nil {
			return nil, nil, e
		}
		bySeq := map[uint64]row{}
		for _, r := range rows {
			if _, ok := bySeq[r.Seq]; ok {
				return nil, nil, errors.New("duplicate persisted sequence")
			}
			bySeq[r.Seq] = r
			remaining++
		}
		actual[b.ID] = bySeq
	}
	state := map[uint64]map[[2]int64]sr.Sample{}
	last := map[uint64]uint64{}
	var records []pr.Record
	for _, b := range batches {
		if b.Order <= last[b.ID] {
			return nil, nil, errors.New("nonincreasing original apply order")
		}
		last[b.ID] = b.Order
		policy := policies[b.ID]
		if state[b.ID] == nil {
			state[b.ID] = map[[2]int64]sr.Sample{}
		}
		var input []sr.Sample
		for _, want := range b.Points {
			got, ok := actual[b.ID][want.Seq]
			if !ok || got != want {
				return nil, nil, fmt.Errorf("persisted row differs ID%d sequence%d", b.ID, want.Seq)
			}
			delete(actual[b.ID], want.Seq)
			remaining--
			input = append(input, sr.Sample{Time: got.Time, Bits: got.Bits, Sequence: got.Seq})
		}
		slots := state[b.ID]
		deltas, e := sr.Reduce(policy.Config, func(a int, s int64) (sr.Sample, bool) { p, ok := slots[[2]int64{int64(a), s}]; return p, ok }, []sr.Batch{{Order: b.Order, Now: b.Clock, Points: input}})
		if e != nil {
			return nil, nil, e
		}
		for _, d := range deltas {
			slots[[2]int64{int64(d.Archive), d.Slot}] = d.Sample
			records = append(records, pr.Record{ID: b.ID, Step: policy.Config.Archives[d.Archive].Step, Sample: pr.Sample{Time: d.Sample.Time, Bits: d.Sample.Bits, Sequence: d.Order}})
		}
	}
	if remaining != 0 {
		return nil, nil, errors.New("unrepresented persisted rows")
	}
	names := map[string]pr.Metric{}
	for id, p := range policies {
		if _, ok := names[p.Name]; ok {
			return nil, nil, errors.New("duplicate canonical name")
		}
		metric := pr.Metric{ID: id, XFilesFactor: p.Config.XFilesFactor}
		for _, a := range p.Config.Archives {
			metric.Archives = append(metric.Archives, pr.Archive{Step: a.Step, Points: a.Count})
		}
		names[p.Name] = metric
	}
	cat := pr.CatalogFuncs{LookupFunc: func(n string) (pr.Metric, bool) { m, ok := names[n]; return m, ok }, FindFunc: func(context.Context, string, func(string) error) error {
		return errors.New("oracle catalog supports exact lookup only")
	}}
	return records, cat, nil
}
