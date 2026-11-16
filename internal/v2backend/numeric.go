package v2backend

import (
	"bytes"
	"errors"
	"math"
	"slices"
	"sort"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
)

type sealSeries struct {
	metric        *resolvedMetric
	batches       []sealrollup.Batch
	clock         int64
	cursor, order uint64
}

// Only the admission producer touches these reusable slices. AppendNumeric
// detaches rows/spans before return, so no receipt registry or policy copy is
// needed. Original clocks, apply boundaries and within-apply sequence survive.
func (o *owner) appendCore(fs []frame) error {
	if len(fs) == 0 {
		return nil
	}
	rows, applies := o.numericRows[:0], o.numericApplies[:0]
	cursor := fs[0].cursor
	for _, f := range fs {
		if f.resolved != nil && f.resolved.globalID == 0 {
			var err error
			f.resolved, err = o.s.numeric.Load().intern(o.s, f.resolved)
			if err != nil {
				return err
			}
		}
		if f.resolved == nil || f.resolved.globalID == 0 || f.cursor != cursor || f.resolved.owner != o.id {
			return errors.New("invalid numeric core apply")
		}
		applies = append(applies, v2core.Apply{ID: f.resolved.globalID, Order: f.order, Clock: f.clock, Offset: len(rows), Count: len(f.points)})
		for i, p := range f.points {
			rows = append(rows, v2core.Row{ID: f.resolved.globalID, Time: p.Timestamp, Bits: math.Float64bits(p.Value), Seq: uint64(i + 1)})
		}
	}
	err := o.s.core.AppendNumeric(o.id, rows, applies, cursor, false)
	o.numericRows, o.numericApplies = rows[:0], applies[:0]
	if err == nil {
		o.coreCursor = cursor
	}
	return err
}

func (s *Store) planCoreSeal(id int, in v2core.SealInput) (v2core.SealPlan, error) {
	if len(in.Batches) != 0 {
		return v2core.SealPlan{}, errors.New("unexpected opaque core receipt")
	}
	if len(in.Applies) == 0 {
		return v2core.SealPlan{ReplaceRaw: true}, nil
	}
	// Numeric identities must be durable before data/frontier publication. The
	// journal itself retains canonical names and exact policy definitions.
	if err := s.numeric.Load().sync(); err != nil {
		return v2core.SealPlan{}, err
	}
	if err := s.log.Sync(); err != nil {
		return v2core.SealPlan{}, err
	}
	// One flat sample allocation, regardless of original apply count.
	samples := make([]sealrollup.Sample, len(in.Rows))
	for i, p := range in.Rows {
		samples[i] = sealrollup.Sample{Time: p.Time, Bits: p.Bits, Sequence: p.Seq}
	}
	byID := make(map[uint64]*sealSeries)
	byOwner := make(map[int]map[string]*sealSeries)
	type receipt struct{ cursor, order, count uint64 }
	receipts := make(map[int]receipt)
	for _, a := range in.Applies {
		v := byID[a.ID]
		if v == nil {
			x, ok := s.numeric.Load().byID.Load(a.ID)
			if !ok {
				return v2core.SealPlan{}, errors.New("missing global numeric identity")
			}
			r := x.(*resolvedMetric)
			if r.owner%s.opts.Shards != id {
				return v2core.SealPlan{}, errors.New("numeric seal route mismatch")
			}
			v = &sealSeries{metric: r}
			byID[a.ID] = v
			if byOwner[r.owner] == nil {
				byOwner[r.owner] = make(map[string]*sealSeries)
			}
			byOwner[r.owner][r.name] = v
		}
		if a.Producer != v.metric.owner || a.Offset < 0 || a.Count <= 0 || a.Offset > len(in.Rows)-a.Count || a.Cursor <= s.owners[a.Producer].meta.Cursor {
			return v2core.SealPlan{}, errors.New("invalid numeric seal boundary")
		}
		for _, p := range in.Rows[a.Offset : a.Offset+a.Count] {
			if p.ID != a.ID {
				return v2core.SealPlan{}, errors.New("numeric apply identity changed")
			}
		}
		v.batches = append(v.batches, sealrollup.Batch{Order: a.Order, Now: a.Clock, Points: samples[a.Offset : a.Offset+a.Count]})
		v.clock, v.cursor, v.order = a.Clock, a.Cursor, a.Order
		r := receipts[a.Producer]
		r.cursor, r.order, r.count = a.Cursor, a.Order, r.count+uint64(a.Count)
		receipts[a.Producer] = r
	}
	var transactions []*sealTransaction
	plan := v2core.SealPlan{ReplaceRaw: true}
	plan.Abort = func() {
		for _, tx := range transactions {
			tx.abort()
		}
	}
	plan.Commit = func() {
		for _, tx := range transactions {
			tx.commit()
		}
	}
	metadata := make(map[string][]v2core.KV)
	for ownerID, series := range byOwner {
		r := receipts[ownerID]
		tx, err := s.owners[ownerID].prepareSeries(series, r.cursor, r.order, r.count)
		if err != nil {
			plan.Abort()
			return v2core.SealPlan{}, err
		}
		transactions = append(transactions, tx)
		for _, rows := range tx.files {
			file := make([]v2core.KV, len(rows))
			for i, row := range rows {
				file[i] = v2core.KV{Key: row.key, Value: row.value}
			}
			if bytes.HasPrefix(rows[0].key, []byte{'G', 'C', 'P', 3}) {
				plan.Files = append(plan.Files, file)
			} else {
				ns := string(rows[0].key[:4])
				metadata[ns] = append(metadata[ns], file...)
			}
		}
	}
	for _, file := range metadata {
		slices.SortFunc(file, func(a, b v2core.KV) int { return bytes.Compare(a.Key, b.Key) })
		plan.Files = append(plan.Files, file)
	}
	sort.Slice(plan.Files, func(i, j int) bool { return bytes.Compare(plan.Files[i][0].Key, plan.Files[j][0].Key) < 0 })
	return plan, nil
}
