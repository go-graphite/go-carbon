package v2backend

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"path/filepath"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/points"
)

func (s *Store) openCore() error {
	// The first adapter keeps journal routing and physical database routing
	// identical. A geometry change requires an explicit on-disk migration.
	if s.opts.Owners != s.opts.Shards {
		return errors.New("core write engine requires owners equal shards")
	}
	s.coreRoutes = make([]uint64, s.opts.Owners)
	for id, found := uint64(1), 0; found < len(s.coreRoutes); id++ {
		owner := v2core.Shard(id, len(s.coreRoutes))
		if s.coreRoutes[owner] == 0 {
			s.coreRoutes[owner] = id
			found++
		}
	}
	engine, err := v2core.Open(filepath.Join(s.opts.DataDir, "core"), v2core.Config{
		JournalIdentity: s.log.Identity(), Shards: s.opts.Shards, Producers: s.opts.Owners,
		Owners: s.opts.Shards, MaxRows: 8 << 20, PartitionSeconds: 86400, AppendParts: true,
	}, v2core.Hooks{PlanSeal: s.planCoreSeal, Publish: func(id int, snap *pebble.Snapshot, _ []v2core.Row) error {
		_ = snap.Close()
		if s.coreViewsClosed.Load() {
			return nil
		}
		o := s.owners[id]
		if base := o.readBase.Load(); base != nil && base.cursor == o.meta.Cursor {
			return nil // Other producers' empty frontier traffic changed no backend data.
		}
		return o.publish()
	}})
	if err != nil {
		return err
	}
	s.core = engine
	s.dbs = engine.DBs() // Borrowed until Close; the engine owns their lifetime.
	return nil
}

// appendCore submits one journal envelope, rather than allocating one core
// receipt for every point. Policy contains original apply descriptors; numeric
// handles only index this process's immutable name/placement/policy catalogue.
// Neither routing tokens nor handles appear in portable SST data.
func (o *owner) appendCore(fs []frame) error {
	if len(fs) == 0 {
		return nil
	}
	cursor := fs[0].cursor
	meta := append(make([]byte, 0, 16+28*len(fs)), "V2E1"...)
	meta = binary.BigEndian.AppendUint64(meta, cursor)
	meta = binary.BigEndian.AppendUint32(meta, uint32(len(fs)))
	count := 0
	for _, f := range fs {
		if f.resolved == nil || f.cursor != cursor || f.resolved.owner != o.id {
			return errors.New("invalid core receipt envelope")
		}
		meta = binary.BigEndian.AppendUint64(meta, f.resolved.coreID)
		meta = binary.BigEndian.AppendUint64(meta, uint64(f.clock))
		meta = binary.BigEndian.AppendUint64(meta, f.order)
		meta = binary.BigEndian.AppendUint32(meta, uint32(len(f.points)))
		count += len(f.points)
	}
	rows := make([]v2core.Row, 0, count)
	id := o.s.coreRoutes[o.id]
	for _, f := range fs {
		for _, p := range f.points {
			rows = append(rows, v2core.Row{ID: id, Time: p.Timestamp, Bits: math.Float64bits(p.Value), Seq: uint64(len(rows) + 1)})
		}
	}
	if err := o.s.core.AppendBatches(o.id, []v2core.Batch{{ID: id, Clock: fs[0].clock, Order: fs[len(fs)-1].order, Policy: meta, Points: rows}}, cursor, false); err != nil {
		return err
	}
	o.coreCursor = cursor
	return nil
}

func (s *Store) coreFrames(id int, batches []v2core.Batch) ([]frame, error) {
	var fs []frame
	for _, batch := range batches {
		b := batch.Policy
		if len(b) < 16 || !bytes.Equal(b[:4], []byte("V2E1")) || batch.ID != s.coreRoutes[id] {
			return nil, errors.New("invalid core envelope header")
		}
		cursor, count := binary.BigEndian.Uint64(b[4:12]), uint64(binary.BigEndian.Uint32(b[12:16]))
		b = b[16:]
		if count > uint64(len(b)/28) || uint64(len(b)) != count*28 || cursor == 0 {
			return nil, errors.New("invalid core apply descriptors")
		}
		rows := batch.Points
		// Original applies borrow disjoint ranges of one detached point array.
		// A one-point receiver apply must not allocate a new array per point.
		data := make([]points.Point, len(rows))
		for i, row := range rows {
			if row.ID != batch.ID {
				return nil, errors.New("core row route mismatch")
			}
			data[i] = points.Point{Timestamp: row.Time, Value: math.Float64frombits(row.Bits)}
		}
		for range count {
			handle := binary.BigEndian.Uint64(b)
			clock, order, n := int64(binary.BigEndian.Uint64(b[8:])), binary.BigEndian.Uint64(b[16:]), uint64(binary.BigEndian.Uint32(b[24:]))
			b = b[28:]
			x, ok := s.coreCatalog.Load(handle)
			if !ok || n == 0 || n > uint64(len(rows)) {
				return nil, errors.New("unknown core catalogue handle or apply length")
			}
			r := x.(*resolvedMetric)
			if r.owner != id {
				return nil, errors.New("core catalogue route mismatch")
			}
			f := frame{name: r.name, resolved: r, group: r.group, policy: r.policy, cursor: cursor, clock: clock, order: order, points: data[:int(n):int(n)]}
			rows = rows[int(n):]
			data = data[int(n):]
			fs = append(fs, f)
		}
		if len(rows) != 0 {
			return nil, errors.New("trailing core envelope points")
		}
	}
	return fs, nil
}

func (s *Store) planCoreSeal(id int, in v2core.SealInput) (v2core.SealPlan, error) {
	fs, err := s.coreFrames(id, in.Batches)
	if err != nil {
		return v2core.SealPlan{}, err
	}
	if len(fs) == 0 {
		return v2core.SealPlan{ReplaceRaw: true}, nil
	}
	// During startup the complete journal is already synced and intake is
	// closed. A replay callback must never recursively Sync its own stream.
	if !s.coreReplay.Load() {
		if err := s.log.Sync(); err != nil {
			return v2core.SealPlan{}, err
		}
	}
	tx, err := s.owners[id].prepareSeal(fs)
	if err != nil {
		return v2core.SealPlan{}, err
	}
	plan := v2core.SealPlan{ReplaceRaw: true, Commit: tx.commit, Abort: tx.abort}
	for _, rows := range tx.files {
		file := make([]v2core.KV, len(rows))
		for i, row := range rows {
			file[i] = v2core.KV{Key: row.key, Value: row.value}
		}
		plan.Files = append(plan.Files, file)
	}
	return plan, nil
}

// Already published suffixes need no barrier. Otherwise serialize a durable
// producer barrier with intake; the core reports any failed ingest through it.
// Publication skips other producers' frontier-only seals in openCore.
func (o *owner) awaitCorePublication(fs []frame) error {
	if len(fs) == 0 {
		return nil
	}
	if base := o.readBase.Load(); base != nil && base.cursor >= fs[len(fs)-1].cursor {
		return nil
	}
	durable := make(chan answer, 1)
	o.requests <- command{op: 's', answer: durable}
	return (<-durable).err
}
