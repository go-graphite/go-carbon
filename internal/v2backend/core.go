package v2backend

import (
	"bytes"
	"encoding/binary"
	"errors"
	"path/filepath"
	"slices"
	"sort"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/v2core"
)

func (s *Store) openCore() error {
	s.coreRoutes = make([]uint64, s.opts.Owners)
	for id, found := uint64(1), 0; found < s.opts.Shards; id++ {
		owner := v2core.Shard(id, s.opts.Shards)
		if s.coreRoutes[owner] == 0 {
			s.coreRoutes[owner] = id
			found++
		}
	}
	engine, err := v2core.Open(filepath.Join(s.opts.DataDir, "core"), v2core.Config{
		JournalIdentity: s.log.Identity(), Shards: s.opts.Shards, Producers: s.opts.Owners,
		Owners: s.opts.Shards, MaxRows: 2 << 20, PartitionSeconds: 86400, AppendParts: true, RawCodec: "run1",
	}, v2core.Hooks{PlanSeal: s.planCoreSeal, Publish: func(id int, snap *pebble.Snapshot, _ []v2core.Row) error {
		_ = snap.Close()
		if !s.coreViewsReady.Load() || s.coreViewsClosed.Load() {
			return nil
		}
		for _, o := range s.owners {
			if o.id%s.opts.Shards != id {
				continue
			}
			if base := o.readBase.Load(); base != nil && base.cursor == o.meta.Cursor {
				continue
			}
			if err := o.publish(); err != nil {
				return err
			}
		}
		return nil
	}})
	if err != nil {
		return err
	}
	for i := s.opts.Shards; i < len(s.coreRoutes); i++ {
		s.coreRoutes[i] = s.coreRoutes[i%s.opts.Shards]
	}
	s.core = engine
	s.dbs = engine.DBs() // Borrowed until Close; the engine owns their lifetime.
	return nil
}

// coreEnvelopeKey is process-local. The synced journal, never this registry,
// is the restart source of truth. A planner borrows original immutable applies
// once per envelope instead of rebuilding point rows and policy per point.
type coreEnvelopeKey struct {
	owner  int
	cursor uint64
}

func (o *owner) appendCore(fs []frame) error {
	if len(fs) == 0 {
		return nil
	}
	cursor := fs[0].cursor
	for _, f := range fs {
		if f.resolved == nil || f.cursor != cursor || f.resolved.owner != o.id {
			return errors.New("invalid core receipt envelope")
		}
	}
	key := coreEnvelopeKey{o.id, cursor}
	if _, loaded := o.s.coreEnvelopes.LoadOrStore(key, slices.Clone(fs)); loaded {
		return errors.New("duplicate core receipt envelope")
	}
	meta := append([]byte(nil), "V2E2"...)
	meta = binary.BigEndian.AppendUint32(meta, uint32(o.id))
	meta = binary.BigEndian.AppendUint64(meta, cursor)
	meta = binary.BigEndian.AppendUint32(meta, uint32(len(fs)))
	id := o.s.coreRoutes[o.id]
	// Replacement batches carry opaque envelope metadata. The replacement
	// planner owns raw/archive encoding; sending duplicate numeric rows would
	// copy every accepted point twice only to discard them at seal time.
	if err := o.s.core.AppendBatches(o.id, []v2core.Batch{{ID: id, Clock: fs[0].clock, Order: fs[len(fs)-1].order, Policy: meta}}, cursor, false); err != nil {
		o.s.coreEnvelopes.Delete(key)
		return err
	}
	o.coreCursor = cursor
	return nil
}

func (s *Store) coreFrames(id int, batches []v2core.Batch) ([]frame, error) {
	var fs []frame
	for _, batch := range batches {
		b := batch.Policy
		if len(b) != 20 || string(b[:4]) != "V2E2" || len(batch.Points) != 0 {
			return nil, errors.New("invalid core envelope header")
		}
		ownerID, cursor, count := int(binary.BigEndian.Uint32(b[4:])), binary.BigEndian.Uint64(b[8:]), int(binary.BigEndian.Uint32(b[16:]))
		if ownerID >= len(s.owners) || ownerID%s.opts.Shards != id || batch.ID != s.coreRoutes[ownerID] || cursor == 0 {
			return nil, errors.New("core envelope route mismatch")
		}
		x, ok := s.coreEnvelopes.Load(coreEnvelopeKey{ownerID, cursor})
		if !ok {
			return nil, errors.New("missing original core envelope")
		}
		frames := x.([]frame)
		if len(frames) != count {
			return nil, errors.New("core envelope length mismatch")
		}
		fs = append(fs, frames...)
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
	// Replay snapshots release the journal owner before invoking callbacks.
	// Always sync: another producer may already admit live writes while this
	// producer is replaying its historical prefix.
	if err := s.log.Sync(); err != nil {
		return v2core.SealPlan{}, err
	}
	byOwner := make(map[int][]frame)
	for _, f := range fs {
		byOwner[f.resolved.owner] = append(byOwner[f.resolved.owner], f)
	}
	keys := make([]coreEnvelopeKey, len(in.Batches))
	for i, batch := range in.Batches {
		b := batch.Policy
		keys[i] = coreEnvelopeKey{int(binary.BigEndian.Uint32(b[4:])), binary.BigEndian.Uint64(b[8:])}
	}
	var transactions []*sealTransaction
	plan := v2core.SealPlan{ReplaceRaw: true}
	plan.Abort = func() {
		for _, tx := range transactions {
			tx.abort()
		}
		for _, key := range keys {
			s.coreEnvelopes.Delete(key)
		}
	}
	plan.Commit = func() {
		for _, tx := range transactions {
			tx.commit()
		}
		for _, key := range keys {
			s.coreEnvelopes.Delete(key)
		}
	}
	// Physical DBs serialize their own planners. Distinct journal owners keep
	// independent receipts/reducer state and may share a physical database.
	metadata := make(map[string][]v2core.KV)
	for ownerID, frames := range byOwner {
		tx, err := s.owners[ownerID].prepareSeal(frames)
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
				namespace := string(rows[0].key[:4])
				metadata[namespace] = append(metadata[namespace], file...)
			}
		}
	}
	// Owner-specific metadata ranges can interleave; one sorted metadata file
	// keeps the atomic ingest nonoverlapping without rewriting portable parts.
	for _, file := range metadata {
		slices.SortFunc(file, func(a, b v2core.KV) int { return bytes.Compare(a.Key, b.Key) })
		plan.Files = append(plan.Files, file)
	}
	sort.Slice(plan.Files, func(i, j int) bool { return bytes.Compare(plan.Files[i][0].Key, plan.Files[j][0].Key) < 0 })
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
