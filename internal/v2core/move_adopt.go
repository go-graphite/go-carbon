package v2core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// MovePart names a verified transport file. Preparation retains hard links, so
// the sender's lease may end after PrepareMove returns successfully.
type MovePart struct {
	Path       string
	Descriptor v2part.Descriptor
}

// PreparedMove is an offline receiving catalog, independent of the live owner.
// Keep it open until the serial producer's AdoptMove call returns.
type PreparedMove struct {
	db       *pebble.DB
	snapshot *pebble.Snapshot
	index    *RollupSnapshot
	parts    []MovePart
}

func PrepareMove(ctx context.Context, dir string, parts []MovePart) (*PreparedMove, error) {
	if len(parts) == 0 {
		return nil, errors.New("empty move inventory")
	}
	db, err := pebble.Open(filepath.Join(dir, "read-index"), &pebble.Options{})
	if err != nil {
		return nil, err
	}
	p := &PreparedMove{db: db}
	fail := func(err error) (*PreparedMove, error) { _ = p.Close(); return nil, err }
	for _, part := range parts {
		// The preparation root is caller-owned and can be reopened after a crash.
		// Retain a link independently of the transport process's lifetime.
		name := fmt.Sprintf("%x.sst", part.Descriptor.SHA256)
		path := filepath.Join(dir, name)
		if err = os.Link(part.Path, path); err != nil && !errors.Is(err, os.ErrExist) {
			return fail(err)
		}
		if _, err = InstallCopiedPart(ctx, db, dir, path, part.Descriptor); err != nil {
			return fail(err)
		}
		part.Path = path
		p.parts = append(p.parts, part)
	}
	p.snapshot = db.NewSnapshot()
	p.index, err = NewRollupSnapshot(ctx, p.snapshot, nil, 0)
	if err != nil {
		return fail(err)
	}
	return p, nil
}

func (p *PreparedMove) Close() error {
	var err error
	if p.snapshot != nil {
		err = p.snapshot.Close()
		p.snapshot = nil
	}
	if p.db != nil {
		err = errors.Join(err, p.db.Close())
		p.db = nil
	}
	return err
}

// Policies contains canonical names, never donor numeric identities.
func (p *PreparedMove) Policies() map[string][]byte {
	out := map[string][]byte{}
	for name, m := range p.index.metrics {
		out[name] = bytes.Clone(m.latest.definition)
	}
	return out
}

var moveAuthorityPrefix = []byte{255, 'V', '2', 'A', 0}

// moveAuthority binds local identity and a coarse baseline in the same manifest
// edit as the imported parts. Raw bytes retain their original class and order.
// AtOrder is exclusively a receiving-stream counter; it never advances replay.
type moveAuthority struct {
	Version int
	Meta    RollupMetadata
	AtOrder uint64
	Coarse  []sealrollup.Record
}

func moveAuthorityKey(name string) []byte { return append(bytes.Clone(moveAuthorityPrefix), name...) }

// AdoptMove runs on the existing owner queue after preceding intake is sealed.
// The initial attachment implementation deliberately supports one serial
// producer/owner. No producer-wide mutex is introduced into the write path.
func (e *coreEngine) AdoptMove(ctx context.Context, prepared *PreparedMove, ids map[string]uint64, now int64) error {
	if !e.cfg.Rollups || e.cfg.Producers != 1 || len(e.owners) != 1 {
		return errors.New("move adoption requires one rollup producer and owner")
	}
	if err := e.submissionError(); err != nil {
		return err
	}
	ack := make(chan error, 1)
	o := e.owners[0]
	packet := &corePacket{ack: ack, operation: func(o *coreOwner) error { return o.adoptMove(ctx, prepared, ids, now) }}
	if err := o.queues[0].push(packet, o.dead); err != nil {
		return err
	}
	coreWake(e.routers[0].wake)
	// Once queued, wait for the definitive outcome even when ctx is canceled.
	// The owner checks cancellation before committing the atomic manifest edit.
	select {
	case err := <-ack:
		return err
	case <-o.dead:
		return errors.New("move owner stopped")
	}
}

type moveWinner struct {
	record sealrollup.Record
	origin partOrigin
}

// The first attachment handles recent histories whose complete coarse windows
// still fit in retained fine data. Older coarse history needs a boundary
// inventory; fail closed rather than replace it with a partial aggregate.
func validateMoveFineCoverage(ctx context.Context, index *RollupSnapshot, name string, now int64) error {
	m := index.metrics[name]
	if m == nil {
		return nil
	}
	fine := m.latest.schema().Archives[0]
	it, err := newArchiveIter(ctx, index.snapshot, nil)
	if err != nil {
		return err
	}
	defer it.Close()
	for _, part := range m.parts {
		for archive, a := range m.latest.schema().Archives {
			if archive == 0 {
				continue
			}
			k := part.part.key
			k.Metric = part.policy.Name
			k.Archive = uint32(archive)
			k.BlockStart = 0
			k.Ordinal = 0
			lower, err := v2part.Encode(k)
			if err != nil {
				return err
			}
			prefix := lower[:len(lower)-12]
			cutoff := now - fine.Step*fine.Count
			if remainder := cutoff % a.Step; remainder != 0 {
				cutoff += a.Step - remainder
			}
			for ok := it.SeekGE(lower); ok && bytes.HasPrefix(it.Key(), prefix); ok = it.Next() {
				if err := ctx.Err(); err != nil {
					return err
				}
				key, err := v2part.Decode(it.Key())
				if err != nil {
					return err
				}
				records, err := v2part.DecodeSlots(uint32(archive), key.BlockStart, it.Value())
				if err != nil {
					return err
				}
				for _, record := range records {
					if index.recordVisible(m, part.part.origin, record.Order) && record.Sample.Time < cutoff {
						return errors.New("move coarse history extends outside complete fine coverage")
					}
				}
			}
		}
	}
	return it.Error()
}

// collectMoveRaw uses the same physical-slot precedence as reads and recovery.
// It reads immutable snapshots, never the live reducer.
func collectMoveRaw(ctx context.Context, index *RollupSnapshot, name string, winners map[int64]moveWinner) error {
	m := index.metrics[name]
	if m == nil {
		return nil
	}
	it, err := newArchiveIter(ctx, index.snapshot, nil)
	if err != nil {
		return err
	}
	defer it.Close()
	for _, part := range m.parts {
		k := part.part.key
		k.Metric = part.policy.Name
		k.Archive = 0
		k.BlockStart = 0
		k.Ordinal = 0
		lower, err := v2part.Encode(k)
		if err != nil {
			return err
		}
		prefix := lower[:len(lower)-12]
		for ok := it.SeekGE(lower); ok && bytes.HasPrefix(it.Key(), prefix); ok = it.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			key, err := v2part.Decode(it.Key())
			if err != nil {
				return err
			}
			records, err := v2part.DecodeSlots(0, key.BlockStart, it.Value())
			if err != nil {
				return err
			}
			for _, record := range records {
				if !index.recordVisible(m, part.part.origin, record.Order) {
					continue
				}
				old, exists := winners[record.Slot]
				newer := !exists
				if exists {
					newer, err = newerOrigin(part.part.origin, record, old.origin, old.record)
					if err != nil {
						return err
					}
				}
				if newer {
					winners[record.Slot] = moveWinner{record, part.part.origin}
				} else if part.part.origin == old.origin && record.Sample.Time == old.record.Sample.Time && record.Order == old.record.Order && record != old.record {
					return errors.New("conflicting move raw order")
				}
			}
		}
	}
	return it.Error()
}

func (o *coreOwner) adoptMove(ctx context.Context, prepared *PreparedMove, ids map[string]uint64, now int64) error {
	if prepared == nil || prepared.snapshot == nil {
		return errors.New("closed move preparation")
	}
	var incoming []MovePart
	var metadata []coreKV
	for _, part := range prepared.parts {
		k, err := v2part.Decode(part.Descriptor.FirstKey)
		if err != nil {
			return err
		}
		key, err := moveCatalogKey(k)
		if err != nil {
			return err
		}
		d := part.Descriptor
		binding, err := json.Marshal(partImport{Version: 1, Origin: partOrigin{CopiedPart, d.Source.Incarnation, d.Source.Stream}, Descriptor: d})
		if err != nil {
			return err
		}
		if value, closer, err := o.db.Get(key); err == nil {
			var old partImport
			decodeErr := json.Unmarshal(value, &old)
			_ = closer.Close()
			pending := old.Pending
			old.Pending = false
			normalized, marshalErr := json.Marshal(old)
			if decodeErr != nil || marshalErr != nil || !bytes.Equal(normalized, binding) {
				return errors.New("copied part identity changed")
			}
			if !pending {
				continue
			}
		} else if !errors.Is(err, pebble.ErrNotFound) {
			return err
		}
		incoming = append(incoming, part)
		metadata = append(metadata, coreKV{Key: key, Value: binding})
	}
	if len(incoming) == 0 {
		return nil
	}
	snapshot := o.db.NewSnapshot()
	defer snapshot.Close()
	current, err := NewRollupSnapshot(ctx, snapshot, nil, 0)
	if err != nil {
		return err
	}
	// A UUID that is already live must never be reclassified as copied.
	for _, part := range incoming {
		key, _ := v2part.Decode(part.Descriptor.FirstKey)
		for _, physical := range current.parts {
			for _, metric := range physical.metrics {
				if metric.part.key.Part == key.Part && metric.part.key.Group == key.Group {
					return errors.New("cannot reclassify an existing live part")
				}
			}
		}
	}
	next := newCoreRollups()
	next.identities = o.rollups.identities
	next.parallelism = o.rollups.parallelism
	for id, m := range o.rollups.metrics {
		next.metrics[id] = m
		next.names[m.policy.Name] = id
	}
	next.stage, next.expire, next.now = o.rollups.stage, o.rollups.expire, o.rollups.now
	for name, donor := range prepared.index.metrics {
		id := ids[name]
		if id == 0 {
			return errors.New("missing receiving numeric identity")
		}
		policy := donor.latest.rollupPolicy()
		// This first attachment requires a proof of full fine coverage. Longer
		// coarse histories need a separate boundary inventory; do not guess.
		fine := policy.Config.Archives[0]
		for _, archive := range policy.Config.Archives {
			if archive.Step > fine.Step*fine.Count {
				return errors.New("move needs complete retained fine coverage of coarse archives")
			}
		}
		if existing := current.metrics[name]; existing != nil && !bytes.Equal(existing.latest.definition, donor.latest.definition) {
			return errors.New("move policy mismatch")
		}
		if err = validateMoveFineCoverage(ctx, prepared.index, name, now); err != nil {
			return err
		}
		if err = validateMoveFineCoverage(ctx, current, name, now); err != nil {
			return err
		}
		winners := map[int64]moveWinner{}
		if err = collectMoveRaw(ctx, prepared.index, name, winners); err != nil {
			return err
		}
		if err = collectMoveRaw(ctx, current, name, winners); err != nil {
			return err
		}
		var raw []sealrollup.Record
		for _, winner := range winners {
			raw = append(raw, winner.record)
		}
		local := RollupMetadata{Version: 1, ID: id, Policy: bytes.Clone(donor.latest.definition), Revision: 1, LastUpdate: now * 1e9}
		local.Frontier.Policy = donor.latest.prepared.Digest()
		if old := o.rollups.metrics[id]; old != nil {
			if !bytes.Equal(old.definition, local.Policy) {
				return errors.New("receiving ID belongs to another name")
			}
			local = old.metadata()
		}
		local.LastUpdate = max(local.LastUpdate, donor.latest.meta.LastUpdate)
		local.Revision++
		rebuilt, err := PlanMoveRollupRebuild(policy, raw, now, max(uint64(1), local.Frontier.Order))
		if err != nil {
			return err
		}
		m, err := next.metric(id, local.Policy)
		if err != nil {
			return err
		}
		m.meta = compactMetadata(local)
		authority := moveAuthority{Version: 1, Meta: local, AtOrder: local.Frontier.Order}
		for _, record := range rebuilt {
			m.slots.Install(record.Delta)
			m.meta.Frontier.MaxTime = max(m.meta.Frontier.MaxTime, record.Sample.Time)
			if record.Archive > 0 {
				authority.Coarse = append(authority.Coarse, record)
			}
		}
		authority.Meta = m.metadata()
		next.metrics[id] = m
		next.names[policy.Name] = id
		value, err := json.Marshal(authority)
		if err != nil {
			return err
		}
		metadata = append(metadata, coreKV{Key: moveAuthorityKey(name), Value: value})
	}
	stage, err := os.MkdirTemp(o.dir, ".move-adopt-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stage)
	// Pebble rejects overlapping local SST ranges in one Ingest. Stage each
	// unchanged file with an atomic hidden binding, then activate every binding
	// and reducer authority together. Interrupted staging remains invisible.
	for _, part := range incoming {
		if _, err = installCopiedPart(ctx, o.db, o.dir, part.Path, part.Descriptor, true); err != nil {
			return err
		}
		if o.stage != nil {
			o.stage("move-staged-part")
		}
	}
	slices.SortFunc(metadata, func(a, b coreKV) int { return bytes.Compare(a.Key, b.Key) })
	path := filepath.Join(stage, "authority.sst")
	f, err := vfs.Default.Create(path)
	if err != nil {
		return err
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{})
	for _, kv := range metadata {
		if err = w.Set(kv.Key, kv.Value); err != nil {
			_ = w.Close()
			return err
		}
	}
	if err = w.Close(); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if o.stage != nil {
		o.stage("move-before-ingest")
	}
	if err = o.db.Ingest([]string{path}); err != nil {
		return err
	}
	if o.stage != nil {
		o.stage("move-after-ingest")
	}
	o.rollups = next
	o.plan = next.plan
	return nil
}

func (s *RollupSnapshot) loadMoveAuthorities() error {
	for name, m := range s.metrics {
		if s.moves[m] == nil || !s.moves[m].copied {
			continue
		}
		value, closer, err := s.snapshot.Get(moveAuthorityKey(name))
		if errors.Is(err, pebble.ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		var a moveAuthority
		err = json.Unmarshal(value, &a)
		_ = closer.Close()
		if err != nil {
			return err
		}
		if a.Version != 1 || a.Meta.ID == 0 || !bytes.Equal(a.Meta.Policy, m.latest.definition) || a.Meta.Frontier.Order != a.AtOrder {
			return errors.New("invalid move authority")
		}
		if s.moves[m].origin.Class == LivePart {
			if m.latest.id != a.Meta.ID {
				return errors.New("move receiving identity changed")
			}
			if m.latest.meta.Frontier.Order <= a.AtOrder {
				latest := *m.latest
				latest.meta = compactMetadata(a.Meta)
				latest.PolicyIdentity = &PolicyIdentity{id: a.Meta.ID, policy: latest.policy, definition: latest.definition, prepared: latest.prepared}
				m.latest = &latest
			}
		} else {
			latest := *m.latest
			latest.meta = compactMetadata(a.Meta)
			latest.PolicyIdentity = &PolicyIdentity{id: a.Meta.ID, policy: latest.policy, definition: latest.definition, prepared: latest.prepared}
			m.latest = &latest
		}
		s.moves[m].authority = &a
	}
	return nil
}

func (s *RollupSnapshot) scanAdoptedCoarse(ctx context.Context, name string, archive int, first, last int64, yield func(sealrollup.Record) error) error {
	m := s.metrics[name]
	a := s.moves[m].authority
	winners := map[int64]sealrollup.Record{}
	if m.latest.meta.ResetOrder <= a.AtOrder {
		for _, record := range a.Coarse {
			if record.Archive == archive && record.Slot >= first && record.Slot < last {
				winners[record.Slot] = record
			}
		}
	}
	// Resolve post-adoption live replacements with the ordinary single-stream
	// reader, then overlay them on the rebuilt boundary baseline.
	live := *m
	live.parts = nil

	for _, part := range m.parts {
		if part.part.origin.Class == LivePart {
			live.parts = append(live.parts, part)
		}
	}
	tmp := &RollupSnapshot{snapshot: s.snapshot, metrics: map[string]*snapshotMetric{name: &live}, cache: &rollupRangeCache{entries: map[archiveRange][]*archiveResult{}}}
	if err := tmp.ScanArchive(ctx, name, archive, first, last, func(record sealrollup.Record) error {
		if record.Order > a.AtOrder {
			old, exists := winners[record.Slot]
			// The baseline's projection order can be synthetic when a newly
			// adopted name has no receiving input yet. Its receiving age is
			// AtOrder, so the first local application must be able to replace it.
			old.Order = a.AtOrder
			newer, err := newerOrigin(partOrigin{Class: LivePart}, record, partOrigin{Class: LivePart}, old)
			if err != nil {
				return err
			}
			if !exists || newer {
				winners[record.Slot] = record
			}
		}
		return nil
	}); err != nil {
		return err
	}
	for _, record := range winners {
		if err := yield(record); err != nil {
			return err
		}
	}
	return ctx.Err()
}
