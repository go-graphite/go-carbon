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
	db        *pebble.DB
	snapshot  *pebble.Snapshot
	index     *RollupSnapshot
	parts     []MovePart
	trusted   bool
	Progress  func(owner int, checksum [32]byte, state string) error
	SkipOwner func(int) bool
}

func PrepareMove(ctx context.Context, dir string, parts []MovePart) (*PreparedMove, error) {
	return prepareMove(ctx, dir, parts, false)
}

// PrepareVerifiedMove reopens immutable, pinned files certified by the durable move record.
// The caller must have completed VerifyMovePart before recording verified.
func PrepareVerifiedMove(ctx context.Context, dir string, parts []MovePart) (*PreparedMove, error) {
	return prepareMove(ctx, dir, parts, true)
}

func prepareMove(ctx context.Context, dir string, parts []MovePart, trusted bool) (*PreparedMove, error) {
	if len(parts) == 0 {
		return nil, errors.New("empty move inventory")
	}
	db, err := pebble.Open(filepath.Join(dir, "read-index"), &pebble.Options{})
	if err != nil {
		return nil, err
	}
	p := &PreparedMove{db: db, trusted: trusted}
	fail := func(err error) (*PreparedMove, error) { _ = p.Close(); return nil, err }
	for _, part := range parts {
		// The preparation root is caller-owned and can be reopened after a crash.
		// Retain a link independently of the transport process's lifetime.
		name := fmt.Sprintf("%x.sst", part.Descriptor.SHA256)
		path := filepath.Join(dir, name)
		if err = os.Link(part.Path, path); err != nil && !errors.Is(err, os.ErrExist) {
			return fail(err)
		}
		if _, err = installCopiedPartVerified(ctx, db, dir, path, part.Descriptor, false, trusted); err != nil {
			return fail(err)
		}
		part.Path = path
		p.parts = append(p.parts, part)
	}
	p.snapshot = db.NewSnapshot()
	p.index, err = NewRollupSnapshot(ctx, p.snapshot, nil, nil, nil, 0)
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
		out[name] = bytes.Clone(m.latest.definitionBytes())
	}
	return out
}

var moveAuthorityPrefix = []byte{255, 'V', '2', 'A', 0}

// moveAuthority binds local identity and boundary repairs in the same manifest
// edit as the imported parts. Version 1 stored a full coarse baseline; version 2
// retains untouched history in the original parts. Raw bytes retain their
// original class and order.
// AtOrder is exclusively a receiving-stream counter; it never advances replay.
type moveAuthority struct {
	Version int
	Meta    RollupMetadata
	AtOrder uint64
	Coarse  []sealrollup.Record
	Origins []partOrigin `json:",omitempty"`
}

func moveAuthorityKey(name string) []byte { return append(bytes.Clone(moveAuthorityPrefix), name...) }

// AdoptMove runs once on each receiving core owner. The caller serializes
// submissions from producer zero and has flushed every producer beforehand.
// Completed owner attachments are idempotent across process death.
func (e *coreEngine) AdoptMove(ctx context.Context, prepared *PreparedMove, ids map[string]uint64, now int64) error {
	if !e.cfg.Rollups || prepared == nil || prepared.snapshot == nil {
		return errors.New("move requires open rollup preparation")
	}
	if err := e.submissionError(); err != nil {
		return err
	}
	for owner, o := range e.owners {
		if prepared.SkipOwner != nil && prepared.SkipOwner(owner) {
			continue
		}
		local := map[string]uint64{}
		for name, id := range ids {
			if int(v2part.Route(v2part.ID(name)))%len(e.owners) == owner {
				local[name] = id
			}
		}
		if len(local) == 0 {
			if prepared.Progress != nil {
				if err := prepared.Progress(owner, [32]byte{}, "active"); err != nil {
					return err
				}
			}
			continue
		}
		ack := make(chan error, 1)
		packet := &corePacket{ack: ack, operation: func(o *coreOwner) error { return o.adoptMove(ctx, prepared, local, now, owner, len(e.owners)) }}
		if err := o.queues[0].push(packet, o.dead); err != nil {
			return err
		}
		select {
		case err := <-ack:
			if err != nil {
				return err
			}
			if prepared.Progress != nil {
				if err := prepared.Progress(owner, [32]byte{}, "active"); err != nil {
					return err
				}
			}
		case <-o.dead:
			return errors.New("move owner stopped")
		}
	}
	return nil
}

type moveWinner struct {
	record sealrollup.Record
	origin partOrigin
}

// collectMoveArchive shares the ordinary physical-slot resolver with reads.
// Separate source producers have disjoint names; their counters never compete.
func collectMoveArchive(ctx context.Context, index *RollupSnapshot, name string, archive int, winners map[int64]moveWinner) error {
	m := index.metrics[name]
	if m == nil {
		return nil
	}
	yield := func(record sealrollup.Record, origin partOrigin) error {
		old, exists := winners[record.Slot]
		if exists {
			newer, err := newerOrigin(origin, record, old.origin, old.record)
			if err != nil {
				return err
			}
			if !newer {
				return nil
			}
		}
		winners[record.Slot] = moveWinner{record, origin}
		return nil
	}
	if archive > 0 && index.moves[m] != nil && index.moves[m].authority != nil {
		return index.scanMoveArchive(ctx, name, archive, 0, m.latest.schema().Archives[archive].Count, yield)
	}
	return index.scanArchiveParts(ctx, name, archive, 0, m.latest.schema().Archives[archive].Count, yield)
}

func (o *coreOwner) adoptMove(ctx context.Context, prepared *PreparedMove, ids map[string]uint64, now int64, owner, owners int) error {
	if prepared == nil || prepared.snapshot == nil {
		return errors.New("closed move preparation")
	}
	wanted := map[archivePartIdentity]bool{}
	for name := range ids {
		if metric := prepared.index.metrics[name]; metric != nil {
			for _, part := range prepared.index.metricParts(metric) {
				wanted[archivePartIdentity{part.part.key.Group, part.part.key.Part}] = true
			}
		}
	}
	var incoming []MovePart
	var metadata []coreKV
	for _, part := range prepared.parts {
		if !wanted[archivePartIdentity{part.Descriptor.Scope.Group, part.Descriptor.Scope.Part}] {
			continue
		}
		if descriptorByID(part.Descriptor) {
			return errIDKeyedMove
		}
		k, err := v2part.Decode(part.Descriptor.FirstKey)
		if err != nil {
			return err
		}
		key, err := moveCatalogKey(k)
		if err != nil {
			return err
		}
		d := part.Descriptor
		binding, err := json.Marshal(partImport{Version: 1, Origin: partOrigin{CopiedPart, d.Source.Incarnation, d.Source.Stream}, Descriptor: d, Owner: owner, Owners: owners})
		if err != nil {
			return err
		}
		if value, closer, err := o.db.Get(key); err == nil {
			var old partImport
			decodeErr := json.Unmarshal(value, &old)
			_ = closer.Close()
			pending := old.Pending
			old.Pending = false
			if pending {
				old.Owner, old.Owners = owner, owners
			}
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
	current, err := NewRollupSnapshot(ctx, snapshot, nil, nil, nil, 0)
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
	if err := o.rollups.loadAllCheckpoint(); err != nil {
		return err
	}
	next := newCoreRollups()
	next.db = o.rollups.db
	next.identities = o.rollups.identities
	next.parallelism = o.rollups.parallelism
	next.stage, next.expire, next.now, next.expiryTTL = o.rollups.stage, o.rollups.expire, o.rollups.now, o.rollups.expiryTTL
	for id, m := range o.rollups.metrics {
		next.install(id, m, o.rollups.meta[o.rollups.slotOf(id, m)])
	}
	for name, donor := range prepared.index.metrics {
		id := ids[name]
		if id == 0 {
			continue
		}
		policy := donor.latest.rollupPolicy()
		if existing := current.metrics[name]; existing != nil && !bytes.Equal(existing.latest.definitionBytes(), donor.latest.definitionBytes()) {
			return errors.New("move policy mismatch")
		}
		winning := []sealrollup.Record{}
		targetRaw := []sealrollup.Record{}
		original := map[[2]int64]moveWinner{}
		for archive := range policy.Config.Archives {
			winners := map[int64]moveWinner{}
			if err = collectMoveArchive(ctx, prepared.index, name, archive, winners); err != nil {
				return err
			}
			if err = collectMoveArchive(ctx, current, name, archive, winners); err != nil {
				return err
			}
			for _, winner := range winners {
				winning = append(winning, winner.record)
				original[[2]int64{int64(archive), winner.record.Slot}] = winner
			}
			if archive == 0 {
				localRaw := map[int64]moveWinner{}
				if err = collectMoveArchive(ctx, current, name, 0, localRaw); err != nil {
					return err
				}
				for _, winner := range localRaw {
					if winner.origin.Class == LivePart {
						targetRaw = append(targetRaw, winner.record)
					}
				}
			}
		}
		local := RollupMetadata{Version: 1, ID: id, Policy: bytes.Clone(donor.latest.definitionBytes()), Revision: 1, LastUpdate: now * 1e9}
		local.Frontier.Policy = donor.latest.policy.prepared.Digest()
		if old := o.rollups.metrics[id]; old != nil {
			if !bytes.Equal(old.definitionBytes(), local.Policy) {
				return errors.New("receiving ID belongs to another name")
			}
			local = o.rollups.metadataOf(id, old)
		}
		local.LastUpdate = max(local.LastUpdate, donor.latest.meta.LastUpdate)
		local.Revision++
		rebuilt, err := PlanMoveRollupBoundary(policy, winning, targetRaw, now, max(uint64(1), local.Frontier.Order))
		if err != nil {
			return err
		}
		m, err := next.metric(id, local.Policy)
		if err != nil {
			return err
		}
		meta := compactMetadata(local)
		// Version 2 stores only repaired windows. Mature history remains in
		// its original immutable parts; version 1 full baselines remain readable.
		authority := moveAuthority{Version: 2, Meta: local, AtOrder: local.Frontier.Order}
		priorRepairs := map[[2]int64]sealrollup.Record{}
		if existing := current.metrics[name]; existing != nil && current.moves[existing] != nil && current.moves[existing].authority != nil {
			for _, r := range current.moves[existing].authority.Coarse {
				priorRepairs[[2]int64{int64(r.Archive), r.Slot}] = r
			}
		}
		for _, record := range rebuilt {
			m.residency().Install(record.Delta)
			meta.Frontier.MaxTime = max(meta.Frontier.MaxTime, record.Sample.Time)
			if record.Archive > 0 {
				key := [2]int64{int64(record.Archive), record.Slot}
				prior, exists := original[key]
				oldRepair, hadRepair := priorRepairs[key]
				if exists && prior.record == record && (!hadRepair || oldRepair != record) {
					continue
				}
				authority.Coarse = append(authority.Coarse, record)
				origin := partOrigin{Class: LivePart}
				if exists && prior.record == record {
					origin = prior.origin
				}
				authority.Origins = append(authority.Origins, origin)
			}
		}
		authority.Meta = m.metadata(meta)
		if next.db != nil {
			m.keepRecent(meta.Frontier.MaxTime)
		}
		next.install(id, m, meta)
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
		if _, err = installCopiedPartVerified(ctx, o.db, o.dir, part.Path, part.Descriptor, true, prepared.trusted); err != nil {
			return err
		}
		if prepared.Progress != nil {
			if err = prepared.Progress(owner, part.Descriptor.SHA256, "staged"); err != nil {
				return err
			}
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
	o.plan = next.planParts
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
		if (a.Version != 1 && a.Version != 2) || a.Meta.ID == 0 || !bytes.Equal(a.Meta.Policy, m.latest.definitionBytes()) || a.Meta.Frontier.Order != a.AtOrder {
			return errors.New("invalid move authority")
		}
		if s.moves[m].origin.Class == LivePart {
			if m.latest.id != a.Meta.ID {
				return errors.New("move receiving identity changed")
			}
			if m.latest.meta.Frontier.Order <= a.AtOrder {
				latest := m.latest
				latest.meta = compactMetadata(a.Meta)
				latest.PolicyIdentity = &PolicyIdentity{id: a.Meta.ID, policy: latest.policy, plain: latest.plain, vshard: latest.vshard}
				m.latest = latest
			}
		} else {
			latest := m.latest
			latest.meta = compactMetadata(a.Meta)
			latest.PolicyIdentity = &PolicyIdentity{id: a.Meta.ID, policy: latest.policy, plain: latest.plain, vshard: latest.vshard}
			m.latest = latest
		}
		s.moves[m].authority = &a
	}
	return nil
}

func (s *RollupSnapshot) scanAdoptedCoarse(ctx context.Context, name string, archive int, first, last int64, yield func(sealrollup.Record) error) error {
	return s.scanMoveArchive(ctx, name, archive, first, last, func(record sealrollup.Record, _ partOrigin) error { return yield(record) })
}

func (s *RollupSnapshot) scanMoveArchive(ctx context.Context, name string, archive int, first, last int64, yield func(sealrollup.Record, partOrigin) error) error {
	m := s.metrics[name]
	a := s.moves[m].authority
	winners := map[int64]moveWinner{}
	base := s
	if a.Version == 1 {
		// Legacy authorities contain the entire baseline. Only subsequent local
		// replacements compete with it; old underlying coarse rows stay masked.
		live := *m
		var liveParts []*readPartMetric
		for _, part := range s.metricParts(m) {
			if part.part.origin.Class == LivePart {
				liveParts = append(liveParts, part)
			}
		}
		base = &RollupSnapshot{snapshot: s.snapshot, directory: s.directory, metrics: map[string]*snapshotMetric{name: &live}, partOverrides: map[*snapshotMetric][]*readPartMetric{&live: liveParts}, cache: &rollupRangeCache{entries: map[archiveRange][]*archiveResult{}}}
	}
	if err := base.scanArchiveParts(ctx, name, archive, first, last, func(record sealrollup.Record, origin partOrigin) error {
		if a.Version != 1 || record.Order > a.AtOrder {
			winners[record.Slot] = moveWinner{record, origin}
		}
		return nil
	}); err != nil {
		return err
	}
	if m.latest.meta.ResetOrder <= a.AtOrder {
		for i, record := range a.Coarse {
			if record.Archive != archive || record.Slot < first || record.Slot >= last {
				continue
			}
			origin := partOrigin{Class: LivePart}
			if len(a.Origins) == len(a.Coarse) {
				origin = a.Origins[i]
			}
			old, exists := winners[record.Slot]
			if exists {
				comparison := record
				if origin.Class == LivePart {
					comparison.Order = a.AtOrder
				}
				newer, err := newerOrigin(origin, comparison, old.origin, old.record)
				if err != nil {
					return err
				}
				if !newer && !(origin == old.origin && comparison.Sample.Time == old.record.Sample.Time && comparison.Order == old.record.Order) {
					continue
				}
			}
			winners[record.Slot] = moveWinner{record, origin}
		}
	}
	for _, winner := range winners {
		if err := yield(winner.record, winner.origin); err != nil {
			return err
		}
	}
	return ctx.Err()
}
