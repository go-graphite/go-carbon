package v2core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"unsafe"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// RollupSnapshot is a directory over immutable part keys in a pinned snapshot.
// Atomic bindings enumerate parts; only added parts seek over archive values.
// A prior directory shares immutable per-part metadata; it never shares a snapshot or a writer-owned object.
// The caller retains snapshot until all directory consumers have returned.
type RollupSnapshot struct {
	identities *PolicyCatalog
	// Copy-only state is absent from ordinary per-name directories.
	moves map[*snapshotMetric]*snapshotMove

	snapshot       *pebble.Snapshot
	directory      string
	parts          map[string]*readPart
	metrics        map[string]*snapshotMetric
	cache          *rollupRangeCache
	directoryBytes int64
	bindingSet     *RollupInventory
	inventory      map[string][]string // descriptor binding -> immutable routed part scopes
	hasCopied      bool
}

// Shared only by readers. Entries own scalar records and scope strings, never
// snapshots, metadata objects, writer arenas or an earlier cache generation.
type rollupRangeCache struct {
	mu           sync.Mutex
	entries      map[archiveRange][]*archiveResult
	bytes, limit int64
	clock        uint64
	count        int
}

const rollupReadCacheBytes = 16 << 20
const rollupReadCacheEntries = 64

type archiveRange struct {
	name        string
	archive     int
	first, last int64
}
type archiveResult struct {
	records            []sealrollup.Record
	origins            map[int64]partOrigin
	origin             partOrigin
	parts              map[string]struct{}
	bytes              int64
	frontier, revision uint64
	age                uint64
}

type readPart struct {
	id      string
	key     v2part.Key          // immutable part scope; names come from the metric policy
	ids     map[string]struct{} // shared singleton set for names with only this part
	metrics map[string]*readPartMetric
	bytes   int64
	origin  partOrigin
}
type readPartMetric struct {
	part *readPart
	*PolicyIdentity
	meta metricMetadata
}
type snapshotMetric struct {
	single  [1]*readPartMetric
	latest  *readPartMetric
	parts   []*readPartMetric
	partIDs map[string]struct{}
}
type snapshotMove struct {
	origin    partOrigin
	origins   map[partOrigin]RollupMetadata
	copied    bool
	authority *moveAuthority
}

func NewRollupSnapshot(ctx context.Context, snapshot *pebble.Snapshot, prior *RollupSnapshot, cacheLimit ...int64) (*RollupSnapshot, error) {
	return NewRollupSnapshotWithCatalogAndInventory(ctx, snapshot, prior, nil, nil, cacheLimit...)
}
func NewRollupSnapshotWithCatalog(ctx context.Context, snapshot *pebble.Snapshot, prior *RollupSnapshot, identities *PolicyCatalog, cacheLimit ...int64) (*RollupSnapshot, error) {
	return NewRollupSnapshotWithCatalogAndInventory(ctx, snapshot, prior, identities, nil, cacheLimit...)
}
func NewRollupSnapshotWithInventory(ctx context.Context, snapshot *pebble.Snapshot, prior *RollupSnapshot, bindings *RollupInventory, cacheLimit ...int64) (*RollupSnapshot, error) {
	return NewRollupSnapshotWithCatalogAndInventory(ctx, snapshot, prior, nil, bindings, cacheLimit...)
}

// NewRollupSnapshotWithCatalogAndInventory reuses immutable identities and the owner binding set.
func NewRollupSnapshotWithCatalogAndInventory(ctx context.Context, snapshot *pebble.Snapshot, prior *RollupSnapshot, identities *PolicyCatalog, bindings *RollupInventory, cacheLimit ...int64) (*RollupSnapshot, error) {
	if identities == nil && prior != nil {
		identities = prior.identities
	}
	if identities == nil {
		identities = &PolicyCatalog{}
	}
	if found, err := snapshotHasCopiedParts(snapshot); err != nil {
		return nil, err
	} else if found {
		return newCopiedRollupSnapshot(ctx, snapshot, prior, cacheLimit...)
	}
	limit := int64(rollupReadCacheBytes)
	if len(cacheLimit) > 0 {
		limit = max(0, min(limit, cacheLimit[0]))
	}
	directory, _ := ctx.Value(archiveDirectoryContext{}).(string)
	out := &RollupSnapshot{identities: identities, snapshot: snapshot, directory: directory, bindingSet: bindings, parts: map[string]*readPart{}, metrics: map[string]*snapshotMetric{}}
	if prior != nil && prior.cache != nil && prior.cache.limit == limit {
		out.cache = prior.cache
	} else {
		out.cache = &rollupRangeCache{entries: map[archiveRange][]*archiveResult{}, limit: limit}
	}

	// The descriptor namespace is published atomically with the data. Walking
	// these small adjacent keys avoids seeking every old scope through the SSTs.
	inventory, scopes, err := rollupInventory(ctx, snapshot, prior, bindings)
	if err != nil {
		return nil, err
	}
	out.inventory = inventory
	prefix := []byte{'G', 'C', 'P', 3}
	it, err := newArchiveIter(ctx, snapshot, &pebble.IterOptions{LowerBound: prefix, UpperBound: v2part.PrefixEnd(prefix)})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	if inventory == nil {
		// Bare GCP3 fixtures have no core frontier or atomic inventory bindings.
		for valid := it.First(); valid; {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			key, err := v2part.Decode(it.Key())
			if err != nil {
				return nil, err
			}
			if key.Partition != 0 {
				return nil, errors.New("unexpected rollup partition")
			}
			scope, err := v2part.PartitionPrefixForGroup(key.Group, key.VShard, 0)
			if err != nil {
				return nil, err
			}
			scope = append(scope, key.Part[:]...)
			scopes = append(scopes, string(scope))
			valid = it.SeekGE(v2part.PrefixEnd(scope))
		}
		if err := it.Error(); err != nil {
			return nil, err
		}
	}

	changed := map[string]bool{}
	added := map[string]*readPart{}
	for _, id := range scopes {
		var part *readPart
		if prior != nil {
			part = prior.parts[id]
		}
		if part == nil {
			part, err = readRollupPart(ctx, it, []byte(id), identities)
			if err != nil {
				return nil, err
			}
		}
		if out.parts[id] != nil {
			return nil, errors.New("duplicate rollup scope binding")
		}
		out.parts[id] = part
		out.directoryBytes += part.bytes
		if prior == nil || prior.parts[id] == nil {
			added[id] = part
			for name := range part.metrics {
				changed[name] = true
			}
		}

	}
	removed := map[string]bool{}
	if prior != nil {
		for id, part := range prior.parts {
			if out.parts[id] == nil {
				for name := range part.metrics {
					changed[name] = true
					removed[name] = true
				}
			}
		}
		if len(changed) == 0 {
			out.metrics = prior.metrics
			out.directoryBytes = prior.directoryBytes
			return out, nil
		}
		out.metrics = maps.Clone(prior.metrics)
	}
	// Detach only affected entries before extending the immutable directory.
	for name := range changed {
		if prior != nil && !removed[name] {
			if old := prior.metrics[name]; old != nil {
				copy := *old
				copy.parts = slices.Clone(old.parts)
				copy.partIDs = maps.Clone(old.partIDs)
				out.metrics[name] = &copy
				continue
			}
		}
		delete(out.metrics, name)
	}
	// Walk actual entries once, rather than probing every part for each name.
	addPart := func(part *readPart, onlyRemoved bool) error {
		for name, pm := range part.metrics {
			if onlyRemoved && !removed[name] {
				continue
			}
			m := out.metrics[name]
			if m == nil {
				m = &snapshotMetric{latest: pm, partIDs: part.ids}
				m.parts = m.single[:0]
				out.metrics[name] = m
			}
			if m.latest.PolicyIdentity != pm.PolicyIdentity {
				return errors.New("inconsistent rollup identity/policy")
			}
			if pm.meta.Frontier.Order > m.latest.meta.Frontier.Order || (pm.meta.Frontier.Order == m.latest.meta.Frontier.Order && pm.meta.Revision > m.latest.meta.Revision) {
				m.latest = pm
			}
			if len(m.parts) > 0 {
				// Detach the immutable singleton before adding a second part.
				if len(m.parts) == 1 {
					m.partIDs = maps.Clone(m.partIDs)
				}
				m.partIDs[part.id] = struct{}{}
			}
			m.parts = append(m.parts, pm)
		}
		return nil
	}
	for _, part := range added {
		if err := addPart(part, false); err != nil {
			return nil, err
		}
	}
	if len(removed) > 0 {
		for id, part := range out.parts {
			if added[id] == nil {
				if err := addPart(part, true); err != nil {
					return nil, err
				}
			}
		}
	}
	for _, m := range out.metrics {
		out.directoryBytes += int64(64 + len(m.parts)*48)
	}
	for id, scopes := range out.inventory {
		out.directoryBytes += int64(64 + len(id) + len(scopes)*16)
	}
	return out, nil
}

// RetainedBytes conservatively charges directory storage plus the maximum
// bounded range cache. Shared immutable entries are charged independently by
// each pinned generation; SST and pending-arena accounting remains external.
func (s *RollupSnapshot) RetainedBytes() int64 {
	n := s.directoryBytes + s.cache.limit
	if s.bindingSet != nil {
		n += s.bindingSet.bytes
	}
	return n
}

// Metadata visits only the directory, never archive values.
func (s *RollupSnapshot) Metadata(visit func(string, RollupPolicy, RollupMetadata)) {
	for name, m := range s.metrics {
		visit(name, m.latest.rollupPolicy(), m.latest.metadata())
	}
}

// LookupMetadata reads the immutable directory without constructing a second
// all-name map in each consumer generation.
func (s *RollupSnapshot) LookupMetadata(name string) (RollupPolicy, RollupMetadata, bool) {
	m := s.metrics[name]
	if m == nil {
		return RollupPolicy{}, RollupMetadata{}, false
	}
	return m.latest.rollupPolicy(), m.latest.metadata(), true
}

// MetricVersion identifies an immutable name's complete part set and metadata.
// It owns no snapshot; equality proves archive records are unchanged, including
// reset, retirement and rewind. Consumers may retain it in bounded caches.
func (s *RollupSnapshot) MetricVersion(name string) any {
	if m := s.metrics[name]; m != nil {
		return m
	}
	return nil
}

// ScanArchive seeks only the named archive's physical interval in each part.
// A predecessor seek includes dense runs beginning before first. The same
// iterator/decoder is reused across parts; singleton records decode on the stack.
// Winners resolve by logical timestamp before retention filters. A read-side cache
// carries only immutable results and part identities across generations. New
// snapshots decode only added parts; removed parts invalidate cached authority.
func (s *RollupSnapshot) ScanArchive(ctx context.Context, name string, archive int, first, last int64, yield func(sealrollup.Record) error) error {
	m := s.metrics[name]
	if m == nil || m.latest.meta.Expired || first >= last {
		return ctx.Err()
	}
	if archive < 0 || archive >= len(m.latest.schema().Archives) || first < 0 || last > m.latest.schema().Archives[archive].Count {
		return errors.New("invalid archive range")
	}
	if archive > 0 && s.moves[m] != nil && s.moves[m].authority != nil {
		return s.scanAdoptedCoarse(ctx, name, archive, first, last, yield)
	}
	if archive > 0 && s.moves[m] != nil && len(s.moves[m].origins) > 1 {
		return errors.New("mixed-origin coarse reads require installed rollup rebuild authority")
	}
	// A full archive already owns every physical-slot winner for this name.
	// Reuse it for narrower ranges instead of reconstructing identical immutable
	// parts under a different range key. The full scan below still validates
	// part membership and decodes only additions after publication changes.
	archiveCount := m.latest.policy.Config.Archives[archive].Count
	if first != 0 || last != archiveCount {
		s.cache.mu.Lock()
		covered := len(s.cache.entries[archiveRange{name, archive, 0, archiveCount}]) > 0
		s.cache.mu.Unlock()
		if covered {
			return s.ScanArchive(ctx, name, archive, 0, archiveCount, func(record sealrollup.Record) error {
				if record.Slot >= first && record.Slot < last {
					return yield(record)
				}
				return nil
			})
		}
	}
	cacheKey := archiveRange{name, archive, first, last}
	// Keep a small bounded history: concurrent requests can pin different
	// generations. A single latest entry forces older pins into full rescans.
	var candidates [8]*archiveResult
	s.cache.mu.Lock()
	count := copy(candidates[:], s.cache.entries[cacheKey])
	s.cache.mu.Unlock()
	var cached *archiveResult
	parts := m.partIDs
	for _, candidate := range candidates[:count] {
		// Cache aging uses an order only inside the same original stream.
		// Changing the winning class/source invalidates that age comparison.
		if candidate.origin != s.metricOrigin(m) {
			continue
		}
		if candidate.frontier > m.latest.meta.Frontier.Order || len(candidate.parts) > len(parts) {
			continue
		}
		if cached != nil && candidate.frontier < cached.frontier {
			continue
		}
		valid := true
		for id := range candidate.parts {
			if _, ok := parts[id]; !ok {
				valid = false
				break
			}
		}
		if valid {
			cached = candidate
		}
	}
	if cached != nil && len(cached.parts) == len(parts) {
		for _, record := range cached.records {
			if s.recordVisible(m, cachedOrigin(cached.origins, record.Slot), record.Order) {
				if err := yield(record); err != nil {
					return err
				}
			}
		}
		return ctx.Err()
	}
	winners := map[int64]sealrollup.Record{}
	var origins map[int64]partOrigin
	if s.moves[m] != nil && s.moves[m].copied {
		origins = map[int64]partOrigin{}
	}
	if cached != nil {
		for _, record := range cached.records {
			if s.recordVisible(m, cachedOrigin(cached.origins, record.Slot), record.Order) {
				winners[record.Slot] = record
				if origins != nil {
					origins[record.Slot] = cachedOrigin(cached.origins, record.Slot)
				}
			}
		}
	}
	if s.directory != "" {
		ctx = WithRollupDirectory(ctx, s.directory)
	}
	selected := make(map[archivePartIdentity]bool, len(m.parts))
	for _, part := range m.parts {
		selected[archivePartIdentity{part.part.key.Group, part.part.key.Part}] = true
	}
	it, err := newArchiveIterSelected(ctx, s.snapshot, nil, selected)
	if err != nil {
		return err
	}
	defer it.Close()
	for _, part := range m.parts {
		if cached != nil {
			if _, ok := cached.parts[part.part.id]; ok {
				continue
			}
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		if !s.recordVisible(m, part.part.origin, part.meta.Frontier.Order) {
			continue
		}
		key := part.part.key
		key.Metric = part.policy.Name
		key.Archive = uint32(archive)
		key.BlockStart = first
		lower, err := v2part.Encode(key)
		if err != nil {
			return err
		}
		prefix := lower[:len(lower)-12]
		// A run may straddle the lower bound. Never cross metric/archive/part.
		valid := it.SeekLT(lower)
		if !valid || !bytes.HasPrefix(it.Key(), prefix) {
			valid = it.SeekGE(lower)
		}
		for valid && bytes.HasPrefix(it.Key(), prefix) {
			key, err = v2part.Decode(it.Key())
			if err != nil {
				return err
			}
			if key.BlockStart >= last {
				break
			}
			if key.Ordinal != 0 {
				return errors.New("unexpected rollup ordinal")
			}
			emit := func(record sealrollup.Record) error {
				if record.Slot >= m.latest.schema().Archives[archive].Count || record.Order > part.meta.Frontier.Order {
					return errors.New("invalid rollup slot/frontier")
				}
				if record.Slot >= first && record.Slot < last && s.recordVisible(m, part.part.origin, record.Order) {
					old, ok := winners[record.Slot]
					newer := !ok
					if ok {
						var e error
						newer, e = newerOrigin(part.part.origin, record, cachedOrigin(origins, record.Slot), old)
						if e != nil {
							return e
						}
					}
					if newer {
						winners[record.Slot] = record
						if origins != nil {
							origins[record.Slot] = part.part.origin
						}
					} else if part.part.origin == cachedOrigin(origins, record.Slot) && record.Sample.Time == old.Sample.Time && record.Order == old.Order && record.Delta != old.Delta {
						return errors.New("conflicting rollup replacement order")
					}
					return nil
				}
				return nil
			}
			value := it.Value()
			if len(value) >= 4 && bytes.Equal(value[:4], []byte{'G', 'C', 'D', 1}) {
				records, err := v2part.DecodeSlots(key.Archive, key.BlockStart, value)
				if err != nil {
					return err
				}
				for _, record := range records {
					if err = emit(record); err != nil {
						return err
					}
				}
			} else {
				record, err := sealrollup.DecodeRecord(archive, key.BlockStart, value)
				if err != nil {
					return err
				}
				if err = emit(record); err != nil {
					return err
				}
			}
			valid = it.Next()
		}
	}
	if err = it.Error(); err != nil {
		return err
	}
	result := &archiveResult{origin: s.metricOrigin(m), frontier: m.latest.meta.Frontier.Order, revision: m.latest.meta.Revision, parts: parts, origins: origins, records: make([]sealrollup.Record, 0, len(winners))}
	for _, record := range winners {
		result.records = append(result.records, record)
	}
	result.bytes = int64(256+len(name)) + int64(len(result.records))*int64(unsafe.Sizeof(sealrollup.Record{}))
	for _, origin := range origins {
		result.bytes += int64(unsafe.Sizeof(partOrigin{})) + 48 + int64(len(origin.Stream))
	}
	for id := range parts {
		result.bytes += int64(64 + len(id))
	}
	s.cache.mu.Lock()
	if result.bytes <= s.cache.limit {
		entries := s.cache.entries[cacheKey]
		duplicate := false
		for _, old := range entries {
			if old.origin == result.origin && old.frontier == result.frontier && old.revision == result.revision && len(old.parts) == len(parts) {
				duplicate = true
				for id := range old.parts {
					if _, ok := parts[id]; !ok {
						duplicate = false
						break
					}
				}
				if duplicate {
					break
				}
			}
		}
		if !duplicate {
			if len(entries) == len(candidates) {
				s.cache.bytes -= entries[0].bytes
				s.cache.count--
				entries = entries[1:]
				s.cache.entries[cacheKey] = entries
			}
			// FIFO over at most 64 entries; no unbounded LRU or generation chain.
			for s.cache.count >= rollupReadCacheEntries || s.cache.bytes+result.bytes > s.cache.limit {
				var oldestKey archiveRange
				oldestIndex := -1
				oldestAge := ^uint64(0)
				for k, es := range s.cache.entries {
					for i, e := range es {
						if e.age < oldestAge {
							oldestKey, oldestIndex, oldestAge = k, i, e.age
						}
					}
				}
				if oldestIndex < 0 {
					break
				}
				es := s.cache.entries[oldestKey]
				s.cache.bytes -= es[oldestIndex].bytes
				s.cache.count--
				es = append(es[:oldestIndex:oldestIndex], es[oldestIndex+1:]...)
				if len(es) == 0 {
					delete(s.cache.entries, oldestKey)
				} else {
					s.cache.entries[oldestKey] = es
				}
			}
			s.cache.clock++
			result.age = s.cache.clock
			s.cache.entries[cacheKey] = append(s.cache.entries[cacheKey], result)
			s.cache.bytes += result.bytes
			s.cache.count++
		}
	}
	s.cache.mu.Unlock()
	for _, record := range result.records {
		if err = yield(record); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// Records resolves the canonical full state needed by pending rollup planning.
// Render without pending data uses ScanArchive directly and avoids this work.
func (s *RollupSnapshot) Records(ctx context.Context, name string) ([]sealrollup.Record, error) {
	m := s.metrics[name]
	if m == nil {
		return nil, nil
	}
	var records []sealrollup.Record
	for archive, a := range m.latest.schema().Archives {
		err := s.ScanArchive(ctx, name, archive, 0, a.Count, func(r sealrollup.Record) error { records = append(records, r); return nil })
		if err != nil {
			return nil, err
		}
	}
	return records, nil
}

// readRollupPart decodes metadata only for one newly visible routed part.
func readRollupPart(ctx context.Context, it *archiveIter, scope []byte, identities *PolicyCatalog) (*readPart, error) {
	if !it.SeekGE(scope) || !bytes.HasPrefix(it.Key(), scope) {
		return nil, fmt.Errorf("missing rollup part %x", scope)
	}
	id := string(scope)
	part := &readPart{origin: partOrigin{Class: LivePart}, id: id, ids: map[string]struct{}{id: {}}, metrics: map[string]*readPartMetric{}}
	end := v2part.PrefixEnd(scope)
	for it.Valid() && bytes.Compare(it.Key(), end) < 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key, err := v2part.Decode(it.Key())
		if err != nil {
			return nil, err
		}
		key.Archive = v2part.MetadataArchive
		key.BlockStart = 0
		key.Ordinal = 0
		encoded, err := v2part.Encode(key)
		if err != nil {
			return nil, err
		}
		// Most new metrics have one archive row immediately before metadata.
		// One Next avoids rebuilding the merging iterator for that common case.
		if !bytes.Equal(it.Key(), encoded) {
			it.Next()
		}
		if (!it.Valid() || !bytes.Equal(it.Key(), encoded)) && (!it.SeekGE(encoded) || !bytes.Equal(it.Key(), encoded)) {
			return nil, fmt.Errorf("missing rollup metadata for %s", key.Metric)
		}
		meta, decodeErr := decodeRollupMetadataView(it.Value())
		if decodeErr != nil {
			err = decodeErr
			return nil, err
		}
		h, err := identities.intern(meta.ID, string(key.Metric), meta.Policy)
		if err != nil {
			return nil, err
		}
		policy := h.rollupPolicy()
		meta.Policy = h.definition
		if (meta.Version != 1 && meta.Version != 2) || meta.ID == 0 || meta.Frontier.Order == 0 || policy.Name != key.Metric || policy.Group != key.Group {
			return nil, errors.New("invalid rollup metadata")
		}
		if (meta.Version == 1 && (meta.ResetOrder != 0 || meta.Expired)) || meta.ResetOrder > meta.Frontier.Order || (meta.Expired && meta.ResetOrder != meta.Frontier.Order) {
			return nil, errors.New("invalid rollup reset frontier")
		}
		if h.prepared.Digest() != meta.Frontier.Policy {
			return nil, errors.New("rollup policy/frontier mismatch")
		}
		if len(part.metrics) == 0 {
			part.key = key
			part.key.Metric = ""
		}
		part.metrics[string(policy.Name)] = &readPartMetric{part: part, PolicyIdentity: h, meta: compactMetadata(meta)}
		// The last 16 bytes are archive, physical slot and ordinal.
		it.Next()
		if it.Valid() && bytes.HasPrefix(it.Key(), encoded[:len(encoded)-16]) {
			return nil, errors.New("unexpected keys after rollup metadata")
		}
	}
	part.bytes = int64(128 + len(scope))
	for name, pm := range part.metrics {
		part.bytes += int64(512 + len(name)*2 + len(pm.definition)*2 + len(pm.policy.Group)*2 + len(pm.schema().Archives)*32)
	}
	return part, it.Error()
}

func newCopiedRollupSnapshot(ctx context.Context, snapshot *pebble.Snapshot, prior *RollupSnapshot, cacheLimit ...int64) (*RollupSnapshot, error) {
	limit := int64(rollupReadCacheBytes)
	if len(cacheLimit) > 0 {
		limit = max(0, min(limit, cacheLimit[0]))
	}
	directory, _ := ctx.Value(archiveDirectoryContext{}).(string)
	out := &RollupSnapshot{snapshot: snapshot, directory: directory, parts: map[string]*readPart{}, metrics: map[string]*snapshotMetric{}, moves: map[*snapshotMetric]*snapshotMove{}}
	var err error
	out.hasCopied, err = snapshotHasCopiedParts(snapshot)
	if err != nil {
		return nil, err
	}
	if prior != nil && prior.cache != nil && prior.cache.limit == limit {
		out.cache = prior.cache
	} else {
		out.cache = &rollupRangeCache{entries: map[archiveRange][]*archiveResult{}, limit: limit}
	}
	prefix := []byte{'G', 'C', 'P', 3}
	it, err := newArchiveIter(ctx, snapshot, &pebble.IterOptions{LowerBound: prefix, UpperBound: v2part.PrefixEnd(prefix)})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	for valid := it.First(); valid; {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key, err := v2part.Decode(it.Key())
		if err != nil {
			return nil, err
		}
		if key.Partition != 0 {
			return nil, errors.New("unexpected rollup partition")
		}
		scope, err := v2part.PartitionPrefixForGroup(key.Group, key.VShard, 0)
		if err != nil {
			return nil, err
		}
		scope = append(scope, key.Part[:]...)
		scopeID := string(scope)
		var part *readPart
		if prior != nil {
			part = prior.parts[scopeID]
		}
		if part == nil {
			part = &readPart{id: scopeID, ids: map[string]struct{}{scopeID: {}}, key: key, metrics: map[string]*readPartMetric{}}
			part.key.Metric = ""
			origin := partOrigin{Class: LivePart}
			if out.hasCopied {
				origin, err = readPartOrigin(snapshot, key)
				if err != nil {
					return nil, err
				}
			}
			if origin.Class == hiddenPart {
				valid = it.SeekGE(v2part.PrefixEnd(scope))
				continue
			}
			part.origin = origin
			end := v2part.PrefixEnd(scope)
			for it.Valid() && bytes.Compare(it.Key(), end) < 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				key, err = v2part.Decode(it.Key())
				if err != nil {
					return nil, err
				}
				key.Archive = v2part.MetadataArchive
				key.BlockStart = 0
				key.Ordinal = 0
				encoded, err := v2part.Encode(key)
				if err != nil {
					return nil, err
				}
				if !it.SeekGE(encoded) || !bytes.Equal(it.Key(), encoded) {
					return nil, fmt.Errorf("missing rollup metadata for %s", key.Metric)
				}
				meta, decodeErr := DecodeRollupMetadata(it.Value())
				if err = decodeErr; err != nil {
					return nil, err
				}
				var policy RollupPolicy
				if err = json.Unmarshal(meta.Policy, &policy); err != nil {
					return nil, err
				}
				if (meta.Version != 1 && meta.Version != 2) || meta.ID == 0 || meta.Frontier.Order == 0 || policy.Name != key.Metric || policy.Group != key.Group {
					return nil, errors.New("invalid rollup metadata")
				}
				if (meta.Version == 1 && (meta.ResetOrder != 0 || meta.Expired)) || meta.ResetOrder > meta.Frontier.Order || (meta.Expired && meta.ResetOrder != meta.Frontier.Order) {
					return nil, errors.New("invalid rollup reset frontier")
				}
				digest, err := sealrollup.PolicyDigest(policy.Config)
				if err != nil {
					return nil, err
				}
				if digest != meta.Frontier.Policy {
					return nil, errors.New("rollup policy/frontier mismatch")
				}
				prepared, err := sealrollup.Prepare(policy.Config)
				if err != nil {
					return nil, err
				}
				part.metrics[string(key.Metric)] = &readPartMetric{part: part, PolicyIdentity: &PolicyIdentity{id: meta.ID, policy: policyName{Name: policy.Name, Group: policy.Group}, definition: meta.Policy, prepared: prepared}, meta: compactMetadata(meta)}
				// The last 16 bytes are archive, physical slot and ordinal.
				it.SeekGE(v2part.PrefixEnd(encoded[:len(encoded)-16]))
			}
		}
		out.parts[scopeID] = part
		out.directoryBytes += int64(128 + len(scopeID))
		for name, pm := range part.metrics {
			out.directoryBytes += int64(512 + len(name)*2 + len(pm.definition)*2 + len(pm.policy.Group)*2 + len(pm.schema().Archives)*32)
			if pm.part.origin.Class == CopiedPart {
				out.directoryBytes += int64(len(pm.part.origin.Stream) + 128)
			}
		}
		for name, p := range part.metrics {
			m := out.metrics[name]
			if m == nil {
				m = &snapshotMetric{latest: p, partIDs: map[string]struct{}{}}
				out.moves[m] = &snapshotMove{origin: p.part.origin, origins: map[partOrigin]RollupMetadata{}}
				out.metrics[name] = m
			}
			if m.latest.policy.Group != p.policy.Group || !bytes.Equal(m.latest.definition, p.definition) {
				return nil, errors.New("inconsistent rollup identity/policy")
			}
			out.moves[m].copied = out.moves[m].copied || p.part.origin.Class == CopiedPart
			old, seen := out.moves[m].origins[p.part.origin]
			if seen && old.ID != p.id {
				return nil, errors.New("source changed numeric metric identity")
			}
			if !seen || p.meta.Frontier.Order > old.Frontier.Order || (p.meta.Frontier.Order == old.Frontier.Order && p.meta.Revision > old.Revision) {
				out.moves[m].origins[p.part.origin] = p.metadata()
			}
			newer, err := newerSource(p.part.origin, p.meta.Frontier.Order, out.moves[m].origin, m.latest.meta.Frontier.Order)
			if err != nil {
				return nil, err
			}
			if newer || (p.part.origin == out.moves[m].origin && p.meta.Frontier.Order == m.latest.meta.Frontier.Order && p.meta.Revision > m.latest.meta.Revision) {
				m.latest, out.moves[m].origin = p, p.part.origin
			}
			m.parts = append(m.parts, p)
			m.partIDs[p.part.id] = struct{}{}
		}
		valid = it.SeekGE(v2part.PrefixEnd(scope))
	}
	if err = it.Error(); err != nil {
		return nil, err
	}
	if err := out.loadMoveAuthorities(); err != nil {
		return nil, err
	}
	return out, nil
}

// Resets are source-local counters too. A receiver reset also masks all donor
// history, since every imported part precedes every local lifecycle event.
func (s *RollupSnapshot) recordVisible(m *snapshotMetric, origin partOrigin, order uint64) bool {
	if s.moves[m] == nil || !s.moves[m].copied {
		return !m.latest.meta.Expired && order > m.latest.meta.ResetOrder
	}
	if s.moves[m].origin.Class == LivePart && origin.Class == CopiedPart && m.latest.meta.ResetOrder != 0 {
		return false
	}
	meta := s.moves[m].origins[origin]
	return !meta.Expired && order > meta.ResetOrder
}

func cachedOrigin(origins map[int64]partOrigin, slot int64) partOrigin {
	if origins == nil {
		return partOrigin{Class: LivePart}
	}
	return origins[slot]
}

func (s *RollupSnapshot) metricOrigin(m *snapshotMetric) partOrigin {
	if s.moves[m] == nil {
		return partOrigin{Class: LivePart}
	}
	return s.moves[m].origin
}
