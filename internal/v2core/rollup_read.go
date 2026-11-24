package v2core

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"unsafe"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// RollupSnapshot is a directory over immutable part keys in a pinned snapshot.
// Atomic bindings enumerate parts; only added parts seek over archive values.
// A prior directory shares immutable scopes and latest metadata. Per-name part
// membership stays in the sorted SST, accessed through the pinned snapshot.
// The caller retains snapshot until all directory consumers have returned.
type RollupSnapshot struct {
	opened     *OpenedRollupParts
	identities *PolicyCatalog
	// Copy-only state is absent from ordinary per-name directories.
	moves            map[*snapshotMetric]*snapshotMove
	sourceIdentities map[partOrigin]*PolicyCatalog
	partOverrides    map[*snapshotMetric][]*readPartMetric

	snapshot       *pebble.Snapshot
	directory      string
	routes         map[partRoute][]*readPart
	parts          map[string]*readPart
	metrics        map[string]*snapshotMetric
	cache          *RollupReadCache
	directoryBytes int64
	bindingSet     *RollupInventory
	inventory      map[string][]string // descriptor binding -> immutable routed part scopes
	hasCopied      bool
}

// Shared only by readers. Entries own scalar records and scope strings, never
// snapshots, metadata objects, writer arenas or an earlier cache generation.
type rollupRangeCache = RollupReadCache

type RollupReadCache struct {
	mu           sync.Mutex
	entries      map[archiveRange][]*archiveResult
	bytes, limit int64
	clock        uint64
	count        int
}

// NewRollupReadCache owns only bounded scalar records, never a generation pin.
func NewRollupReadCache(limit int64) *RollupReadCache {
	return &RollupReadCache{entries: map[archiveRange][]*archiveResult{}, limit: max(0, limit)}
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

type partRoute struct {
	group v2part.Group
	shard uint16
}

type readPart struct {
	// Bare-snapshot reads reuse the descriptor validated while loading this
	// immutable scope. Registry-backed reads already retain their opened parts.
	descriptor  *v2part.Descriptor
	first, last string
	id          string
	key         v2part.Key // immutable part scope; names come from the metric policy
	metrics     map[string]*readPartMetric
	bytes       int64
	origin      partOrigin
	imported    *partImport
}
type readMetricState struct {
	*PolicyIdentity
	meta metricMetadata
}
type readPartMetric struct {
	part *readPart
	readMetricState
}

// A generation owns one latest metadata value per name, independent of how
// many immutable parts contain it. No separate per-part metric object survives.
type snapshotMetric struct{ latest readMetricState }
type sourceMetadata struct {
	ID uint64
	metricMetadata
}
type snapshotMove struct {
	origin    partOrigin
	origins   map[partOrigin]sourceMetadata
	copied    bool
	authority *moveAuthority
}

// NewRollupSnapshot reuses immutable identities and the owner binding set;
// nil identities come from prior, nil bindings scan the snapshot.
func NewRollupSnapshot(ctx context.Context, snapshot *pebble.Snapshot, prior *RollupSnapshot, identities *PolicyCatalog, bindings *RollupInventory, cacheLimit ...int64) (*RollupSnapshot, error) {
	if identities == nil && prior != nil {
		identities = prior.identities
	}
	if identities == nil {
		identities = &PolicyCatalog{}
	}
	// Opened parts exist only for stores without copied parts. With them and
	// no legacy rows, the directory and its record reads use only registry-held
	// parts and the inventory, so snapshot may be nil.
	opened, _ := ctx.Value(openedPartsContext{}).(*OpenedRollupParts)
	if opened == nil {
		if found, err := snapshotHasCopiedParts(snapshot); err != nil {
			return nil, err
		} else if found {
			return newCopiedRollupSnapshot(ctx, snapshot, prior, identities, cacheLimit...)
		}
	}
	limit := int64(rollupReadCacheBytes)
	if len(cacheLimit) > 0 {
		limit = max(0, min(limit, cacheLimit[0]))
	}
	directory, _ := ctx.Value(archiveDirectoryContext{}).(string)
	out := &RollupSnapshot{opened: opened, identities: identities, snapshot: snapshot, directory: directory, bindingSet: bindings, parts: map[string]*readPart{}, metrics: map[string]*snapshotMetric{}}
	if prior != nil && prior.cache != nil && prior.cache.limit == limit {
		out.cache = prior.cache
	} else {
		out.cache = &RollupReadCache{entries: map[archiveRange][]*archiveResult{}, limit: limit}
	}

	// The descriptor namespace is published atomically with the data. Walking
	// these small adjacent keys avoids seeking every old scope through the SSTs.
	selected := map[string]v2part.Descriptor{}
	inventory, scopes, err := rollupInventory(ctx, snapshot, prior, bindings, selected)
	if err != nil {
		return nil, err
	}
	out.inventory = inventory
	prefix := []byte{'G', 'C', 'P', 3}
	var it *archiveIter
	if inventory == nil {
		it, err = newArchiveIter(ctx, snapshot, &pebble.IterOptions{LowerBound: prefix, UpperBound: v2part.PrefixEnd(prefix)})
		if err != nil {
			return nil, err
		}
		defer it.Close()
		// Bare GCP3 fixtures have no core frontier or atomic inventory
		// bindings. ID-keyed parts are only ever published with descriptors.
		for valid := it.First(); valid; {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			key, err := v2part.Decode(it.Key())
			if err != nil {
				return nil, err
			}
			scope, err := v2part.PartitionPrefixForGroup(key.Group, key.VShard, key.Partition)
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

	if prior != nil {
		live := make(map[string]bool, len(scopes))
		for _, id := range scopes {
			live[id] = true
		}
		for id := range prior.parts {
			if !live[id] {
				fresh, err := NewRollupSnapshot(ctx, snapshot, nil, identities, bindings, cacheLimit...)
				if err == nil && prior.cache != nil && prior.cache.limit == limit {
					fresh.cache = prior.cache
				}
				return fresh, err
			}
		}
	}

	// Latest metadata is the only name-sized read structure. Each newly loaded
	// part uses a transient sorted SST walk; its membership remains in the SST.
	// A part is merged and dropped before the next is read, so one decoded part
	// is live at a time. A part's own directory is read through only that file
	// (merging every live file per metadata seek makes catch-up quadratic); a
	// descriptor's scopes are adjacent in scopes, so one file is open at a time.
	if prior != nil {
		out.metrics = make(map[string]*snapshotMetric, len(prior.metrics))
		for name, m := range prior.metrics {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			out.metrics[name] = m
		}
	}
	out.routes = map[partRoute][]*readPart{}
	changed := false
	var partIt *archiveIter
	var partDigest v2part.Digest
	defer func() {
		if partIt != nil {
			_ = partIt.Close()
		}
	}()
	for _, id := range scopes {
		var part *readPart
		if prior != nil {
			part = prior.parts[id]
		}
		if descriptor, ok := selected[id]; part == nil && ok {
			if partIt == nil || partDigest != descriptor.SHA256 {
				if partIt != nil {
					err, partIt = partIt.Close(), nil
					if err != nil {
						return nil, err
					}
				}
				if partIt, err = newArchiveIterSelected(ctx, snapshot, nil, nil, descriptor); err != nil {
					return nil, err
				}
				partDigest = descriptor.SHA256
			}
			if part, err = readRollupPart(ctx, partIt, []byte(id), identities); err != nil {
				return nil, err
			}
			part.descriptor = &descriptor
		}
		if part == nil {
			if it == nil {
				return nil, errors.New("missing rollup scope descriptor")
			}
			part, err = readRollupPart(ctx, it, []byte(id), identities)
			if err != nil {
				return nil, err
			}
		}
		if out.parts[id] != nil {
			return nil, errors.New("duplicate rollup scope binding")
		}
		out.parts[id] = part
		route := partRoute{part.key.Group, part.key.VShard}
		out.routes[route] = append(out.routes[route], part)
		out.directoryBytes += part.bytes
		if prior != nil && prior.parts[id] != nil {
			continue
		}
		changed = true
		for name, pm := range part.metrics {
			old := out.metrics[name]
			latest := pm.readMetricState
			if old != nil {
				if old.latest.PolicyIdentity != pm.PolicyIdentity {
					return nil, errors.New("inconsistent rollup identity/policy")
				}
				if !pm.meta.after(old.latest.meta) {
					latest = old.latest
				}
			}
			out.metrics[name] = &snapshotMetric{latest: latest}
		}
		part.metrics = nil // retire every per-name/per-part object except latest metadata
	}
	if !changed && prior != nil {
		out.metrics = prior.metrics
	}
	out.directoryBytes += int64(len(out.metrics)) * int64(unsafe.Sizeof(snapshotMetric{})+64)

	for id, scopes := range out.inventory {
		out.directoryBytes += int64(64 + len(id) + len(scopes)*16)
	}
	return out, nil
}

// metricParts probes only the routed scopes whose sorted name bounds overlap.
// Common parts retain no series directory: metadata is decoded by the request's
// existing iterator. Use the request name instead of reverse-resolving its ID.
// Source-local authority is kept separately for moved names.
func (s *RollupSnapshot) metricParts(name string, m *snapshotMetric) []*readPartMetric {
	if parts, ok := s.partOverrides[m]; ok {
		return parts
	}
	scoped := s.routes[partRoute{m.latest.policy.Group, m.latest.vshard}]
	out := make([]*readPartMetric, 0, len(scoped))
	for _, p := range scoped {
		if name < p.first || name > p.last {
			continue
		}
		out = append(out, &readPartMetric{part: p, readMetricState: readMetricState{PolicyIdentity: m.latest.PolicyIdentity}})
	}
	return out
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
	_ = s.MetadataContext(context.Background(), visit)
}

// MetadataContext permits discovery to leave its publication promptly on cancel.
func (s *RollupSnapshot) MetadataContext(ctx context.Context, visit func(string, RollupPolicy, RollupMetadata)) error {
	for name, m := range s.metrics {
		if err := ctx.Err(); err != nil {
			return err
		}
		policy, meta := m.latest.metadataForName(name)
		visit(name, policy, meta)
	}
	return ctx.Err()
}

// LookupMetadata reads the immutable directory without constructing a second
// all-name map in each consumer generation.
func (s *RollupSnapshot) LookupMetadata(name string) (RollupPolicy, RollupMetadata, bool) {
	m := s.metrics[name]
	if m == nil {
		return RollupPolicy{}, RollupMetadata{}, false
	}
	policy, meta := m.latest.metadataForName(name)
	return policy, meta, true
}

// MetricVersion identifies an immutable name's complete part set and metadata.
// It owns no snapshot; equality proves archive records are unchanged, including
// reset, retirement and rewind. Consumers may retain it in bounded caches.
func (s *RollupSnapshot) MetricVersion(name string) any {
	if m := s.metrics[name]; m != nil {
		if s.opened != nil {
			return openedMetricVersion{state: m.latest, parts: s.metricPartIdentity(name, m)}
		}
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

	if m := s.metrics[name]; m != nil && !m.latest.meta.Expired && archive > 0 && archive < len(m.latest.schema().Archives) && first >= 0 && first < last && last <= m.latest.schema().Archives[archive].Count {
		if move := s.moves[m]; move != nil && move.authority != nil && move.authority.Version < 3 {
			return s.scanLegacyMoveArchive(ctx, name, archive, first, last, func(r sealrollup.Record, _ partOrigin) error {
				r.Sample.Sequence = r.Order
				return yield(r)
			})
		}
		return s.scanWindows(ctx, name, archive, first, last, yield)
	}
	return s.scanStoredArchive(ctx, name, archive, first, last, yield)
}
func (s *RollupSnapshot) scanStoredArchive(ctx context.Context, name string, archive int, first, last int64, yield func(sealrollup.Record) error) error {
	m := s.metrics[name]
	if m == nil || m.latest.meta.Expired || first >= last {
		return ctx.Err()
	}
	if archive < 0 || archive >= len(m.latest.schema().Archives) || first < 0 || last > m.latest.schema().Archives[archive].Count {
		return errors.New("invalid archive range")
	}
	return s.scanArchiveParts(ctx, name, archive, first, last, func(record sealrollup.Record, _ partOrigin) error {
		record.Sample.Sequence = record.Order
		return yield(record)
	})
}

// scanArchiveParts resolves immutable parts without applying move repairs. The
// move reader overlays its small repair set on this same cached range resolver.
func (s *RollupSnapshot) scanArchiveParts(ctx context.Context, name string, archive int, first, last int64, yield func(sealrollup.Record, partOrigin) error) error {
	m := s.metrics[name]
	// Reuse a cached full archive for covered ranges, preserving origins for
	// the move repair resolver as well as ordinary reads.
	archiveCount := m.latest.schema().Archives[archive].Count
	if first != 0 || last != archiveCount {
		s.cache.mu.Lock()
		covered := len(s.cache.entries[archiveRange{name, archive, 0, archiveCount}]) > 0
		s.cache.mu.Unlock()
		if covered {
			return s.scanArchiveParts(ctx, name, archive, 0, archiveCount, func(record sealrollup.Record, origin partOrigin) error {
				if record.Slot >= first && record.Slot < last {
					return yield(record, origin)
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
	partList := s.metricParts(name, m)
	parts := make(map[string]struct{}, len(partList))
	for _, part := range partList {
		parts[part.part.id] = struct{}{}
	}
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
			if s.recordVisible(m, cachedOrigin(cached.origins, record.Slot), record.Sample.Sequence, record.Order) {
				if err := yield(record, cachedOrigin(cached.origins, record.Slot)); err != nil {
					return err
				}
			}
		}
		return ctx.Err()
	}
	// Each part is searched on its own and its visible records are kept in
	// part order. A stable sort by slot then reduces every slot in the same
	// order a merged scan would, with no per-slot map.
	copied := s.moves[m] != nil && s.moves[m].copied
	var found []slotCandidate
	if cached != nil {
		for _, record := range cached.records {
			if origin := cachedOrigin(cached.origins, record.Slot); s.recordVisible(m, origin, record.Sample.Sequence, record.Order) {
				found = append(found, slotCandidate{record, origin})
			}
		}
	}
	var skip func(*readPartMetric) bool
	if cached != nil {
		skip = func(part *readPartMetric) bool { _, ok := cached.parts[part.part.id]; return ok }
	}
	if err := s.eachPartRecord(ctx, name, m, partList, archive, first, last, false, skip, func(record sealrollup.Record, origin partOrigin) error {
		found = append(found, slotCandidate{record, origin})
		return nil
	}); err != nil {
		return err
	}
	if !slices.IsSortedFunc(found, compareCandidateSlot) {
		slices.SortStableFunc(found, compareCandidateSlot)
	}
	var origins map[int64]partOrigin
	if copied {
		origins = map[int64]partOrigin{}
	}
	result := &archiveResult{origin: s.metricOrigin(m), frontier: m.latest.meta.Frontier.Order, revision: m.latest.meta.Revision, parts: parts, origins: origins, records: make([]sealrollup.Record, 0, len(found))}
	for i := 0; i < len(found); {
		winner := found[i]
		for i++; i < len(found) && found[i].record.Slot == winner.record.Slot; i++ {
			c := found[i]
			newer, err := newerOrigin(c.origin, c.record, winner.origin, winner.record)
			if err != nil {
				return err
			}
			if newer {
				winner = c
			} else if c.origin == winner.origin && c.record.Sample.Sequence == winner.record.Sample.Sequence && c.record.Sample.Time == winner.record.Sample.Time && c.record.Order == winner.record.Order && c.record.Delta != winner.record.Delta {
				return errors.New("conflicting rollup replacement order")
			}
		}
		result.records = append(result.records, winner.record)
		if origins != nil {
			origins[winner.record.Slot] = winner.origin
		}
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
		if err := yield(record, cachedOrigin(result.origins, record.Slot)); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// eachPartRecord visits the visible records of archive in physical slots
// [first, last) of every part in partList that skip does not exclude, in part
// order: each slot's per-slot winner per part, or with versions every stored
// version (kinds and Upto included).
func (s *RollupSnapshot) eachPartRecord(ctx context.Context, name string, m *snapshotMetric, partList []*readPartMetric, archive int, first, last int64, versions bool, skip func(*readPartMetric) bool, visit func(sealrollup.Record, partOrigin) error) error {
	archiveCount := m.latest.schema().Archives[archive].Count
	if s.directory != "" {
		ctx = WithRollupDirectory(ctx, s.directory)
	}
	var shared *archiveIter
	if s.opened == nil {
		// Copied and bare-fixture stores keep one merged iterator over the
		// legacy namespace and every selected file.
		selected := make(map[archivePartIdentity]bool, len(partList))
		var exact []v2part.Descriptor
		seen := make(map[v2part.Digest]bool, len(partList))
		complete := true
		for _, part := range partList {
			id := archivePartIdentity{part.part.key.Group, part.part.key.Part}
			selected[id] = true
			if d := part.part.descriptor; d != nil {
				if !seen[d.SHA256] {
					seen[d.SHA256] = true
					exact = append(exact, *d)
				}
			} else {
				complete = false
			}
		}
		// Legacy/copied scopes without retained descriptors keep the manifest
		// scan. Exact descriptors belong to this pinned immutable inventory.
		if !complete {
			exact = nil
		}
		var err error
		if shared, err = newArchiveIterSelected(ctx, s.snapshot, nil, selected, exact...); err != nil {
			return err
		}
		defer shared.Close()
	}
	// ID-keyed parts resolve the name to its dictionary ID once per routed
	// (group, vshard), not per part.
	ids := metricIDs{dict: s.identities.partDict(), known: m.latest.PolicyIdentity}
	scan := func(it archiveCursor, part *readPartMetric) error {
		origin := part.part.origin
		var seq uint64
		if b := part.part.imported; b != nil {
			seq = b.Descriptor.Source.Seq
		} else if d := part.part.descriptor; d != nil {
			seq = d.Source.Seq
		}
		ids := &ids
		if part.part.origin.Class == CopiedPart {
			ids = &metricIDs{known: part.PolicyIdentity}
		}
		if part.meta.Frontier.Order == 0 {
			key, ok, e := ids.key(part.part.key, v2part.ID(name))
			if e != nil || !ok {
				return e
			}
			key.Archive = v2part.MetadataArchive
			key.BlockStart = 0
			key.Ordinal = 0
			encoded, e := v2part.Encode(key)
			if e != nil {
				return e
			}
			if !it.SeekGE(encoded) || !bytes.Equal(it.Key(), encoded) {
				return nil
			}
			// compactMetadata keeps no definition: decode the header only, so
			// slim rows do not rebuild one per part.
			meta, _, e := decodeRollupMetadataHeader(it.Value())
			if e != nil {
				return e
			}
			part.meta = compactMetadata(meta)
		}
		if !s.recordVisible(m, origin, seq, part.meta.Frontier.Order) {
			return nil
		}
		key, ok, err := ids.key(part.part.key, v2part.ID(name))
		if err != nil || !ok {
			return err
		}
		key.Archive = uint32(archive)
		key.BlockStart = first
		lower, err := v2part.Encode(key)
		if err != nil {
			return err
		}
		prefix := lower[:archivePrefixLen(lower, key.ByID)]
		summaryAllowed := len(m.latest.schema().Archives) == 1 && (part.meta.Version >= 4 || part.meta.ResetOrder == 0) && (m.latest.meta.Version >= 4 || m.latest.meta.ResetOrder == 0)
		if move := s.moves[m]; move != nil && move.copied {
			meta := move.origins[origin]
			summaryAllowed = summaryAllowed && (meta.Version >= 4 || meta.ResetOrder == 0)
		}
		emit := func(record sealrollup.Record) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if record.Slot >= archiveCount || record.Order > part.meta.Frontier.Order {
				return errors.New("invalid rollup slot/frontier")
			}
			if record.Slot >= first && record.Slot < last && s.recordVisible(m, origin, seq, record.Order) {
				if versions && s.hasCopied {
					var err error
					record, err = moveVersionRecord(record, part.part.imported)
					if err != nil {
						return err
					}
				}
				// Decoding duplicates Order in Sample.Sequence. Reuse that scalar
				// for part precedence through read/cache/merge; public scans restore it.
				record.Sample.Sequence = seq
				if err := visit(record, origin); err != nil {
					return err
				}
			}
			return nil
		}
		// A run may straddle the lower bound. Never cross metric/archive/part.
		valid := it.SeekLT(lower)
		if !valid || !bytes.HasPrefix(it.Key(), prefix) {
			valid = it.SeekGE(lower)
		}
		for valid && bytes.HasPrefix(it.Key(), prefix) {
			if err := ctx.Err(); err != nil {
				return err
			}
			key, err = v2part.DecodeKey(it.Key())
			if err != nil {
				return err
			}
			if key.BlockStart >= last {
				break
			}
			if key.Ordinal != 0 {
				return errors.New("unexpected rollup ordinal")
			}
			value := it.Value()
			if v2part.HasOrderSummary(value) && !summaryAllowed {
				return errors.New("order summary under history policy or legacy reset")
			}
			if len(value) >= 4 && bytes.Equal(value[:4], []byte{'G', 'C', 'D', 1}) {
				var records []sealrollup.Record
				if versions {
					records, err = v2part.DecodeRecordsInto(nil, key.Archive, key.BlockStart, value)
				} else {
					records, err = v2part.DecodeSlots(key.Archive, key.BlockStart, value)
				}
				if err != nil {
					return err
				}
				if v2part.HasOrderSummary(value) && len(records) > 0 {
					d := part.part.descriptor
					if part.part.imported != nil {
						d = &part.part.imported.Descriptor
					}
					if d != nil && (records[0].Order < d.Source.MinOrder || records[0].Order > d.Source.MaxOrder) {
						return errors.New("order summary outside source frontier")
					}
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
		return nil
	}
	for _, part := range partList {
		if skip != nil && skip(part) {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if shared != nil {
			if err := scan(shared, part); err != nil {
				return err
			}
			continue
		}
		it, err := s.opened.partCursor(ctx, s.snapshot, archivePartIdentity{part.part.key.Group, part.part.key.Part})
		if err != nil {
			return err
		}
		if it == nil {
			continue
		}
		if err = errors.Join(scan(it, part), it.Error(), it.Close()); err != nil {
			return err
		}
	}
	if shared != nil {
		if err := shared.Error(); err != nil {
			return err
		}
	}
	return nil
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
	part := &readPart{origin: partOrigin{Class: LivePart}, id: id, metrics: map[string]*readPartMetric{}}
	end := v2part.PrefixEnd(scope)
	// Per-row scratch, reused across the part: the metadata key and a slim
	// row's definition only feed the seek and intern below.
	var encoded, definition []byte
	names := keyNames{dict: identities.partDict()}
	for it.Valid() && bytes.Compare(it.Key(), end) < 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key, err := v2part.DecodeKey(it.Key())
		if err != nil {
			return nil, err
		}
		key.Archive = v2part.MetadataArchive
		key.BlockStart = 0
		key.Ordinal = 0
		// A decoded key re-encodes to itself; only the tail (archive,
		// physical slot, ordinal) differs in the metadata key.
		raw := it.Key()
		tail := partKeyTail(key.ByID)
		encoded = appendEnvelopeKey(encoded[:0], raw, tail)
		// Most new metrics have one archive row immediately before metadata.
		// One Next avoids rebuilding the merging iterator for that common case.
		if !bytes.Equal(it.Key(), encoded) {
			it.Next()
		}
		if (!it.Valid() || !bytes.Equal(it.Key(), encoded)) && (!it.SeekGE(encoded) || !bytes.Equal(it.Key(), encoded)) {
			return nil, fmt.Errorf("missing rollup metadata for %s (ID %d)", key.Metric, key.Local)
		}
		meta, slim, err := decodeRollupMetadataHeader(it.Value())
		if err == nil {
			err = names.resolve(&key)
		}
		if err != nil {
			return nil, err
		}
		h, err := identities.readIdentity(meta, slim, key, &definition)
		if err != nil {
			return nil, err
		}
		if len(part.metrics) == 0 {
			part.key = key
			part.key.Metric, part.key.Local = "", 0
		}
		name := string(key.Metric)
		if part.first == "" || name < part.first {
			part.first = name
		}
		if name > part.last {
			part.last = name
		}
		part.metrics[name] = &readPartMetric{part: part, readMetricState: readMetricState{PolicyIdentity: h, meta: compactMetadata(meta)}}
		// The tail is archive, physical slot and (GCP3) ordinal.
		it.Next()
		if it.Valid() && bytes.HasPrefix(it.Key(), encoded[:len(encoded)-tail]) {
			return nil, errors.New("unexpected keys after rollup metadata")
		}
	}
	part.bytes = int64(unsafe.Sizeof(readPart{}) + uintptr(len(scope)) + 64)
	return part, it.Error()
}

func newCopiedRollupSnapshot(ctx context.Context, snapshot *pebble.Snapshot, prior *RollupSnapshot, identities *PolicyCatalog, cacheLimit ...int64) (*RollupSnapshot, error) {
	limit := int64(rollupReadCacheBytes)
	if len(cacheLimit) > 0 {
		limit = max(0, min(limit, cacheLimit[0]))
	}
	directory, _ := ctx.Value(archiveDirectoryContext{}).(string)
	out := &RollupSnapshot{identities: identities, partOverrides: map[*snapshotMetric][]*readPartMetric{}, snapshot: snapshot, directory: directory, parts: map[string]*readPart{}, metrics: map[string]*snapshotMetric{}, moves: map[*snapshotMetric]*snapshotMove{}, routes: map[partRoute][]*readPart{}, sourceIdentities: map[partOrigin]*PolicyCatalog{}}
	var err error
	out.hasCopied, err = snapshotHasCopiedParts(snapshot)
	if err != nil {
		return nil, err
	}
	if prior != nil && prior.cache != nil && prior.cache.limit == limit {
		out.cache = prior.cache
	} else {
		out.cache = &RollupReadCache{entries: map[archiveRange][]*archiveResult{}, limit: limit}
	}
	selected := map[string]v2part.Descriptor{}
	if _, _, err := rollupInventory(ctx, snapshot, nil, nil, selected); err != nil {
		return nil, err
	}
	for _, prefix := range partKeyRanges {
		it, err := newArchiveIter(ctx, snapshot, &pebble.IterOptions{LowerBound: prefix, UpperBound: v2part.PrefixEnd(prefix)})
		if err != nil {
			return nil, err
		}
		defer it.Close()
		for valid := it.First(); valid; {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			key, err := v2part.DecodeKey(it.Key())
			if err != nil {
				return nil, err
			}
			scope, err := scopePrefix(key.Group, key.VShard, key.Partition, key.ByID)
			if err != nil {
				return nil, err
			}
			scope = append(scope, key.Part[:]...)
			scopeID := string(scope)
			var part *readPart
			if prior != nil {
				part = prior.parts[scopeID]
			}
			if part != nil && part.metrics == nil {
				part = nil
			}
			if part == nil {
				part = &readPart{id: scopeID, key: key, metrics: map[string]*readPartMetric{}}
				part.key.Metric, part.key.Local = "", 0
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
				if origin.Class == CopiedPart {
					key, e := moveCatalogKey(key)
					if e != nil {
						return nil, e
					}
					b, closer, e := snapshot.Get(key)
					if e != nil {
						return nil, e
					}
					var binding partImport
					e = json.Unmarshal(b, &binding)
					_ = closer.Close()
					if e != nil {
						return nil, e
					}
					part.imported = &binding
				}
				if d, ok := selected[scopeID]; ok {
					part.descriptor = &d
				}
				part.origin = origin
				end := v2part.PrefixEnd(scope)
				for it.Valid() && bytes.Compare(it.Key(), end) < 0 {
					if err := ctx.Err(); err != nil {
						return nil, err
					}
					key, err = v2part.DecodeKey(it.Key())
					if err != nil {
						return nil, err
					}
					if origin.Class == CopiedPart {
						if err = part.imported.Dictionary.resolve(&key); err != nil {
							return nil, err
						}
					} else {
						if name, e := keyName(key, identities.partDict(), nil); e != nil {
							return nil, e
						} else {
							key.Metric = name
						}
					}
					key.Archive = v2part.MetadataArchive
					key.BlockStart = 0
					key.Ordinal = 0
					encoded := appendEnvelopeKey(nil, it.Key(), partKeyTail(key.ByID))
					if !it.SeekGE(encoded) || !bytes.Equal(it.Key(), encoded) {
						return nil, fmt.Errorf("missing rollup metadata for %s", key.Metric)
					}
					if origin.Class == CopiedPart {
						visible, err := copiedNameVisible(snapshot, key)
						if err != nil {
							return nil, err
						}
						if !visible {
							it.SeekGE(v2part.PrefixEnd(encoded[:len(encoded)-partKeyTail(key.ByID)]))
							continue
						}
					}
					meta, decodeErr := DecodeRollupMetadata(key.Metric, key.Group, it.Value())
					if err = decodeErr; err != nil {
						return nil, err
					}
					catalog := out.sourceIdentities[origin]
					if catalog == nil && prior != nil {
						catalog = prior.sourceIdentities[origin]
					}
					if catalog == nil {
						catalog = &PolicyCatalog{}
					}
					out.sourceIdentities[origin] = catalog
					h, err := catalog.intern(meta.ID, string(key.Metric), meta.Policy)
					if err != nil {
						return nil, err
					}
					if key.ByID {
						h.dictID.Store(key.Local + 1)
					}
					policy := h.rollupPolicy()
					if (meta.Version < 1 || meta.Version > 4) || meta.ID == 0 || meta.Frontier.Order == 0 || policy.Name != key.Metric || policy.Group != key.Group {
						return nil, errors.New("invalid rollup metadata")
					}
					if (meta.Version == 1 && (meta.ResetOrder != 0 || meta.Expired)) || meta.Version < 4 && meta.ResetOrder > meta.Frontier.Order || (meta.Expired && meta.Version < 4 && meta.ResetOrder != meta.Frontier.Order) {
						return nil, errors.New("invalid rollup reset frontier")
					}
					if h.policy.prepared.Digest() != meta.Frontier.Policy {
						return nil, errors.New("rollup policy/frontier mismatch")
					}
					name := string(policy.Name)
					if part.first == "" || name < part.first {
						part.first = name
					}
					if name > part.last {
						part.last = name
					}
					part.metrics[name] = &readPartMetric{part: part, readMetricState: readMetricState{PolicyIdentity: h, meta: compactMetadata(meta)}}
					// The last 16 bytes are archive, physical slot and ordinal.
					it.SeekGE(v2part.PrefixEnd(encoded[:len(encoded)-partKeyTail(key.ByID)]))
				}
			}
			out.parts[scopeID] = part
			out.routes[partRoute{part.key.Group, part.key.VShard}] = append(out.routes[partRoute{part.key.Group, part.key.VShard}], part)
			out.directoryBytes += int64(unsafe.Sizeof(readPart{}) + uintptr(len(scopeID)) + 64)
			for name, p := range part.metrics {
				m := out.metrics[name]
				if m == nil {
					m = &snapshotMetric{latest: p.readMetricState}
					out.moves[m] = &snapshotMove{origin: p.part.origin, origins: map[partOrigin]sourceMetadata{}}
					out.metrics[name] = m
				}
				out.partOverrides[m] = append(out.partOverrides[m], p)
				if m.latest.policy.Group != p.policy.Group || !bytes.Equal(m.latest.definitionBytes(), p.definitionBytes()) {
					return nil, errors.New("inconsistent rollup identity/policy")
				}
				out.moves[m].copied = out.moves[m].copied || p.part.origin.Class == CopiedPart
				old, seen := out.moves[m].origins[p.part.origin]
				if seen && old.ID != p.id {
					return nil, errors.New("source changed numeric metric identity")
				}
				if !seen || p.meta.after(old.metricMetadata) {
					out.moves[m].origins[p.part.origin] = sourceMetadata{ID: p.id, metricMetadata: p.meta}
				}
				newer, err := newerSource(p.part.origin, p.meta.Frontier.Order, out.moves[m].origin, m.latest.meta.Frontier.Order)
				if err != nil {
					return nil, err
				}
				if newer || (p.part.origin == out.moves[m].origin && p.meta.Frontier.Order == m.latest.meta.Frontier.Order && p.meta.after(m.latest.meta)) {
					m.latest, out.moves[m].origin = p.readMetricState, p.part.origin
				}
			}
			part.metrics = nil

			valid = it.SeekGE(v2part.PrefixEnd(scope))
		}
		if err = it.Error(); err != nil {
			return nil, err
		}
	}
	out.directoryBytes += int64(len(out.metrics)) * int64(unsafe.Sizeof(snapshotMetric{})+64+256)
	if err := out.loadMoveAuthorities(); err != nil {
		return nil, err
	}
	return out, nil
}

// Resets are source-local counters too. A receiver reset also masks all donor
// history, since every imported part precedes every local lifecycle event.
func (s *RollupSnapshot) recordVisible(m *snapshotMetric, origin partOrigin, seq, order uint64) bool {
	if s.moves[m] == nil || !s.moves[m].copied {
		return !m.latest.meta.Expired && !m.latest.meta.beforeReset(seq, order)
	}
	if s.moves[m].origin.Class == LivePart && origin.Class == CopiedPart && m.latest.meta.ResetOrder != 0 {
		return false
	}
	meta := s.moves[m].origins[origin]
	return !meta.Expired && !meta.metricMetadata.beforeReset(seq, order)
}

// slotCandidate is one visible stored record with the origin it came from.
type slotCandidate struct {
	record sealrollup.Record
	origin partOrigin
}

func compareCandidateSlot(a, b slotCandidate) int { return cmp.Compare(a.record.Slot, b.record.Slot) }

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

type openedMetricVersion struct {
	state readMetricState
	parts string
}

func (s *RollupSnapshot) metricPartIdentity(name string, m *snapshotMetric) string {
	parts := s.metricParts(name, m)
	ids := make([]string, 0, len(parts))
	for _, p := range parts {
		ids = append(ids, p.part.id)
	}
	slices.Sort(ids)
	var b strings.Builder
	for _, id := range ids {
		b.WriteString(id)
	}
	return b.String()
}
