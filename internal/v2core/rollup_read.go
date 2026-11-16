package v2core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"unsafe"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// RollupSnapshot is a directory over immutable part keys in a pinned snapshot.
// Building it seeks over archive values. A prior directory can share immutable
// per-part metadata; it never shares a snapshot or a writer-owned object.
// The caller retains snapshot until all directory consumers have returned.
type RollupSnapshot struct {
	snapshot       *pebble.Snapshot
	parts          map[string]*readPart
	metrics        map[string]*snapshotMetric
	cache          *rollupRangeCache
	directoryBytes int64
}

// Shared only by readers. Entries own scalar records and scope strings, never
// snapshots, metadata objects, writer arenas or an earlier cache generation.
type rollupRangeCache struct {
	mu           sync.Mutex
	entries      map[archiveRange]*archiveResult
	bytes, limit int64
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
	parts              map[string]struct{}
	bytes              int64
	frontier, revision uint64
}

type readPart struct{ metrics map[string]*readPartMetric }
type readPartMetric struct {
	id     string
	key    v2part.Key
	policy RollupPolicy
	meta   RollupMetadata
}
type snapshotMetric struct {
	policy RollupPolicy
	meta   RollupMetadata
	parts  []*readPartMetric
}

func NewRollupSnapshot(ctx context.Context, snapshot *pebble.Snapshot, prior *RollupSnapshot, cacheLimit ...int64) (*RollupSnapshot, error) {
	limit := int64(rollupReadCacheBytes)
	if len(cacheLimit) > 0 {
		limit = max(0, min(limit, cacheLimit[0]))
	}
	out := &RollupSnapshot{snapshot: snapshot, parts: map[string]*readPart{}, metrics: map[string]*snapshotMetric{}}
	if prior != nil && prior.cache.limit == limit {
		out.cache = prior.cache
	} else {
		out.cache = &rollupRangeCache{entries: map[archiveRange]*archiveResult{}, limit: limit}
	}
	prefix := []byte{'G', 'C', 'P', 3}
	it, err := snapshot.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: v2part.PrefixEnd(prefix)})
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
			part = &readPart{metrics: map[string]*readPartMetric{}}
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
				var meta RollupMetadata
				if err = json.Unmarshal(it.Value(), &meta); err != nil {
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
				part.metrics[string(key.Metric)] = &readPartMetric{id: scopeID, key: key, policy: policy, meta: meta}
				// The last 16 bytes are archive, physical slot and ordinal.
				it.SeekGE(v2part.PrefixEnd(encoded[:len(encoded)-16]))
			}
		}
		out.parts[scopeID] = part
		out.directoryBytes += int64(128 + len(scopeID))
		for name, pm := range part.metrics {
			out.directoryBytes += int64(512 + len(name)*2 + len(pm.meta.Policy)*2 + len(pm.policy.Group)*2 + len(pm.policy.Config.Archives)*32)
		}
		for name, p := range part.metrics {
			m := out.metrics[name]
			if m == nil {
				m = &snapshotMetric{policy: p.policy, meta: p.meta}
				out.metrics[name] = m
			}
			if m.policy.Group != p.policy.Group || m.meta.ID != p.meta.ID || !bytes.Equal(m.meta.Policy, p.meta.Policy) {
				return nil, errors.New("inconsistent rollup identity/policy")
			}
			if p.meta.Frontier.Order > m.meta.Frontier.Order || (p.meta.Frontier.Order == m.meta.Frontier.Order && p.meta.Revision > m.meta.Revision) {
				m.meta = p.meta
			}
			m.parts = append(m.parts, p)
		}
		valid = it.SeekGE(v2part.PrefixEnd(scope))
	}
	if err = it.Error(); err != nil {
		return nil, err
	}
	return out, nil
}

// RetainedBytes conservatively charges directory storage plus the maximum
// bounded range cache. Shared immutable entries are charged independently by
// each pinned generation; SST and pending-arena accounting remains external.
func (s *RollupSnapshot) RetainedBytes() int64 { return s.directoryBytes + s.cache.limit }

// Metadata visits only the directory, never archive values.
func (s *RollupSnapshot) Metadata(visit func(string, RollupPolicy, RollupMetadata)) {
	for name, m := range s.metrics {
		visit(name, m.policy, m.meta)
	}
}

// ScanArchive seeks only the named archive's physical interval in each part.
// A predecessor seek includes dense runs beginning before first. The same
// iterator/decoder is reused across parts; singleton records decode on the stack.
// Winners resolve by order before timestamp filters. A bounded read-side cache
// carries only immutable results and part identities across generations. New
// snapshots decode only added parts; removed parts invalidate cached authority.
func (s *RollupSnapshot) ScanArchive(ctx context.Context, name string, archive int, first, last int64, yield func(sealrollup.Record) error) error {
	m := s.metrics[name]
	if m == nil || m.meta.Expired || first >= last {
		return ctx.Err()
	}
	if archive < 0 || archive >= len(m.policy.Config.Archives) || first < 0 || last > m.policy.Config.Archives[archive].Count {
		return errors.New("invalid archive range")
	}
	cacheKey := archiveRange{name, archive, first, last}
	s.cache.mu.Lock()
	cached := s.cache.entries[cacheKey]
	s.cache.mu.Unlock()
	// Exact cached coverage is common for multiple readers of one publication.
	if cached != nil && len(cached.parts) == len(m.parts) {
		exact := true
		for _, part := range m.parts {
			if _, ok := cached.parts[part.id]; !ok {
				exact = false
				break
			}
		}
		if exact {
			for _, record := range cached.records {
				if record.Order > m.meta.ResetOrder {
					if err := yield(record); err != nil {
						return err
					}
				}
			}
			return ctx.Err()
		}
	}
	winners := map[int64]sealrollup.Record{}
	parts := make(map[string]struct{}, len(m.parts))
	for _, part := range m.parts {
		parts[part.id] = struct{}{}
	}
	if cached != nil {
		for part := range cached.parts {
			if _, ok := parts[part]; !ok {
				cached = nil
				break
			}
		}
	}
	if cached != nil {
		for _, record := range cached.records {
			if record.Order > m.meta.ResetOrder {
				winners[record.Slot] = record
			}
		}
	}
	it, err := s.snapshot.NewIter(nil)
	if err != nil {
		return err
	}
	defer it.Close()
	for _, part := range m.parts {
		if cached != nil {
			if _, ok := cached.parts[part.id]; ok {
				continue
			}
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		if part.meta.Frontier.Order <= m.meta.ResetOrder {
			continue
		}
		key := part.key
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
				if record.Slot >= m.policy.Config.Archives[archive].Count || record.Order > m.meta.Frontier.Order {
					return errors.New("invalid rollup slot/frontier")
				}
				if record.Slot >= first && record.Slot < last && record.Order > m.meta.ResetOrder {
					if old, ok := winners[record.Slot]; !ok || record.Order > old.Order {
						winners[record.Slot] = record
					} else if record.Order == old.Order && record.Delta != old.Delta {
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
	result := &archiveResult{frontier: m.meta.Frontier.Order, revision: m.meta.Revision, parts: parts, records: make([]sealrollup.Record, 0, len(winners))}
	for _, record := range winners {
		result.records = append(result.records, record)
	}
	result.bytes = int64(256+len(name)) + int64(len(result.records))*int64(unsafe.Sizeof(sealrollup.Record{}))
	for id := range parts {
		result.bytes += int64(64 + len(id))
	}
	s.cache.mu.Lock()
	old := s.cache.entries[cacheKey]
	// A slow old-snapshot query must not evict a more recent result. Scope
	// membership still validates reuse after removals and compaction.
	if old == nil || result.frontier > old.frontier || (result.frontier == old.frontier && result.revision >= old.revision) {
		if old != nil {
			delete(s.cache.entries, cacheKey)
			s.cache.bytes -= old.bytes
		}
		if result.bytes <= s.cache.limit {
			if len(s.cache.entries) >= rollupReadCacheEntries || s.cache.bytes+result.bytes > s.cache.limit {
				clear(s.cache.entries)
				s.cache.bytes = 0
			}
			s.cache.entries[cacheKey] = result
			s.cache.bytes += result.bytes
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
	for archive, a := range m.policy.Config.Archives {
		err := s.ScanArchive(ctx, name, archive, 0, a.Count, func(r sealrollup.Record) error { records = append(records, r); return nil })
		if err != nil {
			return nil, err
		}
	}
	return records, nil
}
