package v2core

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// OpenedRollupParts owns a publication's immutable part references. Files and
// their SST indexes are opened once and shared with every surviving generation.
// Pebble snapshots remain the file-retirement fence.
type OpenedRollupParts struct {
	blockCache *pebble.Cache
	cacheBytes int64
	parts      map[string]*openedRollupPart
	routes     map[uint16][]*openedRollupPart
	byIdentity map[archivePartIdentity]*openedRollupPart
	directory  string
	identities *PolicyCatalog
}
type openedRollupPart struct {
	descriptor v2part.Descriptor
	refs       atomic.Int64
	gate       chan struct{}
	reader     *sstable.Reader
	openErr    error
	opened     bool
}

// OpenRollupParts performs no file I/O for ordinary descriptor publications.
// Legacy and copied-part manifests retain their existing authority resolver.
func OpenRollupParts(ctx context.Context, snapshot *pebble.Snapshot, inventory *RollupInventory, prior *OpenedRollupParts, identities *PolicyCatalog, cacheLimit ...int64) (*OpenedRollupParts, error) {
	if inventory == nil {
		return nil, nil
	}
	copied, err := snapshotHasCopiedParts(snapshot)
	if err != nil || copied {
		return nil, err
	}
	directory, _ := ctx.Value(archiveDirectoryContext{}).(string)
	if directory == "" {
		value, closer, e := snapshot.Get(rollupDirectoryKey)
		if e != nil {
			return nil, e
		}
		directory = string(value)
		if e = closer.Close(); e != nil {
			return nil, e
		}
	}
	if identities == nil {
		if prior != nil {
			identities = prior.identities
		} else {
			identities = &PolicyCatalog{}
		}
	}
	out := &OpenedRollupParts{parts: make(map[string]*openedRollupPart, len(inventory.bindings)), routes: map[uint16][]*openedRollupPart{}, byIdentity: map[archivePartIdentity]*openedRollupPart{}, directory: directory, identities: identities}
	if prior != nil {
		out.blockCache = prior.blockCache
		out.cacheBytes = prior.cacheBytes
		out.blockCache.Ref()
	} else {
		limit := int64(4 << 20)
		if len(cacheLimit) > 0 {
			limit = max(0, min(limit, cacheLimit[0]))
		}
		out.cacheBytes = limit
		out.blockCache = pebble.NewCacheWithShards(limit, 1)
	}
	ok := false
	defer func() {
		if !ok {
			out.Close()
		}
	}()
	for id, value := range inventory.bindings {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var p *openedRollupPart
		if prior != nil {
			p = prior.parts[id]
		}
		if p == nil {
			d, e := v2part.ParseDescriptor(value)
			if e != nil {
				return nil, e
			}
			if len(id) != len(sealedPartPrefix)+32 || !bytes.Equal([]byte(id)[len(sealedPartPrefix):], d.SHA256[:]) {
				return nil, errors.New("sealed descriptor binding mismatch")
			}
			if len(d.VShards) == 0 {
				return nil, nil
			}
			p = &openedRollupPart{descriptor: d, gate: make(chan struct{}, 1)}
			p.gate <- struct{}{}
		}
		p.refs.Add(1)
		out.parts[id] = p
		identity := archivePartIdentity{p.descriptor.Scope.Group, p.descriptor.Scope.Part}
		if out.byIdentity[identity] != nil {
			return nil, errors.New("duplicate sealed part identity")
		}
		out.byIdentity[identity] = p
		for _, v := range p.descriptor.VShards {
			out.routes[v] = append(out.routes[v], p)
		}
	}
	ok = true
	return out, nil
}
func (s *OpenedRollupParts) Close() {
	if s == nil {
		return
	}
	for _, p := range s.parts {
		if p.refs.Add(-1) == 0 && p.reader != nil {
			_ = p.reader.Close()
		}
	}
	s.blockCache.Unref()
}
func (p *openedRollupPart) open(ctx context.Context, directory string, cache *pebble.Cache) (_ *sstable.Reader, err error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.gate:
	}
	defer func() { p.gate <- struct{}{} }()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if p.opened {
		return p.reader, p.openErr
	}
	p.opened = true
	defer func() { p.openErr = err }()
	f, err := os.Open(filepath.Join(directory, "parts", hex.EncodeToString(p.descriptor.SHA256[:])+".sst"))
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil || uint64(stat.Size()) != p.descriptor.Bytes {
		_ = f.Close()
		if err != nil {
			return nil, err
		}
		return nil, errors.New("archive part size mismatch")
	}
	readable, err := sstable.NewSimpleReadable(f)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	p.reader, err = sstable.NewReader(readable, sstable.ReaderOptions{Cache: cache})
	return p.reader, err
}

// SnapshotForNames decodes only requested metadata entries in the name's exact
// vshard. It owns no shared maps and borrows the caller's generation pin.
func (s *OpenedRollupParts) SnapshotForNames(ctx context.Context, snapshot *pebble.Snapshot, names []string, cache *RollupReadCache) (*RollupSnapshot, error) {
	out := &RollupSnapshot{identities: s.identities, snapshot: snapshot, directory: s.directory, parts: map[string]*readPart{}, metrics: map[string]*snapshotMetric{}, routes: map[partRoute][]*readPart{}, opened: s, cache: cache}
	out.directoryBytes = s.cacheBytes + int64(len(s.parts))*256
	for _, name := range names {
		shard := v2part.Route(v2part.ID(name))
		for _, p := range s.routes[shard] {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			key := v2part.Key{Group: p.descriptor.Scope.Group, VShard: shard, Partition: p.descriptor.Scope.Partition, Part: p.descriptor.Scope.Part, Metric: v2part.ID(name), Archive: v2part.MetadataArchive}
			encoded, err := v2part.Encode(key)
			if err != nil {
				return nil, err
			}
			r, err := p.open(ctx, s.directory, s.blockCache)
			if err != nil {
				return nil, err
			}
			it, err := r.NewIterWithBlockPropertyFiltersAndContextEtc(ctx, nil, nil, nil, false, true, nil, sstable.TrivialReaderProvider{Reader: r})
			if err != nil {
				return nil, err
			}
			k, v := it.SeekGE(encoded, 0)
			if k == nil || !bytes.Equal(k.UserKey, encoded) {
				err = it.Error()
				_ = it.Close()
				if err != nil {
					return nil, err
				}
				continue
			}
			raw, _, err := v.Value(nil)
			if err != nil {
				_ = it.Close()
				return nil, err
			}
			meta, err := decodeRollupMetadataView(raw)
			if err != nil {
				_ = it.Close()
				return nil, err
			}
			h, err := s.identities.intern(meta.ID, name, meta.Policy)
			if err == nil {
				err = validateReadMetadata(h, meta, key)
			}
			if err != nil {
				_ = it.Close()
				return nil, err
			}
			state := readMetricState{PolicyIdentity: h, meta: compactMetadata(meta)}
			if err = it.Close(); err != nil {
				return nil, err
			}
			scope, _ := v2part.PartitionPrefixForGroup(key.Group, shard, key.Partition)
			scope = append(scope, key.Part[:]...)
			id := string(scope)
			if out.parts[id] == nil {
				key.Metric = ""
				rp := &readPart{id: id, key: key, first: name, last: name, origin: partOrigin{Class: LivePart}}
				out.parts[id] = rp
				route := partRoute{key.Group, shard}
				out.routes[route] = append(out.routes[route], rp)
			} else {
				rp := out.parts[id]
				if name < rp.first {
					rp.first = name
				}
				if name > rp.last {
					rp.last = name
				}
			}
			old := out.metrics[name]
			if old != nil && old.latest.PolicyIdentity != h {
				return nil, errors.New("inconsistent rollup identity/policy")
			}
			if old == nil || state.meta.Frontier.Order > old.latest.meta.Frontier.Order || (state.meta.Frontier.Order == old.latest.meta.Frontier.Order && state.meta.Revision > old.latest.meta.Revision) {
				out.metrics[name] = &snapshotMetric{latest: state}
			}
		}
	}
	return out, ctx.Err()
}
func validateReadMetadata(h *PolicyIdentity, meta RollupMetadata, key v2part.Key) error {
	if (meta.Version != 1 && meta.Version != 2) || meta.ID == 0 || meta.Frontier.Order == 0 || h.policy.Name != key.Metric || h.policy.Group != key.Group {
		return errors.New("invalid rollup metadata")
	}
	if (meta.Version == 1 && (meta.ResetOrder != 0 || meta.Expired)) || meta.ResetOrder > meta.Frontier.Order || (meta.Expired && meta.ResetOrder != meta.Frontier.Order) {
		return errors.New("invalid rollup reset frontier")
	}
	if h.policy.prepared.Digest() != meta.Frontier.Policy {
		return errors.New("rollup policy/frontier mismatch")
	}
	return nil
}

// tableCursor adapts a shared immutable SST reader to the existing archive merge.
// Each request owns its iterator and decoded value; the part owns the reader.
type tableCursor struct {
	it         sstable.Iterator
	key, value []byte
	err        error
}

func (c *tableCursor) First() bool {
	k, v := c.it.First()
	c.key = nil
	if k != nil {
		c.key = k.UserKey
		c.value, _, c.err = v.Value(nil)
	}
	return c.Valid()
}
func (c *tableCursor) SeekGE(key []byte) bool {
	k, v := c.it.SeekGE(key, 0)
	c.key = nil
	if k != nil {
		c.key = k.UserKey
		c.value, _, c.err = v.Value(nil)
	}
	return c.Valid()
}
func (c *tableCursor) SeekLT(key []byte) bool {
	k, v := c.it.SeekLT(key, 0)
	c.key = nil
	if k != nil {
		c.key = k.UserKey
		c.value, _, c.err = v.Value(nil)
	}
	return c.Valid()
}
func (c *tableCursor) Next() bool {
	k, v := c.it.Next()
	c.key = nil
	if k != nil {
		c.key = k.UserKey
		c.value, _, c.err = v.Value(nil)
	}
	return c.Valid()
}
func (c *tableCursor) Valid() bool   { return c.key != nil && c.err == nil }
func (c *tableCursor) Key() []byte   { return c.key }
func (c *tableCursor) Value() []byte { return c.value }
func (c *tableCursor) Error() error  { return errors.Join(c.err, c.it.Error()) }
func (c *tableCursor) Close() error  { return c.it.Close() }
func (s *OpenedRollupParts) archiveIter(ctx context.Context, snapshot *pebble.Snapshot, selected map[archivePartIdentity]bool) (_ *archiveIter, err error) {
	out := &archiveIter{selected: -1}
	defer func() {
		if err != nil {
			_ = out.Close()
		}
	}()
	legacy, err := snapshot.NewIter(nil)
	if err != nil {
		return nil, err
	}
	out.iterators = append(out.iterators, legacy)
	// Routes already pruned the store. Do not enumerate descriptor bindings here.
	seen := map[*openedRollupPart]bool{}
	for _, p := range s.routesForSelection(selected) {
		if seen[p] {
			continue
		}
		seen[p] = true
		r, e := p.open(ctx, s.directory, s.blockCache)
		if e != nil {
			return nil, e
		}
		it, e := r.NewIterWithBlockPropertyFiltersAndContextEtc(ctx, nil, nil, nil, false, true, nil, sstable.TrivialReaderProvider{Reader: r})
		if e != nil {
			return nil, e
		}
		out.iterators = append(out.iterators, &tableCursor{it: it})
	}
	return out, nil
}
func (s *OpenedRollupParts) routesForSelection(selected map[archivePartIdentity]bool) []*openedRollupPart {
	var out []*openedRollupPart
	for id := range selected {
		if p := s.byIdentity[id]; p != nil {
			out = append(out, p)
		}
	}
	return out
}

type openedPartsContext struct{}

// WithOpenedRollupParts shares file/index ownership with discovery's incremental
// metadata walk. Its caller must retain the corresponding publication.
func WithOpenedRollupParts(ctx context.Context, parts *OpenedRollupParts) context.Context {
	return context.WithValue(ctx, openedPartsContext{}, parts)
}
