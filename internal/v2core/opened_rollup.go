package v2core

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// OpenedRollupParts owns a publication's part references, taken from the
// owner's registry: files and SST indexes open once per owner and stay open
// while any publication holds them.
type OpenedRollupParts struct {
	registry   *partRegistry
	blockCache *pebble.Cache
	cacheBytes int64
	parts      map[string]*openedRollupPart
	routes     map[uint16][]*openedRollupPart
	byIdentity map[archivePartIdentity]*openedRollupPart
	directory  string
	identities *PolicyCatalog
	// legacyRows records whether the opening snapshot holds GCP3 rows in the
	// LSM itself. Core never ingests rollup rows there, so reads skip the
	// Pebble iterator unless an older store or fixture put rows in it.
	legacyRows bool
}
type openedRollupPart struct {
	descriptor v2part.Descriptor
	refs       int64 // guarded by partRegistry.mu
	gate       chan struct{}
	reader     *sstable.Reader
	openErr    error
	opened     bool
}

// partRegistry holds one owner's opened parts with their reference counts.
// The part GC unlinks a retired file only when no publication holds it, as
// VictoriaMetrics drops a partWrapper at refCount == 0 && mustDrop.
type partRegistry struct {
	mu    sync.Mutex
	parts map[v2part.Digest]*openedRollupPart
}

// acquire references the part bound under key, parsing its descriptor only
// when no publication holds it. A part without vshards returns nil.
func (r *partRegistry) acquire(key string, value []byte) (*openedRollupPart, error) {
	if len(key) != len(sealedPartPrefix)+32 {
		return nil, errors.New("sealed descriptor binding mismatch")
	}
	var id v2part.Digest
	copy(id[:], key[len(sealedPartPrefix):])
	r.mu.Lock()
	defer r.mu.Unlock()
	if p := r.parts[id]; p != nil {
		p.refs++
		return p, nil
	}
	d, err := v2part.ParseDescriptor(value)
	if err != nil {
		return nil, err
	}
	if d.SHA256 != id {
		return nil, errors.New("sealed descriptor binding mismatch")
	}
	if len(d.VShards) == 0 {
		return nil, nil
	}
	p := &openedRollupPart{descriptor: d, gate: make(chan struct{}, 1), refs: 1}
	p.gate <- struct{}{}
	if r.parts == nil {
		r.parts = map[v2part.Digest]*openedRollupPart{}
	}
	r.parts[id] = p
	return p, nil
}

func (r *partRegistry) release(p *openedRollupPart) {
	r.mu.Lock()
	p.refs--
	last := p.refs == 0
	if last {
		delete(r.parts, p.descriptor.SHA256)
	}
	r.mu.Unlock()
	if last && p.reader != nil {
		_ = p.reader.Close()
	}
}

func (r *partRegistry) held(id v2part.Digest) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.parts[id] != nil
}

// OpenRollupParts opens parts on a caller-owned block cache that several
// owners may share; each part takes a reference until Close. Pebble keys
// cached blocks by a per-reader cache ID, so parts never alias. accountBytes
// is the share charged to reads. It performs no file I/O. A nil result (no
// inventory, copied parts, or a part without vshards) reads the snapshot.
func OpenRollupParts(ctx context.Context, snapshot *pebble.Snapshot, inventory *RollupInventory, identities *PolicyCatalog, cache *pebble.Cache, accountBytes int64) (*OpenedRollupParts, error) {
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
		identities = &PolicyCatalog{}
	}
	legacyRows, err := snapshotHasLegacyRows(snapshot)
	if err != nil {
		return nil, err
	}
	out := &OpenedRollupParts{registry: inventory.registry, blockCache: cache, cacheBytes: accountBytes, parts: make(map[string]*openedRollupPart, len(inventory.bindings)), routes: map[uint16][]*openedRollupPart{}, byIdentity: map[archivePartIdentity]*openedRollupPart{}, directory: directory, identities: identities, legacyRows: legacyRows}
	cache.Ref()
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
		p, err := out.registry.acquire(id, value)
		if err != nil || p == nil {
			return nil, err
		}
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

// NeedsSnapshot reports whether reads still go through the opening snapshot:
// stores with legacy LSM rows, copied parts or no inventory (nil parts). Other
// readers close the snapshot once parts are open; registry references keep
// the files.
func (s *OpenedRollupParts) NeedsSnapshot() bool { return s == nil || s.legacyRows }

// Retain takes the caller's own registry reference on every part, for a
// publication that binds the same parts. The caller holds s; the immutable
// routing maps are shared.
func (s *OpenedRollupParts) Retain() *OpenedRollupParts {
	if s == nil {
		return nil
	}
	out := *s
	s.registry.mu.Lock()
	for _, p := range s.parts {
		p.refs++
	}
	s.registry.mu.Unlock()
	s.blockCache.Ref()
	return &out
}

func (s *OpenedRollupParts) Close() {
	if s == nil {
		return
	}
	for _, p := range s.parts {
		s.registry.release(p)
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
	var definition []byte
	for _, name := range names {
		shard := v2part.Route(v2part.ID(name))
		ids := metricIDs{dict: s.identities.partDict()}
		for _, p := range s.routes[shard] {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			key := v2part.Key{Group: p.descriptor.Scope.Group, VShard: shard, Partition: p.descriptor.Scope.Partition, Part: p.descriptor.Scope.Part, ByID: descriptorByID(p.descriptor)}
			key, ok, err := ids.key(key, v2part.ID(name))
			if err != nil {
				return nil, err
			}
			if !ok {
				continue // no ID: no ID-keyed part holds the name
			}
			key.Archive = v2part.MetadataArchive
			encoded, err := v2part.Encode(key)
			if err != nil {
				return nil, err
			}
			key.Metric = v2part.ID(name)
			c, err := p.cursor(ctx, s.directory, s.blockCache)
			if err != nil {
				return nil, err
			}
			if !c.SeekGE(encoded) || !bytes.Equal(c.Key(), encoded) {
				err = c.Error()
				_ = c.Close()
				if err != nil {
					return nil, err
				}
				continue
			}
			raw := c.Value()
			meta, slim, err := decodeRollupMetadataHeader(raw)
			if err != nil {
				_ = c.Close()
				return nil, err
			}
			h, err := s.identities.readIdentity(meta, slim, key, &definition)
			if err != nil {
				_ = c.Close()
				return nil, err
			}
			state := readMetricState{PolicyIdentity: h, meta: compactMetadata(meta)}
			if err = c.Close(); err != nil {
				return nil, err
			}
			scope, _ := scopePrefix(key.Group, shard, key.Partition, key.ByID)
			scope = append(scope, key.Part[:]...)
			id := string(scope)
			if out.parts[id] == nil {
				key.Metric, key.Local = "", 0
				rp := &readPart{id: id, key: key, first: name, last: name, origin: partOrigin{Class: LivePart}, descriptor: &p.descriptor}
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
			if old == nil || state.meta.after(old.latest.meta) {
				out.metrics[name] = &snapshotMetric{latest: state}
			}
		}
	}
	return out, ctx.Err()
}

// readIdentity reuses a slim row's validated dictionary identity and digest.
// Full definitions and the first encounter still pass the canonical byte oracle.
func (c *PolicyCatalog) readIdentity(meta RollupMetadata, slim bool, key v2part.Key, definition *[]byte) (*PolicyIdentity, error) {
	h := c.lookup(meta.ID)
	known := key.ByID && h != nil && h.dictID.Load() != 0
	if known && (h.policy.Group != key.Group || h.vshard != key.VShard || h.dictID.Load() != key.Local+1) {
		return nil, errors.New("inconsistent rollup dictionary identity")
	}
	var err error
	if !slim || !known {
		if slim {
			*definition, err = appendRollupDefinition((*definition)[:0], key.Metric, key.Group, meta.Frontier.Policy)
			meta.Policy = *definition
		}
		if err == nil {
			h, err = c.intern(meta.ID, string(key.Metric), meta.Policy)
		}
	}
	if err == nil {
		err = validateReadMetadata(h, meta, key)
	}
	if err != nil {
		return nil, err
	}
	if key.ByID && !known {
		h.rememberDictID(key.Local)
	}
	return h, nil
}

func validateReadMetadata(h *PolicyIdentity, meta RollupMetadata, key v2part.Key) error {
	if (meta.Version < 1 || meta.Version > 4) || meta.ID == 0 || meta.Frontier.Order == 0 || h.policy.Group != key.Group || h.vshard != key.VShard || (!(key.ByID && h.dictID.Load() == key.Local+1) && !h.matchesName(string(key.Metric))) {
		return errors.New("invalid rollup metadata")
	}
	if (meta.Version == 1 && (meta.ResetOrder != 0 || meta.Expired)) || meta.Version < 4 && meta.ResetOrder > meta.Frontier.Order || (meta.Expired && meta.Version < 4 && meta.ResetOrder != meta.Frontier.Order) {
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
	if s.legacyRows {
		legacy, err := snapshot.NewIter(nil)
		if err != nil {
			return nil, err
		}
		out.iterators = append(out.iterators, newHeaderCursor(legacy, nil))
	}
	// Routes already pruned the store. Do not enumerate descriptor bindings here.
	seen := map[*openedRollupPart]bool{}
	for _, p := range s.routesForSelection(selected) {
		if seen[p] {
			continue
		}
		seen[p] = true
		c, e := p.cursor(ctx, s.directory, s.blockCache)
		if e != nil {
			return nil, e
		}
		out.iterators = append(out.iterators, c)
	}
	return out, nil
}

// partCursor reads one part on its own, as VM searches each part
// independently. Legacy LSM rows, when present, merge in with the
// conflicting-duplicate check. A nil cursor means the part has no rows.
func (s *OpenedRollupParts) partCursor(ctx context.Context, snapshot *pebble.Snapshot, id archivePartIdentity) (archiveCursor, error) {
	var out archiveIter
	out.selected = -1
	if s.legacyRows {
		legacy, err := snapshot.NewIter(nil)
		if err != nil {
			return nil, err
		}
		out.iterators = append(out.iterators, newHeaderCursor(legacy, nil))
	}
	if p := s.byIdentity[id]; p != nil {
		c, err := p.cursor(ctx, s.directory, s.blockCache)
		if err != nil {
			return nil, errors.Join(err, out.Close())
		}
		if !s.legacyRows {
			return c, nil
		}
		out.iterators = append(out.iterators, c)
	}
	if len(out.iterators) == 0 {
		return nil, nil
	}
	return &out, nil
}

// cursor reads one part through headerCursor, so a folded envelope reads at
// its own key as in parts that store it separately.
func (p *openedRollupPart) cursor(ctx context.Context, directory string, cache *pebble.Cache) (*headerCursor, error) {
	r, err := p.open(ctx, directory, cache)
	if err != nil {
		return nil, err
	}
	it, err := r.NewIterWithBlockPropertyFiltersAndContextEtc(ctx, nil, nil, nil, false, true, nil, sstable.TrivialReaderProvider{Reader: r})
	if err != nil {
		return nil, err
	}
	return newHeaderCursor(&tableCursor{it: it}, nil), nil
}

// snapshotHasLegacyRows probes the LSM's own GCP3 namespace once per opening.
func snapshotHasLegacyRows(snapshot *pebble.Snapshot) (bool, error) {
	prefix := []byte{'G', 'C', 'P', 3}
	it, err := snapshot.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: v2part.PrefixEnd(prefix)})
	if err != nil {
		return false, err
	}
	found := it.First()
	return found, errors.Join(it.Error(), it.Close())
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
