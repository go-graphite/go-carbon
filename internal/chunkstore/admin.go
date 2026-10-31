package chunkstore

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
)

var ErrConflict = errors.New("metric changed or configuration is incompatible")

// Archive preserves one physical retention without applying rollups.
type Archive struct {
	Retention Retention
	Points    []Point
}

// Snapshot is an archive-preserving, point-in-time metric transfer unit.
type Snapshot struct {
	Metadata Metadata
	Archives []Archive
	// SourceState is an immutable, fenced source used only during in-process
	// conversion. It is never persisted, exported over the transfer API or used
	// by ordinary reads after ownership is published.
	SourceState string `json:"-"`
}

// PageLister is the catalog paging contract implemented by Store.ListPage.
type PageLister func(ctx context.Context, prefix, after string, limit int) ([]Metadata, error)

// EachPage walks the catalog under prefix in limit-sized pages and calls fn
// for each page until the catalog is exhausted or fn returns an error. Pages
// are read successively, so concurrent catalog changes may affect entries
// between pages.
func EachPage(ctx context.Context, list PageLister, prefix string, limit int, fn func([]Metadata) error) error {
	after := ""
	for {
		page, err := list(ctx, prefix, after, limit)
		if err != nil {
			return err
		}
		if err := fn(page); err != nil {
			return err
		}
		if len(page) < limit {
			return nil
		}
		after = page[len(page)-1].Name
	}
}

// CatalogHighWatermark bounds the IDs present at this instant. Catalog IDs are
// monotonic, so metrics created later cannot inherit a baseline's eligibility.
func (s *Store) CatalogHighWatermark(ctx context.Context) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	data, closer, err := s.db.Get(sequenceKey())
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer closer.Close()
	if len(data) != 8 {
		return 0, errors.New("invalid catalog sequence")
	}
	return binary.BigEndian.Uint64(data), nil
}

// List returns every metric whose name starts with prefix.
func (s *Store) List(ctx context.Context, prefix string) ([]Metadata, error) {
	var result []Metadata
	err := EachPage(ctx, s.ListPage, prefix, 10000, func(page []Metadata) error {
		result = append(result, page...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ListPage returns a snapshot-consistent catalog page, exclusive of after.
func (s *Store) ListPage(ctx context.Context, prefix, after string, limit int) ([]Metadata, error) {
	return s.listPage(ctx, prefix, after, limit, true)
}

// ListCatalogPage lists names and policies for discovery, without loading
// mutable revisions or activity times. Use ListPage for conditional mutations.
func (s *Store) ListCatalogPage(ctx context.Context, prefix, after string, limit int) ([]Metadata, error) {
	return s.listPage(ctx, prefix, after, limit, false)
}

func (s *Store) listPage(ctx context.Context, prefix, after string, limit int, revisions bool) ([]Metadata, error) {
	if err := s.fileOwnershipError(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit < 1 || limit > 10000 {
		return nil, errors.New("catalog page limit must be 1..10000")
	}
	lower, upper := catalogPageBounds(prefix, after)
	if upper == nil || strings.Compare(string(lower), string(upper)) >= 0 {
		return []Metadata{}, nil
	}
	snap := s.db.NewSnapshot()
	defer snap.Close()
	it, err := snap.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, fmt.Errorf("open catalog page: %w", err)
	}
	defer it.Close()
	// Reuse the revision iterator while deletes change the live memtables.
	reader := transferReader{reader: snap}
	defer reader.close()
	result := make([]Metadata, 0, limit)
	for it.First(); it.Valid() && len(result) < limit; it.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		m, err := decodeCatalog(string(it.Key()[2:]), it.Value())
		if err == nil && revisions {
			m, err = reader.revision(m)
		}
		if err != nil {
			return nil, err
		}
		if err := s.checkFileOwnership(m); errors.Is(err, ErrNotFound) {
			continue
		} else if err != nil {
			return nil, err
		}
		result = append(result, m)
	}
	if err := it.Error(); err != nil {
		return nil, fmt.Errorf("read catalog page: %w", err)
	}
	return result, nil
}

func catalogPageBounds(prefix, after string) ([]byte, []byte) {
	lower := catalogKey(prefix)
	if after != "" && strings.Compare(after, prefix) >= 0 {
		lower = append(catalogKey(after), 0)
	}
	return lower, prefixEnd(catalogKey(prefix))
}

func (s *Store) Delete(ctx context.Context, name string) error { return s.deleteMetric(ctx, name, nil) }

// PurgeInvalidNames removes every catalog record whose name metric creation
// now refuses (see ErrNameTooLong) and compacts that key range, returning how
// many it removed. Such names entered the catalog before the limit existed;
// their keys bloat the index of every table that holds them. Only the catalog
// and revision records are deleted, with point deletes: deleting each metric
// whole writes range tombstones, and thousands of those in a memtable make
// every reader re-fragment them. The metrics' data keys are short and are left
// unreferenced. The compaction then rewrites only the tables over that key
// range, dropping the long keys and their tombstones.
func (s *Store) PurgeInvalidNames(ctx context.Context) (int, error) {
	s.purgeRunning.Store(true)
	defer s.purgeRunning.Store(false)
	view := s.db.NewSnapshot()
	iter, err := view.NewIter(&pebble.IterOptions{LowerBound: []byte("m/"), UpperBound: []byte("m0")})
	if err != nil {
		view.Close()
		return 0, err
	}
	type record struct {
		name  string
		value []byte
	}
	var records []record
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			break
		}
		if name := string(iter.Key()[2:]); validateName(name) != nil {
			records = append(records, record{name, bytes.Clone(iter.Value())})
		}
	}
	err = errors.Join(ctx.Err(), iter.Error(), iter.Close(), view.Close())
	if err != nil {
		return 0, err
	}
	if len(records) == 0 {
		// An earlier run may have deleted them and stopped before its
		// compaction; the deleted long keys still bloat those tables' indexes.
		return 0, s.compactRange([]byte("m/"), []byte("m0"))
	}
	s.purgeFound.Store(uint64(len(records)))
	first, last := catalogKey(records[0].name), catalogKey(records[len(records)-1].name)
	// One commit per stripe, under that stripe's lock: a commit per record
	// queues each behind every concurrent writer in the commit pipeline.
	byStripe := make(map[uint64][]record)
	for _, r := range records {
		stripe := s.metricStripe(r.name)
		byStripe[stripe] = append(byStripe[stripe], r)
	}
	removed := 0
	for _, group := range byStripe {
		if err := ctx.Err(); err != nil {
			return removed, err
		}
		unlock := s.lockMetric(group[0].name)
		for _, r := range group {
			s.beginStateMutation(r.name)
		}
		b := s.db.NewBatch()
		var err error
		for _, r := range group {
			m, decodeErr := decodeCatalog(r.name, r.value)
			if err = b.Delete(catalogKey(r.name), nil); err == nil && decodeErr == nil {
				err = b.Delete(revisionKey(m), nil)
			}
			if err == nil {
				err = b.Delete(sidecarCatalogKey(r.name), nil)
			}
			if err != nil {
				break
			}
		}
		s.sidecars.mu.Lock()
		if err == nil {
			err = b.Commit(pebble.NoSync)
			if err == nil {
				for _, r := range group {
					s.catalogChanged(r.name)
					s.sidecars.set(r.name, SidecarSummary{})
				}
			}
		}
		s.sidecars.mu.Unlock()
		b.Close()
		// Invalidate under the lock, committed or not, as lockMetricMutation does.
		ring := &s.metadataCache[s.metricStripe(group[0].name)]
		for _, r := range group {
			for i := range ring.entries {
				if ring.entries[i].Name == r.name {
					ring.entries[i] = Metadata{}
				}
			}
			s.endStateMutation(r.name)
		}
		unlock()
		if err != nil {
			return removed, err
		}
		removed += len(group)
		s.purgeRemoved.Add(uint64(len(group)))
	}
	return removed, s.compactRange(first, append(last, 0))
}

// compactRange flushes the memtables and compacts [start, end) in parallel.
func (s *Store) compactRange(start, end []byte) error {
	if err := s.Flush(); err != nil {
		return err
	}
	return s.db.Compact(start, end, true)
}

func (s *Store) DeleteIfUnchanged(ctx context.Context, name string, expected Metadata) error {
	return s.deleteMetric(ctx, name, &expected)
}

// InitializeActivity assigns an activity time to a legacy metric whose
// revision record predates activity tracking. It only mutates the exact
// revision observed by the caller, so an expiration scan cannot race a write.
func (s *Store) InitializeActivity(ctx context.Context, name string, expected Metadata) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	unlock := s.lockMetricMutation(name)
	defer unlock()
	m, err := s.metadataLocked(name)
	if err != nil {
		return err
	}
	if m.ID != expected.ID || m.Generation != expected.Generation || m.Revision != expected.Revision || !m.LastUpdate.IsZero() {
		return ErrConflict
	}
	if m.Revision == math.MaxUint64 {
		return errors.New("metric revision exhausted")
	}
	m.Revision++
	m.LastUpdate = s.nextActivity(time.Time{})
	b := s.db.NewBatch()
	defer b.Close()
	if err := b.Set(revisionKey(m), revisionBytes(m), nil); err != nil {
		return err
	}
	s.canonicalRun(m.ID).Clear(m.ID)
	if err := s.commit(b); err != nil {
		return fmt.Errorf("commit activity initialization %s: %w", name, err)
	}
	return nil
}

func (s *Store) deleteMetric(ctx context.Context, name string, expected *Metadata) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	unlock := s.lockMetricMutation(name)
	defer unlock()
	m, err := s.metadataLocked(name)
	if err != nil {
		return err
	}
	if m.FileMigration == FileCleanupPending {
		return fmt.Errorf("original-file cleanup is pending: %w", ErrConflict)
	}
	if expected != nil && (m.ID != expected.ID || m.Generation != expected.Generation || m.Revision != expected.Revision) {
		return ErrConflict
	}
	b := s.db.NewBatch()
	defer b.Close()
	if err := s.appendMetricDelete(ctx, b, m); err != nil {
		return err
	}
	if err := s.commitSidecars(b, name, &SidecarSummary{}, s.commit); err != nil {
		return fmt.Errorf("commit delete %s: %w", name, err)
	}
	s.catalogChanged(name)
	if s.incrementalSource == "" || m.FileMigration != FileUnowned {
		s.invalidateReadCache()
	}
	return nil
}

// appendMetricDelete is shared by administrative and expiration deletes. The
// caller holds the metric mutation lock until the batch has committed.
func (s *Store) appendMetricDelete(ctx context.Context, b *pebble.Batch, m Metadata) error {
	name := m.Name
	if err := b.Delete(catalogKey(name), nil); err != nil {
		return err
	}
	if err := b.Delete(revisionKey(m), nil); err != nil {
		return err
	}
	s.canonicalRun(m.ID).Clear(m.ID)
	metricChunks := chunkPrefix(m.ID, 0)[:9] // every generation of this ID
	if err := b.DeleteRange(metricChunks, prefixEnd(metricChunks), nil); err != nil {
		return err
	}
	if m.Continuation {
		// The metric lock excludes writers, so the database observes the
		// current generation's descriptors.
		if err := s.dropContinuation(ctx, s.db, b, m); err != nil {
			return err
		}
		state := continuationPrefix(m.ID, 0)[:9]
		if err := b.DeleteRange(state, prefixEnd(state), nil); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) Snapshot(ctx context.Context, name string) (Snapshot, error) {
	if err := errors.Join(ctx.Err(), s.fileOwnershipError()); err != nil {
		return Snapshot{}, err
	}
	snap := s.db.NewSnapshot()
	defer snap.Close()
	metadata, err := metadataFrom(snap, name)
	if err != nil {
		return Snapshot{}, err
	}
	if s.incrementalSource != "" {
		if err := s.checkFileOwnership(metadata); err != nil {
			return Snapshot{}, err
		}
	}
	if metadata.Continuation {
		return s.continuationSnapshot(ctx, snap, metadata)
	}
	return snapshotFrom(snap, name)
}

func snapshotFrom(reader pebble.Reader, name string) (Snapshot, error) {
	m, err := metadataFrom(reader, name)
	if err != nil {
		return Snapshot{}, err
	}
	result := Snapshot{Metadata: m, Archives: make([]Archive, len(m.Retentions))}
	for archive, retention := range m.Retentions {
		result.Archives[archive].Retention = retention
		prefix := chunkPrefixArchive(m.ID, m.Generation, archive)
		it, err := reader.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
		if err != nil {
			return Snapshot{}, err
		}
		var c chunk
		for it.First(); it.Valid(); it.Next() {
			if err := decodeChunk(it.Value(), &c); err != nil {
				it.Close()
				return Snapshot{}, err
			}
			for slot := range c.Points {
				if c.has(slot) {
					point := &c.Points[slot]
					result.Archives[archive].Points = append(result.Archives[archive].Points, *point)
				}
			}
		}
		if err := errors.Join(it.Error(), it.Close()); err != nil {
			return Snapshot{}, err
		}
		slices.SortFunc(result.Archives[archive].Points, func(a, b Point) int {
			return cmp.Compare(a.Timestamp, b.Timestamp)
		})
	}
	return result, nil
}

func (s *Store) Replace(ctx context.Context, snapshot Snapshot) (Metadata, error) {
	return s.replaceSnapshot(ctx, snapshot, false)
}

func (s *Store) CreateFromSnapshot(ctx context.Context, snapshot Snapshot) (Metadata, error) {
	return s.replaceSnapshot(ctx, snapshot, true)
}

func (s *Store) replaceSnapshot(ctx context.Context, snapshot Snapshot, mustAbsent bool) (Metadata, error) {
	if s.continuation != nil || snapshot.Metadata.Continuation {
		return Metadata{}, fmt.Errorf("%w: flattened snapshots cannot replace writer continuation state; use verified file conversion", ErrFormat)
	}
	if err := ctx.Err(); err != nil {
		return Metadata{}, err
	}
	if err := validateSnapshot(snapshot); err != nil {
		return Metadata{}, err
	}
	unlock := s.lockMetricMutation(snapshot.Metadata.Name)
	defer unlock()
	chunks, err := encodeSnapshotChunksWith(snapshot.Metadata.Retentions, snapshot.Archives, s.encodeChunk)
	if err != nil {
		return Metadata{}, err
	}
	return s.replaceCatalog(snapshot.Metadata, chunks, mustAbsent)
}

// replaceCatalog publishes pre-encoded chunks under the caller's metric lock.
// The store-wide lock is only taken when a new metric needs an ID.
func (s *Store) replaceCatalog(m Metadata, chunks []map[int][]byte, mustAbsent bool) (Metadata, error) {
	old, err := s.metadataLocked(m.Name)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return Metadata{}, err
	}
	missing := errors.Is(err, ErrNotFound)
	if mustAbsent && !missing {
		return Metadata{}, ErrExists
	}
	if missing {
		m.LastUpdate = s.nextActivity(time.Time{})
		return s.createReplacement(m, chunks)
	}
	m.ID, m.Generation, m.Revision, m.LastUpdate = old.ID, old.Generation+1, old.Revision+1, s.nextActivity(old.LastUpdate)
	m.FileMigration = old.FileMigration
	return s.commitReplacement(m, chunks, &old)
}

func (s *Store) createReplacement(m Metadata, chunks []map[int][]byte) (Metadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, closer, err := s.db.Get(sequenceKey())
	id := uint64(1)
	if err == nil {
		if len(v) != 8 {
			closer.Close()
			return Metadata{}, errors.New("invalid catalog sequence")
		}
		id = binary.BigEndian.Uint64(v)
		closer.Close()
		if id == math.MaxUint64 {
			return Metadata{}, errors.New("metric IDs exhausted")
		}
		id++
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return Metadata{}, err
	}
	m.ID, m.Generation, m.Revision = id, 1, 1
	return s.commitReplacement(m, chunks, nil)
}

func (s *Store) commitReplacement(m Metadata, chunks []map[int][]byte, old *Metadata) (Metadata, error) {
	b := s.db.NewBatch()
	defer b.Close()
	if old != nil {
		if old.Continuation {
			if err := s.dropContinuation(context.Background(), s.db, b, *old); err != nil {
				return Metadata{}, err
			}
			state := continuationPrefix(old.ID, old.Generation)
			if err := b.DeleteRange(state, prefixEnd(state), nil); err != nil {
				return Metadata{}, err
			}
		}
		oldChunks := chunkPrefix(old.ID, old.Generation)
		if err := b.DeleteRange(oldChunks, prefixEnd(oldChunks), nil); err != nil {
			return Metadata{}, err
		}
		if err := b.Delete(revisionKey(*old), nil); err != nil {
			return Metadata{}, err
		}
		s.canonicalRun(old.ID).Clear(old.ID)
	}
	for archive, encoded := range chunks {
		for index, value := range encoded {
			if err := b.Set(chunkKey(m, archive, index), value, nil); err != nil {
				return Metadata{}, err
			}
		}
	}
	encoded, err := s.encodeCatalog(m)
	if err != nil {
		return Metadata{}, err
	}
	if err := b.Set(catalogKey(m.Name), encoded, nil); err != nil {
		return Metadata{}, err
	}
	if err := b.Set(revisionKey(m), revisionBytes(m), nil); err != nil {
		return Metadata{}, err
	}
	if old == nil {
		if err := b.Set(sequenceKey(), uint64Bytes(m.ID), nil); err != nil {
			return Metadata{}, err
		}
	}
	if err := s.commit(b); err != nil {
		return Metadata{}, fmt.Errorf("commit replace %s: %w", m.Name, err)
	}
	s.catalogChanged(m.Name)
	if s.incrementalSource == "" || m.FileMigration != FileUnowned {
		s.invalidateReadCache()
	}
	return m, nil
}

// encodeSnapshotChunks groups archive points into encoded chunks keyed by chunk
// index. It needs no metric ID, so callers can run it before any store lock.
func encodeSnapshotChunks(retentions []Retention, archives []Archive) ([]map[int][]byte, error) {
	return encodeSnapshotChunksWith(retentions, archives, encodeChunk)
}

func encodeSnapshotChunksWith(retentions []Retention, archives []Archive, encode func(*chunk) []byte) ([]map[int][]byte, error) {
	result := make([]map[int][]byte, len(archives))
	for archive, a := range archives {
		chunks, err := snapshotArchiveChunks(retentions[archive], a)
		if err != nil {
			return nil, err
		}
		result[archive] = make(map[int][]byte, len(chunks))
		for index, c := range chunks {
			result[archive][index] = encode(c)
		}
	}
	return result, nil
}

func snapshotArchiveChunks(r Retention, a Archive) (map[int]*chunk, error) {
	chunks, _, err := snapshotArchiveChunksStatus(r, a)
	return chunks, err
}

func snapshotArchiveChunksStatus(r Retention, a Archive) (map[int]*chunk, bool, error) {
	chunks := make(map[int]*chunk)
	canonical := true
	previous := int64(-1)
	for _, p := range a.Points {
		timestamp, err := storageTimestamp(p.Timestamp)
		if err != nil {
			return nil, false, err
		}
		timestamp = align(timestamp, r.Step)
		if int64(timestamp) != p.Timestamp || p.Timestamp <= previous {
			canonical = false
		}
		previous = p.Timestamp
		slot := (timestamp / r.Step) % r.Count
		if slot < 0 {
			return nil, false, errors.New("negative archive slot")
		}
		index := slot / chunkSlots
		c := chunks[index]
		if c == nil {
			c = new(chunk)
			chunks[index] = c
		}
		if c.has(slot % chunkSlots) {
			canonical = false
		}
		// Preserve direct replacement's last-write-wins rule for aliased slots.
		c.set(slot%chunkSlots, Point{Timestamp: int64(timestamp), Value: p.Value})
	}
	return chunks, canonical, nil
}

func validateSnapshot(snapshot Snapshot) error {
	if err := errors.Join(validateName(snapshot.Metadata.Name), validate(snapshot.Metadata.MetricConfig)); err != nil {
		return err
	}
	if len(snapshot.Archives) != len(snapshot.Metadata.Retentions) {
		return errors.New("snapshot archive count does not match retentions")
	}
	for i, archive := range snapshot.Archives {
		if archive.Retention.Step != snapshot.Metadata.Retentions[i].Step || archive.Retention.Count != snapshot.Metadata.Retentions[i].Count {
			return fmt.Errorf("snapshot archive %d retention does not match metadata", i)
		}
	}
	return nil
}

func pointTimestamp(timestamp int64) (int, error) {
	maxInt := int64(^uint(0) >> 1)
	minInt := -maxInt - 1
	if timestamp < minInt || timestamp > maxInt {
		return 0, errors.New("point timestamp overflows int")
	}
	return int(timestamp), nil
}

func storageTimestamp(timestamp int64) (int, error) {
	value, err := pointTimestamp(timestamp)
	if err != nil {
		return 0, err
	}
	if value < 0 {
		return 0, errors.New("negative point timestamp")
	}
	return value, nil
}

func (s *Store) Fill(ctx context.Context, source Snapshot) (Metadata, error) {
	if s.continuation != nil || source.Metadata.Continuation {
		return Metadata{}, fmt.Errorf("%w: flattened fill cannot preserve writer continuation state", ErrFormat)
	}
	if err := ctx.Err(); err != nil {
		return Metadata{}, err
	}
	if err := validateSnapshot(source); err != nil {
		return Metadata{}, err
	}
	unlock := s.lockMetricMutation(source.Metadata.Name)
	defer unlock()
	dest, err := snapshotFrom(s.db, source.Metadata.Name)
	if errors.Is(err, ErrNotFound) {
		return s.createFill(source)
	}
	if err != nil {
		return Metadata{}, err
	}
	if !samePolicy(source.Metadata.MetricConfig, dest.Metadata.MetricConfig) {
		return Metadata{}, ErrConflict
	}
	// Like bucky fill over classic Whisper, which reads both sides through
	// Fetch, a NaN is a gap here: it never overwrites a value and is not kept.
	// Replace copies archives verbatim, NaN included, exactly as classic does.
	for i, archive := range source.Archives {
		points, err := fillArchive(archive.Points, dest.Archives[i].Points, dest.Metadata.Retentions[i].Step)
		if err != nil {
			return Metadata{}, err
		}
		dest.Archives[i].Points = points
	}
	chunks, err := encodeSnapshotChunksWith(dest.Metadata.Retentions, dest.Archives, s.encodeChunk)
	if err != nil {
		return Metadata{}, err
	}
	return s.replaceCatalog(dest.Metadata, chunks, false)
}

func (s *Store) createFill(source Snapshot) (Metadata, error) {
	chunks, err := encodeSnapshotChunksWith(source.Metadata.Retentions, source.Archives, s.encodeChunk)
	if err != nil {
		return Metadata{}, err
	}
	return s.replaceCatalog(source.Metadata, chunks, true)
}

func fillArchive(source, destination []Point, step int) ([]Point, error) {
	byTime := make(map[int64]float64, len(source)+len(destination))
	if err := addFillPoints(byTime, source, step); err != nil {
		return nil, err
	}
	if err := addFillPoints(byTime, destination, step); err != nil {
		return nil, err
	}
	points := make([]Point, 0, len(byTime))
	for timestamp, value := range byTime {
		points = append(points, Point{Timestamp: timestamp, Value: value})
	}
	sort.Slice(points, func(a, b int) bool { return points[a].Timestamp < points[b].Timestamp })
	return points, nil
}

func addFillPoints(byTime map[int64]float64, input []Point, step int) error {
	for _, p := range input {
		if math.IsNaN(p.Value) {
			continue
		}
		timestamp, err := storageTimestamp(p.Timestamp)
		if err != nil {
			return err
		}
		byTime[int64(align(timestamp, step))] = p.Value
	}
	return nil
}

func samePolicy(a, b MetricConfig) bool {
	if a.AggregationMethod != b.AggregationMethod || a.XFilesFactor != b.XFilesFactor || len(a.Retentions) != len(b.Retentions) {
		return false
	}
	for i := range a.Retentions {
		if a.Retentions[i] != b.Retentions[i] {
			return false
		}
	}
	return true
}

func chunkPrefix(id, generation uint64) []byte {
	k := make([]byte, 17)
	k[0] = 'c'
	binary.BigEndian.PutUint64(k[1:], id)
	binary.BigEndian.PutUint64(k[9:], generation)
	return k
}
