package chunkstore

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
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
	result := make([]Metadata, 0, limit)
	for it.First(); it.Valid() && len(result) < limit; it.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		m, err := catalogMetadata(snap, it.Value())
		if err != nil {
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

func catalogMetadata(reader pebble.Reader, value []byte) (Metadata, error) {
	var m Metadata
	if err := json.Unmarshal(value, &m); err != nil {
		return Metadata{}, fmt.Errorf("decode catalog: %w", err)
	}
	v, closer, err := reader.Get(revisionKey(m))
	if err != nil {
		return Metadata{}, fmt.Errorf("read revision: %w", err)
	}
	defer closer.Close()
	if err := decodeRevision(v, &m); err != nil {
		return Metadata{}, err
	}
	return m, nil
}

func (s *Store) Delete(ctx context.Context, name string) error { return s.deleteMetric(ctx, name, nil) }

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
	unlock := s.lockMetric(name)
	defer unlock()
	m, err := metadataFrom(s.db, name)
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
	if err := s.commit(b); err != nil {
		return fmt.Errorf("commit activity initialization %s: %w", name, err)
	}
	return nil
}

func (s *Store) deleteMetric(ctx context.Context, name string, expected *Metadata) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	unlock := s.lockMetric(name)
	defer unlock()
	m, err := metadataFrom(s.db, name)
	if err != nil {
		return err
	}
	if expected != nil && (m.ID != expected.ID || m.Generation != expected.Generation || m.Revision != expected.Revision) {
		return ErrConflict
	}
	b := s.db.NewBatch()
	defer b.Close()
	if err := b.Delete(catalogKey(name), nil); err != nil {
		return err
	}
	if err := b.Delete(revisionKey(m), nil); err != nil {
		return err
	}
	metricChunks := chunkPrefix(m.ID, 0)[:9] // every generation of this ID
	if err := b.DeleteRange(metricChunks, prefixEnd(metricChunks), nil); err != nil {
		return err
	}
	if err := s.commit(b); err != nil {
		return fmt.Errorf("commit delete %s: %w", name, err)
	}
	return nil
}

func (s *Store) Snapshot(ctx context.Context, name string) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	snap := s.db.NewSnapshot()
	defer snap.Close()
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
		sort.Slice(result.Archives[archive].Points, func(i, j int) bool {
			return result.Archives[archive].Points[i].Timestamp < result.Archives[archive].Points[j].Timestamp
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
	if err := ctx.Err(); err != nil {
		return Metadata{}, err
	}
	if err := validateSnapshot(snapshot); err != nil {
		return Metadata{}, err
	}
	unlock := s.lockMetric(snapshot.Metadata.Name)
	defer unlock()
	chunks, err := encodeSnapshotChunks(snapshot.Metadata.Retentions, snapshot.Archives)
	if err != nil {
		return Metadata{}, err
	}
	return s.replaceCatalog(snapshot.Metadata, chunks, mustAbsent)
}

// replaceCatalog publishes pre-encoded chunks under the caller's metric lock.
// The store-wide lock is only taken when a new metric needs an ID.
func (s *Store) replaceCatalog(m Metadata, chunks []map[int][]byte, mustAbsent bool) (Metadata, error) {
	old, err := metadataFrom(s.db, m.Name)
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
		oldChunks := chunkPrefix(old.ID, old.Generation)
		if err := b.DeleteRange(oldChunks, prefixEnd(oldChunks), nil); err != nil {
			return Metadata{}, err
		}
		if err := b.Delete(revisionKey(*old), nil); err != nil {
			return Metadata{}, err
		}
	}
	for archive, encoded := range chunks {
		for index, value := range encoded {
			if err := b.Set(chunkKey(m, archive, index), value, nil); err != nil {
				return Metadata{}, err
			}
		}
	}
	encoded, err := json.Marshal(m)
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
	return m, nil
}

// encodeSnapshotChunks groups archive points into encoded chunks keyed by chunk
// index. It needs no metric ID, so callers can run it before any store lock.
func encodeSnapshotChunks(retentions []Retention, archives []Archive) ([]map[int][]byte, error) {
	result := make([]map[int][]byte, len(archives))
	for archive, a := range archives {
		chunks := make(map[int]*chunk)
		r := retentions[archive]
		for _, p := range a.Points {
			timestamp, err := storageTimestamp(p.Timestamp)
			if err != nil {
				return nil, err
			}
			timestamp = align(timestamp, r.Step)
			slot := (timestamp / r.Step) % r.Count
			if slot < 0 {
				return nil, errors.New("negative archive slot")
			}
			index := slot / chunkSlots
			c := chunks[index]
			if c == nil {
				c = new(chunk)
				chunks[index] = c
			}
			// Whisper's direct archive replacement processes input in order. Keep
			// the same last-write-wins rule when two input points alias a slot.
			c.set(slot%chunkSlots, Point{Timestamp: int64(timestamp), Value: p.Value})
		}
		result[archive] = make(map[int][]byte, len(chunks))
		for index, c := range chunks {
			result[archive][index] = encodeChunk(c)
		}
	}
	return result, nil
}

func validateSnapshot(snapshot Snapshot) error {
	if err := validate(snapshot.Metadata.MetricConfig); err != nil {
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
	if err := ctx.Err(); err != nil {
		return Metadata{}, err
	}
	if err := validateSnapshot(source); err != nil {
		return Metadata{}, err
	}
	unlock := s.lockMetric(source.Metadata.Name)
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
	chunks, err := encodeSnapshotChunks(dest.Metadata.Retentions, dest.Archives)
	if err != nil {
		return Metadata{}, err
	}
	return s.replaceCatalog(dest.Metadata, chunks, false)
}

func (s *Store) createFill(source Snapshot) (Metadata, error) {
	chunks, err := encodeSnapshotChunks(source.Metadata.Retentions, source.Archives)
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
