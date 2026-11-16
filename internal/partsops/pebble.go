package partsops

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/cockroachdb/pebble"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/partsformat"
)

// Metadata lives outside the GCP data-key namespace. The same portable
// placement-group/vshard prefix scopes catalog moves and deletion records.
func metadataPrefix(s Scope) []byte {
	b := make([]byte, 11+len(s.PlacementGroup))
	copy(b, "GCOP\x01")
	binary.BigEndian.PutUint32(b[5:9], uint32(len(s.PlacementGroup)))
	copy(b[9:], s.PlacementGroup)
	binary.BigEndian.PutUint16(b[9+len(s.PlacementGroup):], s.VShard)
	return b
}
func metadataKey(s Scope, kind byte, name string) []byte {
	b := metadataPrefix(s)
	b = append(b, kind)
	return append(b, name...)
}

type persistentMetric struct {
	Metric        Metric
	Revision      uint64
	LastUpdate    time.Time
	Continuation  bool
	FileMigration store.FileMigrationState
}

// Edit can be staged as a metadata SST alongside data/frontier SSTs in a
// future atomic ingestion. RangeEnd is set only for a whole-partition drop;
// Delete with nil RangeEnd deletes a catalog key. No edit requires a data read.
type Edit struct {
	Key, Value, RangeEnd []byte
	Delete               bool
}

func EncodeEdits(b Batch) ([]Edit, error) {
	if err := validateScope(b.Scope); err != nil {
		return nil, err
	}
	if b.Revision != b.ExpectedRevision+1 || b.Revision == 0 {
		return nil, errors.New("invalid catalog batch")
	}
	var out []Edit
	for _, a := range b.Activity {
		data, err := encodeActivity(b.Scope, a)
		if err != nil {
			return nil, err
		}
		out = append(out, Edit{Key: metadataKey(b.Scope, 'a', a.Name), Value: data})
	}
	for _, m := range b.Upserts {
		if err := validateMetric(b.Scope, m); err != nil {
			return nil, err
		}
		data, err := json.Marshal(persistentMetric{Metric: m, Revision: m.Metadata.Revision, LastUpdate: m.Metadata.LastUpdate, Continuation: m.Metadata.Continuation, FileMigration: m.Metadata.FileMigration})
		if err != nil {
			return nil, err
		}
		out = append(out, Edit{Key: metadataKey(b.Scope, 'm', m.Metadata.Name), Value: data})
	}
	for _, d := range b.Deletes {
		if d.Scope != b.Scope || d.Name == "" || d.ThroughGeneration == 0 || d.Revision == 0 {
			return nil, errors.New("invalid deletion scope")
		}
		data, err := json.Marshal(d)
		if err != nil {
			return nil, err
		}
		out = append(out, Edit{Key: metadataKey(b.Scope, 'm', d.Name), Delete: true}, Edit{Key: metadataKey(b.Scope, 'd', d.Name), Value: data})
	}
	for _, p := range b.DropPartitions {
		if p.Scope != b.Scope || !p.Sealed || p.ID == "" || p.End <= p.Start || p.LongestSeconds <= 0 {
			return nil, errors.New("invalid partition deletion scope")
		}
		lo, err := partsformat.PartitionPrefixForGroup(p.Scope.PlacementGroup, p.Scope.VShard, p.Key)
		if err != nil {
			return nil, err
		}
		data, err := json.Marshal(p)
		if err != nil {
			return nil, err
		}
		var key [8]byte
		binary.BigEndian.PutUint64(key[:], uint64(p.Key)^(1<<63))
		out = append(out, Edit{Key: lo, RangeEnd: partsformat.PrefixEnd(lo), Delete: true}, Edit{Key: metadataKey(b.Scope, 'p', string(key[:])), Value: data})
	}
	var revision [8]byte
	binary.BigEndian.PutUint64(revision[:], b.Revision)
	out = append(out, Edit{Key: metadataKey(b.Scope, 'r', ""), Value: slices.Clone(revision[:])})
	return out, nil
}

type pebbleCommitter struct {
	db       *pebble.DB
	scope    Scope
	revision uint64
	failed   bool
}

func (p *pebbleCommitter) Commit(ctx context.Context, b Batch) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.failed {
		return errors.New("catalog commit outcome uncertain; reopen before retry")
	}
	if b.Scope != p.scope || b.ExpectedRevision != p.revision {
		return ErrConflict
	}
	edits, err := EncodeEdits(b)
	if err != nil {
		return err
	}
	batch := p.db.NewBatch()
	defer batch.Close()
	for _, e := range edits {
		switch {
		case e.RangeEnd != nil:
			err = batch.DeleteRange(e.Key, e.RangeEnd, nil)
		case e.Delete:
			err = batch.Delete(e.Key, nil)
		default:
			err = batch.Set(e.Key, e.Value, nil)
		}
		if err != nil {
			return err
		}
	}
	// A failed Sync can have an uncertain durable outcome. Fence this owner
	// until reopen instead of retrying an old expected revision blindly.
	if err := batch.Commit(pebble.Sync); err != nil {
		p.failed = true
		return err
	}
	p.revision = b.Revision
	return nil
}

// OpenCatalog reads metadata only and does not own db.Close. Exactly one Owner
// may mutate a scope. The caller must finish crash replay before Expire/Reclaim.
// A parts engine must additionally publish data, catalog and replay frontier
// atomically; this standalone metadata adapter does not claim that integration.
func OpenCatalog(db *pebble.DB, scope Scope) (*Owner, error) {
	if db == nil {
		return nil, errors.New("nil catalog database")
	}
	if err := validateScope(scope); err != nil {
		return nil, err
	}
	s := Snapshot{Scope: scope}
	var activity []Activity
	hasRevision := false
	prefix := metadataPrefix(scope)
	it, err := db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: partsformat.PrefixEnd(prefix)})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		if len(it.Key()) <= len(prefix) {
			return nil, errors.New("invalid catalog key")
		}
		kind := it.Key()[len(prefix)]
		switch kind {
		case 'a':
			a, err := decodeActivity(string(it.Key()[len(prefix)+1:]), it.Value())
			if err != nil {
				return nil, err
			}
			activity = append(activity, a)
		case 'm':
			var m persistentMetric
			if err := json.Unmarshal(it.Value(), &m); err != nil {
				return nil, err
			}
			m.Metric.Metadata.Revision = m.Revision
			m.Metric.Metadata.LastUpdate = m.LastUpdate
			m.Metric.Metadata.Continuation = m.Continuation
			m.Metric.Metadata.FileMigration = m.FileMigration
			if string(it.Key()[len(prefix)+1:]) != m.Metric.Metadata.Name {
				return nil, errors.New("catalog key/name mismatch")
			}
			s.Metrics = append(s.Metrics, m.Metric)
		case 'd':
			var d Tombstone
			if err := json.Unmarshal(it.Value(), &d); err != nil {
				return nil, err
			}
			if string(it.Key()[len(prefix)+1:]) != d.Name {
				return nil, errors.New("tombstone key/name mismatch")
			}
			s.Tombstones = append(s.Tombstones, d)
		case 'p':
			var p Partition
			if err := json.Unmarshal(it.Value(), &p); err != nil {
				return nil, err
			}
			tail := it.Key()[len(prefix)+1:]
			if len(tail) != 8 || int64(binary.BigEndian.Uint64(tail)^(1<<63)) != p.Key {
				return nil, errors.New("partition key mismatch")
			}
			s.DroppedPartitions = append(s.DroppedPartitions, p)
		case 'r':
			if len(it.Value()) != 8 || len(it.Key()) != len(prefix)+1 {
				return nil, errors.New("invalid catalog revision")
			}
			s.Revision = binary.BigEndian.Uint64(it.Value())
			hasRevision = true
		default:
			return nil, fmt.Errorf("unknown catalog key type %d", kind)
		}
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	byName := make(map[string]int, len(s.Metrics))
	for i, m := range s.Metrics {
		byName[m.Metadata.Name] = i
	}
	dead := make(map[string]uint64, len(s.Tombstones))
	for _, d := range s.Tombstones {
		dead[d.Name] = d.ThroughGeneration
	}
	for _, a := range activity {
		i, ok := byName[a.Name]
		if !ok {
			if a.Generation <= dead[a.Name] {
				continue
			}
			return nil, errors.New("activity has no metric definition")
		}
		m := &s.Metrics[i].Metadata
		if a.Generation < m.Generation {
			continue
		}
		if a.Generation != m.Generation {
			return nil, errors.New("activity incarnation mismatch")
		}
		if a.Revision >= m.Revision {
			m.Revision, m.LastUpdate = a.Revision, a.LastUpdate
		}
	}
	if !hasRevision && len(s.Metrics)+len(s.Tombstones)+len(s.DroppedPartitions)+len(activity) > 0 {
		return nil, errors.New("catalog missing revision")
	}
	return NewOwner(s, &pebbleCommitter{db: db, scope: scope, revision: s.Revision})
}
