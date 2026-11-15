package partsconvert

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/partsformat"
)

// LegacyMetric binds a name to its exact frozen catalog record. ID ownership
// includes old generations: IDs are never reused within a source incarnation.
// Catalog and Revision are retained until reclamation is durably complete.
type LegacyMetric struct {
	Name, Catalog, Revision []byte
	ID, Generation          uint64
}

// LegacyPlan is captured from the immutable source before any conversion or
// deletion. Buckets includes every bucket present in that source incarnation.
// A plan must be persisted before deleting its catalog entries, so a crash never
// loses the ID-to-name mapping needed to finish physical reclamation.
type LegacyPlan struct {
	Source       string
	Group        []byte
	Lower, Upper []byte
	Final        bool
	Metrics      []LegacyMetric
	Buckets      []uint32
}

// PlanLegacy captures a bounded global name batch, covering all routing shards.
// Source must be a quiescent copy. Bucket discovery skips directly to the next
// bucket prefix: it does not scan every archive block for every name batch.
func PlanLegacy(ctx context.Context, source pebble.Reader, identity string, group, lower, upper []byte, final bool, maxMetrics int) (LegacyPlan, error) {
	p := LegacyPlan{Source: identity, Group: bytes.Clone(group), Lower: bytes.Clone(lower), Upper: bytes.Clone(upper), Final: final}
	if identity == "" || maxMetrics < 1 || (len(upper) != 0 && (bytes.Compare(lower, upper) >= 0 || final)) || (len(upper) == 0 && !final) {
		return p, errors.New("invalid legacy name batch")
	}
	hi := []byte("m0")
	if len(upper) != 0 {
		hi = append([]byte("m/"), upper...)
	}
	it, err := source.NewIter(&pebble.IterOptions{LowerBound: append([]byte("m/"), lower...), UpperBound: hi})
	if err != nil {
		return p, err
	}
	for ok := it.First(); ok; ok = it.Next() {
		if err = ctx.Err(); err != nil {
			break
		}
		if len(p.Metrics) == maxMetrics {
			err = errors.New("legacy name batch exceeds metric bound")
			break
		}
		m := LegacyMetric{Name: bytes.Clone(it.Key()[2:]), Catalog: bytes.Clone(it.Value())}
		m.ID, m.Generation, err = legacyIdentity(m.Catalog)
		if err != nil {
			break
		}
		key := legacyGenerationPrefix('r', m.ID, m.Generation)
		value, closer, getErr := source.Get(key)
		if getErr == nil {
			m.Revision = bytes.Clone(value)
			getErr = closer.Close()
		}
		if getErr != nil && !errors.Is(getErr, pebble.ErrNotFound) {
			err = getErr
			break
		}
		p.Metrics = append(p.Metrics, m)
	}
	err = errors.Join(err, it.Error(), it.Close())
	if err != nil {
		return p, err
	}
	it, err = source.NewIter(&pebble.IterOptions{LowerBound: []byte{'t'}, UpperBound: []byte{'u'}})
	if err != nil {
		return p, err
	}
	for ok := it.First(); ok; {
		if err = ctx.Err(); err != nil {
			break
		}
		if len(it.Key()) < 22 || it.Key()[5] != 'u' {
			err = errors.New("invalid legacy time-bucket key")
			break
		}
		p.Buckets = append(p.Buckets, binary.BigEndian.Uint32(it.Key()[1:5]))
		ok = it.SeekGE(partsformat.PrefixEnd(bytes.Clone(it.Key()[:5])))
	}
	err = errors.Join(err, it.Error(), it.Close())
	if err == nil {
		err = p.validate()
	}
	return p, err
}

func legacyIdentity(value []byte) (uint64, uint64, error) {
	var id, generation uint64
	if len(value) > 0 && value[0]&0x7f >= 1 && value[0]&0x7f <= 3 {
		if len(value) < 29 || (len(value)-29)%8 != 0 || uint64(binary.LittleEndian.Uint32(value[25:])) != uint64((len(value)-29)/8) {
			return 0, 0, errors.New("invalid frozen binary catalog")
		}
		id, generation = binary.LittleEndian.Uint64(value[1:]), binary.LittleEndian.Uint64(value[9:])
	} else {
		var m struct{ ID, Generation uint64 }
		if err := json.Unmarshal(value, &m); err != nil {
			return 0, 0, err
		}
		id, generation = m.ID, m.Generation
	}
	if id == 0 || generation == 0 {
		return 0, 0, errors.New("invalid frozen metric identity")
	}
	return id, generation, nil
}

func (p LegacyPlan) validate() error {
	if p.Source == "" || len(p.Upper) == 0 && !p.Final || len(p.Upper) != 0 && (p.Final || bytes.Compare(p.Lower, p.Upper) >= 0) {
		return errors.New("invalid legacy plan bounds")
	}
	ids := make(map[uint64]bool, len(p.Metrics))
	for i, m := range p.Metrics {
		id, generation, err := legacyIdentity(m.Catalog)
		if err != nil || id != m.ID || generation != m.Generation || ids[id] || bytes.Compare(m.Name, p.Lower) < 0 || len(p.Upper) > 0 && bytes.Compare(m.Name, p.Upper) >= 0 || i > 0 && bytes.Compare(p.Metrics[i-1].Name, m.Name) >= 0 {
			return errors.New("invalid legacy plan catalog ownership")
		}
		ids[id] = true
	}
	for i := 1; i < len(p.Buckets); i++ {
		if p.Buckets[i] <= p.Buckets[i-1] {
			return errors.New("invalid legacy bucket order")
		}
	}
	return nil
}

// Units returns exactly the participating shards. Publication must wait for all
// of them; a per-shard receipt cannot advance a global name cursor by itself.
func (p LegacyPlan) Units() []Unit {
	shards := make(map[uint16]bool)
	for _, m := range p.Metrics {
		shards[partsformat.Route(partsformat.ID(m.Name))] = true
	}
	var result []Unit
	for shard := range shards {
		result = append(result, Unit{Group: string(p.Group), VShard: shard, Lower: string(p.Lower), Upper: string(p.Upper), Final: p.Final})
	}
	slices.SortFunc(result, func(a, b Unit) int { return int(a.VShard) - int(b.VShard) })
	return result
}

type legacySpan struct{ lower, upper []byte }

func legacyGenerationPrefix(kind byte, id, generation uint64) []byte {
	k := binary.BigEndian.AppendUint64([]byte{kind}, id)
	return binary.BigEndian.AppendUint64(k, generation)
}

func (p LegacyPlan) spans() []legacySpan {
	var spans []legacySpan
	// Merge only adjacent owned IDs. Never widen across an unconverted ID.
	ids := make([]uint64, len(p.Metrics))
	for i, m := range p.Metrics {
		ids[i] = m.ID
	}
	slices.Sort(ids)
	for i := 0; i < len(ids); {
		j := i + 1
		for j < len(ids) && ids[j-1] != ^uint64(0) && ids[j] == ids[j-1]+1 {
			j++
		}
		for _, kind := range []byte{'c', 'r', 'u'} {
			lo := binary.BigEndian.AppendUint64([]byte{kind}, ids[i])
			hi := partsformat.PrefixEnd(binary.BigEndian.AppendUint64([]byte{kind}, ids[j-1]))
			spans = append(spans, legacySpan{lo, hi})
		}
		for _, bucket := range p.Buckets {
			prefix := append(binary.BigEndian.AppendUint32([]byte{'t'}, bucket), 'u')
			lo := binary.BigEndian.AppendUint64(bytes.Clone(prefix), ids[i])
			hi := partsformat.PrefixEnd(binary.BigEndian.AppendUint64(bytes.Clone(prefix), ids[j-1]))
			spans = append(spans, legacySpan{lo, hi})
		}
		i = j
	}
	return spans
}

// DeleteLegacy performs the physical part of a verified batch's reclamation.
// The caller MUST own the exclusive writable handle, after all old readers and
// read-only handles close, and must Close it before reporting physical recovery.
// It never opens a live daemon's store. Compaction can rewrite shared SSTs: its
// temporary space must be admitted separately; a tombstone is not freed space.
// Repeating this after a crash is safe, including after catalog removal.
func DeleteLegacy(ctx context.Context, db *pebble.DB, p LegacyPlan) error {
	return deleteLegacy(ctx, db, p, nil)
}

func deleteLegacy(ctx context.Context, db *pebble.DB, p LegacyPlan, checkpoint func(string) error) error {
	if err := p.validate(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Before any deletion, revalidate all surviving records, not just the first.
	// Missing records are expected after an ambiguous prior synced delete.
	for _, m := range p.Metrics {
		for _, pair := range []struct{ key, expected []byte }{
			{append([]byte("m/"), m.Name...), m.Catalog},
			{legacyGenerationPrefix('r', m.ID, m.Generation), m.Revision},
		} {
			value, closer, err := db.Get(pair.key)
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			equal := bytes.Equal(value, pair.expected)
			if err = closer.Close(); err != nil {
				return err
			}
			if !equal {
				return fmt.Errorf("legacy source changed for name %x", m.Name)
			}
		}
	}
	spans := p.spans()
	b := db.NewBatch()
	defer b.Close()
	for _, span := range spans {
		if err := b.DeleteRange(span.lower, span.upper, nil); err != nil {
			return err
		}
	}
	for _, m := range p.Metrics {
		for _, prefix := range []string{"m/", "o/"} {
			if err := b.Delete(append([]byte(prefix), m.Name...), nil); err != nil {
				return err
			}
		}
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return err
	}
	if checkpoint != nil {
		if err := checkpoint("legacy-deleted"); err != nil {
			return err
		}
	}
	for _, span := range spans {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := db.Compact(span.lower, span.upper, false); err != nil {
			return err
		}
	}
	if checkpoint != nil {
		return checkpoint("legacy-compacted")
	}
	return nil
}
