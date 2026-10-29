package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"math"

	"github.com/cockroachdb/pebble"
)

// OrphanReclaim counts what ReclaimOrphanedFiles found and changed.
type OrphanReclaim struct {
	Scanned        int      // catalog records
	Unowned        int      // staged imports without published ownership
	SourcePresent  int      // unowned records whose original still exists; left alone
	Reclaimed      int      // unowned records marked reclaimed, or that would be on a dry run
	CleanupPending int      // owned records whose original cleanup has not completed
	Samples        []string // the first names marked
}

var orphanReclaimPage = 10000

// ReclaimOrphanedFiles takes ownership of every staged import whose original
// file is gone. A conversion that removed originals without publishing their
// ownership leaves complete metrics invisible, and the persister discards such
// a metric as an abandoned import on its next write. Each is marked reclaimed
// as CompleteFileCleanup marks a published metric. exists reports whether the
// original of name is present; those metrics are left to the conversion. With
// apply false nothing is written. Run it only while no daemon serves the
// original tree: an original created after its check would be shadowed.
func (s *Store) ReclaimOrphanedFiles(ctx context.Context, exists func(name string) (bool, error), apply bool) (OrphanReclaim, error) {
	var result OrphanReclaim
	if s.incrementalSource == "" {
		return result, errors.New("incremental migration is disabled")
	}
	if err := s.fileOwnershipError(); err != nil {
		return result, err
	}
	var after []byte
	for {
		names, last, err := s.unownedPage(ctx, after, &result)
		if err != nil {
			return result, err
		}
		for _, name := range names {
			present, err := exists(name)
			if err != nil {
				return result, err
			}
			if present {
				result.SourcePresent++
				continue
			}
			if apply {
				marked, err := s.reclaimOrphan(ctx, name)
				if err != nil {
					return result, err
				}
				if !marked {
					continue
				}
			}
			result.Reclaimed++
			if len(result.Samples) < 10 {
				result.Samples = append(result.Samples, name)
			}
		}
		if last == nil {
			break
		}
		after = last
	}
	if apply && result.Reclaimed > 0 {
		s.invalidateReadCache()
	}
	return result, nil
}

// unownedPage scans up to a page of catalog records after the key after and
// returns the unowned names among them, and the last key scanned when more
// records may follow.
func (s *Store) unownedPage(ctx context.Context, after []byte, result *OrphanReclaim) ([]string, []byte, error) {
	lower := []byte("m/")
	if after != nil {
		lower = append(bytes.Clone(after), 0)
	}
	snap := s.db.NewSnapshot()
	defer snap.Close()
	it, err := snap.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: []byte("m0")})
	if err != nil {
		return nil, nil, err
	}
	defer it.Close()
	var names []string
	var last []byte
	scanned := 0
	for valid := it.First(); valid && scanned < orphanReclaimPage; valid = it.Next() {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		scanned++
		name := string(it.Key()[2:])
		m, err := decodeCatalog(name, it.Value())
		if err != nil {
			return nil, nil, err
		}
		switch m.FileMigration {
		case FileUnowned:
			names = append(names, name)
		case FileCleanupPending:
			result.CleanupPending++
		}
		last = bytes.Clone(it.Key())
	}
	if err := it.Error(); err != nil {
		return nil, nil, err
	}
	result.Scanned += scanned
	result.Unowned += len(names)
	if scanned < orphanReclaimPage {
		last = nil
	}
	return names, last, nil
}

// reclaimOrphan marks one staged import reclaimed under its mutation lock,
// rechecking that it is still unowned.
func (s *Store) reclaimOrphan(ctx context.Context, name string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	unlock := s.lockMetricMutation(name)
	defer unlock()
	m, err := s.metadataLocked(name)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if m.FileMigration != FileUnowned {
		return false, nil
	}
	if m.Revision == math.MaxUint64 {
		return false, errors.New("metric revision exhausted")
	}
	m.FileMigration = FileReclaimed
	m.Revision++
	encoded, err := s.encodeCatalog(m)
	if err != nil {
		return false, err
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := batch.Set(catalogKey(m.Name), encoded, nil); err != nil {
		return false, err
	}
	if err := batch.Set(revisionKey(m), revisionBytes(m), nil); err != nil {
		return false, err
	}
	if err := batch.Delete(fileCleanupKey(m.ID), nil); err != nil {
		return false, err
	}
	if err := s.commit(batch); err != nil {
		return false, err
	}
	s.catalogChanged(m.Name)
	return true, nil
}
