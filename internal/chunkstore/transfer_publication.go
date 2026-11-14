package chunkstore

import (
	"context"
	"encoding/binary"
	"errors"

	"github.com/cockroachdb/pebble"
)

const transferPublicationBytes = 256 << 10

// commitTransferPublication runs only after target validation and exact-name
// reservation. Private-ID data can then be applied in bounded WAL batches:
// nobody can read it through the catalog, and the reservation prevents a live
// mutation from invalidating the merge. Old data, catalogs, revisions, sidecar
// inventory and receipts remain in the final atomic batch. The caller's final
// WAL sync covers the private data and publication together.
//
// A failed stage never publishes a catalog or receipt. As with interrupted SST
// staging, unreachable reserved IDs may remain; a new import reserves new IDs.
func (s *Store) commitTransferPublication(ctx context.Context, source *pebble.Batch, metas []Metadata, sidecars []SidecarRecord) error {
	private := make(map[uint64]uint64, len(metas))
	for _, m := range metas {
		private[m.ID] = m.Generation
	}
	final, staged := s.db.NewBatch(), s.db.NewBatch()
	defer final.Close()
	defer staged.Close()
	flush := func() error {
		if staged.Empty() {
			return ctx.Err()
		}
		if err := s.commitTransferBatch(ctx, staged); err != nil {
			return err
		}
		staged.Reset()
		return nil
	}
	r := source.Reader()
	for {
		kind, key, value, ok, err := r.Next()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		b := final
		if transferPrivateKey(key, private) {
			b = staged
			if !staged.Empty() && len(staged.Repr())+len(key)+len(value)+16 > transferPublicationBytes {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		switch kind {
		case pebble.InternalKeyKindSet:
			err = b.Set(key, value, nil)
		case pebble.InternalKeyKindDelete:
			err = b.Delete(key, nil)
		default:
			err = errors.New("unsupported transfer publication operation")
		}
		if err != nil {
			return err
		}
	}
	if err := flush(); err != nil {
		return err
	}
	return s.commitSidecarRecords(final, sidecars, func(b *pebble.Batch) error {
		return s.commitTransferBatch(ctx, b)
	})
}

// Only history belonging to the reserved generation is stageable. In
// particular, retirement deletes and all visibility/verification keys stay
// atomic even if they contain an ID with the same byte representation.
func transferPrivateKey(key []byte, private map[uint64]uint64) bool {
	if len(key) >= 22 && key[0] == continuationBucketKeyPrefix {
		key = key[5:]
	}
	if len(key) < 17 || (key[0] != 'c' && key[0] != 'u') {
		return false
	}
	generation, ok := private[binary.BigEndian.Uint64(key[1:9])]
	return ok && generation == binary.BigEndian.Uint64(key[9:17])
}

func (s *Store) commitTransferBatch(ctx context.Context, b *pebble.Batch) error {
	s.transferCommitMu.Lock()
	defer s.transferCommitMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	return b.Commit(pebble.NoSync)
}
