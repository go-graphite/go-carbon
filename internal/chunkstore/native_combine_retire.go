package chunkstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"github.com/cockroachdb/pebble"
)

// retireNativeCombinePieces retires the generation each published piece of
// identity replaced. The final receipt does this for completed combines; an
// abandoned combine and a retained oversized intent never reach it, and the
// old generation would otherwise lose its last reference once the piece
// record is gone. Pieces are streamed one record at a time and every old
// generation is removed through retireNativeCombine's bounded WAL batches.
//
// A piece record is marked Retired only after its old generation is retired,
// so a crash leaves either the record (and the retire is replayed) or the
// retired state. Retiring an already retired generation deletes nothing.
func (s *Store) retireNativeCombinePieces(ctx context.Context, identity string) (retired int, err error) {
	prefix := nativeCombinePiecePrefix(identity)
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
	if err != nil {
		return 0, err
	}
	defer it.Close()
	b := s.db.NewBatch()
	defer func() { b.Close() }()
	flush := func() error {
		if b.Count() == 0 {
			return nil
		}
		if err := b.Commit(pebble.NoSync); err != nil {
			s.nativeWriteFailed(err)
			return err
		}
		b.Close()
		b = s.db.NewBatch()
		return s.nativeStep("combine-retire-pieces")
	}
	for it.First(); it.Valid(); it.Next() {
		if err := ctx.Err(); err != nil {
			return retired, err
		}
		if len(it.Value()) > 1<<20 {
			return retired, ErrTransferLimit
		}
		var p nativeCombinePiece
		if err := json.Unmarshal(it.Value(), &p); err != nil {
			return retired, err
		}
		if p.Position < 0 || p.Meta.ID == 0 || p.Deferred && p.Old.ID != 0 || !bytes.Equal(it.Key(), nativeCombinePieceKey(identity, p.Position)) {
			return retired, ErrFormat
		}
		// The same pieces the final receipt retires: published, replacing an
		// existing name with a new ID.
		if p.Deferred || p.Old.ID == 0 || p.InPlace || p.Retired {
			continue
		}
		if p.Old.ID == p.Meta.ID {
			return retired, ErrFormat
		}
		// IDs are never reassigned, so after publication only the name's own
		// catalog record could still point at the old ID. Never retire it then.
		current, err := metadataFrom(s.db, p.Meta.Name)
		switch {
		case errors.Is(err, ErrNotFound):
		case err != nil:
			return retired, err
		case current.ID == p.Old.ID:
			continue
		}
		if err := s.retireNativeCombine(ctx, Metadata(p.Old)); err != nil {
			return retired, err
		}
		retired++
		p.Retired = true
		data, err := json.Marshal(p)
		if err != nil {
			return retired, err
		}
		if err := b.Set(it.Key(), data, nil); err != nil {
			return retired, err
		}
		if b.Count() >= nativeCombineBatch || b.Len() >= 1<<20 {
			if err := flush(); err != nil {
				return retired, err
			}
		}
	}
	if err := it.Error(); err != nil {
		return retired, err
	}
	return retired, flush()
}
