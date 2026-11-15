package chunkstore

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/cockroachdb/pebble"
)

// ExpirationReceipt is committed atomically with an expiration deletion. Policy
// is the exact serialized active policy, not merely a rule's display name.
type ExpirationReceipt struct {
	Policy                string
	DeletedAt, LastUpdate int64
}

func expirationReceiptKey(name string) []byte { return []byte("R/expiration/" + name) }

// ExpirationReceiptFor returns evidence only while the name remains absent.
// Every catalog deletion clears prior evidence, so recreation followed by an
// administrative removal cannot resurrect an earlier expiration exemption.
func (s *Store) ExpirationReceiptFor(ctx context.Context, name string) (ExpirationReceipt, error) {
	ctx, finish, err := s.beginRead(ctx)
	if err != nil {
		return ExpirationReceipt{}, err
	}
	defer finish()
	if err := ctx.Err(); err != nil {
		return ExpirationReceipt{}, err
	}
	view := s.db.NewSnapshot()
	defer view.Close()
	_, err = metadataFrom(view, name)
	if err == nil {
		return ExpirationReceipt{}, ErrConflict
	}
	if !errors.Is(err, ErrNotFound) {
		return ExpirationReceipt{}, err
	}
	data, closer, err := view.Get(expirationReceiptKey(name))
	if errors.Is(err, pebble.ErrNotFound) {
		return ExpirationReceipt{}, ErrNotFound
	}
	if err != nil {
		return ExpirationReceipt{}, err
	}
	defer closer.Close()
	var receipt ExpirationReceipt
	err = json.Unmarshal(data, &receipt)
	return receipt, err
}

func (s *Store) DeleteBatchWithExpirationReceipts(ctx context.Context, expected []Metadata, receipts map[string]ExpirationReceipt) ([]error, error) {
	return s.deleteBatchIfUnchanged(ctx, expected, receipts)
}
