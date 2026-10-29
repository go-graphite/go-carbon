package chunkstore

import (
	"bytes"
	"errors"
)

// Bucket discovery reads the descriptors already exported under one snapshot.
// The codec never needs to reposition the database iterator to enumerate them.
type transferRecordIO TransferMetric

var errTransferReadOnly = errors.New("read-only transfer")

func (r transferRecordIO) Get(key []byte) ([]byte, error) {
	for _, kv := range r.Keys {
		if bytes.Equal(kv.Key, key) {
			return bytes.Clone(kv.Value), nil
		}
	}
	return nil, ErrNotFound
}
func (r transferRecordIO) Visit(prefix []byte, visit func([]byte, []byte) error) error {
	for _, kv := range r.Keys {
		if bytes.HasPrefix(kv.Key, prefix) {
			if err := visit(kv.Key, kv.Value); err != nil {
				return err
			}
		}
	}
	return nil
}
func (transferRecordIO) Set([]byte, []byte) error  { return errTransferReadOnly }
func (transferRecordIO) Delete([]byte) error       { return errTransferReadOnly }
func (transferRecordIO) DeletePrefix([]byte) error { return errTransferReadOnly }
