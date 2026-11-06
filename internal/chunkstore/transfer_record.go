package chunkstore

import (
	"bytes"
	"errors"
	"slices"
)

// Bucket discovery reads the descriptors already exported under one snapshot.
// The codec never needs to reposition the database iterator to enumerate them.
type transferRecordIO TransferMetric

var errTransferReadOnly = errors.New("read-only transfer")

func (r transferRecordIO) Get(key []byte) ([]byte, error) {
	// Exporters sort these keys, and both import protocols validate strict
	// order before reaching codec operations. Avoid scanning all archive groups
	// for every block lookup while retaining the detached writable return value.
	i, found := slices.BinarySearchFunc(r.Keys, key, func(kv RelayoutKV, key []byte) int { return bytes.Compare(kv.Key, key) })
	if found {
		return bytes.Clone(r.Keys[i].Value), nil
	}
	return nil, ErrNotFound
}
func (r transferRecordIO) Visit(prefix []byte, visit func([]byte, []byte) error) error {
	start, _ := slices.BinarySearchFunc(r.Keys, prefix, func(kv RelayoutKV, prefix []byte) int { return bytes.Compare(kv.Key, prefix) })
	for _, kv := range r.Keys[start:] {
		if !bytes.HasPrefix(kv.Key, prefix) {
			break
		}
		if err := visit(kv.Key, kv.Value); err != nil {
			return err
		}
	}
	return nil
}
func (transferRecordIO) Set([]byte, []byte) error  { return errTransferReadOnly }
func (transferRecordIO) Delete([]byte) error       { return errTransferReadOnly }
func (transferRecordIO) DeletePrefix([]byte) error { return errTransferReadOnly }
