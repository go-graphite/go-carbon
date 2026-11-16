package statestore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
)

type pebbleEngine struct{ db *pebble.DB }
type pebbleTx struct{ batch *pebble.Batch }

func openPebble(path string) (engine, error) {
	db, err := pebble.Open(path, &pebble.Options{MemTableSize: 64 << 20, BytesPerSync: 64 << 20, MaxConcurrentCompactions: func() int { return 2 }})
	if err != nil {
		return nil, err
	}
	return &pebbleEngine{db}, nil
}
func (e *pebbleEngine) Close() error                { return e.db.Close() }
func (e *pebbleEngine) Begin() (transaction, error) { return &pebbleTx{e.db.NewIndexedBatch()}, nil }

// Pebble lends value buffers until the closer runs; never retain them.
func payload(b []byte) ([]byte, error) {
	n, k := binary.Uvarint(b)
	if k <= 0 || n > uint64(len(b)-k) {
		return nil, fmt.Errorf("corrupt record label")
	}
	return append([]byte(nil), b[k+int(n):]...), nil
}
func (e *pebbleEngine) Get(bucket string, id uint64) ([]byte, error) {
	b, c, err := e.db.Get(key(bucket, id))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	defer c.Close()
	return payload(b)
}
func (t *pebbleTx) Get(bucket string, id uint64) ([]byte, error) {
	b, c, err := t.batch.Get(key(bucket, id))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	defer c.Close()
	return payload(b)
}
func (t *pebbleTx) Put(r Record) error {
	b := binary.AppendUvarint(nil, uint64(len(r.Label)))
	b = append(b, r.Label...)
	b = append(b, r.Payload...)
	return t.batch.Set(key(r.Bucket, r.ID), b, nil)
}
func (t *pebbleTx) Insert(r Record) (bool, error) {
	_, err := t.Get(r.Bucket, r.ID)
	if err == nil {
		return false, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return false, err
	}
	return true, t.Put(r)
}
func (t *pebbleTx) Commit() error { return t.batch.Commit(pebble.Sync) }
func (t *pebbleTx) Close() error  { return t.batch.Close() }

func (e *pebbleEngine) Scan(fn func(Record) error) error {
	it, err := e.db.NewIter(nil)
	if err != nil {
		return err
	}
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		k := it.Key()
		sep := bytes.IndexByte(k, 0)
		if sep < 0 || len(k) != sep+9 {
			return fmt.Errorf("corrupt key")
		}
		b := it.Value()
		n, c := binary.Uvarint(b)
		if c <= 0 || n > uint64(len(b)-c) {
			return fmt.Errorf("corrupt value")
		}
		r := Record{Bucket: string(k[:sep]), ID: binary.BigEndian.Uint64(k[sep+1:]), Label: string(b[c : c+int(n)]), Payload: b[c+int(n):]}
		if err = fn(r); err != nil {
			return err
		}
	}
	return it.Error()
}
