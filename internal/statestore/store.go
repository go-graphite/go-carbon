// Package statestore is an experimental, offline comparison of durable sync
// state engines. It is not used by storectl's production state machine.
package statestore

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
)

var ErrNotFound = errors.New("state record not found")

// Record is a versioned namespace and stable numeric ID. Labels carry small
// operator-visible descriptions; bulk membership uses IDs in Payload.
type Record struct {
	Bucket  string `json:"bucket"`
	ID      uint64 `json:"id"`
	Label   string `json:"label,omitempty"`
	Payload []byte `json:"payload"`
}

type transaction interface {
	Get(string, uint64) ([]byte, error)
	Put(Record) error
	Insert(Record) (bool, error)
	Commit() error
	Close() error
}

type engine interface {
	Scan(func(Record) error) error
	Begin() (transaction, error)
	Get(string, uint64) ([]byte, error)
	Close() error
}

type Store struct {
	mu sync.Mutex
	e  engine
}

func Open(kind, path string) (*Store, error) {
	var e engine
	var err error
	switch kind {
	case "sqlite":
		e, err = openSQLite(path)
	case "pebble":
		e, err = openPebble(path)
	default:
		return nil, fmt.Errorf("unknown engine %q", kind)
	}
	if err != nil {
		return nil, err
	}
	return &Store{e: e}, nil
}

func (s *Store) Close() error                                 { return s.e.Close() }
func (s *Store) Get(bucket string, id uint64) ([]byte, error) { return s.e.Get(bucket, id) }

// Apply durably commits one import page. A production adopter must publish its
// completion marker only after validating all source identities and receipts.
func (s *Store) Apply(records []Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.e.Begin()
	if err != nil {
		return err
	}
	defer t.Close()
	for _, r := range records {
		if err = validateRecord(r); err != nil {
			return err
		}
		if err = t.Put(r); err != nil {
			return err
		}
	}
	return t.Commit()
}

func validateRecord(r Record) error {
	if r.Bucket == "" || strings.ContainsRune(r.Bucket, 0) || r.ID > math.MaxInt64 || r.Payload == nil {
		return errors.New("invalid state record")
	}
	return nil
}

// CommitWork atomically retires unique IDs, records an idempotency token, and
// advances a window's counter. The hook is only for process-kill fault injection.
// Backend side effects need their own durable idempotent receipts: a local DB
// cannot make a remote copy and a local transaction atomic.
func (s *Store) CommitWork(window, token uint64, ids []uint64, beforeCommit func()) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.e.Begin()
	if err != nil {
		return 0, err
	}
	defer t.Close()
	h := sha256.New()
	var raw [8]byte
	for _, id := range ids {
		if id > math.MaxInt64 {
			return 0, errors.New("ID exceeds portable range")
		}
		binary.LittleEndian.PutUint64(raw[:], id)
		h.Write(raw[:])
	}
	fingerprint := fmt.Sprintf("%x", h.Sum(nil))
	tokens := fmt.Sprintf("tokens/%d", window)
	if previous, e := t.Get(tokens, token); e == nil {
		if string(previous) != fingerprint {
			return 0, errors.New("batch token reused for different membership")
		}
		return 0, nil
	} else if !errors.Is(e, ErrNotFound) {
		return 0, e
	}
	var count uint64
	if b, e := t.Get("window", window); e == nil {
		if err = json.Unmarshal(b, &count); err != nil {
			return 0, err
		}
	} else if !errors.Is(e, ErrNotFound) {
		return 0, e
	}
	done := fmt.Sprintf("done/%d", window)
	var added uint64
	for _, id := range ids {
		ok, e := t.Insert(Record{Bucket: done, ID: id, Payload: []byte{1}})
		if e != nil {
			return 0, e
		}
		if ok {
			added++
		}
	}
	b, _ := json.Marshal(count + added)
	if err = t.Put(Record{Bucket: "window", ID: window, Label: "done", Payload: b}); err != nil {
		return 0, err
	}
	if err = t.Put(Record{Bucket: tokens, ID: token, Payload: []byte(fingerprint)}); err != nil {
		return 0, err
	}
	if beforeCommit != nil {
		beforeCommit()
	}
	return added, t.Commit()
}

func key(bucket string, id uint64) []byte {
	b := make([]byte, len(bucket)+9)
	copy(b, bucket)
	binary.BigEndian.PutUint64(b[len(bucket)+1:], id)
	return b
}

func (s *Store) Scan(fn func(Record) error) error { return s.e.Scan(fn) }
