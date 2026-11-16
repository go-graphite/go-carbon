package statestore

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

const importBucket = "__store_import_v1"

// ImportCursor binds a committed byte boundary to an immutable input digest.
// It belongs to the same transaction as the records before that boundary.
// This is an offline spool import cursor, not a legacy-state cutover marker.
type ImportCursor struct {
	Version int      `json:"version"`
	Source  string   `json:"source_sha256"`
	Offset  int64    `json:"offset"`
	Records uint64   `json:"records"`
	XOR     [32]byte `json:"xor_sha256"`
}

func (s *Store) ImportPosition(source string) (ImportCursor, error) {
	if b, err := hex.DecodeString(source); err != nil || len(b) != 32 {
		return ImportCursor{}, errors.New("import source must be a SHA256 digest")
	}
	c := ImportCursor{Version: 1, Source: source}
	b, err := s.Get(importBucket, 0)
	if errors.Is(err, ErrNotFound) {
		return c, nil
	}
	if err != nil {
		return c, err
	}
	if err = json.Unmarshal(b, &c); err != nil {
		return c, err
	}
	if c.Version != 1 || c.Source != source || c.Offset < 0 {
		return c, errors.New("import cursor version, input digest or offset changed")
	}
	return c, nil
}

// ImportBatch compares the previous cursor and atomically stores the batch and
// next cursor. A caller that loses a commit acknowledgement reloads its cursor;
// it must never infer progress from the input reader's buffered position.
func (s *Store) ImportBatch(previous, next ImportCursor, records []Record, beforeCommit func()) error {
	if previous.Version != 1 || next.Version != 1 || previous.Source != next.Source || previous.Offset < 0 || next.Offset <= previous.Offset || next.Records <= previous.Records || next.Records != previous.Records+uint64(len(records)) || len(records) == 0 {
		return errors.New("invalid import cursor advance")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.e.Begin()
	if err != nil {
		return err
	}
	defer t.Close()
	actual := ImportCursor{Version: 1, Source: previous.Source}
	if b, e := t.Get(importBucket, 0); e == nil {
		if err = json.Unmarshal(b, &actual); err != nil {
			return err
		}
	} else if !errors.Is(e, ErrNotFound) {
		return e
	}
	if actual != previous {
		return errors.New("import cursor changed; reload committed position")
	}
	for _, r := range records {
		if r.Bucket == importBucket {
			return fmt.Errorf("reserved import bucket %q", r.Bucket)
		}
		if err = validateRecord(r); err != nil {
			return err
		}
		if err = t.Put(r); err != nil {
			return err
		}
	}
	b, err := json.Marshal(next)
	if err != nil {
		return err
	}
	if err = t.Put(Record{Bucket: importBucket, ID: 0, Payload: b}); err != nil {
		return err
	}
	if beforeCommit != nil {
		beforeCommit()
	}
	return t.Commit()
}

func IsImportMetadata(r Record) bool { return r.Bucket == importBucket }
