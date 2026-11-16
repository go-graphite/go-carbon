package downsize

// The ledger adopter reads authenticated source bytes without truncating torn
// tails or materializing replacement legacy files. It publishes only candidate
// relations, never a runtime backend/configuration switch.

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

func (t *stateTx) bindIssueImport(path, scope, kind string, delta bool) error {
	if kind != "policy" && kind != "quarantine" {
		return errors.New("invalid issue ledger kind")
	}
	result, err := t.tx.Exec(`INSERT INTO ledger_import VALUES(?,?,?,?) ON CONFLICT(path) DO UPDATE SET path=excluded.path WHERE ledger_import.scope=excluded.scope AND ledger_import.kind=excluded.kind AND ledger_import.delta=excluded.delta`, path, scope, kind, delta)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err == nil && n != 1 {
		return errors.New("issue import destination changed")
	}
	return err
}

func (s *stateStore) adoptIssueLedger(relative, scope, kind string, delta bool) (stats stateAdoptStats, err error) {
	f, err := s.adoptSource(relative)
	if err != nil {
		return stats, err
	}
	defer func() { err = errors.Join(err, f.file.Close()) }()
	// Check a completed import's destination too: source authentication alone
	// does not authorize treating the same path as another ledger or child scope.
	var boundScope, boundKind string
	var boundDelta bool
	bindErr := s.db.QueryRow("SELECT scope,kind,delta FROM ledger_import WHERE path=?", relative).Scan(&boundScope, &boundKind, &boundDelta)
	if bindErr != nil && !errors.Is(bindErr, sql.ErrNoRows) {
		return stats, bindErr
	}
	if bindErr == nil && (boundScope != scope || boundKind != kind || boundDelta != delta) {
		return stats, errors.New("issue import destination changed")
	}
	if f.cursor.Complete && bindErr != nil {
		return stats, errors.New("completed issue import has no destination")
	}
	stats.ResumedNames = f.cursor.Records
	if f.cursor.Complete {
		return stats, nil
	}
	commit := func(rows []syncIssue, offset, records int64, complete bool) error {
		next := f.cursor
		next.Offset = offset
		next.Records = records
		next.Complete = complete
		if _, err := io.CopyN(f.prefix, io.NewSectionReader(f.file, f.cursor.Offset, offset-f.cursor.Offset), offset-f.cursor.Offset); err != nil {
			return err
		}
		copy(next.PrefixSHA256[:], f.prefix.Sum(nil))
		if complete && next.PrefixSHA256 != next.Source.SHA256 {
			return errors.New("issue ledger changed after authentication")
		}
		if err := f.commit(next, func(tx *stateTx) error {
			if err := tx.bindIssueImport(relative, scope, kind, delta); err != nil {
				return err
			}
			for _, v := range rows {
				if err := tx.putLedgerIssue(scope, kind, v, delta); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		stats.AdoptedNames += int64(len(rows))
		return nil
	}
	if delta {
		reader := bufio.NewReader(io.NewSectionReader(f.file, f.cursor.Offset, f.cursor.Source.Size-f.cursor.Offset))
		offset, records := f.cursor.Offset, f.cursor.Records
		for {
			line, err := reader.ReadBytes('\n')
			offset += int64(len(line))
			if errors.Is(err, io.EOF) {
				return stats, commit(nil, offset, records, true)
			} // incomplete final append is ignored, including valid JSON without LF
			if err != nil {
				return stats, err
			}
			var rows []syncIssue
			if err := json.Unmarshal(bytes.TrimSpace(line), &rows); err != nil {
				return stats, fmt.Errorf("%s journal: %w", relative, err)
			}
			records += int64(len(rows))
			// Each legacy record is atomic; do not split a record's reducer update
			// from its authenticated line boundary. Snapshot pages are bounded below.
			if err := commit(rows, offset, records, false); err != nil {
				return stats, err
			}
		}
	}
	// Decode a snapshot in bounded pages. A resume authenticates the saved byte
	// prefix, then scans past that many objects; it retains no whole-ledger map.
	decoder := json.NewDecoder(io.NewSectionReader(f.file, 0, f.cursor.Source.Size))
	token, err := decoder.Token()
	if err != nil {
		return stats, err
	}
	if token != nil && token != json.Delim('[') {
		return stats, errors.New("issue snapshot must be an array or null")
	}
	var records int64
	page := make([]syncIssue, 0, statePageNames)
	if token != nil {
		for decoder.More() {
			var v syncIssue
			if err := decoder.Decode(&v); err != nil {
				return stats, err
			}
			records++
			if records <= f.cursor.Records {
				if records == f.cursor.Records && decoder.InputOffset() != f.cursor.Offset {
					return stats, errors.New("issue snapshot cursor is not an object boundary")
				}
				continue
			}
			page = append(page, v)
			if len(page) == statePageNames {
				if err := commit(page, decoder.InputOffset(), records, false); err != nil {
					return stats, err
				}
				page = page[:0]
			}
		}
		if _, err := decoder.Token(); err != nil {
			return stats, err
		}
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("trailing issue snapshot JSON")
		}
		return stats, err
	}
	if records < f.cursor.Records {
		return stats, errors.New("issue snapshot cursor exceeds row count")
	}
	return stats, commit(page, f.cursor.Source.Size, records, true)
}
