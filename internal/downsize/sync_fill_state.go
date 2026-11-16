package downsize

// Fill scheduling uses one receipt owner. The SQLite implementation is injected
// only by tests until phase restoration, native children and publication use the
// same unit owner. A nil adapter preserves the existing production format.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

type syncFillPersistence interface {
	lookup(round int, source, identity string, allowForbidden bool) (syncRow, bool, error)
	append(syncRow) error
}

type legacySyncFill struct{ j *syncJournal }

func (p legacySyncFill) lookup(round int, source, identity string, allowForbidden bool) (syncRow, bool, error) {
	rows := p.j.find(func(r syncRow) bool {
		return r.Kind == "copied" && r.Phase == "fill" && r.Round == round && r.Source == source && r.Identity == identity && (!r.Forbidden || allowForbidden)
	})
	if len(rows) == 0 {
		return syncRow{}, false, nil
	}
	return rows[0], true, nil
}

func (p legacySyncFill) append(r syncRow) error { return p.j.append(r) }

func (s *syncer) fillPersistence() syncFillPersistence {
	if s.fillState != nil {
		return s.fillState
	}
	return legacySyncFill{s.j}
}

func (s *syncer) appendCopyReceipt(r syncRow) error {
	if r.Phase == "fill" {
		return s.fillPersistence().append(r)
	}
	return s.j.append(r)
}

// Both persistence implementations run this controller. A completed phase with
// a missing receipt is a contradiction, never permission to repeat the import.
func (s *syncer) fillFrom(ctx context.Context, round, src int, names []string) (syncCopyResult, bool, error) {
	identity, err := s.fillIdentity(round, src, names)
	if err != nil {
		return syncCopyResult{}, false, err
	}
	complete := s.phaseBeforeResume("fill", round+s.verificationBase)
	r, found, err := s.fillPersistence().lookup(round+s.verificationBase, s.o.from[src], identity, complete)
	if err != nil {
		return syncCopyResult{}, false, err
	}
	if found {
		res := syncCopyResult{Copied: r.Copied, Failed: r.Failed, Forbidden: r.Forbidden}
		for _, item := range r.Resolved {
			res.ExpectedAbsent = append(res.ExpectedAbsent, item.Name)
		}
		return res, true, nil
	}
	if complete {
		return syncCopyResult{}, false, fmt.Errorf("resume: completed fill round %d lacks receipt for %s", round+s.verificationBase, s.o.from[src])
	}
	res, err := s.copyFrom(ctx, "fill", round, src, names)
	return res, false, err
}

type sqliteSyncFill struct {
	store *stateStore
	scope string
}

func (p sqliteSyncFill) lookup(round int, source, identity string, allowForbidden bool) (r syncRow, found bool, err error) {
	tx, err := p.store.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return r, false, err
	}
	defer tx.Rollback()
	var ordinal int64
	err = tx.QueryRow(`SELECT ordinal FROM fill_receipt WHERE scope=? AND round=? AND holder=? AND identity=? AND (forbidden=0 OR ?) ORDER BY ordinal LIMIT 1`, p.scope, round, source, identity, allowForbidden).Scan(&ordinal)
	if errors.Is(err, sql.ErrNoRows) {
		return r, false, nil
	}
	if err == nil {
		r, err = readStateFillReceipt(tx, p.scope, ordinal)
	}
	if err == nil {
		err = tx.Commit()
	}
	return r, err == nil, err
}

func (p sqliteSyncFill) append(r syncRow) error {
	r.Time = time.Now().UTC().Format(time.RFC3339Nano)
	return p.store.update(func(tx *stateTx) error {
		if err := tx.appendFillReceipt(p.scope, r); err != nil {
			return err
		}
		return tx.reduceProgress(p.scope, r)
	})
}
