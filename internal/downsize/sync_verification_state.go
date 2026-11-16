package downsize

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"time"
)

// Active verification looks up only its current batch. Terminal recovery still
// reduces ordered receipts, without consulting a second receipt authority.
type syncVerificationPersistence interface {
	latest(round int, first string) (syncRow, bool, error)
	scan(round int, visit func(syncRow) error) error
	observations(round int, source string, names []string) (map[string]string, error)
	append(syncRow) error
}

type legacySyncVerification struct {
	j       *syncJournal
	round   int
	loaded  bool
	byFirst map[string]syncRow
}

// Retain legacy's one-time round index. SQLite needs no round-sized map.
func (p *legacySyncVerification) latest(round int, first string) (syncRow, bool, error) {
	if !p.loaded || p.round != round {
		p.byFirst = map[string]syncRow{}
		for _, r := range p.j.find(func(r syncRow) bool { return r.Kind == "verified" && r.Round == round }) {
			p.byFirst[r.First] = r
		}
		p.loaded, p.round = true, round
	}
	r, found := p.byFirst[first]
	return r, found, nil
}

func (p *legacySyncVerification) scan(round int, visit func(syncRow) error) error {
	for _, r := range p.j.find(func(r syncRow) bool {
		return r.Round == round && (r.Kind == "verified" || r.Kind == "receipt-reverified" || r.Kind == "ambiguous-receipt")
	}) {
		if err := visit(r); err != nil {
			return err
		}
	}
	return nil
}

func (p *legacySyncVerification) append(r syncRow) error { return p.j.append(r) }

func (s *syncer) verificationPersistence() syncVerificationPersistence {
	if s.verificationState != nil {
		return s.verificationState
	}
	return &legacySyncVerification{j: s.j}
}

func (s *syncer) readVerificationRound(round int) (rows []syncRow, err error) {
	err = s.verificationPersistence().scan(round, func(r syncRow) error { rows = append(rows, r); return nil })
	return rows, err
}

type sqliteSyncVerification struct {
	store *stateStore
	scope string
}

func (p sqliteSyncVerification) latest(round int, first string) (r syncRow, found bool, err error) {
	tx, err := p.store.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return r, false, err
	}
	defer tx.Rollback()
	var ordinal int64
	err = tx.QueryRow("SELECT ordinal FROM verification_receipt WHERE scope=? AND round=? AND kind='verified' AND first_name=? ORDER BY ordinal DESC LIMIT 1", p.scope, round, first).Scan(&ordinal)
	if errors.Is(err, sql.ErrNoRows) {
		return r, false, nil
	}
	if err == nil {
		r, err = readStateVerification(tx, p.scope, ordinal)
	}
	if err == nil {
		err = tx.Commit()
	}
	return r, err == nil, err
}

func (p sqliteSyncVerification) scan(round int, visit func(syncRow) error) error {
	tx, err := p.store.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for after := int64(-1); ; {
		var ordinals []int64
		err = stateTaskRows(tx, "SELECT ordinal FROM verification_receipt WHERE scope=? AND round=? AND ordinal>? ORDER BY ordinal LIMIT 4096", []any{p.scope, round, after}, func(rows *sql.Rows) error {
			var ordinal int64
			if err := rows.Scan(&ordinal); err != nil {
				return err
			}
			ordinals = append(ordinals, ordinal)
			return nil
		})
		if err != nil {
			return err
		}
		if len(ordinals) == 0 {
			return tx.Commit()
		}
		for _, ordinal := range ordinals {
			r, err := readStateVerification(tx, p.scope, ordinal)
			if err != nil {
				return err
			}
			if err = visit(r); err != nil {
				return err
			}
			after = ordinal
		}
	}
}

func (p sqliteSyncVerification) append(r syncRow) error {
	if r.Kind == "verified" && r.Identity == "" {
		r.Identity = rand.Text()
	}
	r.Time = time.Now().UTC().Format(time.RFC3339Nano)
	if r.LegacyObservation && r.LegacyObservationTime == "" {
		r.LegacyObservationTime = r.Time
	}
	return p.store.update(func(tx *stateTx) error {
		if err := tx.appendVerification(p.scope, r); err != nil {
			return err
		}
		return tx.reduceProgress(p.scope, r)
	})
}

// Observation identities come from the latest receipt that actually contains a
// hole for this source/name. A later clean batch does not erase that binding.
func verificationObservation(r syncRow) string {
	observed := r.Time
	if r.LegacyObservationTime != "" {
		observed = r.LegacyObservationTime
	}
	return r.Identity + "/" + observed
}

func (p *legacySyncVerification) observations(round int, source string, names []string) (map[string]string, error) {
	out := make(map[string]string, len(names))
	for _, name := range names {
		out[name] = ""
	}
	err := p.scan(round, func(r syncRow) error {
		if r.Kind != "verified" && r.Kind != "receipt-reverified" {
			return nil
		}
		for _, hole := range r.Holes {
			if _, ok := out[hole.Name]; ok && hole.Source == source {
				out[hole.Name] = verificationObservation(r)
			}
		}
		return nil
	})
	return out, err
}

func (p sqliteSyncVerification) observations(round int, source string, names []string) (map[string]string, error) {
	out := make(map[string]string, len(names))
	tx, err := p.store.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare(`SELECT r.ordinal FROM verification_hole h JOIN verification_receipt r ON r.scope=h.scope AND r.ordinal=h.receipt WHERE h.scope=? AND h.holder=? AND h.name=? AND r.round=? AND r.kind IN ('verified','receipt-reverified') ORDER BY r.ordinal DESC LIMIT 1`)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	checked := map[int64]string{}
	for _, name := range names {
		out[name] = ""
		var ordinal int64
		err := stmt.QueryRow(p.scope, source, name, round).Scan(&ordinal)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, err
		}
		observed, ok := checked[ordinal]
		if !ok {
			r, err := readStateVerification(tx, p.scope, ordinal)
			if err != nil {
				return nil, err
			}
			observed = verificationObservation(r)
			checked[ordinal] = observed
		}
		out[name] = observed
	}
	return out, tx.Commit()
}
