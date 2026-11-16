package downsize

// Lossless adapters for the legacy scheduling position, progress reducer,
// residual evidence and binary proof frontier. These are candidate-format
// adapters, not an activation path: all runtime persistence paths and whole-unit
// adoption must cross this boundary before sync can select this format.

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

const stateSemanticDDL = `
CREATE TABLE phase_summary(scope TEXT PRIMARY KEY REFERENCES phase(scope), version INTEGER NOT NULL CHECK(version=1), options TEXT NOT NULL, discovered INTEGER NOT NULL, on_target INTEGER NOT NULL, missing INTEGER NOT NULL, not_owned INTEGER NOT NULL, short INTEGER NOT NULL, both INTEGER NOT NULL, copied INTEGER NOT NULL, copy_failed INTEGER NOT NULL, residual_present INTEGER NOT NULL CHECK(residual_present IN (0,1))) STRICT;
CREATE TABLE phase_residual(scope TEXT NOT NULL REFERENCES phase_summary(scope), ordinal INTEGER NOT NULL CHECK(ordinal>=0), version INTEGER NOT NULL CHECK(version=1), name TEXT NOT NULL, source TEXT NOT NULL, window TEXT NOT NULL, step INTEGER NOT NULL, points INTEGER NOT NULL, first INTEGER NOT NULL, last INTEGER NOT NULL, reason TEXT NOT NULL, PRIMARY KEY(scope,ordinal)) STRICT, WITHOUT ROWID;
CREATE TABLE progress(scope TEXT PRIMARY KEY REFERENCES scope(id), version INTEGER NOT NULL CHECK(version=1), legacy_adopted INTEGER NOT NULL, legacy_reverified INTEGER NOT NULL, ambiguous INTEGER NOT NULL, contradictions INTEGER NOT NULL, regressions INTEGER NOT NULL, legacy_revalidated INTEGER NOT NULL, round INTEGER NOT NULL, discovered INTEGER NOT NULL, verified INTEGER NOT NULL, filled INTEGER NOT NULL, copied INTEGER NOT NULL, holes INTEGER NOT NULL, cursors_present INTEGER NOT NULL CHECK(cursors_present IN (0,1)), redo_present INTEGER NOT NULL CHECK(redo_present IN (0,1))) STRICT;
CREATE TABLE progress_cursor(scope TEXT NOT NULL REFERENCES progress(scope), holder TEXT NOT NULL, version INTEGER NOT NULL CHECK(version=1), cursor TEXT NOT NULL, PRIMARY KEY(scope,holder)) STRICT, WITHOUT ROWID;
CREATE TABLE progress_redo(scope TEXT NOT NULL REFERENCES progress(scope), phase TEXT NOT NULL, version INTEGER NOT NULL CHECK(version=1), names INTEGER NOT NULL, PRIMARY KEY(scope,phase)) STRICT, WITHOUT ROWID;
CREATE TABLE issue_evidence(scope TEXT NOT NULL, window TEXT NOT NULL, holder TEXT NOT NULL, kind TEXT NOT NULL, name_id INTEGER NOT NULL, ordinal INTEGER NOT NULL CHECK(ordinal>=0), version INTEGER NOT NULL CHECK(version=1), step INTEGER NOT NULL, points INTEGER NOT NULL, first INTEGER NOT NULL, last INTEGER NOT NULL, proof TEXT NOT NULL, PRIMARY KEY(scope,window,holder,kind,name_id,ordinal), FOREIGN KEY(scope,window,holder,kind,name_id) REFERENCES issue(scope,window,holder,kind,name_id)) STRICT, WITHOUT ROWID;
CREATE TABLE frontier_generation(scope TEXT NOT NULL REFERENCES scope(id), window TEXT NOT NULL, holder TEXT NOT NULL, generation TEXT NOT NULL, version INTEGER NOT NULL CHECK(version=1), identity TEXT NOT NULL, settle INTEGER NOT NULL, now INTEGER NOT NULL CHECK(now>0), PRIMARY KEY(scope,window,holder,generation)) STRICT, WITHOUT ROWID;
CREATE TABLE redo_work(scope TEXT NOT NULL REFERENCES scope(id), operation BLOB NOT NULL CHECK(length(operation)=8), first INTEGER NOT NULL CHECK(first>=0), last INTEGER NOT NULL CHECK(last>=first), version INTEGER NOT NULL CHECK(version=1), PRIMARY KEY(scope,operation,first)) STRICT, WITHOUT ROWID;
`

var stateSemanticDigestQueries = map[string]string{
	"native_receipt":             "SELECT * FROM native_receipt ORDER BY scope,identity",
	"native_receipt_table":       "SELECT * FROM native_receipt_table ORDER BY scope,identity,ordinal",
	"native_receipt_mapping":     "SELECT * FROM native_receipt_mapping ORDER BY scope,identity,ordinal",
	"native_receipt_skip":        "SELECT * FROM native_receipt_skip ORDER BY scope,identity,ordinal",
	"native_receipt_window_skip": "SELECT * FROM native_receipt_window_skip ORDER BY scope,identity,entry",
	"window_points":              "SELECT * FROM window_points ORDER BY scope,operation",
	"recovery":                   "SELECT * FROM recovery ORDER BY child_scope",
	"recovery_window":            "SELECT * FROM recovery_window ORDER BY child_scope,kind,ordinal",
	"recovery_process":           "SELECT * FROM recovery_process ORDER BY child_scope,holder",
	"phase_summary":              "SELECT * FROM phase_summary ORDER BY scope",
	"phase_residual":             "SELECT * FROM phase_residual ORDER BY scope,ordinal",
	"progress":                   "SELECT * FROM progress ORDER BY scope",
	"progress_cursor":            "SELECT * FROM progress_cursor ORDER BY scope,holder",
	"progress_redo":              "SELECT * FROM progress_redo ORDER BY scope,phase",
	"issue_evidence":             "SELECT * FROM issue_evidence ORDER BY scope,window,holder,kind,name_id,ordinal",
	"frontier_generation":        "SELECT * FROM frontier_generation ORDER BY scope,window,holder,generation",
	"redo_work":                  "SELECT * FROM redo_work ORDER BY scope,operation,first",
}

// Keep the position and its entire summary in one transaction. Residuals are
// ordered, sparse evidence: several ranges/holders may refer to the same name.
// Legacy done may exceed an old display total; do not clamp the durable floor.
func (t *stateTx) putPhasePosition(scope string, p syncPhasePosition) error {
	if p.Options == "" {
		return errors.New("phase position requires execution options")
	}
	if err := t.putPhase(statePhase{Scope: scope, Phase: p.Phase, Round: p.Round, Done: p.Done, Total: p.Total, Complete: p.Complete}); err != nil {
		return err
	}
	v := p.Summary
	_, err := t.tx.Exec(`INSERT INTO phase_summary VALUES(?,1,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(scope) DO UPDATE SET options=excluded.options,discovered=excluded.discovered,on_target=excluded.on_target,missing=excluded.missing,not_owned=excluded.not_owned,short=excluded.short,both=excluded.both,copied=excluded.copied,copy_failed=excluded.copy_failed,residual_present=excluded.residual_present`, scope, p.Options, v.Discovered, v.OnTarget, v.Missing, v.NotOwned, v.Short, v.Both, v.Copied, v.CopyFailed, v.Residual != nil)
	if err != nil {
		return err
	}
	if _, err = t.tx.Exec("DELETE FROM phase_residual WHERE scope=?", scope); err != nil {
		return err
	}
	for ordinal, r := range v.Residual {
		if _, err = t.tx.Exec("INSERT INTO phase_residual VALUES(?,?,1,?,?,?,?,?,?,?,?)", scope, ordinal, r.Name, r.Source, r.Window, r.Step, r.Points, r.First, r.Last, r.Reason); err != nil {
			return err
		}
	}
	return nil
}

func readStatePhasePosition(tx *sql.Tx, scope string) (p syncPhasePosition, err error) {
	h, err := readStatePhase(tx, scope)
	if err != nil {
		return p, err
	}
	p.Phase, p.Round, p.Done, p.Total, p.Complete = h.Phase, h.Round, h.Done, h.Total, h.Complete
	v := &p.Summary
	var present bool
	err = tx.QueryRow("SELECT options,discovered,on_target,missing,not_owned,short,both,copied,copy_failed,residual_present FROM phase_summary WHERE scope=?", scope).Scan(&p.Options, &v.Discovered, &v.OnTarget, &v.Missing, &v.NotOwned, &v.Short, &v.Both, &v.Copied, &v.CopyFailed, &present)
	if err != nil {
		return p, err
	}
	if present {
		v.Residual = []syncResidual{}
	}
	rows, err := tx.Query("SELECT name,source,window,step,points,first,last,reason FROM phase_residual WHERE scope=? ORDER BY ordinal", scope)
	if err != nil {
		return p, err
	}
	defer rows.Close()
	for rows.Next() {
		var r syncResidual
		if err = rows.Scan(&r.Name, &r.Source, &r.Window, &r.Step, &r.Points, &r.First, &r.Last, &r.Reason); err != nil {
			return p, err
		}
		v.Residual = append(v.Residual, r)
	}
	return p, rows.Err()
}

// Persist only the authoritative journal reducer, not the progress ticker's
// display rate, wall clock, transient waiting status or effective CLI policy.
func readStateProgress(tx *sql.Tx, scope string) (p progressSnapshot, err error) {
	var cursors, redo bool
	err = tx.QueryRow("SELECT legacy_adopted,legacy_reverified,ambiguous,contradictions,regressions,legacy_revalidated,round,discovered,verified,filled,copied,holes,cursors_present,redo_present FROM progress WHERE scope=?", scope).Scan(&p.LegacyAdopted, &p.LegacyReverified, &p.AmbiguousReceiptNames, &p.ReceiptContradictions, &p.PhaseRegressions, &p.LegacyRevalidated, &p.Round, &p.Discovered, &p.Verified, &p.Filled, &p.Copied, &p.Holes, &cursors, &redo)
	if err != nil {
		return p, err
	}
	if cursors {
		p.Cursors = map[string]string{}
	}
	if redo {
		p.RedoNames = map[string]int64{}
	}
	rows, err := tx.Query("SELECT holder,cursor FROM progress_cursor WHERE scope=? ORDER BY holder", scope)
	if err != nil {
		return p, err
	}
	for rows.Next() {
		var h, c string
		if err = rows.Scan(&h, &c); err != nil {
			rows.Close()
			return p, err
		}
		p.Cursors[h] = c
	}
	if err = errors.Join(rows.Err(), rows.Close()); err != nil {
		return p, err
	}
	rows, err = tx.Query("SELECT phase,names FROM progress_redo WHERE scope=? ORDER BY phase", scope)
	if err != nil {
		return p, err
	}
	defer rows.Close()
	for rows.Next() {
		var phase string
		var n int64
		if err = rows.Scan(&phase, &n); err != nil {
			return p, err
		}
		p.RedoNames[phase] = n
	}
	return p, rows.Err()
}

func (t *stateTx) reduceProgress(scope string, r syncRow) error {
	p, err := readStateProgress(t.tx, scope)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	// This projection is intentionally independent of progressSnapshot.apply;
	// differential tests use that actual legacy reducer as their oracle.
	if r.Discovery != nil {
		p.Cursors = cloneCursors(r.Discovery.Cursors)
		if r.Discovery.Cursors == nil {
			p.Cursors = nil
		}
		p.Discovered = int64(r.Discovery.Names)
	}
	switch r.Kind {
	case "ambiguous-receipt":
		p.AmbiguousReceiptNames += int64(r.Names)
	case "receipt-contradiction":
		p.ReceiptContradictions++
	case "phase-regression":
		p.PhaseRegressions++
	case "phase-position":
		if r.Position != nil {
			p.Round = r.Position.Round
		}
	case "redo":
		if p.RedoNames == nil {
			p.RedoNames = map[string]int64{}
		}
		p.RedoNames[r.Phase] += int64(r.Names)
	case "discovered":
		p.Discovered = int64(r.Names)
	case "verified":
		p.LegacyAdopted += int64(r.LegacyAdopted)
		p.LegacyReverified += int64(r.LegacyReverified)
		p.LegacyRevalidated += int64(r.LegacyRevalidated)
		p.Verified += int64(r.Names)
		for _, h := range r.Holes {
			p.Holes += int64(h.Points)
		}
	case "copied":
		if r.Phase == "fill" {
			p.Filled += int64(r.Copied)
		} else {
			p.Copied += int64(r.Copied)
		}
	}
	if (r.Kind == "verified" || r.Phase == "fill") && r.Round > p.Round {
		p.Round = r.Round
	}
	_, err = t.tx.Exec(`INSERT INTO progress VALUES(?,1,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(scope) DO UPDATE SET legacy_adopted=excluded.legacy_adopted,legacy_reverified=excluded.legacy_reverified,ambiguous=excluded.ambiguous,contradictions=excluded.contradictions,regressions=excluded.regressions,legacy_revalidated=excluded.legacy_revalidated,round=excluded.round,discovered=excluded.discovered,verified=excluded.verified,filled=excluded.filled,copied=excluded.copied,holes=excluded.holes,cursors_present=excluded.cursors_present,redo_present=excluded.redo_present`, scope, p.LegacyAdopted, p.LegacyReverified, p.AmbiguousReceiptNames, p.ReceiptContradictions, p.PhaseRegressions, p.LegacyRevalidated, p.Round, p.Discovered, p.Verified, p.Filled, p.Copied, p.Holes, p.Cursors != nil, p.RedoNames != nil)
	if err != nil {
		return err
	}
	if _, err = t.tx.Exec("DELETE FROM progress_cursor WHERE scope=?", scope); err != nil {
		return err
	}
	for holder, cursor := range p.Cursors {
		if _, err = t.tx.Exec("INSERT INTO progress_cursor VALUES(?,?,1,?)", scope, holder, cursor); err != nil {
			return err
		}
	}
	if _, err = t.tx.Exec("DELETE FROM progress_redo WHERE scope=?", scope); err != nil {
		return err
	}
	for phase, n := range p.RedoNames {
		if _, err = t.tx.Exec("INSERT INTO progress_redo VALUES(?,?,1,?)", scope, phase, n); err != nil {
			return err
		}
	}
	return nil
}

type stateIssueEvidence struct {
	Step, Points, First, Last int64
	Proof                     string
}

func (t *stateTx) putIssueEvidence(i stateIssue, evidence []stateIssueEvidence) error {
	if err := t.putIssue(i); err != nil {
		return err
	}
	if _, err := t.tx.Exec("DELETE FROM issue_evidence WHERE scope=? AND window=? AND holder=? AND kind=? AND name_id=?", i.Scope, i.Window, i.Holder, i.Kind, i.NameID); err != nil {
		return err
	}
	for n, e := range evidence {
		if _, err := t.tx.Exec("INSERT INTO issue_evidence VALUES(?,?,?,?,?,?,1,?,?,?,?,?)", i.Scope, i.Window, i.Holder, i.Kind, i.NameID, n, e.Step, e.Points, e.First, e.Last, e.Proof); err != nil {
			return err
		}
	}
	return nil
}

type stateFrontierGeneration struct {
	Scope, Window, Holder, Generation string
	Identity                          string
	Settle                            time.Duration
	Now                               int64
}

// Match the legacy scope JSON byte-for-byte: it participates in the seal.
func (g stateFrontierGeneration) key() string {
	data, _ := json.Marshal(struct {
		Identity string
		Settle   time.Duration
		Now      int64
	}{g.Identity, g.Settle, g.Now})
	return string(data)
}

func (t *stateTx) putFrontierGeneration(g stateFrontierGeneration) error {
	if g.Generation == "" || g.Identity == "" || g.Now <= 0 {
		return errors.New("invalid frontier generation")
	}
	r, err := t.tx.Exec(`INSERT INTO frontier_generation VALUES(?,?,?,?,1,?,?,?) ON CONFLICT(scope,window,holder,generation) DO UPDATE SET identity=excluded.identity WHERE frontier_generation.identity=excluded.identity AND frontier_generation.settle=excluded.settle AND frontier_generation.now=excluded.now`, g.Scope, g.Window, g.Holder, g.Generation, g.Identity, int64(g.Settle), g.Now)
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err == nil && n != 1 {
		return errors.New("immutable frontier generation changed")
	}
	return err
}

// The legacy file leaves ID 0 unused. Preserve every raw record, including
// zero and torn records: only the existing name/scope seal can authorize reuse.
// A page uses at most 128KiB, independent of the catalog or frontier extent.
func (t *stateTx) importFrontierPage(g stateFrontierGeneration, src io.ReaderAt, first int64) error {
	if first < 1 || (first-1)%statePageNames != 0 || first > t.unit.CatalogNames {
		return errors.New("invalid frontier page")
	}
	if err := t.putFrontierGeneration(g); err != nil {
		return err
	}
	data := make([]byte, min(int64(statePageNames), t.unit.CatalogNames-first+1)*32)
	if _, err := src.ReadAt(data, first*32); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return t.putProof(stateProofPage{Scope: g.Scope, Window: g.Window, Holder: g.Holder, Generation: g.Generation, FirstID: first, Records: data})
}

// FNV fingerprints occupy the full unsigned 64-bit domain; SQLite INTEGER is
// signed. An eight-byte key avoids truncating the high bit during adoption.
func (t *stateTx) rememberRedo(scope string, w syncRedoWork) error {
	for _, key := range w.Keys {
		encoded := binary.BigEndian.AppendUint64(nil, key)
		var ranges []syncRedoRange
		rows, err := t.tx.Query("SELECT first,last FROM redo_work WHERE scope=? AND operation=? ORDER BY first", scope, encoded)
		if err != nil {
			return err
		}
		for rows.Next() {
			var r syncRedoRange
			if err = rows.Scan(&r.First, &r.Last); err != nil {
				rows.Close()
				return err
			}
			ranges = append(ranges, r)
		}
		if err = errors.Join(rows.Err(), rows.Close()); err != nil {
			return err
		}
		for _, r := range w.IDs {
			if r.First < 0 || r.Last < r.First {
				return errors.New("invalid redo receipt range")
			}
			ranges = redoMerge(ranges, r)
		}
		if _, err = t.tx.Exec("DELETE FROM redo_work WHERE scope=? AND operation=?", scope, encoded); err != nil {
			return err
		}
		for _, r := range ranges {
			if _, err = t.tx.Exec("INSERT INTO redo_work VALUES(?,?,?,?,1)", scope, encoded, r.First, r.Last); err != nil {
				return err
			}
		}
	}
	return nil
}

func readStateProofPage(tx *sql.Tx, g stateFrontierGeneration, first int64) ([]byte, error) {
	var data, want []byte
	err := tx.QueryRow("SELECT records,sha256 FROM proof_page WHERE scope=? AND window=? AND holder=? AND generation=? AND first_id=?", g.Scope, g.Window, g.Holder, g.Generation, first).Scan(&data, &want)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(data)
	// putProof stores SHA256 over the exact fixed-width bytes.
	if !bytes.Equal(sum[:], want) {
		return nil, fmt.Errorf("frontier page checksum mismatch at %d", first)
	}
	return data, nil
}
