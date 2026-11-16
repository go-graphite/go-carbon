package downsize

// This is the typed boundary for the indexed state format. It deliberately has
// no generic bucket/JSON-record API. The legacy runtime must not publish this
// format until every persistence adapter uses this boundary; see the design.

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/bits"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"sync"

	_ "modernc.org/sqlite"
)

const stateSchema = 5
const stateApplicationID = 0x5343544c // SCTL, distinct from the offline benchmark
const statePageNames = 4096

type stateUnit struct {
	Identity, Options, CatalogGeneration string
	// A writer may expect -1 while discovery owns the final size. Stored units
	// always have a nonnegative count, bound to the discovery seal on reopen.
	CatalogNames int64
}

type statePhase struct {
	Scope, Phase string
	Round        int
	Done, Total  int64
	Complete     bool
}

type stateWindow struct {
	ID, Scope, Holder string
	From, Until       int64
	Selection         string // pending, selected, complete
	Transfer          string // pending, issued, complete
	Verification      string // pending, complete
}

type stateMemberKey struct {
	Scope, Phase, Window, Holder, Kind string
	Round                              int
}

type stateOperation struct {
	ID, Scope, Window, Holder, Plan, Stream string
	Start, End                              int64
	Status                                  string // intent, acknowledged, complete
	Selection, Receipt                      []byte
}

type stateIssue struct {
	Scope, Window, Holder, Kind, Reason, Phase string
	NameID                                     int64
	FirstSeen, LastSeen                        int64
}

type stateProofPage struct {
	Scope, Window, Holder, Generation string
	FirstID                           int64
	// Fixed-width, scope-bound mutation/integrity records, including zero and
	// incomplete records. Adoption may not manufacture a successful proof.
	Records []byte
}

type stateReady struct {
	Scope, ProofScope, RecoveryScope string
	Report                           readyReport
}

type stateStore struct {
	mu      sync.Mutex
	db      *sql.DB
	path    string
	unit    stateUnit
	lock    *os.File
	failure error // a failed commit is ambiguous; refuse subsequent writes
	closed  bool
}

type stateTx struct {
	tx   *sql.Tx
	unit stateUnit
}

// All durable relations carry an explicit version. There is no second journal
// written by this store. JSON is confined to the existing typed READY report;
// membership, scheduling, source cursors and native receipts have typed keys.
const stateDDL = `
CREATE TABLE unit(singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER NOT NULL CHECK(version=1), identity TEXT NOT NULL, options TEXT NOT NULL, catalog_generation TEXT NOT NULL, catalog_names INTEGER NOT NULL CHECK(catalog_names>=0)) STRICT;
CREATE TABLE scope(id TEXT PRIMARY KEY, version INTEGER NOT NULL CHECK(version=1), parent TEXT REFERENCES scope(id), reason TEXT NOT NULL) STRICT;
CREATE TABLE phase(scope TEXT PRIMARY KEY REFERENCES scope(id), version INTEGER NOT NULL CHECK(version=1), phase TEXT NOT NULL, round INTEGER NOT NULL CHECK(round>=0), done INTEGER NOT NULL CHECK(done>=0), total INTEGER NOT NULL CHECK(total>=0), complete INTEGER NOT NULL CHECK(complete IN (0,1))) STRICT;
CREATE TABLE window(id TEXT PRIMARY KEY, version INTEGER NOT NULL CHECK(version=1), scope TEXT NOT NULL REFERENCES scope(id), holder TEXT NOT NULL, start INTEGER NOT NULL CHECK(start>=0), until INTEGER NOT NULL CHECK(until>start), selection TEXT NOT NULL CHECK(selection IN ('pending','selected','complete')), transfer TEXT NOT NULL CHECK(transfer IN ('pending','issued','complete')), verification TEXT NOT NULL CHECK(verification IN ('pending','complete'))) STRICT;
CREATE TABLE catalog_page(first_id INTEGER PRIMARY KEY CHECK(first_id>0 AND (first_id-1)%4096=0), version INTEGER NOT NULL CHECK(version=1), generation TEXT NOT NULL, names INTEGER NOT NULL CHECK(names>0 AND names<=4096), first_name TEXT NOT NULL, last_name TEXT NOT NULL, sha256 BLOB NOT NULL CHECK(length(sha256)=32), data BLOB NOT NULL) STRICT;
CREATE UNIQUE INDEX catalog_page_name ON catalog_page(first_name);
CREATE TABLE membership_page(scope TEXT NOT NULL REFERENCES scope(id), window TEXT NOT NULL, holder TEXT NOT NULL, phase TEXT NOT NULL, round INTEGER NOT NULL CHECK(round>=0), kind TEXT NOT NULL CHECK(kind IN ('missing','pending','done','residual','selected','resolved','retry','dirty')), first_id INTEGER NOT NULL CHECK(first_id>0 AND (first_id-1)%4096=0), version INTEGER NOT NULL CHECK(version=1), names INTEGER NOT NULL CHECK(names>=0 AND names<=4096), bits BLOB NOT NULL CHECK(length(bits)=512), PRIMARY KEY(scope,phase,round,window,holder,kind,first_id)) STRICT, WITHOUT ROWID;
CREATE TABLE operation(id TEXT PRIMARY KEY, version INTEGER NOT NULL CHECK(version=1), scope TEXT NOT NULL REFERENCES scope(id), window TEXT NOT NULL, holder TEXT NOT NULL, plan TEXT NOT NULL, stream TEXT NOT NULL, start INTEGER NOT NULL CHECK(start>=0), end INTEGER NOT NULL CHECK(end>start), status TEXT NOT NULL CHECK(status IN ('intent','acknowledged','complete')), selection BLOB NOT NULL, selection_sha256 BLOB NOT NULL CHECK(length(selection_sha256)=32), receipt BLOB NOT NULL) STRICT;
CREATE TABLE batch(scope TEXT NOT NULL REFERENCES scope(id), phase TEXT NOT NULL, round INTEGER NOT NULL CHECK(round>=0), token TEXT NOT NULL, version INTEGER NOT NULL CHECK(version=1), sha256 BLOB NOT NULL CHECK(length(sha256)=32), added INTEGER NOT NULL CHECK(added>=0), PRIMARY KEY(scope,phase,round,token)) STRICT, WITHOUT ROWID;
CREATE TABLE reason(id INTEGER PRIMARY KEY, version INTEGER NOT NULL CHECK(version=1), text TEXT NOT NULL UNIQUE CHECK(text<>'')) STRICT;
CREATE TABLE issue(scope TEXT NOT NULL REFERENCES scope(id), window TEXT NOT NULL, holder TEXT NOT NULL, kind TEXT NOT NULL CHECK(kind IN ('policy','quarantine','terminal','residual')), name_id INTEGER NOT NULL CHECK(name_id>0), version INTEGER NOT NULL CHECK(version=1), reason INTEGER NOT NULL REFERENCES reason(id), phase TEXT NOT NULL, first_seen INTEGER NOT NULL CHECK(first_seen>0), last_seen INTEGER NOT NULL CHECK(last_seen>=first_seen), PRIMARY KEY(scope,window,holder,kind,name_id)) STRICT, WITHOUT ROWID;
CREATE TABLE proof_page(scope TEXT NOT NULL REFERENCES scope(id), window TEXT NOT NULL, holder TEXT NOT NULL, generation TEXT NOT NULL, first_id INTEGER NOT NULL CHECK(first_id>0 AND (first_id-1)%4096=0), version INTEGER NOT NULL CHECK(version=1), records BLOB NOT NULL CHECK(length(records)>0 AND length(records)<=131072 AND length(records)%32=0), sha256 BLOB NOT NULL CHECK(length(sha256)=32), PRIMARY KEY(scope,window,holder,generation,first_id)) STRICT, WITHOUT ROWID;
CREATE TABLE ready(scope TEXT PRIMARY KEY REFERENCES scope(id), version INTEGER NOT NULL CHECK(version=1), proof_scope TEXT NOT NULL, recovery_scope TEXT NOT NULL, verdict TEXT NOT NULL CHECK(verdict IN ('READY','READY_EXCEPT','NOT_READY')), report BLOB NOT NULL) STRICT;
CREATE TABLE adoption(path TEXT PRIMARY KEY, version INTEGER NOT NULL CHECK(version=1), size INTEGER NOT NULL CHECK(size>=0), sha256 BLOB NOT NULL CHECK(length(sha256)=32), device TEXT NOT NULL, inode TEXT NOT NULL, modified INTEGER NOT NULL, offset INTEGER NOT NULL CHECK(offset>=0 AND offset<=size), records INTEGER NOT NULL CHECK(records>=0), prefix_sha256 BLOB NOT NULL CHECK(length(prefix_sha256)=32), complete INTEGER NOT NULL CHECK(complete IN (0,1))) STRICT;
CREATE VIEW issue_detail AS SELECT i.scope,i.window,i.holder,i.kind,i.name_id,r.text AS reason,i.phase,i.first_seen,i.last_seen FROM issue i JOIN reason r ON r.id=i.reason;
CREATE VIEW member_counts AS SELECT scope,phase,round,window,holder,kind,sum(names) AS names FROM membership_page GROUP BY scope,phase,round,window,holder,kind;
`

// createStateStore reserves a private database name under the same exclusive
// unit lock used by sync.jsonl. It never truncates a pre-existing database.
func createStateStore(path string, unit stateUnit) (_ *stateStore, err error) {
	if unit.Identity == "" || unit.Options == "" || unit.CatalogGeneration == "" || unit.CatalogNames < 0 {
		return nil, errors.New("incomplete indexed state identity")
	}
	lock, err := lockResumeState(filepath.Join(filepath.Dir(path), "sync.jsonl"))
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, lock.Close())
		}
	}()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = f.Close(); err != nil {
		return nil, err
	}
	db, err := connectStateDB(path, false)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil && db != nil {
			err = errors.Join(err, db.Close())
		}
	}()
	t, err := db.Begin()
	if err != nil {
		return nil, err
	}
	defer t.Rollback()
	if _, err = t.Exec(stateDDL + stateSemanticDDL + stateRecoveryDDL + stateNativeReceiptDDL + stateTransferTaskDDL + stateIssuesDDL + stateSelectionDDL + stateDiscoveryDDL + stateFillDDL + stateVerificationDDL); err == nil {
		_, err = t.Exec("INSERT INTO unit VALUES(1,1,?,?,?,?)", unit.Identity, unit.Options, unit.CatalogGeneration, unit.CatalogNames)
	}
	if err == nil {
		_, err = t.Exec(fmt.Sprintf("PRAGMA application_id=%d; PRAGMA user_version=%d", stateApplicationID, stateSchema))
	}
	if err == nil {
		err = t.Commit()
	}
	if err != nil {
		return nil, err
	}
	return &stateStore{db: db, path: path, unit: unit, lock: lock}, nil
}

// Validate the format before any schema creation or journal-mode mutation.
// Unknown/future databases must remain byte-for-byte untouched.
func openStateStore(path string, expected stateUnit, readOnly bool) (_ *stateStore, err error) {
	if !readOnly && expected == (stateUnit{}) {
		return nil, errors.New("indexed writer requires the expected unit identity")
	}
	var lock *os.File
	if !readOnly {
		lock, err = lockResumeState(filepath.Join(filepath.Dir(path), "sync.jsonl"))
		if err != nil {
			return nil, err
		}
	}
	defer func() {
		if err != nil && lock != nil {
			err = errors.Join(err, lock.Close())
		}
	}()
	db, err := connectStateDB(path, true)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil && db != nil {
			err = errors.Join(err, db.Close())
		}
	}()
	var app, version int
	if err = db.QueryRow("PRAGMA application_id").Scan(&app); err == nil {
		err = db.QueryRow("PRAGMA user_version").Scan(&version)
	}
	if err != nil {
		return nil, err
	}
	if app != stateApplicationID || version != stateSchema {
		return nil, fmt.Errorf("unsupported indexed state application/version: %x/%d", app, version)
	}
	var unit stateUnit
	if err = db.QueryRow("SELECT identity,options,catalog_generation,catalog_names FROM unit WHERE singleton=1 AND version=1").Scan(&unit.Identity, &unit.Options, &unit.CatalogGeneration, &unit.CatalogNames); err != nil {
		return nil, err
	}
	if expected != (stateUnit{}) && unit != expected {
		if expected.CatalogNames != -1 {
			return nil, errors.New("indexed state identity, options or catalog generation changed")
		}
		expected.CatalogNames = unit.CatalogNames
		if unit != expected {
			return nil, errors.New("indexed discovery identity, options or catalog generation changed")
		}
		var names int64
		var complete bool
		if err = db.QueryRow("SELECT names,complete FROM discovery WHERE generation=?", unit.CatalogGeneration).Scan(&names, &complete); err != nil {
			return nil, fmt.Errorf("unknown final catalog size requires discovery state: %w", err)
		}
		if complete && names != unit.CatalogNames || !complete && unit.CatalogNames != 0 {
			return nil, errors.New("indexed discovery seal differs from final catalog size")
		}
	}
	if !readOnly {
		if err = db.Close(); err != nil {
			return nil, err
		}
		db, err = connectStateDB(path, false)
		if err != nil {
			return nil, err
		}
	}
	return &stateStore{db: db, path: path, unit: unit, lock: lock}, nil
}

func connectStateDB(path string, readOnly bool) (*sql.DB, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	u := url.URL{Scheme: "file", Path: abs}
	q := u.Query()
	q.Set("mode", "rw") // the caller must reserve/create explicitly
	if readOnly {
		q.Set("mode", "ro")
		q.Add("_pragma", "query_only=ON")
	} else {
		q.Add("_pragma", "journal_mode=WAL")
		q.Add("_pragma", "synchronous=FULL")
		q.Add("_pragma", "foreign_keys=ON")
		q.Add("_pragma", "busy_timeout=5000")
		q.Add("_pragma", "cache_size=-32768")
		// Close must use the explicit checkpoint error path, not rely on the
		// driver's best-effort last-connection checkpoint.
		q.Add("_pragma", "wal_autocheckpoint=0")
	}
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err == nil {
		db.SetMaxOpenConns(1)
		err = db.Ping()
	}
	if err != nil {
		if db != nil {
			err = errors.Join(err, db.Close())
		}
		return nil, err
	}
	return db, nil
}

func (s *stateStore) update(fn func(*stateTx) error) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("indexed state is closed")
	}
	if s.failure != nil {
		return s.failure
	}
	t, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if rollbackErr := t.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			s.failure = fmt.Errorf("indexed state rollback failed; reopen before continuing: %w", rollbackErr)
			err = errors.Join(err, s.failure)
		}
	}()
	next := &stateTx{t, s.unit}
	if err = fn(next); err != nil {
		return err
	}
	if err = t.Commit(); err != nil {
		s.failure = fmt.Errorf("indexed state commit failed; reopen and reconcile before continuing: %w", err)
		return s.failure
	}
	if next.unit != s.unit {
		s.unit = next.unit
	}
	return nil
}

func (s *stateStore) currentUnit() stateUnit {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.unit
}

func (t *stateTx) beginScope(id, parent, reason string) error {
	if id == "" || reason == "" || id == parent {
		return errors.New("invalid indexed work scope")
	}
	var p any
	if parent != "" {
		p = parent
	}
	_, err := t.tx.Exec("INSERT INTO scope VALUES(?,1,?,?)", id, p, reason)
	return err
}

func readStatePhase(q interface{ QueryRow(string, ...any) *sql.Row }, scope string) (p statePhase, err error) {
	p.Scope = scope
	err = q.QueryRow("SELECT phase,round,done,total,complete FROM phase WHERE scope=?", scope).Scan(&p.Phase, &p.Round, &p.Done, &p.Total, &p.Complete)
	return p, err
}

func (t *stateTx) putPhase(p statePhase) error {
	if p.Scope == "" || phaseOrder(p.Phase, p.Round) < 1 || p.Round < 0 || p.Done < 0 || p.Total < 0 {
		return errors.New("invalid indexed phase position")
	}
	old, err := readStatePhase(t.tx, p.Scope)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err == nil {
		same := p.Phase == old.Phase && p.Round == old.Round
		if phaseOrder(p.Phase, p.Round) < phaseOrder(old.Phase, old.Round) || same && (p.Done < old.Done || old.Complete && (!p.Complete || p.Done != old.Done || p.Total != old.Total)) {
			return fmt.Errorf("indexed phase regression: saved %s/%d/%d complete=%t; attempted %s/%d/%d complete=%t", old.Phase, old.Round, old.Done, old.Complete, p.Phase, p.Round, p.Done, p.Complete)
		}
	}
	_, err = t.tx.Exec("INSERT INTO phase VALUES(?,1,?,?,?,?,?) ON CONFLICT(scope) DO UPDATE SET phase=excluded.phase,round=excluded.round,done=excluded.done,total=excluded.total,complete=excluded.complete", p.Scope, p.Phase, p.Round, p.Done, p.Total, p.Complete)
	return err
}

func (t *stateTx) putWindow(w stateWindow) error {
	// A changed range or holder is a new immutable identity, never an update.
	result, err := t.tx.Exec(`INSERT INTO window VALUES(?,1,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET selection=excluded.selection,transfer=excluded.transfer,verification=excluded.verification
WHERE window.scope=excluded.scope AND window.holder=excluded.holder AND window.start=excluded.start AND window.until=excluded.until
AND CASE excluded.selection WHEN 'pending' THEN 0 WHEN 'selected' THEN 1 ELSE 2 END >= CASE window.selection WHEN 'pending' THEN 0 WHEN 'selected' THEN 1 ELSE 2 END
AND CASE excluded.transfer WHEN 'pending' THEN 0 WHEN 'issued' THEN 1 ELSE 2 END >= CASE window.transfer WHEN 'pending' THEN 0 WHEN 'issued' THEN 1 ELSE 2 END
AND (window.verification='pending' OR excluded.verification='complete')`, w.ID, w.Scope, w.Holder, w.From, w.Until, w.Selection, w.Transfer, w.Verification)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err == nil && n != 1 {
		err = errors.New("window identity changed or status regressed")
	}
	return err
}

func (t *stateTx) setMembers(k stateMemberKey, ids []int64, present bool) (int64, error) {
	if k.Scope == "" || phaseOrder(k.Phase, k.Round) < 1 || k.Round < 0 {
		return 0, errors.New("membership requires a scheduling scope, phase and round")
	}
	pages := map[int64][]int64{}
	for _, id := range ids {
		if id < 1 || id > t.unit.CatalogNames {
			return 0, errors.New("membership ID outside immutable catalog generation")
		}
		first := (id-1)/statePageNames*statePageNames + 1
		pages[first] = append(pages[first], id-first)
	}
	var changed int64
	for first, offsets := range pages {
		var data []byte
		err := t.tx.QueryRow("SELECT bits FROM membership_page WHERE scope=? AND phase=? AND round=? AND window=? AND holder=? AND kind=? AND first_id=?", k.Scope, k.Phase, k.Round, k.Window, k.Holder, k.Kind, first).Scan(&data)
		if errors.Is(err, sql.ErrNoRows) {
			data = make([]byte, statePageNames/8)
		} else if err != nil {
			return 0, err
		}
		for _, offset := range offsets {
			i, bit := offset/8, byte(1)<<uint(offset%8)
			was := data[i]&bit != 0
			if present != was {
				changed++
				if present {
					data[i] |= bit
				} else {
					data[i] &^= bit
				}
			}
		}
		count := 0
		for _, b := range data {
			count += bits.OnesCount8(b)
		}
		if count == 0 {
			if _, err = t.tx.Exec("DELETE FROM membership_page WHERE scope=? AND phase=? AND round=? AND window=? AND holder=? AND kind=? AND first_id=?", k.Scope, k.Phase, k.Round, k.Window, k.Holder, k.Kind, first); err != nil {
				return 0, err
			}
			continue
		}
		if _, err = t.tx.Exec("INSERT INTO membership_page(scope,phase,round,window,holder,kind,first_id,version,names,bits) VALUES(?,?,?,?,?,?,?,1,?,?) ON CONFLICT(scope,phase,round,window,holder,kind,first_id) DO UPDATE SET names=excluded.names,bits=excluded.bits", k.Scope, k.Phase, k.Round, k.Window, k.Holder, k.Kind, first, count, data); err != nil {
			return 0, err
		}
	}
	return changed, nil
}

// A token binds the complete requested outcome, not only a membership hash.
// Overlapping/out-of-order workers advance by unique IDs. Replaying a token
// cannot reapply stale phase counters or native receipts.
func (s *stateStore) completeMembers(k stateMemberKey, token string, ids []int64, operation *stateOperation) (added int64, err error) {
	if token == "" || len(ids) == 0 || k.Kind != "done" || phaseOrder(k.Phase, k.Round) < 1 {
		return 0, errors.New("completion requires a token and done membership")
	}
	ids = append([]int64(nil), ids...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	unique := ids[:0]
	for _, id := range ids {
		if len(unique) == 0 || unique[len(unique)-1] != id {
			unique = append(unique, id)
		}
	}
	payload, err := json.Marshal(struct {
		Key       stateMemberKey
		IDs       []int64
		Operation *stateOperation
	}{k, unique, operation})
	if err != nil {
		return 0, err
	}
	digest := sha256.Sum256(payload)
	err = s.update(func(t *stateTx) error {
		var previous []byte
		err := t.tx.QueryRow("SELECT sha256 FROM batch WHERE scope=? AND phase=? AND round=? AND token=?", k.Scope, k.Phase, k.Round, token).Scan(&previous)
		if err == nil {
			if string(previous) != string(digest[:]) {
				return errors.New("batch token reused with different outcome")
			}
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		p, err := readStatePhase(t.tx, k.Scope)
		if err != nil {
			return err
		}
		if p.Complete || p.Phase != k.Phase || p.Round != k.Round {
			return errors.New("completed phase requires an explicit new work scope")
		}
		added, err = t.setMembers(k, unique, true)
		if err != nil {
			return err
		}
		pending := k
		pending.Kind = "pending"
		if _, err = t.setMembers(pending, unique, false); err != nil {
			return err
		}
		if operation != nil {
			if operation.Scope != k.Scope || operation.Window != k.Window || operation.Holder != k.Holder || operation.Status != "complete" {
				return errors.New("completion receipt belongs to another scope or is incomplete")
			}
			if err = t.putOperation(*operation); err != nil {
				return err
			}
		}
		p.Done += added
		if err = t.putPhase(p); err != nil {
			return err
		}
		_, err = t.tx.Exec("INSERT INTO batch VALUES(?,?,?,?,1,?,?)", k.Scope, k.Phase, k.Round, token, digest[:], added)
		return err
	})
	if err != nil {
		return 0, err
	}
	return added, nil
}

func (t *stateTx) putOperation(o stateOperation) error {
	if o.ID == "" || len(o.Selection) == 0 || o.Status != "intent" && len(o.Receipt) == 0 {
		return errors.New("native operation lacks immutable selection or acknowledged receipt")
	}
	digest := sha256.Sum256(o.Selection)
	receipt := o.Receipt
	if receipt == nil {
		receipt = []byte{}
	}
	r, err := t.tx.Exec(`INSERT INTO operation VALUES(?,1,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET status=excluded.status,receipt=excluded.receipt
WHERE operation.scope=excluded.scope AND operation.window=excluded.window AND operation.holder=excluded.holder AND operation.plan=excluded.plan AND operation.stream=excluded.stream AND operation.start=excluded.start AND operation.end=excluded.end AND operation.selection=excluded.selection
AND (length(operation.receipt)=0 OR operation.receipt=excluded.receipt)
AND CASE excluded.status WHEN 'intent' THEN 0 WHEN 'acknowledged' THEN 1 ELSE 2 END >= CASE operation.status WHEN 'intent' THEN 0 WHEN 'acknowledged' THEN 1 ELSE 2 END`, o.ID, o.Scope, o.Window, o.Holder, o.Plan, o.Stream, o.Start, o.End, o.Status, o.Selection, digest[:], receipt)
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err == nil && n != 1 {
		err = errors.New("native operation identity/selection/receipt changed or status regressed")
	}
	return err
}

func (t *stateTx) putIssue(i stateIssue) error {
	if i.NameID < 1 || i.NameID > t.unit.CatalogNames || i.Reason == "" || i.Phase == "" {
		return errors.New("invalid indexed issue")
	}
	// The quarantine and policy ledgers retain the first diagnostic. Other
	// sparse issue kinds represent the current residual/terminal observation.
	if i.Kind == "quarantine" || i.Kind == "policy" {
		var reason, phase string
		err := t.tx.QueryRow("SELECT reason,phase FROM issue_detail WHERE scope=? AND window=? AND holder=? AND kind=? AND name_id=?", i.Scope, i.Window, i.Holder, i.Kind, i.NameID).Scan(&reason, &phase)
		if err == nil {
			i.Reason, i.Phase = reason, phase
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	if _, err := t.tx.Exec("INSERT OR IGNORE INTO reason(version,text) VALUES(1,?)", i.Reason); err != nil {
		return err
	}
	_, err := t.tx.Exec(`INSERT INTO issue VALUES(?,?,?,?,?,1,(SELECT id FROM reason WHERE text=?),?,?,?) ON CONFLICT(scope,window,holder,kind,name_id) DO UPDATE SET reason=excluded.reason,phase=excluded.phase,first_seen=min(issue.first_seen,excluded.first_seen),last_seen=max(issue.last_seen,excluded.last_seen)`, i.Scope, i.Window, i.Holder, i.Kind, i.NameID, i.Reason, i.Phase, i.FirstSeen, i.LastSeen)
	return err
}

func (t *stateTx) putProof(p stateProofPage) error {
	if p.Generation == "" || p.FirstID < 1 || p.FirstID > t.unit.CatalogNames || len(p.Records)%32 != 0 || int64(len(p.Records)/32) > t.unit.CatalogNames-p.FirstID+1 {
		return errors.New("proof page outside catalog or without a generation")
	}
	digest := sha256.Sum256(p.Records)
	_, err := t.tx.Exec("INSERT INTO proof_page VALUES(?,?,?,?,?,1,?,?) ON CONFLICT(scope,window,holder,generation,first_id) DO UPDATE SET records=excluded.records,sha256=excluded.sha256", p.Scope, p.Window, p.Holder, p.Generation, p.FirstID, p.Records, digest[:])
	return err
}

func (t *stateTx) putReady(r stateReady) error {
	if r.ProofScope == "" || r.Report.ScopeKey == "" {
		return errors.New("READY requires an authenticated proof scope")
	}
	b, err := json.Marshal(r.Report)
	if err != nil {
		return err
	}
	result, err := t.tx.Exec(`INSERT INTO ready VALUES(?,1,?,?,?,?) ON CONFLICT(scope) DO UPDATE SET proof_scope=excluded.proof_scope,recovery_scope=excluded.recovery_scope,verdict=excluded.verdict,report=excluded.report
WHERE ready.verdict='NOT_READY' OR (ready.proof_scope=excluded.proof_scope AND ready.recovery_scope=excluded.recovery_scope AND ready.verdict=excluded.verdict AND ready.report=excluded.report)`, r.Scope, r.ProofScope, r.RecoveryScope, r.Report.Verdict, b)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err == nil && n != 1 {
		err = errors.New("terminal READY attestation is immutable; create an explicit recovery scope")
	}
	return err
}

type stateRelationDigest struct {
	Rows   int64  `json:"rows"`
	SHA256 string `json:"sha256"`
}

// The ordering and typed, length-prefixed encoding are part of version 1.
// Hash actual stored relation values in one snapshot, including empty tables.
// The independent legacy reducer must produce this same semantic inventory;
// a source byte hash alone is not evidence of reducer equivalence.
func (s *stateStore) relationDigests() (map[string]stateRelationDigest, error) {
	t, err := s.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer t.Rollback()
	out, err := stateRelationDigests(t)
	if err != nil {
		return nil, err
	}
	return out, t.Commit()
}

func stateRelationDigests(t *sql.Tx) (map[string]stateRelationDigest, error) {
	queries := map[string]string{
		"unit":            "SELECT * FROM unit ORDER BY singleton",
		"scope":           "SELECT * FROM scope ORDER BY id",
		"phase":           "SELECT * FROM phase ORDER BY scope",
		"window":          "SELECT * FROM window ORDER BY id",
		"catalog_page":    "SELECT * FROM catalog_page ORDER BY first_id",
		"membership_page": "SELECT * FROM membership_page ORDER BY scope,phase,round,window,holder,kind,first_id",
		"operation":       "SELECT * FROM operation ORDER BY id",
		"batch":           "SELECT * FROM batch ORDER BY scope,phase,round,token",
		// Reason IDs are allocation details, so canonicalize their text.
		"reason":     "SELECT version,text FROM reason WHERE id IN (SELECT reason FROM issue UNION SELECT reason FROM ledger_issue UNION SELECT reason FROM ledger_repair) ORDER BY text",
		"issue":      "SELECT i.scope,i.window,i.holder,i.kind,i.name_id,i.version,r.text,i.phase,i.first_seen,i.last_seen FROM issue i JOIN reason r ON r.id=i.reason ORDER BY i.scope,i.window,i.holder,i.kind,i.name_id",
		"proof_page": "SELECT * FROM proof_page ORDER BY scope,window,holder,generation,first_id",
		"ready":      "SELECT * FROM ready ORDER BY scope",
	}
	for name, query := range stateVerificationDigestQueries {
		queries[name] = query
	}
	for name, query := range stateFillDigestQueries {
		queries[name] = query
	}
	for name, query := range stateSelectionDigestQueries {
		queries[name] = query
	}
	for name, query := range stateDiscoveryDigestQueries {
		queries[name] = query
	}
	for name, query := range stateIssuesDigestQueries {
		queries[name] = query
	}
	for name, query := range stateTransferDigestQueries {
		queries[name] = query
	}
	for name, query := range stateSemanticDigestQueries {
		queries[name] = query
	}
	out := map[string]stateRelationDigest{}
	for name, query := range queries {
		rows, err := t.Query(query)
		if err != nil {
			return nil, err
		}
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			return nil, err
		}
		h := sha256.New()
		var count int64
		for rows.Next() {
			values := make([]any, len(columns))
			pointers := make([]any, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err = rows.Scan(pointers...); err != nil {
				rows.Close()
				return nil, err
			}
			for _, value := range values {
				var tag byte
				var data []byte
				switch v := value.(type) {
				case nil:
					tag = 0
				case int64:
					tag, data = 1, binary.BigEndian.AppendUint64(nil, uint64(v))
				case string:
					tag, data = 2, []byte(v)
				case []byte:
					tag, data = 3, v
				default:
					rows.Close()
					return nil, fmt.Errorf("unexpected indexed relation value %T", value)
				}
				h.Write([]byte{tag})
				h.Write(binary.BigEndian.AppendUint64(nil, uint64(len(data))))
				h.Write(data)
			}
			count++
		}
		if err = errors.Join(rows.Err(), rows.Close()); err != nil {
			return nil, err
		}
		out[name] = stateRelationDigest{count, hex.EncodeToString(h.Sum(nil))}
	}
	return out, nil
}

// Explicit checkpoint errors and filesystem sync/close errors are returned.
// This does not fix errors ignored inside the SQLite VFS itself. Directory-EIO
// injection remains a format-activation gate; do not claim power-loss safety.
func (s *stateStore) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return s.failure
	}
	s.closed = true
	var err error
	if s.lock != nil {
		var busy, log, checkpointed int
		err = s.db.QueryRow("PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &log, &checkpointed)
		if err == nil && busy != 0 {
			err = fmt.Errorf("indexed state checkpoint busy: %d/%d frames", checkpointed, log)
		}
	}
	err = errors.Join(s.failure, err, s.db.Close())
	if s.lock != nil {
		f, e := os.OpenFile(s.path, os.O_RDWR, 0)
		if e == nil {
			e = errors.Join(f.Sync(), f.Close())
		}
		err = errors.Join(err, e, syncDirectory(filepath.Dir(s.path)), s.lock.Close())
	}
	s.failure = err
	return err
}
