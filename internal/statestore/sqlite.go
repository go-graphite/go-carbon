package statestore

import (
	"database/sql"
	"errors"
	"net/url"

	_ "modernc.org/sqlite"
)

type sqliteEngine struct{ db *sql.DB }
type sqliteTx struct {
	tx               *sql.Tx
	put, insert, get *sql.Stmt
}

func openSQLite(path string) (engine, error) {
	u := url.URL{Scheme: "file", Path: path}
	db, err := sql.Open("sqlite", u.String()+"?_pragma=journal_mode%3DWAL&_pragma=synchronous%3DFULL&_pragma=busy_timeout%3D5000&_pragma=cache_size%3D-65536")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS records(bucket TEXT NOT NULL,id INTEGER NOT NULL,label TEXT NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(bucket,id)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS records_label ON records(bucket,label) WHERE label<>'';`)
	if err != nil {
		db.Close()
		return nil, err
	}
	return &sqliteEngine{db}, nil
}

func (e *sqliteEngine) Close() error { return e.db.Close() }
func (e *sqliteEngine) Get(bucket string, id uint64) ([]byte, error) {
	var b []byte
	err := e.db.QueryRow("SELECT payload FROM records WHERE bucket=? AND id=?", bucket, id).Scan(&b)
	if errors.Is(err, sql.ErrNoRows) {
		err = ErrNotFound
	}
	return b, err
}
func (e *sqliteEngine) Begin() (transaction, error) {
	t, err := e.db.Begin()
	if err != nil {
		return nil, err
	}
	x := &sqliteTx{tx: t}
	x.put, err = t.Prepare("INSERT INTO records VALUES(?,?,?,?) ON CONFLICT(bucket,id) DO UPDATE SET label=excluded.label,payload=excluded.payload")
	if err == nil {
		x.insert, err = t.Prepare("INSERT OR IGNORE INTO records VALUES(?,?,?,?)")
	}
	if err == nil {
		x.get, err = t.Prepare("SELECT payload FROM records WHERE bucket=? AND id=?")
	}
	if err != nil {
		x.Close()
		return nil, err
	}
	return x, nil
}
func (t *sqliteTx) Get(bucket string, id uint64) ([]byte, error) {
	var b []byte
	err := t.get.QueryRow(bucket, id).Scan(&b)
	if errors.Is(err, sql.ErrNoRows) {
		err = ErrNotFound
	}
	return b, err
}
func (t *sqliteTx) Put(r Record) error {
	_, err := t.put.Exec(r.Bucket, r.ID, r.Label, r.Payload)
	return err
}
func (t *sqliteTx) Insert(r Record) (bool, error) {
	r0, err := t.insert.Exec(r.Bucket, r.ID, r.Label, r.Payload)
	if err != nil {
		return false, err
	}
	n, err := r0.RowsAffected()
	return n > 0, err
}
func (t *sqliteTx) Commit() error { return t.tx.Commit() }
func (t *sqliteTx) Close() error {
	if t.put != nil {
		t.put.Close()
	}
	if t.insert != nil {
		t.insert.Close()
	}
	if t.get != nil {
		t.get.Close()
	}
	err := t.tx.Rollback()
	if errors.Is(err, sql.ErrTxDone) {
		return nil
	}
	return err
}

func (e *sqliteEngine) Scan(fn func(Record) error) error {
	rows, err := e.db.Query("SELECT bucket,id,label,payload FROM records ORDER BY bucket,id")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var r Record
		if err = rows.Scan(&r.Bucket, &r.ID, &r.Label, &r.Payload); err != nil {
			return err
		}
		if err = fn(r); err != nil {
			return err
		}
	}
	return rows.Err()
}
