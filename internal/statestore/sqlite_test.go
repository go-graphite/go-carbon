package statestore

import (
	"errors"
	"path/filepath"
	"strconv"
	"testing"
)

func TestSQLiteDurabilitySettings(t *testing.T) {
	s, err := Open("sqlite", filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	db := s.e.(*sqliteEngine).db
	var mode string
	var sync int
	if err = db.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow("PRAGMA synchronous").Scan(&sync); err != nil {
		t.Fatal(err)
	}
	if mode != "wal" || sync != 2 {
		t.Fatalf("journal=%s synchronous=%d", mode, sync)
	}
}

func TestSQLiteFullRollsBackMembershipAndCounter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	db := s.e.(*sqliteEngine).db
	if _, err = s.CommitWork(1, 1, []uint64{1, 2}, nil); err != nil {
		t.Fatal(err)
	}
	var pages int
	if err = db.QueryRow("PRAGMA page_count").Scan(&pages); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("PRAGMA max_page_count=" + strconv.Itoa(pages)); err != nil {
		t.Fatal(err)
	}
	ids := make([]uint64, 1<<16)
	for i := range ids {
		ids[i] = uint64(i + 100)
	}
	if _, err = s.CommitWork(1, 2, ids, nil); err == nil {
		t.Fatal("expected SQLITE_FULL")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	b, err := s.Get("window", 1)
	if err != nil || string(b) != "2" {
		t.Fatalf("counter=%s error=%v", b, err)
	}
	if _, err = s.Get("done/1", 100); !errors.Is(err, ErrNotFound) {
		t.Fatalf("partial membership: %v", err)
	}
	if _, err = s.Get("tokens/1", 2); !errors.Is(err, ErrNotFound) {
		t.Fatalf("partial token: %v", err)
	}
	var integrity string
	if err = s.e.(*sqliteEngine).db.QueryRow("PRAGMA integrity_check").Scan(&integrity); err != nil || integrity != "ok" {
		t.Fatal(integrity, err)
	}
}
