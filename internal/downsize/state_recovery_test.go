package downsize

import (
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestTypedStateWindowPointAccountingParity(t *testing.T) {
	s := newTypedState(t, 10)
	before, after, bad := int64(100), int64(123), int64(99)
	fixtures := []windowPointCount{
		{Source: "source", Window: syncRange{"w", 100, 200}, Before: &before, After: &after},
		{Source: "source", Window: syncRange{"w", 100, 200}, Before: &before},
		{Source: "source", Window: syncRange{"w", 100, 200}, Before: &before, After: &bad},
		{Source: "source", Window: syncRange{"w", 100, 200}, Before: &before, Superseded: true},
	}
	legacy := t.TempDir()
	for n, c := range fixtures {
		operation := string(rune('a' + n))
		if err := atomicStateJSON(filepath.Join(legacy, operation+".window-points.json"), c); err != nil {
			t.Fatal(err)
		}
		if err := s.update(func(tx *stateTx) error { return tx.putWindowPoints("root", operation, c) }); err != nil {
			t.Fatal(err)
		}
	}
	want, err := collectWindowPointCounts(legacy)
	if err != nil {
		t.Fatal(err)
	}
	stateReadTx(t, s, func(tx *sql.Tx) {
		got, err := readStateWindowRepair(tx, "root")
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("point totals: got %+v want %+v: %v", got, want, err)
		}
	})
	// Actual legacy resume clears an interrupted baseline, even if it was known.
	wantCount, err := readWindowPointCount(filepath.Join(legacy, "b.window-points.json"), windowPointCount{})
	if err != nil {
		t.Fatal(err)
	}
	var gotCount windowPointCount
	if err = s.update(func(tx *stateTx) error { var e error; gotCount, e = tx.resumeWindowPoints("root", "b"); return e }); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(wantCount, gotCount) {
		t.Fatalf("interrupted baseline: got %+v want %+v", gotCount, wantCount)
	}
	// A completed measurement cannot be overwritten by a fresh pre-import count.
	snap := typedDigests(t, s)
	changed := fixtures[0]
	changed.Before = &bad
	if err = s.update(func(tx *stateTx) error { return tx.putWindowPoints("root", "a", changed) }); err == nil || !reflect.DeepEqual(snap, typedDigests(t, s)) {
		t.Fatal("completed baseline changed")
	}
}

func TestTypedStateReadyRecoveryParityAndRestart(t *testing.T) {
	s := newTypedState(t, 1)
	now := time.Unix(1234567, 0)
	o := syncOptions{to: "http://target", from: []string{"http://source"}, state: t.TempDir(), now: func() time.Time { return now }}
	identity, err := syncIdentity(o)
	if err != nil {
		t.Fatal(err)
	}
	// Use the same scope identity at the typed writer boundary and legacy reader.
	s.unit.Identity = identity
	if _, err = s.db.Exec("UPDATE unit SET identity=?", identity); err != nil {
		t.Fatal(err)
	}
	intent := readyRecovery{Identity: identity, Scope: "scope", State: "ready-recovery-1", RestartWindow: true, Windows: syncRanges{{"gap", 100, 200}}, Restarts: syncRanges{{"restart", 150, 160}}, Processes: map[string]readyProcess{"http://source": {Start: 1230000.123456, Observed: 1234000}, "http://target": {Start: 1230001.987654, Observed: 1234001}}}
	legacy := filepath.Join(o.state, "ready-recovery.json")
	if err = atomicStateJSON(legacy, intent); err != nil {
		t.Fatal(err)
	}
	expected, err := readReadyRecovery(legacy, o, "scope")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.update(func(tx *stateTx) error {
		if err := tx.beginScope("recovery-1", "root", "process restarted"); err != nil {
			return err
		}
		return tx.putRecovery("root", "recovery-1", intent)
	}); err != nil {
		t.Fatal(err)
	}
	if err = s.close(); err != nil {
		t.Fatal(err)
	}
	next, err := openStateStore(s.path, s.unit, false)
	if err != nil {
		t.Fatal(err)
	}
	s = next
	defer s.close()
	stateReadTx(t, s, func(tx *sql.Tx) {
		got, err := readStateRecovery(tx, "root", "recovery-1")
		if err != nil || !reflect.DeepEqual(got, *expected) {
			t.Fatalf("recovery differs after restart: got %+v want %+v %v", got, *expected, err)
		}
	})
	snapshot := typedDigests(t, s)
	if err = s.update(func(tx *stateTx) error { return tx.putRecovery("root", "recovery-1", intent) }); err != nil || !reflect.DeepEqual(snapshot, typedDigests(t, s)) {
		t.Fatal("same recovery changed state", err)
	}
	changed := intent
	changed.Windows = syncRanges{{"gap", 100, 300}}
	if err = s.update(func(tx *stateTx) error { return tx.putRecovery("root", "recovery-1", changed) }); err == nil || !reflect.DeepEqual(snapshot, typedDigests(t, s)) {
		t.Fatal("widened recovery accepted")
	}
	if err = s.update(func(tx *stateTx) error { return tx.putRecovery("other", "recovery-1", intent) }); err == nil || !reflect.DeepEqual(snapshot, typedDigests(t, s)) {
		t.Fatal("wrong parent accepted")
	}
	// Fault after scope creation must not leave a phantom independent recovery.
	fail := errors.New("after intent")
	if err = s.update(func(tx *stateTx) error {
		if err := tx.beginScope("recovery-2", "root", "another restart"); err != nil {
			return err
		}
		r := intent
		r.State = "ready-recovery-2"
		if err := tx.putRecovery("root", "recovery-2", r); err != nil {
			return err
		}
		return fail
	}); !errors.Is(err, fail) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(snapshot, typedDigests(t, s)) {
		t.Fatal("partial recovery transaction survived")
	}
}
