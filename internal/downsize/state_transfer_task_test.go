package downsize

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

func typedTaskFixture() task {
	r := typedNativeReceiptFixture()
	deferred := r
	deferred.Identity = strings.Repeat("d", 64)
	return task{Kind: "native-range", Checkpoint: "pending-checkpoint", Retries: 3, Stream: strings.Repeat("e", 64), Start: 100, End: 103, Source: "source", Target: "target", Phase: "verified", Bytes: 1234,
		Native: &nativeRangeState{Combine: true, Receipt: r, Done: []byte{2},
			Pending: &task{NativeParent: strings.Repeat("e", 64) + ":100:103", NativeOrdinals: []int{0, 2}, Names: []string{"a", "c"}, Kind: "fallback", Checkpoint: "child-checkpoint", Retries: 2, Stream: strings.Repeat("e", 64), Start: 100, End: 103, Source: "source", Target: "target", Phase: "verified", Bytes: 19,
				Proofs: []chunkstore.TransferProof{{KeyCount: 7, Sampled: true, Source: chunkstore.TransferVersion{Name: "a", ID: math.MaxUint64, Generation: 1 << 63, Revision: 9}, Target: chunkstore.TransferVersion{Name: "a", ID: 1 << 63, Generation: math.MaxUint64, Revision: 3}, Digest: "proof-digest", Counts: []int{0, 12}, Merged: true}, {Counts: []int{}}}},
			Combines: []nativeDeferredAttempt{{Selection: []byte{1}, Receipt: &deferred}, {Selection: []byte{4}}},
		},
	}
}

func stateTaskEqual(t *testing.T, got, want task) {
	t.Helper()
	// Compare all in-memory fields as well as the exact legacy serialized form.
	a, ae := json.Marshal(got)
	b, be := json.Marshal(want)
	if ae != nil || be != nil || !bytes.Equal(a, b) || !reflect.DeepEqual(got, want) {
		t.Fatalf("task differs\ngot %s\nwant %s\nerrors %v %v", a, b, ae, be)
	}
}

func TestTypedStateTransferTaskLegacyRestart(t *testing.T) {
	s := newTypedState(t, 3)
	original := typedTaskFixture()
	legacyPath := filepath.Join(t.TempDir(), "transfer.jsonl")
	legacy, err := openJournal(legacyPath, "plan")
	if err != nil {
		t.Fatal(err)
	}
	operation := legacy.operation
	if err = legacy.save(original); err != nil {
		t.Fatal(err)
	}
	legacy.close()
	save := func(v task) error {
		return s.update(func(tx *stateTx) error {
			if err := tx.putTransferIdentity("root", "window-child", operation, "plan"); err != nil {
				return err
			}
			return tx.putTransferTask("root", "window-child", v)
		})
	}
	if err = save(original); err != nil {
		t.Fatal(err)
	}
	before := typedDigests(t, s)
	if err = save(original); err != nil || !reflect.DeepEqual(before, typedDigests(t, s)) {
		t.Fatal("same task changed state", err)
	}
	if err = s.close(); err != nil {
		t.Fatal(err)
	}
	s, err = openStateStore(s.path, s.unit, false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	legacy, err = openJournal(legacyPath, "plan")
	if err != nil {
		t.Fatal(err)
	}
	defer legacy.close()
	stateReadTx(t, s, func(tx *sql.Tx) {
		got, err := readStateTransferTask(tx, "root", "window-child", taskKey(original))
		if err != nil {
			t.Fatal(err)
		}
		stateTaskEqual(t, got, legacy.tasks[taskKey(original)])
		// Resume dispatches only unfinished original ordinals, including the exact
		// saved fallback; no completed metric is selected again after reopening.
		var pending []int
		for i := range got.Native.Receipt.Count {
			if !got.Native.done(i) {
				pending = append(pending, i)
			}
		}
		if !reflect.DeepEqual(pending, []int{0, 2}) {
			t.Fatalf("redo of copied ordinal: %v", pending)
		}
	})
	fail := errors.New("after native task and receipt")
	changed := original
	changed.Bytes++
	if err = s.update(func(tx *stateTx) error {
		if err := tx.putTransferTask("root", "window-child", changed); err != nil {
			return err
		}
		return fail
	}); !errors.Is(err, fail) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, typedDigests(t, s)) {
		t.Fatal("partial task survived transaction")
	}
	// Fail an update after it has removed the previous task. The transaction
	// must retain its pending fallback and all of its proof rows.
	malformed := original
	native := *original.Native
	pending := *native.Pending
	pending.Native = &nativeRangeState{}
	native.Pending = &pending
	malformed.Native = &native
	if err = save(malformed); err == nil || !reflect.DeepEqual(before, typedDigests(t, s)) {
		t.Fatal("partial invalid task survived", err)
	}
	if err = s.update(func(tx *stateTx) error {
		return tx.putTransferIdentity("root", "window-child", strings.Repeat("f", 64), "plan")
	}); err == nil || !reflect.DeepEqual(before, typedDigests(t, s)) {
		t.Fatal("operation identity changed", err)
	}
	// Old checkpoints cannot erase acknowledged native work or change an
	// immutable deferred selection into a new remote operation.
	for _, mutate := range []func(*nativeRangeState){
		func(n *nativeRangeState) { n.Done = []byte{0} },
		func(n *nativeRangeState) { n.Intent = true },
		func(n *nativeRangeState) { n.Combines = nil },
		func(n *nativeRangeState) {
			n.Combines = append([]nativeDeferredAttempt(nil), n.Combines...)
			n.Combines[1].Selection = []byte{1}
		},
	} {
		changed := original
		next := *original.Native
		mutate(&next)
		changed.Native = &next
		if err = save(changed); err == nil || !reflect.DeepEqual(before, typedDigests(t, s)) {
			t.Fatal("native work regressed", err)
		}
	}
	// Finishing drops the in-flight fallback, while receipt and completed floor
	// remain immutable. An old checkpoint cannot reopen this work.
	completed := original
	completed.Native = nil
	completed.Phase = "done"
	if err = save(completed); err != nil {
		t.Fatal(err)
	}
	before = typedDigests(t, s)
	if err = save(original); err == nil || !reflect.DeepEqual(before, typedDigests(t, s)) {
		t.Fatal("completed task reopened", err)
	}
}

func TestTypedStateTransferTaskIntentAndEmpty(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(map[bool]string{false: "nil", true: "empty"}[empty], func(t *testing.T) {
			s := newTypedState(t, 1)
			v := task{Stream: "stream", Start: 0, End: 1, Source: "source", Target: "target", Phase: "verified", Native: &nativeRangeState{Intent: true}}
			if empty {
				v.Names = []string{}
				v.NativeOrdinals = []int{}
				v.Proofs = []chunkstore.TransferProof{}
				v.Native.Done = []byte{}
				v.Native.Combines = []nativeDeferredAttempt{}
			}
			if err := s.update(func(tx *stateTx) error {
				if err := tx.putTransferIdentity("root", "child", strings.Repeat("a", 64), "plan"); err != nil {
					return err
				}
				return tx.putTransferTask("root", "child", v)
			}); err != nil {
				t.Fatal(err)
			}
			stateReadTx(t, s, func(tx *sql.Tx) {
				got, err := readStateTransferTask(tx, "root", "child", taskKey(v))
				if err != nil {
					t.Fatal(err)
				}
				stateTaskEqual(t, got, v)
			})
		})
	}
}

func TestTypedStateTransferTaskProjectionCorruption(t *testing.T) {
	s := newTypedState(t, 3)
	v := typedTaskFixture()
	if err := s.update(func(tx *stateTx) error {
		if err := tx.putTransferIdentity("root", "child", strings.Repeat("a", 64), "plan"); err != nil {
			return err
		}
		return tx.putTransferTask("root", "child", v)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("UPDATE transfer_task_count SET value=value+1"); err != nil {
		t.Fatal(err)
	}
	stateReadTx(t, s, func(tx *sql.Tx) {
		if _, err := readStateTransferTask(tx, "root", "child", taskKey(v)); err == nil {
			t.Fatal("corrupt proof count accepted")
		}
	})
}

// The real controller persists a partial receipt in its intent: identity,
// names hash and count, before Digest/Copied exist. It is not a valid final
// NativeReceipt and must survive an ambiguous remote acknowledgement exactly.
func TestTypedStateTransferTaskRealNativeIntent(t *testing.T) {
	s := newTypedState(t, 3)
	v := typedTaskFixture()
	r := v.Native.Receipt
	v.Native = &nativeRangeState{Intent: true, Combine: true, Receipt: chunkstore.NativeReceipt{Identity: r.Identity, NamesSHA256: r.NamesSHA256, Count: r.Count}}
	save := func(v task) error {
		return s.update(func(tx *stateTx) error {
			if err := tx.putTransferIdentity("root", "child", strings.Repeat("a", 64), "plan"); err != nil {
				return err
			}
			return tx.putTransferTask("root", "child", v)
		})
	}
	if err := save(v); err != nil {
		t.Fatal(err)
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	var err error
	s, err = openStateStore(s.path, s.unit, false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	stateReadTx(t, s, func(tx *sql.Tx) {
		got, e := readStateTransferTask(tx, "root", "child", taskKey(v))
		if e != nil {
			t.Fatal(e)
		}
		stateTaskEqual(t, got, v)
	})
	before := typedDigests(t, s)
	wrong := typedTaskFixture()
	wrongNative := *wrong.Native
	wrongNative.Receipt.Identity = strings.Repeat("9", 64)
	wrong.Native = &wrongNative
	if err = save(wrong); err == nil || !reflect.DeepEqual(before, typedDigests(t, s)) {
		t.Fatal("intent acknowledged as a different operation", err)
	}
	if err = save(typedTaskFixture()); err != nil {
		t.Fatal(err)
	}
}
