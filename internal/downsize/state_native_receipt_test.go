package downsize

import (
	"bytes"
	"database/sql"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

func typedNativeReceiptFixture() chunkstore.NativeReceipt {
	return chunkstore.NativeReceipt{
		Identity: strings.Repeat("a", 64), NamesSHA256: strings.Repeat("b", 64), Digest: strings.Repeat("c", 64), Count: 3, CopiedCount: 1, DeferredCount: 2, Copied: []byte{2}, Bytes: 1234, FirstID: math.MaxUint64, ReservedIDs: 3,
		Tables:        []chunkstore.NativeSSTReceipt{{Namespace: "metric", Lower: []byte{0, 1, 2}, Upper: []byte{0xff, 0}, Bytes: math.MaxUint64, Entries: 1 << 63}, {Namespace: "points", Lower: nil, Upper: []byte{}, Bytes: 2, Entries: 3}},
		Mappings:      []chunkstore.NativeMapping{{Ordinal: 1, SourceID: math.MaxUint64, SourceGeneration: 1 << 63, SourceRevision: 2, TargetID: 3, TargetGeneration: 4, TargetRevision: 5}},
		Skipped:       []chunkstore.NativeSkip{{Ordinal: 0, Name: "metric.long", Bytes: 100, PhysicalPoints: 12, ArchiveSlots: 1000, MemoryBytes: 999, Reason: "size limit"}},
		WindowSkipped: []chunkstore.NativeWindowSkip{{Ordinal: 1, Archive: 2, Timestamp: 100, Reason: "boundary"}, {Ordinal: 1, Archive: 0, Timestamp: 200, Reason: "another boundary"}},
	}
}

func TestTypedStateNativeReceiptAllFieldsAndRestart(t *testing.T) {
	s := newTypedState(t, 3)
	r := typedNativeReceiptFixture()
	if err := s.update(func(tx *stateTx) error { return tx.putNativeReceipt("root", r) }); err != nil {
		t.Fatal(err)
	}
	before := typedDigests(t, s)
	if err := s.update(func(tx *stateTx) error { return tx.putNativeReceipt("root", r) }); err != nil || !reflect.DeepEqual(before, typedDigests(t, s)) {
		t.Fatal("receipt replay changed state", err)
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openStateStore(s.path, s.unit, false)
	if err != nil {
		t.Fatal(err)
	}
	s = reopened
	defer s.close()
	stateReadTx(t, s, func(tx *sql.Tx) {
		got, err := readStateNativeReceipt(tx, "root", r.Identity)
		if err != nil || !reflect.DeepEqual(got, r) {
			t.Fatalf("receipt lost fields: got %#v want %#v err %v", got, r, err)
		}
	})
	before = typedDigests(t, s)
	changed := r
	changed.Bytes++
	if err := s.update(func(tx *stateTx) error { return tx.putNativeReceipt("root", changed) }); err == nil || !reflect.DeepEqual(before, typedDigests(t, s)) {
		t.Fatal("changed receipt accepted")
	}
	fail := errors.New("after header and children")
	changed.Identity = strings.Repeat("d", 64)
	if err := s.update(func(tx *stateTx) error {
		if err := tx.putNativeReceipt("root", changed); err != nil {
			return err
		}
		return fail
	}); !errors.Is(err, fail) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, typedDigests(t, s)) {
		t.Fatal("partial receipt survived rollback")
	}
}

func TestTypedStateNativeReceiptNilAndEmpty(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(map[bool]string{false: "nil", true: "empty"}[empty], func(t *testing.T) {
			s := newTypedState(t, 1)
			r := chunkstore.NativeReceipt{Identity: strings.Repeat("a", 64), NamesSHA256: strings.Repeat("b", 64), Digest: strings.Repeat("c", 64)}
			if empty {
				r.Copied = []byte{}
				r.Tables = []chunkstore.NativeSSTReceipt{}
				r.Mappings = []chunkstore.NativeMapping{}
				r.Skipped = []chunkstore.NativeSkip{}
				r.WindowSkipped = []chunkstore.NativeWindowSkip{}
			}
			if err := s.update(func(tx *stateTx) error { return tx.putNativeReceipt("root", r) }); err != nil {
				t.Fatal(err)
			}
			stateReadTx(t, s, func(tx *sql.Tx) {
				got, err := readStateNativeReceipt(tx, "root", r.Identity)
				if err != nil || !reflect.DeepEqual(got, r) {
					t.Fatalf("nil/empty receipt changed: got %#v want %#v err %v", got, r, err)
				}
			})
		})
	}
}

func TestTypedStateNativeReceiptProjectionCorruption(t *testing.T) {
	s := newTypedState(t, 3)
	r := typedNativeReceiptFixture()
	if err := s.update(func(tx *stateTx) error { return tx.putNativeReceipt("root", r) }); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("UPDATE native_receipt_mapping SET source_revision=?", bytes.Repeat([]byte{1}, 8)); err != nil {
		t.Fatal(err)
	}
	stateReadTx(t, s, func(tx *sql.Tx) {
		if _, err := readStateNativeReceipt(tx, "root", r.Identity); err == nil {
			t.Fatal("receipt projection corruption was accepted")
		}
	})
}
