package chunkstore_test

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

func TestProofTTLConcurrentImport(t *testing.T) {
	for _, continuation := range []bool{false, true} {
		t.Run(fmt.Sprint(continuation), func(t *testing.T) {
			var clock atomic.Int64
			clock.Store(time.Unix(1000000, 0).UnixNano())
			open := func(ttl time.Duration) *store.Store {
				t.Helper()
				o := store.Options{ExactBlocks: true, MutationProofScopeTTL: &ttl, Now: func() time.Time { return time.Unix(0, clock.Load()) }, CacheSize: 1 << 20, MemTableSize: 1 << 20, SyncInterval: time.Hour}
				if continuation {
					o.Continuation = &whisperio.ContinuationCodec{ReuseBlockKeys: true, DirectLateWrites: true}
					o.TimeBuckets = true
				}
				db, err := store.Open(t.TempDir(), o)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := db.Close(); err != nil {
						t.Error(err)
					}
				})
				return db
			}
			source, target, oracle := open(0), open(time.Second), open(0)
			name := "proof.import"
			putTransferPoints(t, source, name, []store.Point{{Timestamp: 999760, Value: 1.5}})
			for _, db := range []*store.Store{target, oracle} {
				putTransferPoints(t, db, name, []store.Point{{Timestamp: 999880, Value: 7}})
				if _, _, err := db.MutationProof(t.Context(), name, 999700, 999900); err != nil {
					t.Fatal(err)
				}
			}
			old, _, err := target.MutationProof(t.Context(), name, 999700, 999900)
			if err != nil {
				t.Fatal(err)
			}
			records, err := source.ExportTransfer(t.Context(), []string{name})
			if err != nil {
				t.Fatal(err)
			}
			entered, release := make(chan struct{}), make(chan struct{})
			target.SetTransferCommitHookForTest(func() { close(entered); <-release })
			imported := make(chan error, 1)
			go func() { _, err := target.ImportTransfer(t.Context(), records); imported <- err }()
			<-entered
			clock.Add(int64(2 * time.Second))
			points := []store.Point{{Timestamp: 999780, Value: 99}}
			// An expired history never bypasses the pending-import reservation.
			if err := target.TryUpdateMany(t.Context(), name, points); !errors.Is(err, store.ErrImportPending) {
				close(release)
				<-imported
				t.Fatalf("pending import: %v", err)
			}
			written := make(chan error, 1)
			go func() { written <- target.UpdateMany(t.Context(), name, points) }()
			select {
			case err := <-written:
				close(release)
				<-imported
				t.Fatalf("expiry write overtook import: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			close(release)
			if err := <-imported; err != nil {
				t.Fatal(err)
			}
			if err := <-written; err != nil {
				t.Fatal(err)
			}
			target.SetTransferCommitHookForTest(nil)
			if _, err := oracle.ImportTransfer(t.Context(), records); err != nil {
				t.Fatal(err)
			}
			if err := oracle.UpdateMany(t.Context(), name, points); err != nil {
				t.Fatal(err)
			}
			a, err := target.Snapshot(t.Context(), name)
			if err != nil {
				t.Fatal(err)
			}
			b, err := oracle.Snapshot(t.Context(), name)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.EqualSnapshots(a, b); err != nil {
				t.Fatal(err)
			}
			kv, err := target.RawKeyValuesForTest()
			if err != nil {
				t.Fatal(err)
			}
			want, err := oracle.RawKeyValuesForTest()
			if err != nil {
				t.Fatal(err)
			}
			filtered := func(kvs [][2][]byte) [][2][]byte {
				var out [][2][]byte
				for _, v := range kvs {
					if !strings.HasPrefix(string(v[0]), "proof/ranges/") {
						out = append(out, v)
					}
				}
				return out
			}
			gotKeys, wantKeys := filtered(kv), filtered(want)
			if len(gotKeys) != len(wantKeys) {
				t.Fatal("non-proof key count differs")
			}
			for i := range gotKeys {
				if !bytes.Equal(gotKeys[i][0], wantKeys[i][0]) || !bytes.Equal(gotKeys[i][1], wantKeys[i][1]) {
					t.Fatalf("non-proof bytes differ at %x", gotKeys[i][0])
				}
			}
			fresh, _, err := target.MutationProof(t.Context(), name, 999700, 999900)
			if err != nil || fresh == old {
				t.Fatal("old token survived import and expiry write", err)
			}
		})
	}
}
