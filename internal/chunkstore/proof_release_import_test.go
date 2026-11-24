package chunkstore_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestProofReleaseDrainsConcurrentImports(t *testing.T) {
	for _, continuation := range []bool{false, true} {
		for _, reset := range []bool{false, true} {
			t.Run(fmt.Sprintf("continuation%v/reset%v", continuation, reset), func(t *testing.T) {
				source := transferStoreCached(t, continuation, true, 8<<20)
				target := transferStoreCached(t, continuation, true, 8<<20)
				oracle := transferStoreCached(t, continuation, true, 8<<20)
				name := "proof.import"
				putTransferPoints(t, source, name, []store.Point{{Timestamp: 999760, Value: 1.5}})
				for _, db := range []*store.Store{target, oracle} {
					putTransferPoints(t, db, name, []store.Point{{Timestamp: 999880, Value: 7}})
					if _, _, err := db.MutationProof(t.Context(), name, 999700, 999900); err != nil {
						t.Fatal(err)
					}
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
				retired := make(chan error, 1)
				go func() {
					if reset {
						_, err := target.ResetMutationProofs(t.Context())
						retired <- err
					} else {
						r := target.ReleaseMutationProofs(t.Context(), []store.ProofRelease{{Name: name}})
						if r[0].Status != "released" && r[0].Status != "absent" {
							retired <- fmt.Errorf("%+v", r)
						} else {
							retired <- nil
						}
					}
				}()
				select {
				case err := <-retired:
					close(release)
					<-imported
					t.Fatalf("retirement overtook import: %v", err)
				case <-time.After(20 * time.Millisecond):
				}
				close(release)
				if err := <-imported; err != nil {
					t.Fatal(err)
				}
				if err := <-retired; err != nil {
					t.Fatal(err)
				}
				if _, err := oracle.ImportTransfer(t.Context(), records); err != nil {
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
				activeKey := []byte("proof/ranges/")
				activeKey = binary.BigEndian.AppendUint64(activeKey, a.Metadata.ID)
				activeKey = binary.BigEndian.AppendUint64(activeKey, a.Metadata.Generation)
				for _, pair := range kv {
					if bytes.Equal(pair[0], activeKey) || reset && strings.HasPrefix(string(pair[0]), "proof/ranges/") {
						t.Fatalf("import resurrected active history: %x", pair[0])
					}
				}
				// Compare all non-proof database bytes, including transfer receipts.
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
					t.Fatalf("key counts: %d != %d", len(gotKeys), len(wantKeys))
				}
				for i := range gotKeys {
					if !bytes.Equal(gotKeys[i][0], wantKeys[i][0]) || !bytes.Equal(gotKeys[i][1], wantKeys[i][1]) {
						t.Fatalf("non-proof bytes changed at %x", gotKeys[i][0])
					}
				}
			})
		}
	}
}
