package chunkstore

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

// Exercise both publication paths, including stale stripe epochs, with a
// colliding writer at exactly the revision read. Compare the final database
// bytes with the same operations performed serially.
func TestTransferValidationDoesNotHoldSiblingStripes(t *testing.T) {
	for _, partitioned := range []bool{false, true} {
		t.Run(fmt.Sprint(partitioned), func(t *testing.T) {
			ctx := context.Background()
			source, now := openTestStore(t, "/source", Options{fs: vfs.NewMem(), ExactBlocks: true, SyncInterval: time.Hour})
			target, _ := openTestStore(t, "/target", Options{fs: vfs.NewMem(), ExactBlocks: true, SyncInterval: time.Hour})
			oracle, _ := openTestStore(t, "/oracle", Options{fs: vfs.NewMem(), ExactBlocks: true, SyncInterval: time.Hour})
			defer closeTestStore(t, source)
			defer closeTestStore(t, target)
			defer closeTestStore(t, oracle)
			base := int64(now - now%60)
			names := []string{"validation.a", "validation.b"}
			collision := ""
			for i := 0; ; i++ {
				collision = fmt.Sprintf("validation.sibling.%d", i)
				if target.metricStripe(collision) == target.metricStripe(names[0]) {
					break
				}
			}
			for _, name := range append(append([]string{}, names...), collision) {
				for _, s := range []*Store{source, target, oracle} {
					createTestMetric(t, s, name)
				}
			}
			values := []uint64{0x8000000000000000, 1, 0x3ff0000000000001, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000001}
			for _, name := range names {
				var points []Point
				for i, v := range values {
					points = append(points, Point{Timestamp: base - int64(60*(len(values)+1-i)), Value: math.Float64frombits(v)})
				}
				if err := source.UpdateMany(ctx, name, points); err != nil {
					t.Fatal(err)
				}
				for _, s := range []*Store{target, oracle} {
					if err := s.UpdateMany(ctx, name, []Point{{Timestamp: base - 60, Value: 99}}); err != nil {
						t.Fatal(err)
					}
				}
			}
			records, err := source.ExportTransfer(ctx, names)
			if err != nil {
				t.Fatal(err)
			}
			live := []Point{{Timestamp: base - 60, Value: math.Float64frombits(1 << 63)}, {Timestamp: base, Value: math.SmallestNonzeroFloat64}}
			// Force a revision read even when the epoch shortcut could prove it current.
			target.transferRereadMetadata = true
			checked := 0
			siblingWrites := 0
			target.transferReadTestHook = func() {
				checked++
				stripe := &target.metricMu[target.metricStripe(names[0])]
				if !stripe.TryLock() {
					t.Error("revision validation holds a sibling stripe")
					return
				}
				stripe.Unlock()
				if err := target.TryUpdateMany(ctx, names[0], live); !errors.Is(err, ErrImportPending) {
					t.Errorf("reserved name: %v", err)
					return
				}
				if err := target.UpdateMany(ctx, collision, live); err != nil {
					t.Error(err)
				}
				siblingWrites++
			}
			if partitioned {
				calls := 0
				target.transferApplyTestHook = func() {
					calls++
					if calls == 1 {
						if err := target.UpdateMany(ctx, names[1], live); err != nil {
							t.Error(err)
						}
					}
				}
				if err := oracle.UpdateMany(ctx, names[1], live); err != nil {
					t.Fatal(err)
				}
			}
			if _, err = target.ImportTransfer(ctx, records); err != nil {
				t.Fatal(err)
			}
			if partitioned && target.TransferApplyStats().SplitGroups == 0 {
				t.Fatal("partitioned path was not exercised")
			}
			if checked == 0 {
				t.Fatal("revision validation was not exercised")
			}
			target.transferReadTestHook = nil
			if _, err = oracle.ImportTransfer(ctx, records); err != nil {
				t.Fatal(err)
			}
			// Repeated sibling writes are identical overwrites, hence this serial
			// execution is the byte-exact oracle for every validation interleaving.
			for range siblingWrites {
				if err = oracle.UpdateMany(ctx, collision, live); err != nil {
					t.Fatal(err)
				}
			}
			for _, s := range []*Store{target, oracle} {
				if err = s.UpdateMany(ctx, names[0], live); err != nil {
					t.Fatal(err)
				}
			}
			digest := func(s *Store) string {
				raw, err := s.RawKeyValuesForTest()
				if err != nil {
					t.Fatal(err)
				}
				h := sha256.New()
				var size [8]byte
				for _, kv := range raw {
					for _, v := range kv {
						binary.LittleEndian.PutUint64(size[:], uint64(len(v)))
						h.Write(size[:])
						h.Write(v)
					}
				}
				return fmt.Sprintf("%x", h.Sum(nil))
			}
			if got, want := digest(target), digest(oracle); got != want {
				t.Fatalf("database bytes differ: %s want %s", got, want)
			}
			t.Log("database SHA256", digest(target))
		})
	}
}
