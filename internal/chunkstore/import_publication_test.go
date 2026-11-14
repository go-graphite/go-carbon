package chunkstore_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// Pause after validation, exactly where a slow Pebble prepare/apply/publication
// used to own the mutation stripe. The same name must defer without blocking;
// a different name on that very stripe must commit. Retried live inputs must
// produce precisely the bytes of serial import-then-update, with caches on/off.
func TestImportPublicationRequeuesLiveWithoutHoldingStripe(t *testing.T) {
	for _, cacheBytes := range []int64{0, 8 << 20} {
		for _, continuation := range []bool{false, true} {
			t.Run(fmt.Sprintf("cache%d/continuation%v", cacheBytes, continuation), func(t *testing.T) {
				ctx := context.Background()
				source := transferStoreCached(t, continuation, true, cacheBytes)
				target := transferStoreCached(t, continuation, true, cacheBytes)
				oracle := transferStoreCached(t, continuation, true, cacheBytes)
				name := "publication.live"
				collision := ""
				for i := 0; ; i++ {
					collision = fmt.Sprintf("publication.collision.%d", i)
					if target.MetricStripeForTest(collision) == target.MetricStripeForTest(name) {
						break
					}
				}
				putTransferPoints(t, source, name, []store.Point{{Timestamp: 999760, Value: math.Float64frombits(0x3ff0000000000001)}, {Timestamp: 999820, Value: 11}})
				for _, dst := range []*store.Store{target, oracle} {
					putTransferPoints(t, dst, name, []store.Point{{Timestamp: 999880, Value: 7}})
					putTransferPoints(t, dst, collision, []store.Point{{Timestamp: 999880, Value: 3}})
				}
				records, err := source.ExportTransfer(ctx, []string{name})
				if err != nil {
					t.Fatal(err)
				}
				live := []store.Point{{Timestamp: 999880, Value: math.Float64frombits(1 << 63)}, {Timestamp: 999940, Value: math.SmallestNonzeroFloat64}}
				entered, release := make(chan struct{}), make(chan struct{})
				target.SetTransferCommitHookForTest(func() { close(entered); <-release })
				finished := make(chan error, 1)
				go func() { _, err := target.ImportTransfer(ctx, records); finished <- err }()
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					t.Fatal("import did not reach commit")
				}
				// Always release a failed test before store cleanup.
				released := false
				defer func() {
					if !released {
						close(release)
						<-finished
					}
				}()
				check := make(chan error, 1)
				go func() {
					if e := target.TryUpdateMany(ctx, name, live); !errors.Is(e, store.ErrImportPending) {
						check <- fmt.Errorf("single pending: %v", e)
						return
					}
					if e := target.UpdateBatch(ctx, []store.MetricUpdate{{Name: name, Points: live}}, nil); !errors.Is(e, store.ErrImportPending) {
						check <- fmt.Errorf("batch pending: %v", e)
						return
					}
					check <- target.UpdateMany(ctx, collision, live)
				}()
				select {
				case err := <-check:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("live worker waited behind import commit")
				}
				close(release)
				released = true
				if err := <-finished; err != nil {
					t.Fatal(err)
				}
				target.SetTransferCommitHookForTest(nil)
				if _, err := oracle.ImportTransfer(ctx, records); err != nil {
					t.Fatal(err)
				}
				if err := oracle.UpdateMany(ctx, collision, live); err != nil {
					t.Fatal(err)
				}
				for _, dst := range []*store.Store{target, oracle} {
					if continuation {
						err = dst.UpdateBatch(ctx, []store.MetricUpdate{{Name: name, Points: live}}, nil)
					} else {
						err = dst.UpdateMany(ctx, name, live)
					}
					if err != nil {
						t.Fatal(err)
					}
				}
				got, err := target.RawKeyValuesForTest()
				if err != nil {
					t.Fatal(err)
				}
				want, err := oracle.RawKeyValuesForTest()
				if err != nil {
					t.Fatal(err)
				}
				if len(got) != len(want) {
					t.Fatalf("keys %d want %d", len(got), len(want))
				}
				for i := range want {
					if !bytes.Equal(got[i][0], want[i][0]) || !bytes.Equal(got[i][1], want[i][1]) {
						t.Fatalf("key %x differs: %x want %x", want[i][0], got[i][1], want[i][1])
					}
				}
			})
		}
	}
}
