package v2read

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
)

// TestHeldMergeKeepsPublishing holds a merge after it linked and synced its
// output, before it publishes. Appends must still become visible: a merge
// takes the owner's binding fence only for its synced commit, never for its
// file work.
func TestHeldMergeKeepsPublishing(t *testing.T) {
	cfg := testConfig()
	cfg.SealInterval = time.Hour
	var orders sync.Map
	atMerge, release := make(chan struct{}), make(chan struct{})
	var held sync.Once
	// sealing excludes a held merge from an explicit seal in flight: the seal
	// would wait for the held merge's locks instead of the test's appends.
	var sealing sync.Mutex
	reader, _ := New(1, 1<<30, 64<<20)
	engine, err := v2core.Open(t.TempDir(), cfg, v2core.Hooks{
		PublishBatchesWithInventory: reader.PublishWithInventory,
		// Idle closure stays below the data watermark, so no maintenance seal
		// (a synchronous seal on the owner loop) starts during the hold.
		Now: func() time.Time { return time.Unix(1, 0) },
		Visible: func(_ int, batches []v2core.Batch) {
			for _, b := range batches {
				orders.Store(b.Order, true)
			}
		},
		Stage: func(s string) {
			if s != "merge-retained" {
				return
			}
			if !sealing.TryLock() {
				return
			}
			defer sealing.Unlock()
			first := false
			held.Do(func() { first = true })
			if first {
				close(atMerge)
				select {
				case <-release:
				case <-time.After(20 * time.Second):
				}
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var releaseOnce sync.Once
	defer func() {
		releaseOnce.Do(func() { close(release) })
		reader.Close()
		if err := engine.Close(); err != nil {
			t.Error(err)
		}
	}()
	p := v2core.RollupPolicy{Name: partsformat.ID("held.merge"), Group: "test", Config: sealrollup.Config{Method: sealrollup.Average, XFilesFactor: .5, Archives: []sealrollup.Archive{{Step: 1, Count: 86400}, {Step: 60, Count: 43200}}}}
	policy, err := v2core.EncodeRollupPolicy(p)
	if err != nil {
		t.Fatal(err)
	}
	clock := int64(1794700800)
	var cursor uint64
	appendVisible := func(flush bool, within time.Duration) bool {
		cursor++
		clock += 10
		b := v2core.Batch{ID: 1, Policy: policy, Order: cursor, Clock: clock, Points: []v2core.Row{{ID: 1, Time: clock, Bits: math.Float64bits(float64(cursor)), Seq: cursor}}}
		if err := engine.AppendBatches(0, []v2core.Batch{b}, cursor, flush); err != nil {
			t.Fatal(err)
		}
		for deadline := time.Now().Add(within); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
			if _, ok := orders.Load(b.Order); ok {
				return true
			}
		}
		return false
	}
	// Explicit seals make same-group parts until a four-way merge starts.
	for merging := false; !merging; {
		select {
		case <-atMerge:
			merging = true
			continue
		case <-time.After(100 * time.Millisecond):
		}
		if cursor > 32 {
			t.Fatal("no merge started")
		}
		if !sealing.TryLock() {
			continue
		}
		ok := appendVisible(true, 5*time.Second)
		sealing.Unlock()
		if !ok {
			t.Fatal("publication timeout before the merge")
		}
	}
	start := time.Now()
	for range 3 {
		ok := appendVisible(false, 2*time.Second)
		t.Logf("append %d visible=%v after %v", cursor, ok, time.Since(start))
		if !ok {
			t.Fatal("append not visible while a merge is held after linking its output")
		}
	}
	releaseOnce.Do(func() { close(release) })
}
