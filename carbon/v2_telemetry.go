package carbon

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2core"
)

// Clock is persisted in whole seconds. Visibility is therefore reported as a
// conservative upper bound, including the 10ms histogram rounding.
// This observer owns no input queue and never changes admission/publication.
type v2WriteHistogram struct{ buckets [12002]atomic.Uint64 }

func (h *v2WriteHistogram) add(d time.Duration, n uint64) {
	i := max(0, min(len(h.buckets)-1, int(d/(10*time.Millisecond))))
	h.buckets[i].Add(n)
}
func (h *v2WriteHistogram) report() map[string]interface{} {
	var counts [12002]uint64
	var total uint64
	for i := range counts {
		counts[i] = h.buckets[i].Load()
		total += counts[i]
	}
	q := func(num uint64) interface{} {
		if total == 0 {
			return 0
		}
		target := (total*num + 99) / 100
		var count uint64
		for i, n := range counts {
			count += n
			if count >= target {
				if i == len(counts)-1 {
					return nil
				}
				return float64(i+1) / 100
			}
		}
		return nil
	}
	return map[string]interface{}{"count": total, "p50_seconds_upper": q(50), "p99_seconds_upper": q(99), "overflow": counts[len(counts)-1]}
}

type v2VisibleOwner struct {
	mu     sync.Mutex
	orders []uint64
}
type v2WriteTelemetry struct {
	owners                                    []v2VisibleOwner
	producers                                 int
	visibleLatency, seal, rollup, publication v2WriteHistogram
}

func newV2WriteTelemetry(shards, producers int) *v2WriteTelemetry {
	t := &v2WriteTelemetry{owners: make([]v2VisibleOwner, shards), producers: producers}
	for i := range t.owners {
		t.owners[i].orders = make([]uint64, producers)
	}
	return t
}
func (t *v2WriteTelemetry) visible(owner int, batches []v2core.Batch) {
	t.visiblePending(owner, v2core.PendingBatches(batches))
}

func (t *v2WriteTelemetry) visiblePending(owner int, pending v2core.PendingView) {
	o := &t.owners[owner]
	o.mu.Lock()
	defer o.mu.Unlock()
	now := time.Now()
	// The same head can be published repeatedly, then sealed. Original per-stream
	// order is monotonic on each owner; advance only after scanning the whole head.
	next := append([]uint64(nil), o.orders...)
	counts := map[int64]uint64{}
	pending.Each(func(b *v2core.PendingApplication) bool {
		p := int((b.ID - 1) % uint64(t.producers))
		if b.Order > o.orders[p] {
			next[p] = max(next[p], b.Order)
			counts[b.Clock] += uint64(b.Points)
		}
		return true
	})
	copy(o.orders, next)
	for clock, n := range counts {
		t.visibleLatency.add(now.Sub(time.Unix(clock, 0)), n)
	}
}
func (t *v2WriteTelemetry) observeStage(phase string, d time.Duration) {
	switch phase {
	case "seal_total":
		t.seal.add(d, 1)
	case "rollup_plan":
		t.rollup.add(d, 1)
	case "publication":
		t.publication.add(d, 1)
	}
}
func (t *v2WriteTelemetry) report() map[string]interface{} {
	return map[string]interface{}{"append_to_visible": t.visibleLatency.report(), "seal_total": t.seal.report(), "rollup_plan": t.rollup.report(), "publication": t.publication.report(), "clock_resolution_seconds": 1, "stage_bucket_seconds": 0.01, "visibility_weight": "points", "stage_weight": "owner seals", "producers": t.producers, "shards": len(t.owners)}
}
