package main

import (
	"context"
	"fmt"
	"math/bits"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/helper"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

type latency struct {
	buckets [64]atomic.Uint64
	max     atomic.Uint64
}

func (h *latency) record(d time.Duration) {
	n := uint64(d)
	i := min(bits.Len64(n), 63)
	h.buckets[i].Add(1)
	for old := h.max.Load(); n > old; old = h.max.Load() {
		if h.max.CompareAndSwap(old, n) {
			break
		}
	}
}
func (h *latency) stats() map[string]float64 {
	var count uint64
	for i := range h.buckets {
		count += h.buckets[i].Load()
	}
	out := map[string]float64{"writes": float64(count), "max_ms": float64(h.max.Load()) / 1e6}
	for _, p := range []int{50, 95, 99} {
		var n uint64
		for i := range h.buckets {
			n += h.buckets[i].Load()
			if n >= (count*uint64(p)+99)/100 {
				out[fmt.Sprintf("p%d_upper_ms", p)] = float64(uint64(1)<<i) / 1e6
				break
			}
		}
	}
	return out
}

// Intentionally adversarial collision probe: different ordinary metrics use
// the same 4096 high-hash-bit stripes as the first imported names. All stores
// and writes are private. The probe does not alter any imported name.
func foregroundProbe(ctx context.Context, target *store.Store, imported []string, workers int, epoch int64) (map[string]float64, func() (map[string]float64, error), error) {
	if workers == 0 {
		return nil, func() (map[string]float64, error) { return nil, nil }, nil
	}
	names := make([]string, workers)
	for i := range names {
		stripe := (helper.HashString(imported[i%len(imported)]) >> 32) % 4096
		for j := 0; ; j++ {
			name := fmt.Sprintf("foreground.private.%d.%d", i, j)
			if (helper.HashString(name)>>32)%4096 == stripe {
				names[i] = name
				break
			}
		}
		if _, err := target.Create(ctx, store.MetricConfig{Name: names[i], Retentions: []store.Retention{{Step: 60, Count: 256}}, AggregationMethod: store.Sum}); err != nil {
			return nil, nil, err
		}
	}
	var control, loaded latency
	var first atomic.Pointer[error]
	write := func(name string, n int, h *latency) {
		began := time.Now()
		err := target.UpdateMany(ctx, name, []store.Point{{Timestamp: epoch - epoch%60 - 60, Value: float64(n)}})
		h.record(time.Since(began))
		if err != nil {
			first.CompareAndSwap(nil, &err)
		}
	}
	var wg sync.WaitGroup
	for _, name := range names {
		wg.Go(func() {
			for i := range 1000 {
				write(name, i, &control)
			}
		})
	}
	wg.Wait()
	stop := make(chan struct{})
	for _, name := range names {
		wg.Go(func() {
			for i := 1000; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				write(name, i, &loaded)
			}
		})
	}
	return control.stats(), func() (map[string]float64, error) {
		close(stop)
		wg.Wait()
		if e := first.Load(); e != nil {
			return loaded.stats(), *e
		}
		return loaded.stats(), nil
	}, nil
}
