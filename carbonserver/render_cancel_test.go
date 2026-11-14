package carbonserver

import (
	"context"
	"fmt"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// A slow storage reader must not keep a canceled render's entire worker pool
// queued for store admission. Leave every slot occupied until after the render
// has returned, so completing a storage read cannot hide lost cancellation.
func TestRenderCancellationReleasesQueuedWorkers(t *testing.T) {
	for _, count := range []int{1, 12} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			l, files, leafs := fetchWorkersFixture(t, count)
			l.maxFetchDataGoroutines = 3
			l.maxMetricsRendered = count
			held := fillReadSlots(t, namedReadSlotStore{l.getMetricStore(), files[0]})
			if len(held) == 0 {
				t.Fatal("fixture did not occupy store read slots")
			}
			done := make(chan struct{})
			defer func() {
				for _, release := range held {
					release()
				}
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("render did not exit after releasing storage")
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			go func() {
				l.fetchTargets(ctx, protoV2Format,
					map[timeRange][]target{{from: 1789999872, until: 1790000000}: {{Name: "fetch.*"}}},
					map[string]globs{"fetch.*": {Files: files, Leafs: leafs}}, func(response) { t.Error("canceled render emitted data") })
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("canceled render still waiting for occupied read slots")
			}
		})
	}
}

// Catalog-negative names bypass store admission; occupy slots with a real metric.
type namedReadSlotStore struct {
	*store.Store
	name string
}

func (s namedReadSlotStore) AdmitRead(ctx context.Context, _ string) (context.Context, func(), error) {
	return s.Store.AdmitRead(ctx, s.name)
}
