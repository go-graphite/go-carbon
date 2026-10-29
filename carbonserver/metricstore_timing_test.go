package carbonserver

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/points"
)

func TestMetricStoreDiskWaitExcludesCacheWait(t *testing.T) {
	t.Parallel()
	const now = 1_700_000_000
	db, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, Now: func() time.Time { return time.Unix(now, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	populateMetricStoreCatalog(context.Background(), t, db, now)
	listener := newMetricStoreTestListener(t, cache.New(), db, false)
	// Warm the tiny storage read. A separate blocked cache lookup must be
	// charged to cache wait, as in the CWhisper read path, rather than disk wait.
	if _, err := db.Fetch(context.Background(), "servers.api.cpu.user", now-10, now); err != nil {
		t.Fatal(err)
	}
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		listener.cacheGet = func(string) []points.Point {
			calls++
			time.Sleep(100 * time.Millisecond)
			return nil
		}
		if _, err := listener.fetchFromDisk("servers.api.cpu.user", now-10, now); err != nil {
			t.Fatal(err)
		}
		diskWait := time.Duration(atomic.LoadUint64(&listener.metrics.DiskWaitTimeNS))
		cacheWait := time.Duration(atomic.LoadUint64(&listener.metrics.CacheWaitTimeFetchNS))
		if calls != 1 || cacheWait < 100*time.Millisecond {
			t.Fatalf("blocked cache lookup was not measured: calls=%d cache=%s", calls, cacheWait)
		}
		if diskWait >= cacheWait {
			t.Fatalf("disk wait includes the separate cache wait: disk=%s cache=%s", diskWait, cacheWait)
		}
		t.Logf("storage read=%s, separate cache wait=%s", diskWait, cacheWait)
	})
}
