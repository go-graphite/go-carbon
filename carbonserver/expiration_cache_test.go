package carbonserver

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/dgryski/go-expirecache"
	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	protov2 "github.com/go-graphite/protocol/carbonapi_v2_pb"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMetricStoreExpirationPreservesCacheUntilTTL(t *testing.T) {
	for _, trie := range []bool{false, true} {
		for _, clear := range []bool{false, true} {
			t.Run(fmt.Sprintf("trie=%v/clear=%v", trie, clear), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					const now = 1700000000
					ctx := context.Background()
					db, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, SyncInterval: time.Hour, Now: func() time.Time { return time.Unix(now, 0) }})
					if err != nil {
						t.Fatal(err)
					}
					defer db.Close()
					populateMetricStoreCatalog(ctx, t, db, now)
					listener := newMetricStoreTestListener(t, cache.New(), db, trie)
					listener.SetMaxMetricsGlobbed(100)
					listener.maxMetricsRendered = 100
					listener.queryCache = expireCache{ec: expirecache.New(1 << 20)}
					listener.SetQueryCacheEnabled(true)
					listener.SetStreamingQueryCacheEnabled(true)
					listener.SetFindCacheEnabled(true)
					listener.SetGlobCacheEnabled(true)
					atomic.StoreUint64(&listener.metrics.RenderRequests, 1)
					atomic.StoreUint64(&listener.metrics.FetchSize, 1024)
					victim, hot, glob := "servers.api.cpu.user", "servers.api.cpu.system", "servers.api.cpu.*"
					targets := func(name string) map[timeRange][]target {
						return map[timeRange][]target{{from: now - 10, until: now}: {{Name: name, PathExpression: name}}}
					}
					fresh := false
					fetch := func(name string, wantHit bool, count int) {
						t.Helper()
						result, hit, err := listener.fetchWithCache(ctx, zap.NewNop(), jsonFormat, targets(name), &traceLogEntries{})
						if err != nil || hit != wantHit || result.metricsFetched != count {
							t.Fatalf("render %s hit=%v want=%v count=%d want=%d: %v", name, hit, wantHit, result.metricsFetched, count, err)
						}
						if fresh && bytes.Contains(result.data, []byte(victim)) {
							t.Fatal("expired metric remained in render")
						}
					}
					stream := func(name string, wantHit bool, count int) {
						t.Helper()
						fetch := func() (chan response, error) {
							expanded, _, err := listener.getExpandedGlobsWithCache(ctx, zap.NewNop(), "render", []string{name})
							if err != nil {
								return nil, err
							}
							ch := make(chan response, 3)
							listener.prepareDataStream(ctx, protoV2Format, targets(name), getMetricGlobMapFromExpandedGlobs(expanded), ch)
							return ch, nil
						}
						ch, hit, err := listener.renderResponseChannel(targets(name), protoV2Format, &traceLogEntries{}, fetch)
						if err != nil || hit != wantHit {
							t.Fatalf("stream %s hit=%v want=%v: %v", name, hit, wantHit, err)
						}
						n := 0
						for range ch {
							n++
						}
						if n != count {
							t.Fatalf("stream %s count=%d want=%d", name, n, count)
						}
					}
					for _, query := range []struct {
						name  string
						count int
					}{{victim, 1}, {glob, 2}, {hot, 1}} {
						fetch(query.name, false, query.count)
						fetch(query.name, true, query.count)
						stream(query.name, false, query.count)
						stream(query.name, true, query.count)
					}
					m, err := db.Metadata(ctx, victim)
					if err != nil {
						t.Fatal(err)
					}
					epoch := db.ReadCacheEpoch()
					outcomes, err := db.DeleteBatchIfUnchanged(ctx, []store.Metadata{m})
					if err != nil || len(outcomes) != 1 || outcomes[0] != nil {
						t.Fatalf("delete %v: %v", outcomes, err)
					}
					if db.ReadCacheEpoch() != epoch {
						t.Fatal("expiration invalidated the response cache")
					}
					if err := listener.RefreshMetricStoreIndex(); err != nil {
						t.Fatal(err)
					}
					if _, err := listener.Find(ctx, &protov2.GlobRequest{Query: victim}); status.Code(err) != codes.NotFound {
						t.Fatalf("expired find: %v", err)
					}
					found, err := listener.Find(ctx, &protov2.GlobRequest{Query: glob})
					if err != nil || len(found.Matches) != 1 || found.Matches[0].Path != hot {
						t.Fatalf("expired wildcard find: %+v %v", found, err)
					}
					// Expiration keeps the existing TTL for exact and wildcard replies,
					// including streaming replies and unrelated cached metrics.
					fetch(victim, true, 1)
					fetch(glob, true, 2)
					fetch(hot, true, 1)
					stream(victim, true, 1)
					stream(glob, true, 2)
					stream(hot, true, 1)
					if clear {
						listener.queryCache = expireCache{ec: expirecache.New(1 << 20)}
					} else {
						time.Sleep(61 * time.Second)
					}
					fresh = true
					fetch(victim, false, 0)
					fetch(glob, false, 1)
					stream(victim, false, 0)
					stream(glob, false, 1)
					if db.ReadCacheEpoch() != epoch {
						t.Fatal("cache expiry changed the store epoch")
					}
				})
			})
		}
	}
}
