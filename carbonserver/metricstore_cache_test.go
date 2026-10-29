package carbonserver

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgryski/go-expirecache"
	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"go.uber.org/zap"
)

func TestMetricStoreRenderCacheInvalidation(t *testing.T) {
	t.Parallel()
	const now = 1700000000
	ctx := context.Background()
	db, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, Now: func() time.Time { return time.Unix(now, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	populateMetricStoreCatalog(ctx, t, db, now)
	listener := newMetricStoreTestListener(t, cache.New(), db, false)
	listener.maxMetricsRendered = 100
	listener.queryCache = expireCache{ec: expirecache.New(1 << 20)}
	listener.SetQueryCacheEnabled(true)
	listener.SetStreamingQueryCacheEnabled(true)
	atomic.StoreUint64(&listener.metrics.RenderRequests, 1)
	atomic.StoreUint64(&listener.metrics.FetchSize, 1024)
	targets := map[timeRange][]target{{from: now - 10, until: now}: {{Name: "servers.api.cpu.user", PathExpression: "servers.api.cpu.user"}}}
	fetch := func(wantHit bool) []byte {
		t.Helper()
		result, hit, err := listener.fetchWithCache(ctx, zap.NewNop(), jsonFormat, targets, &traceLogEntries{})
		if err != nil || hit != wantHit {
			t.Fatalf("HTTP cache hit=%v want=%v err=%v", hit, wantHit, err)
		}
		if result.metricsFetched != 1 {
			t.Fatalf("expected one rendered metric, got %d: %q", result.metricsFetched, result.data)
		}
		return result.data
	}
	first := fetch(false)
	if !bytes.Equal(first, fetch(true)) {
		t.Fatal("cache changed response bytes")
	}
	// Ordinary ingestion retains the same configured TTL as the file backend.
	if err := db.UpdateMany(ctx, "servers.api.cpu.user", []store.Point{{Timestamp: now - 2, Value: 17}}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, fetch(true)) {
		t.Fatal("ordinary ingestion bypassed the configured cache TTL")
	}
	snapshot, err := db.Snapshot(ctx, "servers.api.cpu.user")
	if err != nil {
		t.Fatal(err)
	}
	for i := range snapshot.Archives[0].Points {
		snapshot.Archives[0].Points[i].Value = 99
	}
	if _, err := db.Replace(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	replaced := fetch(false)
	if bytes.Equal(first, replaced) {
		t.Fatalf("restore returned the pre-restore response: %q; snapshot=%+v", first, snapshot.Archives[0].Points)
	}
	if !bytes.Equal(replaced, fetch(true)) {
		t.Fatal("restored response was not cached")
	}

	calls := 0
	fetchStream := func() (chan response, error) { //nolint:unparam // Required streaming callback signature.
		calls++
		ch := make(chan response, 1)
		ch <- response{Name: "servers.api.cpu.user", Values: []float64{float64(calls)}}
		close(ch)
		return ch, nil
	}
	stream := func(wantHit bool) float64 {
		t.Helper()
		ch, hit, err := listener.renderResponseChannel(targets, protoV2Format, &traceLogEntries{}, fetchStream)
		if err != nil || hit != wantHit {
			t.Fatalf("stream cache hit=%v want=%v err=%v", hit, wantHit, err)
		}
		var value float64
		for r := range ch {
			value = r.Values[0]
		}
		return value
	}
	if stream(false) != 1 || stream(true) != 1 || calls != 1 {
		t.Fatal("stream cache did not reuse the response")
	}
	if _, err := db.Fill(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	if stream(false) != 2 || stream(true) != 2 || calls != 2 {
		t.Fatal("fill did not invalidate streaming response")
	}
	before, _ := listener.getRenderCacheKeyAndSize(targets, "json")
	if err := db.Delete(ctx, "servers.api.cpu.user"); err != nil {
		t.Fatal(err)
	}
	after, _ := listener.getRenderCacheKeyAndSize(targets, "json")
	if before == after {
		t.Fatal("delete retained the old response namespace")
	}
}
