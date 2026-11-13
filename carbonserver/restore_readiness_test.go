package carbonserver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/points"
	"go.uber.org/zap"
)

func TestRestoreReadGateExcludesCoarseArchivesAndCachedResponses(t *testing.T) {
	const now = 1_700_000_000
	db, err := store.Open(t.TempDir(), store.Options{Now: func() time.Time { return time.Unix(now, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	populateMetricStoreCatalog(context.Background(), t, db, now)
	c := cache.New()
	c.AddRestoredBeforeInput(points.OnePoint("servers.api.cpu.user", 9, now-180))
	c.TrackRestorePrefix()
	l := newMetricStoreTestListener(t, c, db, true)
	l.SetConcurrentIndex(true)
	l.SetMaxMetricsGlobbed(100)
	l.SetMaxMetricsRendered(100)
	l.SetRestorePending(c.RestorePending)
	wildcard := map[timeRange][]target{{from: now - 3600, until: now}: {{Name: "servers.api.cpu.*"}}}
	if _, err := l.prepareDataProto(context.Background(), zap.NewNop(), jsonFormat, wildcard, &traceLogEntries{}); !errors.Is(err, errRestorePending) {
		t.Fatal("wildcard returned a partial response", err)
	}
	recorder := httptest.NewRecorder()
	l.renderHandler(recorder, httptest.NewRequest(http.MethodGet, fmt.Sprintf("/render/?target=servers.api.cpu.user&from=%d&until=%d&format=json", now-3600, now), nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatal("recovering metric did not report temporary unavailability", recorder.Code, recorder.Body.String())
	}
	for _, from := range []int32{now - 60, now - 3600} {
		if _, err := l.fetchSingleMetric("servers.api.cpu.user", "*", from, now); !errors.Is(err, errRestorePending) {
			t.Fatal("incomplete archive read escaped gate", from, err)
		}
		if _, err := l.fetchSingleMetric("servers.api.cpu.system", "*", from, now); err != nil {
			t.Fatal("unrelated metric blocked", err)
		}
	}
	l.queryCacheEnabled = true
	targets := map[timeRange][]target{{from: now - 3600, until: now}: {{Name: "servers.api.cpu.user"}, {Name: "servers.api.cpu.system"}}}
	if _, cached, err := l.fetchWithCache(context.Background(), zap.NewNop(), jsonFormat, targets, &traceLogEntries{}); !errors.Is(err, errRestorePending) || cached {
		t.Fatal("partial response reached query cache", err)
	}
	p, _ := c.PopNotConfirmed("servers.api.cpu.user")
	if err := db.UpdateMany(context.Background(), p.Metric, p.Data); err != nil {
		t.Fatal(err)
	}
	c.Confirm(p)
	if _, err := l.fetchSingleMetric("servers.api.cpu.user", "*", now-3600, now); err != nil {
		t.Fatal("confirmed metric stayed gated", err)
	}
}

func TestRestoreIndexIncludesUncreatedNamesInQuotas(t *testing.T) {
	db, err := store.Open(t.TempDir(), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	l := newMetricStoreTestListener(t, cache.New(), db, true)
	l.SetConcurrentIndex(true)
	l.SetQuotaUsageReportFrequency(time.Hour)
	l.SetEstimateSize(func(string) (int64, int64, int64) { return 100, 200, 300 })
	l.CurrentFileIndex().trieIdx.estimateSize = l.estimateSize
	l.SetQuotas([]*Quota{{Pattern: "/", Metrics: 1, DroppingPolicy: QDPNew}})
	if err := l.PrepareRestoreIndex(slices.Values([]string{"restored.only"})); err != nil {
		t.Fatal(err)
	}
	if !l.MetricExists("restored.only") {
		t.Fatal("uncreated restored metric absent from discovery")
	}
	if !l.ShouldThrottleMetric(points.OnePoint("live.new", 1, 1), false) {
		t.Fatal("restored name missing from live quota accounting")
	}
}

// A saved mixed index predates a newly persisted restored metric. That metric
// has left the cache prefix, so only the catalog can close the discovery gap.
func TestRestoreMixedIndexUnionsCommittedCatalog(t *testing.T) {
	db, err := store.Open(t.TempDir(), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	l := newMetricStoreTestListener(t, cache.New(), db, true)
	l.SetConcurrentIndex(true)
	l.SetMetricStoreFileIndex(true)
	if _, err := l.CurrentFileIndex().trieIdx.insert("/original/only.wsp", 10, 10, 10, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Create(context.Background(), store.MetricConfig{Name: "restored.already.persisted", Retentions: []store.Retention{{Step: 1, Count: 3600}}, AggregationMethod: store.Average}); err != nil {
		t.Fatal(err)
	}
	if err := l.PrepareRestoreIndex(slices.Values([]string{"restored.still.pending"})); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"original.only", "restored.already.persisted", "restored.still.pending"} {
		if !l.MetricExists(name) {
			t.Fatal("mixed discovery omitted", name)
		}
	}
}
