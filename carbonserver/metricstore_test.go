package carbonserver

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/points"
	protov2 "github.com/go-graphite/protocol/carbonapi_v2_pb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMetricStoreCatalogAndReadPaths(t *testing.T) {
	const now = 1_700_000_000
	ctx := context.Background()
	metricStore, err := store.Open(t.TempDir(), store.Options{Now: func() time.Time { return time.Unix(now, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	defer metricStore.Close()

	config := func(name string) store.MetricConfig {
		return store.MetricConfig{
			Name:              name,
			Retentions:        []store.Retention{{Step: 1, Count: 120}, {Step: 60, Count: 120}},
			AggregationMethod: store.Average,
			XFilesFactor:      0,
		}
	}
	for _, metric := range []string{"servers.api.cpu.user", "servers.api.cpu.system"} {
		if _, err := metricStore.Create(ctx, config(metric)); err != nil {
			t.Fatal(err)
		}
		if err := metricStore.UpdateMany(ctx, metric, []store.Point{
			{Timestamp: now - 180, Value: 1}, {Timestamp: now - 2, Value: 2},
		}); err != nil {
			t.Fatal(err)
		}
	}

	for _, test := range []struct {
		name string
		trie bool
	}{{name: "trigram"}, {name: "trie", trie: true}} {
		t.Run(test.name, func(t *testing.T) {
			cache := cache.New()
			listener := NewCarbonserverListener(cache.Get)
			listener.logger = zap.NewNop()
			listener.accessLogger = zap.NewNop()
			listener.SetWhisperData(t.TempDir())
			listener.SetMaxGlobs(100)
			listener.SetTrieIndex(test.trie)
			listener.SetTrigramIndex(!test.trie)
			listener.SetMetricStore(metricStore)
			if err := listener.RefreshMetricStoreIndex(); err != nil {
				t.Fatal(err)
			}

			entries, err := os.ReadDir(listener.whisperData)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 0 {
				t.Fatalf("shared-store catalog created metric files: %v", entries)
			}
			assertStoreGlob(t, listener, "servers.api.cpu", true)
			assertStoreGlob(t, listener, "servers.api.cpu.*", true)

			cache.Add(points.OnePoint("servers.api.cpu.user", 99, now-2))
			fine, err := listener.fetchSingleMetric("servers.api.cpu.user", "", now-10, now)
			if err != nil {
				t.Fatal(err)
			}
			hasCacheValue := false
			for _, value := range fine.Values {
				hasCacheValue = hasCacheValue || value == 99
			}
			if fine.StepTime != 1 || !hasCacheValue {
				t.Fatalf("fine fetch did not overlay cache: step=%d values=%v", fine.StepTime, fine.Values)
			}
			coarse, err := listener.fetchSingleMetric("servers.api.cpu.user", "", now-240, now-121)
			if err != nil {
				t.Fatal(err)
			}
			if coarse.StepTime != 60 {
				t.Fatalf("coarse fetch step=%d, want 60", coarse.StepTime)
			}

			assertMetricStoreHTTPMetadata(t, listener)
			grpcInfo, err := listener.Info(ctx, &protov2.InfoRequest{Name: "servers.api.cpu.user"})
			if err != nil {
				t.Fatal(err)
			}
			if len(grpcInfo.Retentions) != 2 || grpcInfo.MaxRetention != 7200 {
				t.Fatalf("unexpected gRPC info: %#v", grpcInfo)
			}

			if !test.trie {
				if err := metricStore.Delete(ctx, "servers.api.cpu.system"); err != nil {
					t.Fatal(err)
				}
				if err := listener.RefreshMetricStoreIndex(); err != nil {
					t.Fatal(err)
				}
				assertStoreGlob(t, listener, "servers.api.cpu.system", false)
			}

			restarted := NewCarbonserverListener(cache.Get)
			restarted.logger = zap.NewNop()
			restarted.SetTrieIndex(test.trie)
			restarted.SetTrigramIndex(!test.trie)
			restarted.SetMetricStore(metricStore)
			if err := restarted.RefreshMetricStoreIndex(); err != nil {
				t.Fatal(err)
			}
			assertStoreGlob(t, restarted, "servers.api.cpu.user", true)
		})
	}
}

func TestMetricStoreListenerStopsCatalogScanner(t *testing.T) {
	metricStore, err := store.Open(t.TempDir(), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer metricStore.Close()

	listener := NewCarbonserverListener(cache.New().Get)
	listener.logger = zap.NewNop()
	listener.SetWhisperData(t.TempDir())
	listener.SetMetricStore(metricStore)
	listener.SetTrieIndex(true)
	listener.SetTrigramIndex(false)
	if err := listener.Listen("127.0.0.1:0"); errors.Is(err, syscall.EPERM) {
		t.Skip("sandbox denies loopback listeners")
	} else if err != nil {
		t.Fatal(err)
	}
	if err := listener.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestMetricStoreFetchKeepsSchemaAndSeriesTogether(t *testing.T) {
	const now = 1_700_000_000
	ctx := context.Background()
	metricStore, err := store.Open(t.TempDir(), store.Options{Now: func() time.Time { return time.Unix(now, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	defer metricStore.Close()

	configs := []store.MetricConfig{
		{Name: "schema.switch", Retentions: []store.Retention{{Step: 1, Count: 120}, {Step: 60, Count: 120}}, AggregationMethod: store.Average, XFilesFactor: 0.1},
		{Name: "schema.switch", Retentions: []store.Retention{{Step: 5, Count: 24}, {Step: 60, Count: 120}}, AggregationMethod: store.Sum, XFilesFactor: 0.9},
	}
	if _, err := metricStore.Create(ctx, configs[0]); err != nil {
		t.Fatal(err)
	}

	listener := NewCarbonserverListener(cache.New().Get)
	listener.logger = zap.NewNop()
	listener.SetMetricStore(metricStore)
	done := make(chan error, 1)
	go func() {
		for i := 0; i < 200; i++ {
			config := configs[i%len(configs)]
			snapshot := store.Snapshot{
				Metadata: store.Metadata{MetricConfig: config},
				Archives: []store.Archive{
					{Retention: config.Retentions[0], Points: []store.Point{{Timestamp: now - 10, Value: 1}}},
					{Retention: config.Retentions[1]},
				},
			}
			if _, err := metricStore.Replace(ctx, snapshot); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	for i := 0; i < 200; i++ {
		response, err := listener.fetchSingleMetric("schema.switch", "", now-10, now)
		if err != nil {
			t.Fatal(err)
		}
		switch response.StepTime {
		case 1:
			if response.ConsolidationFunc != "Average" || response.XFilesFactor != 0.1 {
				t.Fatalf("fine response has mixed schema: %#v", response)
			}
		case 5:
			if response.ConsolidationFunc != "Sum" || response.XFilesFactor != 0.9 {
				t.Fatalf("five-second response has mixed schema: %#v", response)
			}
		default:
			t.Fatalf("unexpected response step: %#v", response)
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestMetricStoreConcurrentRefreshDoesNotResurrectDeletedMetric(t *testing.T) {
	ctx := context.Background()
	metricStore, err := store.Open(t.TempDir(), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer metricStore.Close()
	config := store.MetricConfig{
		Name:              "deleted.metric",
		Retentions:        []store.Retention{{Step: 1, Count: 60}},
		AggregationMethod: store.Average,
	}
	if _, err := metricStore.Create(ctx, config); err != nil {
		t.Fatal(err)
	}
	listener := NewCarbonserverListener(cache.New().Get)
	listener.logger = zap.NewNop()
	listener.SetMaxGlobs(100)
	listener.SetMetricStore(metricStore)

	refresh := func() error { return listener.RefreshMetricStoreIndex() }
	var refreshes sync.WaitGroup
	for i := 0; i < 8; i++ {
		refreshes.Add(1)
		go func() {
			defer refreshes.Done()
			_ = refresh()
		}()
	}
	if err := metricStore.Delete(ctx, config.Name); err != nil {
		t.Fatal(err)
	}
	refreshes.Wait()
	for i := 0; i < 8; i++ {
		if err := refresh(); err != nil {
			t.Fatal(err)
		}
	}
	assertStoreGlob(t, listener, config.Name, false)
}

func TestMetricStoreRejectsLateRequests(t *testing.T) {
	metricStore, err := store.Open(t.TempDir(), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer metricStore.Close()
	listener := NewCarbonserverListener(cache.New().Get)
	listener.SetMetricStore(metricStore)
	listener.sharedRequestMu.Lock()
	listener.sharedStoreStopped = true
	listener.sharedRequestMu.Unlock()

	httpCalled := false
	httpRequest := httptest.NewRequest(http.MethodGet, "/render/", nil)
	httpResponse := httptest.NewRecorder()
	listener.rateLimitRequest(func(http.ResponseWriter, *http.Request) { httpCalled = true })(httpResponse, httpRequest)
	if httpResponse.Code != http.StatusServiceUnavailable || httpCalled {
		t.Fatalf("late HTTP request: status=%d called=%t", httpResponse.Code, httpCalled)
	}

	grpcCalled := false
	_, err = listener.UnaryServerRatelimitHandler()(context.Background(), nil, &grpc.UnaryServerInfo{}, func(context.Context, interface{}) (interface{}, error) {
		grpcCalled = true
		return nil, nil
	})
	if status.Code(err) != codes.Unavailable || grpcCalled {
		t.Fatalf("late gRPC request: code=%s called=%t", status.Code(err), grpcCalled)
	}
}

func assertStoreGlob(t *testing.T, listener *CarbonserverListener, query string, want bool) {
	t.Helper()
	result, err := listener.getExpandedGlobs(context.Background(), zap.NewNop(), time.Now(), []string{query})
	if err != nil {
		t.Fatal(err)
	}
	got := len(result) == 1 && len(result[0].Files) > 0
	if got != want {
		t.Fatalf("glob %q got %v (%#v), want %v", query, got, result, want)
	}
}

func assertMetricStoreHTTPMetadata(t *testing.T, listener *CarbonserverListener) {
	t.Helper()
	infoRequest := httptest.NewRequest(http.MethodGet, "/info/?target=servers.api.cpu.user&format=json", nil)
	infoResponse := httptest.NewRecorder()
	listener.infoHandler(infoResponse, infoRequest)
	if infoResponse.Code != http.StatusOK || !strings.Contains(infoResponse.Body.String(), "servers.api.cpu.user") {
		t.Fatalf("HTTP info response: status=%d body=%s", infoResponse.Code, infoResponse.Body.String())
	}

	findRequest := httptest.NewRequest(http.MethodGet, "/metrics/find/?query=servers.api.cpu.*&format=json", nil)
	findResponse := httptest.NewRecorder()
	listener.findHandler(findResponse, findRequest)
	if findResponse.Code != http.StatusOK || !strings.Contains(findResponse.Body.String(), "servers.api.cpu.user") {
		t.Fatalf("HTTP find response: status=%d body=%s", findResponse.Code, findResponse.Body.String())
	}

	detailsRequest := httptest.NewRequest(http.MethodGet, "/metrics/details/?format=json", nil)
	detailsResponse := httptest.NewRecorder()
	listener.detailsHandler(detailsResponse, detailsRequest)
	if detailsResponse.Code != http.StatusOK || !strings.Contains(detailsResponse.Body.String(), "servers.api.cpu.user") {
		t.Fatalf("HTTP details response: status=%d body=%s", detailsResponse.Code, detailsResponse.Body.String())
	}
}

func TestMetricStoreStopDoesNotWaitForeverOnStuckRequest(t *testing.T) {
	metricStore, err := store.Open(t.TempDir(), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer metricStore.Close()
	listener := NewCarbonserverListener(cache.New().Get)
	listener.SetMetricStore(metricStore)
	accepted, locked := listener.beginSharedStoreRequest()
	if !accepted || !locked {
		t.Fatalf("request not admitted: accepted=%t locked=%t", accepted, locked)
	}
	done := make(chan struct{})
	go func() {
		listener.stopSharedStoreRequests(50 * time.Millisecond)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stop blocked on an in-flight shared-store request")
	}
	listener.endSharedStoreRequest(locked)
	if accepted, _ := listener.beginSharedStoreRequest(); accepted {
		t.Fatal("request admitted after stop")
	}
}
