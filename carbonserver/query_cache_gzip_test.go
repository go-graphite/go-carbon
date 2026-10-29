package carbonserver

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/NYTimes/gziphandler"
	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
	"go.uber.org/zap"
)

const queryCacheTestNow = 1700000000

var queryCacheTestMetrics = []string{"servers.api.cpu.user", "servers.api.cpu.system"}

// newQueryCacheTestServer serves a shared store through the production handler
// stack, gziphandler included. Each metric holds points seconds of values of
// a renderSample kind; renderSampleMixed rotates the kinds over the metrics.
func newQueryCacheTestServer(t testing.TB, metrics []string, points, kind int) (*CarbonserverListener, *httptest.Server) {
	t.Helper()
	ctx := context.Background()
	db, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, Now: func() time.Time { return time.Unix(queryCacheTestNow, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	rng := rand.New(rand.NewSource(1))
	for i, metric := range metrics {
		config := store.MetricConfig{Name: metric, Retentions: []store.Retention{{Step: 1, Count: points}}, AggregationMethod: store.Average}
		if _, err := db.Create(ctx, config); err != nil {
			t.Fatal(err)
		}
		var stored []store.Point
		k := kind
		if k == renderSampleMixed {
			k = i % renderSampleKinds
		}
		for j, v := range renderSampleValues(rng, k, points) {
			if !math.IsNaN(v) {
				stored = append(stored, store.Point{Timestamp: int64(queryCacheTestNow - points + 1 + j), Value: v})
			}
		}
		if err := db.UpdateMany(ctx, metric, stored); err != nil {
			t.Fatal(err)
		}
	}
	listener := NewCarbonserverListener(cache.New().Get)
	listener.logger, listener.accessLogger = zap.NewNop(), zap.NewNop()
	listener.SetWhisperData(t.TempDir())
	listener.SetMaxGlobs(100)
	listener.SetMaxMetricsRendered(len(metrics))
	listener.SetTrigramIndex(true)
	listener.SetMetricStore(db)
	if err := listener.RefreshMetricStoreIndex(); err != nil {
		t.Fatal(err)
	}
	listener.SetQueryCacheEnabled(true)
	server := httptest.NewServer(gziphandler.GzipHandler(listener.newHTTPMux()))
	t.Cleanup(server.Close)
	return listener, server
}

// setQueryCacheCompression restarts the query cache in the given mode.
func setQueryCacheCompression(t testing.TB, listener *CarbonserverListener, compression string) {
	t.Helper()
	if err := listener.SetQueryCacheCompression(compression); err != nil {
		t.Fatal(err)
	}
	listener.initializeCaches()
}

type queryCacheTestResponse struct {
	status   int
	header   http.Header
	body     []byte // as received
	decoded  []byte
	encoding string
}

func queryCacheRequest(t testing.TB, server *httptest.Server, method, format, target string, from int, acceptGzip bool) queryCacheTestResponse {
	t.Helper()
	url := fmt.Sprintf("%s/render/?format=%s&target=%s&from=%d&until=%d", server.URL, format, target, from, queryCacheTestNow)
	var body io.Reader = http.NoBody
	if format == "carbonapi_v3_pb" {
		request := protov3.MultiFetchRequest{Metrics: []*protov3.FetchRequest{{Name: target, PathExpression: target, StartTime: int64(from), StopTime: queryCacheTestNow}}}
		payload, err := request.MarshalVT()
		if err != nil {
			t.Fatal(err)
		}
		body = bytes.NewReader(payload)
	}
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatal(err)
	}
	if acceptGzip {
		req.Header.Set("Accept-Encoding", "gzip")
	}
	client := &http.Client{Transport: &http.Transport{DisableCompression: true}}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	r := queryCacheTestResponse{status: resp.StatusCode, header: resp.Header, body: raw, decoded: raw, encoding: resp.Header.Get("Content-Encoding")}
	if r.encoding == "gzip" && method != http.MethodHead {
		zr, err := gzip.NewReader(bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		if r.decoded, err = io.ReadAll(zr); err != nil {
			t.Fatal(err)
		}
	}
	return r
}

func assertSameQueryCacheResponse(t *testing.T, name string, got, want queryCacheTestResponse, headers ...string) {
	t.Helper()
	for _, h := range append([]string{"Content-Type", "Content-Encoding", "Vary"}, headers...) {
		if got.header.Get(h) != want.header.Get(h) {
			t.Errorf("%s: %s=%q, want %q", name, h, got.header.Get(h), want.header.Get(h))
		}
	}
	if got.status != want.status || !bytes.Equal(got.decoded, want.decoded) {
		t.Errorf("%s: status=%d body=%d bytes, want status=%d body=%d bytes; decoded bodies equal=%t",
			name, got.status, len(got.decoded), want.status, len(want.decoded), bytes.Equal(got.decoded, want.decoded))
	}
}

// A gzip cache entry must decode to the response the uncompressed cache
// serves, with the same status and headers, on a miss and on hits, for clients
// with and without gzip, for every format, for small responses that stay
// uncompressed, for HEAD and for errors.
func TestQueryCacheGzipServesSameResponses(t *testing.T) {
	t.Parallel()
	listener, server := newQueryCacheTestServer(t, queryCacheTestMetrics, 600, renderSampleMixed)
	type request struct {
		method, target string
		from           int
		// Parallel fetches order series differently on each miss.
		ordered bool
		// gziphandler compresses the body.
		large bool
	}
	requests := []request{
		{http.MethodGet, "servers.api.cpu.user", queryCacheTestNow - 600, true, true},
		{http.MethodGet, "servers.api.cpu.*", queryCacheTestNow - 600, false, true},
		{http.MethodGet, "servers.api.cpu.user", queryCacheTestNow - 10, true, false},
		{http.MethodHead, "servers.api.cpu.user", queryCacheTestNow - 600, true, false},
		{http.MethodGet, "servers.api.missing", queryCacheTestNow - 600, true, false},
	}
	for _, format := range []string{"json", "protobuf", "carbonapi_v2_pb", "carbonapi_v3_pb", "pickle"} {
		for _, r := range requests {
			name := fmt.Sprintf("%s/%s/%s/from=%d", format, r.method, r.target, queryCacheTestNow-r.from)
			t.Run(name, func(t *testing.T) {
				setQueryCacheCompression(t, listener, "none")
				want := map[bool]queryCacheTestResponse{}
				for _, gz := range []bool{true, false} {
					want[gz] = queryCacheRequest(t, server, r.method, format, r.target, r.from, gz)
				}
				if r.large && (want[true].encoding != "gzip" || want[false].encoding != "" || len(want[false].body) < gziphandler.DefaultMinSize) {
					t.Fatalf("fixture: want a body gziphandler compresses, got %d bytes, encodings %q and %q",
						len(want[false].body), want[true].encoding, want[false].encoding)
				}

				// Pickle encodes maps in random order; compare it to the miss only.
				deterministic := r.ordered && format != "pickle"

				// A miss, then hits for both clients, starting with either client.
				for _, order := range [][]bool{{true, false, true, false}, {false, true, false, true}} {
					setQueryCacheCompression(t, listener, "gzip")
					var miss []byte
					for i, gz := range order {
						got := queryCacheRequest(t, server, r.method, format, r.target, r.from, gz)
						if i == 0 {
							miss = got.decoded
							if deterministic && !bytes.Equal(miss, want[gz].decoded) {
								t.Errorf("miss gzip=%t differs from the uncompressed cache", gz)
							}
						}
						reference := want[gz]
						reference.decoded = miss
						var headers []string
						if r.method == http.MethodHead && deterministic {
							// HEAD takes the path it took before, gziphandler included.
							headers = append(headers, "Content-Length")
						}
						assertSameQueryCacheResponse(t, fmt.Sprintf("request %d gzip=%t", i, gz), got, reference, headers...)
					}
				}
			})
		}
	}
}

func TestQueryCacheGzipStoresEncodedEntries(t *testing.T) {
	t.Parallel()
	listener, server := newQueryCacheTestServer(t, queryCacheTestMetrics, 600, renderSampleMixed)
	setQueryCacheCompression(t, listener, "gzip")
	ctx := context.Background()
	fetch := func(name string, from int32) fetchResponse {
		t.Helper()
		targets := map[timeRange][]target{{from: from, until: queryCacheTestNow}: {{Name: name, PathExpression: name}}}
		response, _, err := listener.fetchWithCache(ctx, listener.logger, protoV2Format, targets, &traceLogEntries{})
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	large := fetch("servers.api.cpu.user", queryCacheTestNow-600)
	if !large.gzipped || len(large.data) >= large.memoryUsed/2 || cap(large.data) > len(large.data)*9/8+64 {
		t.Fatalf("large entry: gzipped=%t len=%d cap=%d raw=%d", large.gzipped, len(large.data), cap(large.data), large.memoryUsed)
	}
	targets := map[timeRange][]target{{from: queryCacheTestNow - 600, until: queryCacheTestNow}: {{Name: "servers.api.cpu.user", PathExpression: "servers.api.cpu.user"}}}
	raw, err := listener.prepareDataProto(ctx, listener.logger, protoV2Format, targets, &traceLogEntries{})
	if err != nil {
		t.Fatal(err)
	}
	var decoded bytes.Buffer
	if err := gunzipTo(&decoded, large.data); err != nil || !bytes.Equal(decoded.Bytes(), raw.data) {
		t.Fatalf("entry decodes to %d bytes, err=%v; want the %d rendered bytes", decoded.Len(), err, len(raw.data))
	}
	small := fetch("servers.api.cpu.user", queryCacheTestNow-10)
	if small.gzipped || len(small.data) >= gziphandler.DefaultMinSize {
		t.Fatalf("small entry: gzipped=%t len=%d", small.gzipped, len(small.data))
	}
	rawBytes, stored := listener.metrics.QueryCacheRawBytes, listener.metrics.QueryCacheStoredBytes
	if want := uint64(len(large.data) + len(small.data)); stored != want || rawBytes != uint64(len(raw.data)+len(small.data)) {
		t.Fatalf("counted raw=%d stored=%d, want raw=%d stored=%d", rawBytes, stored, len(raw.data)+len(small.data), want)
	}
	// Entries are sized by the stored bytes, keys included.
	keys := 0
	for _, from := range []int32{queryCacheTestNow - 600, queryCacheTestNow - 10} {
		key, _ := listener.getRenderCacheKeyAndSize(map[timeRange][]target{{from: from, until: queryCacheTestNow}: {{Name: "servers.api.cpu.user", PathExpression: "servers.api.cpu.user"}}}, protoV2Format.String())
		keys += len(key)
	}
	if got, want := listener.queryCache.sizes.bytes.Load(), stored+uint64(keys); got != want || listener.queryCache.sizes.average() != want/2 {
		t.Fatalf("cache sized entries at %d bytes, want %d", got, want)
	}

	// A decoding error is reported, not written as a body.
	broken := large
	broken.data = broken.data[:len(broken.data)/2]
	rec := httptest.NewRecorder()
	if err := writeRenderResponse(rec, httptest.NewRequest(http.MethodGet, server.URL, http.NoBody), broken); err == nil {
		t.Fatal("truncated gzip entry decoded without error")
	}
}

func TestSetQueryCacheCompression(t *testing.T) {
	t.Parallel()
	listener := NewCarbonserverListener(cache.New().Get)
	for _, tt := range []struct {
		value   string
		gzip    bool
		invalid bool
	}{{"gzip", true, false}, {"none", false, false}, {"zstd", false, true}, {"", false, true}} {
		err := listener.SetQueryCacheCompression(tt.value)
		if (err != nil) != tt.invalid || (err == nil && listener.queryCacheGzip != tt.gzip) {
			t.Errorf("%q: err=%v gzip=%t", tt.value, err, listener.queryCacheGzip)
		}
	}
}
