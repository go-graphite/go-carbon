package carbonserver

import (
	"bytes"
	"context"
	"fmt"
	pickle "github.com/lomik/og-rek"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/points"
	protov2 "github.com/go-graphite/protocol/carbonapi_v2_pb"
	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Compare actual encoded render results during startup with a complete, freshly
// rebuilt catalog. Both missing names and cache-over-disk values must agree.
func TestNativeStartupRenderDifferential(t *testing.T) {
	const now = 1700000000
	for _, trie := range []bool{false, true} {
		t.Run(fmt.Sprint(trie), func(t *testing.T) {
			db, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, IncrementalSource: t.TempDir(), Now: func() time.Time { return time.Unix(now, 0) }})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			populateMetricStoreCatalog(context.Background(), t, db, now)
			metricCache := cache.New()
			metricCache.Add(points.OnePoint("servers.api.cpu.user", 99, now-2))
			listener := NewCarbonserverListener(metricCache.Get)
			listener.logger, listener.accessLogger = zap.NewNop(), zap.NewNop()
			listener.SetWhisperData(t.TempDir())
			listener.SetTrieIndex(trie)
			listener.SetTrigramIndex(!trie)
			listener.SetMaxGlobs(100)
			listener.SetMaxMetricsRendered(100)
			listener.SetMetricStore(db)
			listener.NoServiceWhenIndexIsNotReady = true
			render := func(format responseFormat, name string) fetchResponse {
				t.Helper()
				targets := map[timeRange][]target{{from: now - 10, until: now}: {{Name: name, PathExpression: "alias"}}}
				r, err := listener.prepareDataProto(context.Background(), zap.NewNop(), format, targets, &traceLogEntries{})
				if err != nil {
					t.Fatal(err)
				}
				return r
			}
			for _, format := range []responseFormat{jsonFormat, pickleFormat, protoV2Format, protoV3Format} {
				before := render(format, "servers.api.cpu.user")
				if before.metricsFetched != 1 {
					t.Fatalf("startup returned %d metrics", before.metricsFetched)
				}
				missing := render(format, "servers.missing")
				if missing.metricsFetched != 0 {
					t.Fatal("invented missing metric")
				}
				if err := listener.RefreshMetricStoreIndex(); err != nil {
					t.Fatal(err)
				}
				after := render(format, "servers.api.cpu.user")
				afterMissing := render(format, "servers.missing")
				same := bytes.Equal(before.data, after.data)
				if format == pickleFormat {
					a, err := pickle.NewDecoder(bytes.NewReader(before.data)).Decode()
					if err != nil {
						t.Fatal(err)
					}
					b, err := pickle.NewDecoder(bytes.NewReader(after.data)).Decode()
					if err != nil {
						t.Fatal(err)
					}
					same = reflect.DeepEqual(a, b)
				}
				if !same || !bytes.Equal(missing.data, afterMissing.data) {
					t.Fatalf("format %v differs before/after rebuild", format)
				}
				listener.UpdateFileIndex(nil)
			}
			listener.initializeCaches()
			mux := listener.newHTTPMux()
			for _, tc := range []struct {
				name string
				code int
			}{
				{"servers.api.cpu.user", http.StatusOK},
				{"servers.api.cpu.*", http.StatusServiceUnavailable},
				{"servers.api.cpu.?", http.StatusServiceUnavailable},
				{"servers.api.cpu.[us]*", http.StatusServiceUnavailable},
				{"servers.api.cpu.{user,system}", http.StatusServiceUnavailable},
			} {
				req := httptest.NewRequest("GET", fmt.Sprintf("/render/?format=json&target=%s&from=%d&until=%d", tc.name, now-10, now), nil)
				wr := httptest.NewRecorder()
				mux.ServeHTTP(wr, req)
				if wr.Code != tc.code {
					t.Fatalf("%s: code=%d body=%s", tc.name, wr.Code, wr.Body)
				}
			}

			for _, name := range []string{"servers.api.cpu.user", "servers.api.cpu.*"} {
				payload, err := (&protov3.MultiFetchRequest{Metrics: []*protov3.FetchRequest{{Name: name, StartTime: now - 10, StopTime: now}}}).MarshalVT()
				if err != nil {
					t.Fatal(err)
				}
				wr := httptest.NewRecorder()
				mux.ServeHTTP(wr, httptest.NewRequest("POST", "/render/?format=carbonapi_v3_pb", bytes.NewReader(payload)))
				want := http.StatusOK
				if name == "servers.api.cpu.*" {
					want = http.StatusServiceUnavailable
				}
				if wr.Code != want {
					t.Fatalf("protobuf v3 %s status=%d body=%s", name, wr.Code, wr.Body)
				}
			}
			stream := &restartReadStream{ctx: context.Background()}
			if err := listener.Render(&protov2.MultiFetchRequest{Metrics: []*protov2.FetchRequest{{Name: "servers.api.cpu.user", StartTime: now - 10, StopTime: now}}}, stream); err != nil || len(stream.responses) != 1 {
				t.Fatalf("gRPC literal render err=%v responses=%d", err, len(stream.responses))
			}
			wr := httptest.NewRecorder()
			mux.ServeHTTP(wr, httptest.NewRequest("GET", "/metrics/find/?query=servers.*", nil))
			if wr.Code != http.StatusServiceUnavailable {
				t.Fatalf("find returned %d", wr.Code)
			}
			if err := listener.grpcServerRatelimitHandler(context.Background(), nil, "", time.Now(), true); err != nil {
				t.Fatal(err)
			}
			err = listener.Render(&protov2.MultiFetchRequest{Metrics: []*protov2.FetchRequest{{Name: "servers.*", StartTime: now - 10, StopTime: now}}}, &restartReadStream{ctx: context.Background()})
			if status.Code(err) != codes.Unavailable {
				t.Fatalf("gRPC glob error=%v", err)
			}
		})
	}
}

type restartReadStream struct {
	grpc.ServerStream
	ctx       context.Context
	responses []*protov2.FetchResponse
}

func (s *restartReadStream) Context() context.Context { return s.ctx }
func (s *restartReadStream) Send(r *protov2.FetchResponse) error {
	s.responses = append(s.responses, r)
	return nil
}
