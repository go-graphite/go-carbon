package buckyd

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
	"github.com/golang/snappy"
)

func testService(t *testing.T) (*Service, *chunkstore.Store) {
	t.Helper()
	return testServiceWithSyncInterval(t, 0)
}

func testServiceWithSyncInterval(t *testing.T, interval time.Duration) (*Service, *chunkstore.Store) {
	t.Helper()
	db, err := chunkstore.Open(t.TempDir(), chunkstore.Options{SyncInterval: interval, Now: func() time.Time { return time.Unix(10000, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s, err := New(Config{TmpDir: t.TempDir(), MaxBodyBytes: 1 << 20}, db)
	if err != nil {
		t.Fatal(err)
	}
	return s, db
}

func TestNewCreatesConfiguredTemporaryDirectory(t *testing.T) {
	db, err := chunkstore.Open(filepath.Join(t.TempDir(), "store"), chunkstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	dir := filepath.Join(t.TempDir(), "missing", "transfer")
	if _, err := New(Config{TmpDir: dir}, db); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		t.Fatalf("temporary directory = %v, %v", info, err)
	}
}

func fixture(t *testing.T, value float64) []byte {
	t.Helper()
	path := filepath.Join(t.TempDir(), "metric.wsp")
	r := whisper.NewRetention(10, 12)
	w, err := whisper.Create(path, whisper.Retentions{&r}, whisper.Sum, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.ReplaceArchivePoints(0, []whisper.TimeSeriesPoint{{Time: 9990, Value: value}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestMetricTransferSnappyStatAndConditionalDelete(t *testing.T) {
	for _, interval := range []time.Duration{0, time.Hour} {
		t.Run(interval.String(), func(t *testing.T) {
			checkMetricTransferWithSyncInterval(t, interval)
		})
	}
}

func checkMetricTransferWithSyncInterval(t *testing.T, interval time.Duration) {
	t.Helper()
	s, _ := testServiceWithSyncInterval(t, interval)
	h := s.Handler()
	body := fixture(t, 7)
	var compressed bytes.Buffer
	writer := snappy.NewBufferedWriter(&compressed)
	_, _ = writer.Write(body)
	_ = writer.Close()
	post := httptest.NewRequest(http.MethodPost, "/metrics/team.cpu", bytes.NewReader(compressed.Bytes()))
	post.Header.Set("Content-Encoding", "snappy")
	result := httptest.NewRecorder()
	h.ServeHTTP(result, post)
	if result.Code != 200 {
		t.Fatalf("POST=%d %s", result.Code, result.Body.String())
	}
	var heal MetricHealStats
	if err := json.Unmarshal(result.Body.Bytes(), &heal); err != nil {
		t.Fatalf("POST heal stats: %v", err)
	}
	head := httptest.NewRequest(http.MethodHead, "/metrics/team.cpu", http.NoBody)
	headResult := httptest.NewRecorder()
	h.ServeHTTP(headResult, head)
	if headResult.Code != 200 {
		t.Fatal(headResult.Code)
	}
	var stat MetricData
	if err := json.Unmarshal([]byte(headResult.Header().Get("X-Metric-Stat")), &stat); err != nil {
		t.Fatal(err)
	}
	if stat.StorageVersion == "" {
		t.Fatal("missing storage version")
	}
	get := httptest.NewRequest(http.MethodGet, "/metrics/team.cpu", http.NoBody)
	get.Header.Set("Accept-Encoding", "snappy")
	getResult := httptest.NewRecorder()
	h.ServeHTTP(getResult, get)
	if getResult.Code != 200 || getResult.Header().Get("Content-Encoding") != "snappy" {
		t.Fatalf("GET=%d encoding=%q", getResult.Code, getResult.Header().Get("Content-Encoding"))
	}
	var getStat MetricData
	if err := json.Unmarshal([]byte(getResult.Header().Get("X-Metric-Stat")), &getStat); err != nil {
		t.Fatal(err)
	}
	if getStat.Size != stat.Size {
		t.Fatalf("HEAD size=%d GET size=%d", stat.Size, getStat.Size)
	}
	decoded, err := io.ReadAll(snappy.NewReader(bytes.NewReader(getResult.Body.Bytes())))
	if err != nil || len(decoded) == 0 {
		t.Fatalf("snappy response: %v", err)
	}
	wrong := httptest.NewRequest(http.MethodDelete, "/metrics/team.cpu?version=wrong", http.NoBody)
	wrongResult := httptest.NewRecorder()
	h.ServeHTTP(wrongResult, wrong)
	if wrongResult.Code != http.StatusConflict {
		t.Fatalf("wrong delete=%d", wrongResult.Code)
	}
	del := httptest.NewRequest(http.MethodDelete, "/metrics/team.cpu?version="+stat.StorageVersion, http.NoBody)
	delResult := httptest.NewRecorder()
	h.ServeHTTP(delResult, del)
	if delResult.Code != http.StatusOK {
		t.Fatalf("delete=%d", delResult.Code)
	}
}

func TestFailedReplacePreservesMetricAndListFilters(t *testing.T) {
	s, _ := testService(t)
	h := s.Handler()
	body := fixture(t, 1)
	post := httptest.NewRequest(http.MethodPost, "/metrics/team.cpu", bytes.NewReader(body))
	postResult := httptest.NewRecorder()
	h.ServeHTTP(postResult, post)
	if postResult.Code != 200 {
		t.Fatal(postResult.Code)
	}
	bad := httptest.NewRequest(http.MethodPut, "/metrics/team.cpu", bytes.NewBufferString("bad"))
	badResult := httptest.NewRecorder()
	h.ServeHTTP(badResult, bad)
	if badResult.Code == 200 {
		t.Fatal("bad replace succeeded")
	}
	head := httptest.NewRequest(http.MethodHead, "/metrics/team.cpu", http.NoBody)
	headResult := httptest.NewRecorder()
	h.ServeHTTP(headResult, head)
	if headResult.Code != 200 {
		t.Fatal("failed replace deleted metric")
	}
	list := httptest.NewRequest(http.MethodGet, "/metrics?regex=^team\\.", http.NoBody)
	listResult := httptest.NewRecorder()
	h.ServeHTTP(listResult, list)
	if listResult.Code != 200 || !bytes.Contains(listResult.Body.Bytes(), []byte("team.cpu")) {
		t.Fatalf("list=%d %s", listResult.Code, listResult.Body.String())
	}
}

func TestOffloadUsesReadInterNodeTokenWithoutHashring(t *testing.T) {
	secret := []byte("shared-secret")
	source, sourceDB := testService(t)
	source.secret = secret
	r := whisper.NewRetention(10, 12)
	if _, err := sourceDB.Create(context.Background(), chunkstore.MetricConfig{Name: "source", Retentions: []chunkstore.Retention{{Step: r.SecondsPerPoint(), Count: r.NumberOfPoints()}}, AggregationMethod: chunkstore.Sum}); err != nil {
		t.Fatal(err)
	}
	if err := sourceDB.UpdateManyForArchive(context.Background(), "source", []chunkstore.Point{{Timestamp: 9990, Value: 5}}, 10*12); err != nil {
		t.Fatal(err)
	}
	remote := httptest.NewServer(source.Handler())
	defer remote.Close()
	destination, _ := testService(t)
	destination.secret = secret
	host := remote.URL[len("http://"):]
	request := httptest.NewRequest(http.MethodPost, "/metrics/destination?fetch_offload=true&server="+host+"&metric=source", http.NoBody)
	request.Header.Set(authHeader, token(t, secret, []string{"destination"}, []string{"update"}))
	result := httptest.NewRecorder()
	destination.Handler().ServeHTTP(result, request)
	if result.Code != 200 {
		t.Fatalf("offload=%d %s", result.Code, result.Body.String())
	}
	var heal MetricHealStats
	if err := json.Unmarshal(result.Body.Bytes(), &heal); err != nil {
		t.Fatalf("offload heal stats: %v", err)
	}
	head := httptest.NewRequest(http.MethodHead, "/metrics/destination", http.NoBody)
	head.Header.Set(authHeader, token(t, secret, []string{"*"}, []string{"read"}))
	headResult := httptest.NewRecorder()
	destination.Handler().ServeHTTP(headResult, head)
	if headResult.Code != 200 {
		t.Fatal("destination absent")
	}
}

func TestOffloadClassicWhisperIndependentOfHashring(t *testing.T) {
	secret := []byte("shared-secret")
	body := fixture(t, 7)
	authorizer := &Service{secret: secret}
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/metrics/source" {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		if err := authorizer.allowed("source", "read", r); err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
		metadata, _ := json.Marshal(MetricData{Name: "source", Size: int64(len(body))})
		w.Header().Set("X-Metric-Stat", string(metadata))
		_, _ = w.Write(body)
	}))
	defer remote.Close()
	tests := []struct {
		name  string
		nodes []Node
	}{
		{"omitted nodes", nil},
		{"different ingestion ring", []Node{{Server: "10.214.27.71", Port: 2003, Instance: "a"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, db := testService(t)
			s.secret = secret
			s.nodes = tt.nodes
			s.config.Node = "10.214.27.71"
			s.config.Hash = "jump_fnv1a"
			ringBefore := httptest.NewRecorder()
			ringRequest := httptest.NewRequest(http.MethodGet, "/hashring", http.NoBody)
			ringRequest.Header.Set(authHeader, token(t, secret, []string{"*"}, []string{"read"}))
			s.Handler().ServeHTTP(ringBefore, ringRequest)
			request := httptest.NewRequest(http.MethodPost, "/metrics/destination?fetch_offload=true&server="+remote.Listener.Addr().String()+"&metric=source", http.NoBody)
			request.Header.Set(authHeader, token(t, secret, []string{"destination"}, []string{"update"}))
			response := httptest.NewRecorder()
			s.Handler().ServeHTTP(response, request)
			if response.Code != http.StatusOK {
				t.Fatalf("offload=%d %s", response.Code, response.Body.String())
			}
			snapshot, err := db.Snapshot(context.Background(), "destination")
			if err != nil {
				t.Fatal(err)
			}
			if len(snapshot.Archives) != 1 || len(snapshot.Archives[0].Points) != 1 || snapshot.Archives[0].Points[0].Timestamp != 9990 || snapshot.Archives[0].Points[0].Value != 7 {
				t.Fatalf("classic archive was not preserved: %+v", snapshot.Archives)
			}
			ringAfter := httptest.NewRecorder()
			s.Handler().ServeHTTP(ringAfter, ringRequest)
			if ringBefore.Code != http.StatusOK || ringAfter.Code != http.StatusOK || !bytes.Equal(ringBefore.Body.Bytes(), ringAfter.Body.Bytes()) {
				t.Fatal("offload changed the hashring")
			}
		})
	}
}

func TestOffloadRejectsUnauthorizedCallerBeforeFetching(t *testing.T) {
	s, _ := testService(t)
	s.secret = []byte("shared-secret")
	var calls int32
	remote := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&calls, 1)
	}))
	defer remote.Close()
	request := httptest.NewRequest(http.MethodPost, "/metrics/destination?fetch_offload=true&server="+remote.Listener.Addr().String(), http.NoBody)
	request.Header.Set(authHeader, token(t, s.secret, []string{"destination"}, []string{"read"}))
	response := httptest.NewRecorder()
	s.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusForbidden || atomic.LoadInt32(&calls) != 0 {
		t.Fatalf("unauthorized offload: status=%d calls=%d", response.Code, atomic.LoadInt32(&calls))
	}
}

func TestOffloadPreservesSourceNotFound(t *testing.T) {
	s, db := testService(t)
	remote := httptest.NewServer(http.NotFoundHandler())
	defer remote.Close()
	request := httptest.NewRequest(http.MethodPost, "/metrics/missing?fetch_offload=true&server="+remote.Listener.Addr().String(), http.NoBody)
	response := httptest.NewRecorder()
	s.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("offload missing metric=%d %s", response.Code, response.Body.String())
	}
	if _, err := db.Metadata(context.Background(), "missing"); err == nil {
		t.Fatal("missing source published a metric")
	}
}

func TestLifecycleBindsSynchronouslyAndMutationCallbackRuns(t *testing.T) {
	s, _ := testService(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	s.config.Bind = listener.Addr().String()
	if err := s.Start(); err == nil {
		t.Fatal("Start succeeded on occupied port")
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	called := 0
	s.SetOnChange(func() { called++ })
	body := fixture(t, 2)
	request := httptest.NewRequest(http.MethodPost, "/metrics/callback", bytes.NewReader(body))
	result := httptest.NewRecorder()
	s.Handler().ServeHTTP(result, request)
	if result.Code != 200 || called != 1 {
		t.Fatalf("mutation=%d callback=%d", result.Code, called)
	}
	if err := s.Stop(); err != nil {
		t.Fatal(err)
	}
	late := httptest.NewRecorder()
	s.Handler().ServeHTTP(late, httptest.NewRequest(http.MethodGet, "/metrics/callback", http.NoBody))
	if late.Code != http.StatusServiceUnavailable {
		t.Fatalf("stopped service accepted late request: %d", late.Code)
	}
}

func TestOffloadRejectsOversizedDecodedBody(t *testing.T) {
	s, db := testService(t)
	s.config.MaxBodyBytes = 32
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		metadata, _ := json.Marshal(MetricData{Size: 33})
		w.Header().Set("X-Metric-Stat", string(metadata))
		_, _ = w.Write(bytes.Repeat([]byte{'x'}, 33))
	}))
	defer remote.Close()
	request := httptest.NewRequest(http.MethodPost, "/metrics/oversized?fetch_offload=true&server="+remote.Listener.Addr().String(), http.NoBody)
	response := httptest.NewRecorder()
	s.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("oversized offload=%d", response.Code)
	}
	if _, err := db.Metadata(context.Background(), "oversized"); err == nil {
		t.Fatal("oversized offload published metric")
	}
}

func TestPprofUsesSeparateListenerAndClosesOnStop(t *testing.T) {
	s, _ := testService(t)
	s.config.Bind = "127.0.0.1:0"
	s.config.Pprof = "127.0.0.1:0"
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	if s.server.ReadHeaderTimeout != readHeaderTimeout || s.pprofServer.ReadHeaderTimeout != readHeaderTimeout {
		t.Fatal("servers must bound header reads")
	}
	pprofAddress := s.pprofListener.Addr().String()
	response, err := http.Get("http://" + pprofAddress + "/debug/pprof/goroutine")
	if err != nil {
		t.Fatal(err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("pprof status=%d", response.StatusCode)
	}
	if err := s.Stop(); err != nil {
		t.Fatal(err)
	}

	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	blocked, _ := testService(t)
	blocked.config.Bind = "127.0.0.1:0"
	blocked.config.Pprof = occupied.Addr().String()
	if err := blocked.Start(); err == nil {
		t.Fatal("Start succeeded with an occupied pprof listener")
	}
	if blocked.server != nil || blocked.listener != nil {
		t.Fatal("failed pprof bind left the main listener running")
	}
}

func TestOffloadDoesNotFollowRedirect(t *testing.T) {
	s, db := testService(t)
	s.secret = []byte("shared-secret")
	var calls int32
	redirected := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer redirected.Close()
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, redirected.URL, http.StatusFound)
	}))
	defer remote.Close()
	request := httptest.NewRequest(http.MethodPost, "/metrics/leak?fetch_offload=true&server="+remote.Listener.Addr().String(), http.NoBody)
	request.Header.Set(authHeader, token(t, s.secret, []string{"leak"}, []string{"update"}))
	response := httptest.NewRecorder()
	s.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusBadGateway {
		t.Fatalf("offload redirect=%d", response.Code)
	}
	if atomic.LoadInt32(&calls) != 0 {
		t.Fatal("offload request followed a redirect")
	}
	if _, err := db.Metadata(context.Background(), "leak"); err == nil {
		t.Fatal("rejected offload published metric")
	}
}
