package buckyd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

func newNativeTestService(t *testing.T) (*Service, *chunkstore.Store) {
	t.Helper()
	db, err := chunkstore.Open("/db", chunkstore.Options{ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20, Now: func() time.Time { return time.Unix(10000, 0) }}.WithFS(vfs.NewMem()))
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(Config{TmpDir: t.TempDir(), MaxTransfers: 1, MaxBodyBytes: 1024, TransferSources: []string{"127.0.0.1"}}, db)
	if err != nil {
		t.Fatal(err)
	}
	s.secret = []byte("native-test-secret")
	t.Cleanup(func() {
		s.closeNativeSources()
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return s, db
}
func TestNativeHTTPBypassesLegacyLimitsAndRecoversReceipt(t *testing.T) {
	ctx := context.Background()
	source, donor := newNativeTestService(t)
	destination, target := newNativeTestService(t)
	config := chunkstore.MetricConfig{Name: "native.new", AggregationMethod: chunkstore.Sum, Retentions: []chunkstore.Retention{{Step: 1, Count: 10000}}}
	for _, name := range []string{"native.new", "native.existing"} {
		config.Name = name
		if _, err := donor.Create(ctx, config); err != nil {
			t.Fatal(err)
		}
		var points []chunkstore.Point
		for ts := int64(5000); ts < 9990; ts++ {
			points = append(points, chunkstore.Point{Timestamp: ts, Value: float64(ts % 71)})
		}
		if err := donor.UpdateMany(ctx, name, points); err != nil {
			t.Fatal(err)
		}
	}
	config.Name = "native.existing"
	if _, err := target.Create(ctx, config); err != nil {
		t.Fatal(err)
	}
	if err := target.UpdateMany(ctx, config.Name, []chunkstore.Point{{Timestamp: 9990, Value: 42}}); err != nil {
		t.Fatal(err)
	}
	for source.acquireTransfer() {
		defer source.releaseTransfer()
	}
	for destination.acquireTransfer() {
		defer destination.releaseTransfer()
	}
	remote := httptest.NewServer(source.Handler())
	defer remote.Close()
	root := token(t, source.secret, []string{"*"}, []string{"*"})
	invoke := func(service *Service, method, path string, body any, signed string) *httptest.ResponseRecorder {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		request := httptest.NewRequest(method, path, bytes.NewReader(data))
		request.Header.Set(authHeader, signed)
		response := httptest.NewRecorder()
		service.Handler().ServeHTTP(response, request)
		return response
	}
	response := invoke(source, http.MethodPost, "/v1/native/source", struct{}{}, root)
	if response.Code != 200 {
		t.Fatal(response.Code, response.Body.String())
	}
	var session struct{ ID string }
	if err := json.Unmarshal(response.Body.Bytes(), &session); err != nil {
		t.Fatal(err)
	}
	if err := donor.UpdateMany(ctx, "native.new", []chunkstore.Point{{Timestamp: 9999, Value: 777}}); err != nil {
		t.Fatal(err)
	}
	rangeRequest := chunkstore.NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: strings.Repeat("3", 64), Source: remote.URL, End: 3, Names: []string{"native.new", "native.existing", "native.missing"}}
	pull := nativePullRequest{Range: rangeRequest, Session: session.ID}
	for _, signed := range []string{token(t, source.secret, []string{"native.*"}, []string{"*"}), token(t, source.secret, []string{"*"}, []string{"update"})} {
		if r := invoke(destination, http.MethodPost, "/v1/native/pull", pull, signed); r.Code != 403 {
			t.Fatal("scoped token accepted", r.Code)
		}
	}
	response = invoke(destination, http.MethodPost, "/v1/native/pull", pull, root)
	if response.Code != 200 {
		t.Fatal(response.Code, response.Body.String())
	}
	var receipt chunkstore.NativeReceipt
	if err := json.Unmarshal(response.Body.Bytes(), &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.CopiedCount != 1 || receipt.DeferredCount != 2 || !receipt.IsCopied(0) || receipt.IsCopied(1) || receipt.IsCopied(2) || receipt.Bytes <= destination.config.MaxBodyBytes {
		t.Fatal("native limits/bitmap", receipt)
	}
	snapshot, err := target.Snapshot(ctx, "native.new")
	if err != nil {
		t.Fatal(err)
	}
	for _, point := range snapshot.Archives[0].Points {
		if point.Timestamp == 9999 {
			t.Fatal("pinned snapshot included a later source write")
		}
	}
	existing, err := target.Snapshot(ctx, "native.existing")
	if err != nil {
		t.Fatal(err)
	}
	if len(existing.Archives[0].Points) != 1 || existing.Archives[0].Points[0].Value != 42 {
		t.Fatal("existing target changed")
	}
	if _, err := target.Metadata(ctx, "native.missing"); err == nil {
		t.Fatal("source absence became a target metric")
	}
	if err := target.UpdateMany(ctx, "native.new", []chunkstore.Point{{Timestamp: 9999, Value: 123}}); err != nil {
		t.Fatal(err)
	}
	pull.Session = "expired"
	response = invoke(destination, http.MethodPost, "/v1/native/pull", pull, root)
	if response.Code != 200 {
		t.Fatal("receipt retry tried source", response.Code, response.Body.String())
	}
	var again chunkstore.NativeReceipt
	_ = json.Unmarshal(response.Body.Bytes(), &again)
	if again.Digest != receipt.Digest {
		t.Fatal("receipt changed")
	}
	response = invoke(source, http.MethodDelete, "/v1/native/source/"+session.ID, nil, root)
	if response.Code != 200 {
		t.Fatal(response.Code)
	}
	response = invoke(source, http.MethodPost, "/v1/native/export", nativeExportRequest{Session: session.ID, Names: []string{"native.new"}}, root)
	if response.Code != http.StatusGone {
		t.Fatal("released pin retained", response.Code)
	}
}

func TestNativePullCancellationClosesDonorAndReceiver(t *testing.T) {
	for _, mode := range []string{"client", "service-stop", "dump-stop"} {
		t.Run(mode, func(t *testing.T) { testNativePullCancellation(t, mode) })
	}
}
func testNativePullCancellation(t *testing.T, mode string) {
	destination, target := newNativeTestService(t)
	started, stopped := make(chan struct{}), make(chan struct{})
	donor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Store-Format", strconv.Itoa(target.TransferFormat()))
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
		close(stopped)
	}))
	defer donor.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request := nativePullRequest{Range: chunkstore.NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: strings.Repeat("3", 64), Source: donor.URL, End: 1, Names: []string{"cancelled"}, Combine: true}, Session: "pin"}
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, "/v1/native/pull", bytes.NewReader(body)).WithContext(ctx)
	r.Header.Set(authHeader, token(t, destination.secret, []string{"*"}, []string{"*"}))
	done := make(chan struct{})
	go func() { defer close(done); destination.Handler().ServeHTTP(httptest.NewRecorder(), r) }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("donor not reached")
	}
	switch mode {
	case "client":
		cancel()
	case "service-stop":
		if err := destination.Stop(); err != nil {
			t.Fatal(err)
		}
	case "dump-stop":
		target.CancelNativeTransfers()
	}

	for _, ch := range []<-chan struct{}{stopped, done} {
		select {
		case <-ch:
		case <-time.After(3 * time.Second):
			t.Fatal("native work outlived cancelled client")
		}
	}
	if _, err := target.Metadata(context.Background(), "cancelled"); !errors.Is(err, chunkstore.ErrNotFound) {
		t.Fatal("cancelled input published", err)
	}
}

func TestNativePullRejectsOversizedRangeBeforeDonor(t *testing.T) {
	destination, _ := newNativeTestService(t)
	names := make([]string, 10000)
	for i := range names {
		names[i] = fmt.Sprintf("old.%d", i)
	}
	body, err := json.Marshal(nativePullRequest{Range: chunkstore.NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: strings.Repeat("3", 64), Source: "donor", Start: 0, End: int64(len(names)), Names: names}})
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, "/v1/native/pull", bytes.NewReader(body))
	r.Header.Set(authHeader, token(t, destination.secret, []string{"*"}, []string{"*"}))
	response := httptest.NewRecorder()
	destination.Handler().ServeHTTP(response, r)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatal(response.Code, response.Body.String())
	}
}
