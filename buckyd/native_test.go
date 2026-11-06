package buckyd

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
