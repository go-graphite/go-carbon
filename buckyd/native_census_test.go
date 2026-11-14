package buckyd

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

func censusRequest(t *testing.T, s *Service, method, path string, body any, root string) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(method, path, bytes.NewReader(raw))
	req.Header.Set(authHeader, root)
	out := httptest.NewRecorder()
	s.Handler().ServeHTTP(out, req)
	return out
}
func censusResult(t *testing.T, s *Service) map[string]any {
	t.Helper()
	out := censusRequest(t, s, http.MethodGet, "/v1/native/sources", nil, token(t, s.secret, []string{"*"}, []string{"read"}))
	if out.Code != 200 {
		t.Fatalf("census HTTP%d: %s", out.Code, out.Body)
	}
	var value map[string]any
	if err := json.Unmarshal(out.Body.Bytes(), &value); err != nil {
		t.Fatal(err)
	}
	if value["Complete"] != true || value["CreatingRegisteredBeforePin"] != true || value["IncludesClosing"] != true || value["HistoricalClosureInferred"] != false {
		t.Fatal(value)
	}
	if value["PID"] != float64(os.Getpid()) || value["PinnedBytes"] != nil || value["PinnedBytesKnown"] != false {
		t.Fatal(value)
	}
	return value
}
func TestNativeCensusTracksCreateBeforePinAndCanceledLateResult(t *testing.T) {
	s, store := newNativeTestService(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	store.SetNativePendingSnapshot(func() (map[string]struct{}, error) { close(entered); <-release; return map[string]struct{}{}, nil })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequest(http.MethodPost, "/v1/native/source", bytes.NewReader([]byte(`{}`))).WithContext(ctx)
	root := token(t, s.secret, []string{"*"}, []string{"read"})
	req.Header.Set(authHeader, root)
	out := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { defer close(done); s.Handler().ServeHTTP(out, req) }()
	t.Cleanup(func() { cancel(); once.Do(func() { close(release) }); <-done })
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("pin callback not reached")
	}
	census := censusResult(t, s)
	if census["Count"] != float64(1) {
		t.Fatal(census)
	}
	row := census["Sources"].([]any)[0].(map[string]any)
	if row["State"] != "CREATING" || row["Owner"] != "UNKNOWN" || row["AgeSeconds"].(float64) < 0 || row["PinnedBytes"] != nil {
		t.Fatal(row)
	}
	pending := censusRequest(t, s, http.MethodDelete, "/v1/native/source/"+row["ID"].(string), nil, root)
	if pending.Code != http.StatusConflict {
		t.Fatal("unfinished create falsely acknowledged released", pending.Code, pending.Body)
	}
	cancel()
	once.Do(func() { close(release) })
	<-done
	if out.Code == 200 {
		t.Fatal("canceled create published a ready pin")
	}
	if got := censusResult(t, s); got["Count"] != float64(0) {
		t.Fatal(got)
	}
}
func TestNativeCensusShutdownFencesOutstandingCreation(t *testing.T) {
	s, store := newNativeTestService(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	store.SetNativePendingSnapshot(func() (map[string]struct{}, error) { close(entered); <-release; return map[string]struct{}{}, nil })
	done := make(chan struct{})
	var createErr error
	go func() {
		defer close(done)
		_, _, createErr = s.openNativeSource(context.Background(), store.PinNativeSource)
	}()
	t.Cleanup(func() { once.Do(func() { close(release) }); <-done })
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("pin callback not reached")
	}
	s.closeNativeSources()
	before := censusResult(t, s)
	if before["ServiceClosing"] != true || before["Count"] != float64(1) {
		t.Fatal(before)
	}
	once.Do(func() { close(release) })
	<-done
	if createErr == nil {
		t.Fatal("late pin published after shutdown")
	}
	if after := censusResult(t, s); after["Count"] != float64(0) {
		t.Fatal(after)
	}
}

type censusBlockedWriter struct {
	entered, release chan struct{}
	once             sync.Once
}

func (w *censusBlockedWriter) Write(b []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return len(b), nil
}
func TestNativeCensusTracksClosingUntilActualSnapshotRelease(t *testing.T) {
	s, store := newNativeTestService(t)
	cfg := chunkstore.MetricConfig{Name: "census", AggregationMethod: chunkstore.Sum, Retentions: []chunkstore.Retention{{Step: 1, Count: 10000}}}
	if _, err := store.Create(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	id, session, err := s.openNativeSource(context.Background(), store.PinNativeSource)
	if err != nil {
		t.Fatal(err)
	}
	ready := censusResult(t, s)
	if ready["Count"] != float64(1) || ready["Sources"].([]any)[0].(map[string]any)["State"] != "READY" {
		t.Fatal(ready)
	}
	sink := &censusBlockedWriter{entered: make(chan struct{}), release: make(chan struct{})}
	writeDone := make(chan struct{})
	closeDone := make(chan struct{})
	var once sync.Once
	go func() {
		defer close(writeDone)
		_ = session.source.Write(context.Background(), []string{"census"}, sink)
	}()
	t.Cleanup(func() { once.Do(func() { close(sink.release) }); <-writeDone; <-closeDone })
	select {
	case <-sink.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("writer not reached")
	}
	go func() { defer close(closeDone); _ = s.closeNativeSource(id) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		census := censusResult(t, s)
		if census["Count"] == float64(1) && census["Sources"].([]any)[0].(map[string]any)["State"] == "CLOSING" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("closing resource disappeared", census)
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-closeDone:
		t.Fatal("close acknowledged while snapshot write held its lock")
	default:
	}
	if err := s.closeNativeSource(id); err == nil {
		t.Fatal("concurrent close falsely acknowledged released")
	}
	once.Do(func() { close(sink.release) })
	<-writeDone
	<-closeDone
	if got := censusResult(t, s); got["Count"] != float64(0) {
		t.Fatal(got)
	}
}
func TestNativeCensusRequiresRootReadGrant(t *testing.T) {
	s, _ := newNativeTestService(t)
	for _, signed := range []string{token(t, s.secret, []string{"metric.*"}, []string{"read"}), token(t, s.secret, []string{"*"}, []string{"update"})} {
		out := censusRequest(t, s, http.MethodGet, "/v1/native/sources", nil, signed)
		if out.Code != http.StatusForbidden {
			t.Fatal(out.Code, out.Body)
		}
	}
}

func TestNativeCensusCanceledRequestDoesNotStartPin(t *testing.T) {
	s, _ := newNativeTestService(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	_, _, err := s.openNativeSource(ctx, func() (*chunkstore.NativeSource, error) { called = true; return nil, nil })
	if err == nil || called {
		t.Fatal("canceled request started snapshot creation", called, err)
	}
	if got := censusResult(t, s); got["Count"] != float64(0) {
		t.Fatal(got)
	}
}
