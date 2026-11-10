package buckyd

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestArchiveAnchorMaintenanceAuthorizationAndLimits(t *testing.T) {
	s, _ := testService(t)
	secret := []byte("anchor-secret")
	s.secret = secret
	invoke := func(body, op string) int {
		req := httptest.NewRequest(http.MethodPost, "/v1/maintenance/archive-anchors", bytes.NewBufferString(body))
		req.Header.Set(authHeader, token(t, secret, []string{"*"}, []string{op}))
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, req)
		return w.Code
	}
	if got := invoke(`{"names":["m"],"apply":true}`, "read"); got != 403 {
		t.Fatalf("read token applied: %d", got)
	}
	if got := invoke(`{"names":["m"]}`, "update"); got != 403 {
		t.Fatalf("dry-run needs read: %d", got)
	}
	if got := invoke(`{"names":[]}`, "read"); got != 400 {
		t.Fatalf("empty pass accepted: %d", got)
	}
	if got := invoke(`{"names":["m"],"unknown":1}`, "read"); got != 400 {
		t.Fatalf("unknown field accepted: %d", got)
	}
	if got := invoke(`{"names":["m"]}`, "read"); got != 200 {
		t.Fatalf("dry-run rejected: %d", got)
	}
}

// A complete maintenance request must be admitted even when copies consume
// every MaxTransfers slot; it must never invoke the export/cache-flush hook.
func TestArchiveAnchorAdmissionIndependentOfCopyPool(t *testing.T) {
	s, _ := testService(t)
	secret := []byte("anchor-secret")
	s.secret = secret
	for s.acquireTransfer() {
		defer s.releaseTransfer()
	}
	s.SetTransferFlush(func(context.Context, []string) error { t.Fatal("maintenance flushed cache input"); return nil })
	req := httptest.NewRequest(http.MethodPost, "/v1/maintenance/archive-anchors", bytes.NewBufferString(`{"names":["m"]}`))
	req.Header.Set(authHeader, token(t, secret, []string{"*"}, []string{"read"}))
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("full copy pool starved maintenance: %d %s", w.Code, w.Body)
	}
	if len(s.maintenance) != 0 {
		t.Fatal("maintenance slot leaked")
	}
}

type heldMaintenanceReply struct {
	*httptest.ResponseRecorder
	started chan<- struct{}
	release <-chan struct{}
}

func (w heldMaintenanceReply) Write(data []byte) (int, error) {
	w.started <- struct{}{}
	<-w.release
	return w.ResponseRecorder.Write(data)
}

func TestArchiveAnchorMaintenanceConcurrencyIsBoundedAndReleases(t *testing.T) {
	s, _ := testService(t)
	secret := []byte("anchor-secret")
	s.secret = secret
	handler := s.Handler()
	invoke := func(writer http.ResponseWriter, op string) {
		req := httptest.NewRequest(http.MethodPost, "/v1/maintenance/archive-anchors", bytes.NewBufferString(`{"names":["m"]}`))
		req.Header.Set(authHeader, token(t, secret, []string{"*"}, []string{op}))
		handler.ServeHTTP(writer, req)
	}
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			invoke(heldMaintenanceReply{httptest.NewRecorder(), started, release}, "read")
		}()
	}
	for range 2 {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("two maintenance requests were not admitted")
		}
	}
	rejected := httptest.NewRecorder()
	invoke(rejected, "read")
	if rejected.Code != 429 {
		t.Fatalf("third maintenance request admitted: %d", rejected.Code)
	}
	unauthorized := httptest.NewRecorder()
	invoke(unauthorized, "update")
	if unauthorized.Code != 403 {
		t.Fatal("admission changed authorization")
	}
	if !s.acquireTransfer() {
		t.Fatal("maintenance consumed copy admission")
	}
	s.releaseTransfer()
	unblock()
	wg.Wait()
	next := httptest.NewRecorder()
	invoke(next, "read")
	if next.Code != 200 || len(s.maintenance) != 0 {
		t.Fatalf("maintenance capacity was not released: %d len=%d", next.Code, len(s.maintenance))
	}
}
