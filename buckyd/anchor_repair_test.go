package buckyd

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
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
