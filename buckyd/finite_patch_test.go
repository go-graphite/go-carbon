package buckyd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestFinitePointAuthorizationLimitsAndCapabilities(t *testing.T) {
	s, _ := testService(t)
	s.secret = []byte("finite-secret")
	p := chunkstore.FinitePatch{Expected: chunkstore.TransferVersion{Name: "m", ID: 1, Generation: 1, Revision: 1}, Step: 60, Timestamp: 1793976780, BeforeBits: "4166332100000000", DonorBits: "416632d5c0000000", PlanSHA256: fmt.Sprintf("%064x", 1), Nonce: fmt.Sprintf("%032x", 1), Apply: true}
	invoke := func(body []byte, op string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/v1/maintenance/finite-point", bytes.NewReader(body))
		r.Header.Set(authHeader, token(t, s.secret, []string{"*"}, []string{op}))
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		return w
	}
	body, _ := json.Marshal(p)
	if w := invoke(body, "read"); w.Code != 403 {
		t.Fatalf("read token applied: %d %s", w.Code, w.Body)
	}
	p.Apply = false
	body, _ = json.Marshal(p)
	if w := invoke(body, "update"); w.Code != 403 {
		t.Fatal("dry-run accepted without read authority")
	}
	for _, body := range [][]byte{[]byte(`[]`), []byte(`{"points":[{},{}]}`), append(append([]byte(nil), body...), []byte(` {}`)...), bytes.Repeat([]byte("x"), 8193)} {
		if w := invoke(body, "read"); w.Code != 400 {
			t.Fatalf("invalid/cap>1 request: %d", w.Code)
		}
	}
	request := httptest.NewRequest(http.MethodGet, "/v1/native/capabilities", nil)
	request.Header.Set(authHeader, token(t, s.secret, []string{"*"}, []string{"read"}))
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, request)
	var caps map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &caps); err != nil {
		t.Fatal(err)
	}
	if caps["AtomicFinitePoint"] != false {
		t.Fatal("advertised apply without live cache fence")
	}
}
