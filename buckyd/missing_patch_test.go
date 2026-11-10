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

func TestMissingOnlyAuthorizationLimitsAndCapabilities(t *testing.T) {
	s, _ := testService(t)
	s.secret = []byte("missing-secret")
	p := chunkstore.MissingPatch{Expected: chunkstore.TransferVersion{Name: "m", ID: 1, Generation: 1, Revision: 1}, PlanSHA256: fmt.Sprintf("%064x", 1), ParentProofSHA256: fmt.Sprintf("%064x", 2), ProjectionSHA256: fmt.Sprintf("%064x", 3), SourceBindingSHA256: fmt.Sprintf("%064x", 4), Nonce: fmt.Sprintf("%032x", 1), Apply: true,
		Points: []chunkstore.MissingPoint{{Archive: 0, Step: 60, Count: 100, Timestamp: 1793976780, DonorBits: "8000000000000000"}}}
	p.BatchSHA256 = chunkstore.MissingPointHash(p.Points, "requested")
	invoke := func(body []byte, op string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/v1/maintenance/missing-only", bytes.NewReader(body))
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
	for _, raw := range [][]byte{[]byte(`[]`), []byte(`{"points":[{}]}`), append(append([]byte(nil), body...), []byte(` {}`)...), bytes.Repeat([]byte("x"), (64<<10)+1)} {
		if w := invoke(raw, "read"); w.Code != 400 {
			t.Fatalf("invalid/oversized request: %d", w.Code)
		}
	}
	p.Points[0].BeforeBits = "0000000000000000"
	p.BatchSHA256 = chunkstore.MissingPointHash(p.Points, "requested")
	body, _ = json.Marshal(p)
	if w := invoke(body, "read"); w.Code != 400 {
		t.Fatal("accepted finite preimage")
	}
	r := httptest.NewRequest(http.MethodGet, "/v1/native/capabilities", nil)
	r.Header.Set(authHeader, token(t, s.secret, []string{"*"}, []string{"read"}))
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	var caps map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &caps); err != nil {
		t.Fatal(err)
	}
	if caps["AtomicMissingOnly"] != false || caps["AtomicMissingOnlyProtocol"] != float64(1) || caps["AtomicMissingOnlyMaxPoints"] != float64(128) {
		t.Fatalf("incorrect unfenced capability: %v", caps)
	}
}
