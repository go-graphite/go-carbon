package buckyd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func importTestMetric(t *testing.T, h http.Handler, name string) {
	t.Helper()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/metrics/"+name, bytes.NewReader(fixture(t, 1))))
	if w.Code != http.StatusOK {
		t.Fatalf("import %s = %d %s", name, w.Code, w.Body)
	}
}

func metricStatus(h http.Handler, name string) int {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodHead, "/metrics/"+name, http.NoBody))
	return w.Code
}

func postDeleteBatch(h http.Handler, auth string, body any) *httptest.ResponseRecorder {
	raw, ok := body.(string)
	if !ok {
		b, _ := json.Marshal(body)
		raw = string(b)
	}
	r := httptest.NewRequest(http.MethodPost, deleteBatchEndpoint, strings.NewReader(raw))
	if auth != "" {
		r.Header.Set(authHeader, auth)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

// One request deletes every allowed, unchanged metric and reports each name
// with the status a single DELETE would return.
func TestDeleteBatchReportsEachMetric(t *testing.T) {
	s, db := testService(t)
	h := s.Handler()
	for _, name := range []string{"team.a", "team.b", "team.c", "other.x"} {
		importTestMetric(t, h, name)
	}
	changes := 0
	s.SetOnChange(func() { changes++ })
	secret := []byte("test-secret")
	s.secret = secret
	epoch := db.ReadCacheEpoch()

	w := postDeleteBatch(h, token(t, secret, []string{"team.*"}, []string{"delete"}), DeleteBatchRequest{Metrics: []DeleteBatchMetric{
		{Name: "team.a"}, {Name: "team.b"}, {Name: "team.c", Version: "0:0:0"}, {Name: "team.missing"}, {Name: "other.x"},
	}})
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
	var got DeleteBatchResponse
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]int{"team.a": 200, "team.b": 200, "team.c": 409, "team.missing": 404, "other.x": 403}
	order := []string{"team.a", "team.b", "team.c", "team.missing", "other.x"}
	if len(got.Results) != len(order) {
		t.Fatalf("results %+v", got.Results)
	}
	for i, result := range got.Results {
		if result.Name != order[i] || result.Status != want[result.Name] || (result.Status != 200) != (result.Error != "") {
			t.Errorf("result %d = %+v, want %s with status %d", i, result, order[i], want[order[i]])
		}
	}
	s.secret = nil
	for name, status := range map[string]int{"team.a": 404, "team.b": 404, "team.c": 200, "other.x": 200} {
		if got := metricStatus(h, name); got != status {
			t.Errorf("HEAD %s = %d, want %d", name, got, status)
		}
	}
	if changes != 1 {
		t.Errorf("change callback ran %d times, want once", changes)
	}
	if db.ReadCacheEpoch() == epoch {
		t.Error("deleting metrics kept cached reads")
	}
}

func TestDeleteBatchRejectsWholeRequest(t *testing.T) {
	s, _ := testService(t)
	h := s.Handler()
	importTestMetric(t, h, "team.a")
	tooMany := DeleteBatchRequest{}
	for i := range MaxDeleteBatch + 1 {
		tooMany.Metrics = append(tooMany.Metrics, DeleteBatchMetric{Name: fmt.Sprintf("team.m%d", i)})
	}
	one := DeleteBatchRequest{Metrics: []DeleteBatchMetric{{Name: "team.a"}}}
	for _, tc := range []struct {
		name  string
		body  any
		setup func()
		want  int
	}{
		{name: "empty", body: DeleteBatchRequest{}, want: http.StatusBadRequest},
		{name: "too many", body: tooMany, want: http.StatusBadRequest},
		{name: "duplicate", body: DeleteBatchRequest{Metrics: []DeleteBatchMetric{{Name: "team.a"}, {Name: "team.a"}}}, want: http.StatusBadRequest},
		{name: "path in name", body: DeleteBatchRequest{Metrics: []DeleteBatchMetric{{Name: "team/a"}}}, want: http.StatusBadRequest},
		{name: "not JSON", body: "team.a", want: http.StatusBadRequest},
		{name: "invalid token", body: one, setup: func() { s.secret = []byte("test-secret") }, want: http.StatusForbidden},
		{name: "transfer cap", body: one, setup: func() { s.transferLimit.Store(0) }, want: http.StatusTooManyRequests},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.setup != nil {
				tc.setup()
			}
			w := postDeleteBatch(h, "", tc.body)
			s.secret = nil
			s.SetMaxTransfers(0)
			if w.Code != tc.want {
				t.Fatalf("status %d, want %d: %s", w.Code, tc.want, w.Body)
			}
			if got := metricStatus(h, "team.a"); got != http.StatusOK {
				t.Fatalf("a rejected request deleted team.a (HEAD %d)", got)
			}
		})
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, deleteBatchEndpoint, http.NoBody))
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET status %d", w.Code)
	}
}
