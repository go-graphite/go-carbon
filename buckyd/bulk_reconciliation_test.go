package buckyd

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

func reconciliationRequest(t *testing.T, s *Service, method, path string, body any, signed string) *httptest.ResponseRecorder {
	t.Helper()
	data, ok := body.([]byte)
	if !ok {
		var err error
		data, err = json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
	}
	r := httptest.NewRequest(method, path, bytes.NewReader(data))
	r.Header.Set(authHeader, signed)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	return w
}

func TestBulkReconciliationAuthorization(t *testing.T) {
	s, _ := testService(t)
	s.secret = []byte("reconciliation-secret")
	for _, endpoint := range []struct{ path, operation string }{
		{"stage-copy", "update"}, {"check-staged", "read"}, {"pending-delete", "read"},
	} {
		t.Run(endpoint.path, func(t *testing.T) {
			for _, signed := range []string{"", token(t, s.secret, []string{"plan.*"}, []string{"*"})} {
				w := reconciliationRequest(t, s, http.MethodPost, "/v1/transfer/"+endpoint.path, nil, signed)
				if w.Code != http.StatusForbidden {
					t.Fatalf("missing or scoped token accepted: %d %s", w.Code, w.Body.String())
				}
			}
			for _, operation := range []string{"read", "update", "delete"} {
				if operation == endpoint.operation {
					continue
				}
				signed := token(t, s.secret, []string{"*"}, []string{operation})
				w := reconciliationRequest(t, s, http.MethodPost, "/v1/transfer/"+endpoint.path, nil, signed)
				if w.Code != http.StatusForbidden {
					t.Fatalf("%s token accepted: %d %s", operation, w.Code, w.Body.String())
				}
			}
		})
	}
}

func TestBulkReconciliationRoundTrip(t *testing.T) {
	ctx := context.Background()
	source, db := testService(t)
	target, copied := testService(t)
	source.secret = []byte("reconciliation-secret")
	target.secret = source.secret
	read := token(t, source.secret, []string{"*"}, []string{"read"})
	update := token(t, source.secret, []string{"*"}, []string{"update"})
	remove := token(t, source.secret, []string{"*"}, []string{"delete"})
	request := func(s *Service, path string, body any, signed string, want int) *httptest.ResponseRecorder {
		t.Helper()
		w := reconciliationRequest(t, s, http.MethodPost, "/v1/transfer/"+path, body, signed)
		if w.Code != want {
			t.Fatalf("%s: %d, want %d: %s", path, w.Code, want, w.Body.String())
		}
		return w
	}
	status := request(source, "status", nil, read, http.StatusOK)
	var capabilities struct{ PendingDeleteBatches, StagedCopyVerification bool }
	if err := json.Unmarshal(status.Body.Bytes(), &capabilities); err != nil {
		t.Fatal(err)
	}
	if !capabilities.PendingDeleteBatches || !capabilities.StagedCopyVerification {
		t.Fatal("reconciliation capabilities missing")
	}
	names := []string{"plan.z", "plan.a", "plan.m"} // Catalog order differs from ID order.
	for _, name := range names {
		config := chunkstore.MetricConfig{Name: name, Retentions: []chunkstore.Retention{{Step: 10, Count: 12}}, AggregationMethod: chunkstore.Sum}
		if _, err := db.Create(ctx, config); err != nil {
			t.Fatal(err)
		}
		if err := db.UpdateManyForArchive(ctx, name, []chunkstore.Point{{Timestamp: 9990, Value: 7}}, 120); err != nil {
			t.Fatal(err)
		}
	}
	wire := request(source, "export?wire=2", names, read, http.StatusOK)
	imported := request(target, "import", wire.Body.Bytes(), update, http.StatusOK)
	var proofs []chunkstore.TransferProof
	if err := json.Unmarshal(imported.Body.Bytes(), &proofs); err != nil {
		t.Fatal(err)
	}
	if len(proofs) != len(names) {
		t.Fatal("missing transfer proofs", proofs)
	}
	slices.Reverse(proofs)
	request(target, "verify", proofs, read, http.StatusOK)
	versions := make(map[string]chunkstore.TransferVersion)
	for _, proof := range proofs {
		versions[proof.Source.Name] = proof.Source
		snapshot, err := copied.Snapshot(ctx, proof.Source.Name)
		if err != nil {
			t.Fatal(err)
		}
		if len(snapshot.Archives[0].Points) != 1 || snapshot.Archives[0].Points[0].Value != 7 {
			t.Fatal("copied history differs", snapshot)
		}
	}
	operation := strings.Repeat("1", 64)
	request(source, "pending-delete", map[string]any{"Operation": operation, "Names": make([]string, chunkstore.MaxTransferMetrics+1)}, read, http.StatusRequestEntityTooLarge)
	stage := func(endpoint, name string, signed string) {
		t.Helper()
		request(source, endpoint, map[string]any{"Operation": operation, "Versions": []chunkstore.TransferVersion{versions[name]}}, signed, http.StatusOK)
	}
	pending := func(batch []string, want []string) {
		t.Helper()
		w := request(source, "pending-delete", map[string]any{"Operation": operation, "Names": batch}, read, http.StatusOK)
		var got []string
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		slices.Sort(got)
		if !slices.Equal(got, want) {
			t.Fatalf("pending %v: got %v, want %v", batch, got, want)
		}
	}
	stage("stage-copy", "plan.z", update)
	pending([]string{"plan.z", "missing"}, nil)
	pending(nil, []string{"plan.a", "plan.m"}) // Legacy catalog scan remains available.
	check := map[string]string{"Operation": operation}
	request(source, "check-staged", check, read, http.StatusConflict)
	stage("stage-copy", "plan.a", update)
	stage("stage-delete", "plan.m", remove)
	verified := request(source, "check-staged", check, read, http.StatusOK)
	var count struct{ Verified int }
	if err := json.Unmarshal(verified.Body.Bytes(), &count); err != nil || count.Verified != len(names) {
		t.Fatal("staged verification count", count, err)
	}
	request(source, "check-staged", map[string]string{"Operation": strings.Repeat("2", 64)}, read, http.StatusConflict)
	queued := true
	db.SetTransferPending(func(name string) bool { return queued && name == "plan.a" }, func() bool { return queued })
	pending([]string{"plan.z", "plan.a", "plan.m"}, []string{"plan.a"})
	request(source, "check-staged", check, read, http.StatusConflict)
	queued = false
	if err := db.UpdateManyForArchive(ctx, "plan.a", []chunkstore.Point{{Timestamp: 9980, Value: 9}}, 120); err != nil {
		t.Fatal(err)
	}
	pending([]string{"plan.z", "plan.m", "plan.a"}, []string{"plan.a"})
	request(source, "check-staged", check, read, http.StatusConflict)
	retire := map[string]any{"Operation": operation, "Confirm": true}
	request(source, "retire", retire, remove, http.StatusConflict)
	updated := request(source, "export?wire=2", []string{"plan.a"}, read, http.StatusOK)
	updatedImport := request(target, "import", updated.Body.Bytes(), update, http.StatusOK)
	if err := json.Unmarshal(updatedImport.Body.Bytes(), &proofs); err != nil || len(proofs) != 1 {
		t.Fatal("updated proofs", proofs, err)
	}
	request(target, "verify", proofs, read, http.StatusOK)
	versions["plan.a"] = proofs[0].Source
	stage("stage-copy", "plan.a", update)
	request(source, "check-staged", check, read, http.StatusOK)
	for _, name := range names {
		if _, err := db.Snapshot(ctx, name); err != nil {
			t.Fatal("copy verification deleted source history", err)
		}
	}
	request(source, "retire", retire, remove, http.StatusOK)
	pending(names, nil) // A resumed batch can include already retired names.
	if remaining, err := db.List(ctx, ""); err != nil || len(remaining) != 0 {
		t.Fatal("retirement left catalog entries", remaining, err)
	}
}
