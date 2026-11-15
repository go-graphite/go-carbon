package downsize

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestSyncMutationProofNegotiation(t *testing.T) {
	for _, mode := range []string{"", "sum-v1", "proof-v1"} {
		t.Run(mode, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request struct {
					Mode  string
					Names []string
				}
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
				}
				if request.Mode != "proof-v1" {
					t.Errorf("mode %s", request.Mode)
				}
				_ = json.NewEncoder(w).Encode([]map[string]any{{"Name": "a", "Mode": mode, "Proof": "token"}, {"Name": "missing", "Mode": mode, "Proof": "absence"}})
			}))
			defer server.Close()
			b := newSyncLiveBackend(syncLiveOptions{})
			got, err := b.Proofs(context.Background(), server.URL, []string{"a", "missing"})
			if mode == "proof-v1" {
				if err != nil || !reflect.DeepEqual(got, []string{"token", "absence"}) {
					t.Fatalf("proof=%v %v", got, err)
				}
			} else if !errors.Is(err, errSyncNoProof) {
				t.Fatalf("old endpoint authorized proof: %v %v", got, err)
			}
		})
	}
}
