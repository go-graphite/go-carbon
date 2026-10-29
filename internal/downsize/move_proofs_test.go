package downsize

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestTransferProofNegotiationAndDowngrade(t *testing.T) {
	proofs := []chunkstore.TransferProof{{Source: chunkstore.TransferVersion{Name: "metric"}, Target: chunkstore.TransferVersion{Name: "metric"}, Digest: "digest"}}
	for _, binary := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "binary"}[binary], func(t *testing.T) {
			var downgraded atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/transfer/pull" {
					if r.Header.Get("Accept") != chunkstore.TransferProofContentType {
						t.Error("proof negotiation missing")
					}
					if binary {
						data, err := chunkstore.EncodeTransferProofs(proofs)
						if err != nil {
							t.Error(err)
						}
						w.Header().Set("Content-Type", chunkstore.TransferProofContentType)
						_, _ = w.Write(data)
					} else {
						_ = json.NewEncoder(w).Encode(proofs)
					}
					return
				}
				var got []chunkstore.TransferProof
				var err error
				if r.Header.Get("Content-Type") == chunkstore.TransferProofContentType {
					if !binary || downgraded.Load() {
						http.Error(w, "JSON required", 415)
						return
					}
					got, err = chunkstore.ReadTransferProofs(r.Body)
				} else {
					err = json.NewDecoder(r.Body).Decode(&got)
				}
				if err != nil || !reflect.DeepEqual(got, proofs) {
					t.Error("proof fields changed", got, err)
				}
				_ = json.NewEncoder(w).Encode(map[string]int{"verified": len(got)})
			}))
			defer server.Close()
			m := &mover{client: server.Client()}
			var got []chunkstore.TransferProof
			if _, err := m.requestOnce(context.Background(), server.URL, "/v1/transfer/pull", []string{"metric"}, &got); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, proofs) {
				t.Fatal("pull changed proof", got)
			}
			for _, restart := range []bool{false, false, true} {
				if restart {
					m = &mover{client: server.Client()}
				}
				if _, err := m.requestOnce(context.Background(), server.URL, "/v1/transfer/verify", got, &map[string]int{}); err != nil {
					t.Fatal(err)
				}
				downgraded.Store(true)
			}
		})
	}
}
