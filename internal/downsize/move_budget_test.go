package downsize

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestSingleMetricWaitsForAdaptiveByteBudget(t *testing.T) {
	const maximum = 4 << 20
	var tune *tuner
	var budgets []int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/transfer/verify" {
			_, _ = w.Write([]byte(`{"verified":1}`))
			return
		}
		var request struct {
			Names    []string
			MaxBytes int64
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		budgets = append(budgets, request.MaxBytes)
		if request.MaxBytes < maximum {
			// Model the normal healthy governor ramp after each failed export.
			tune.mu.Lock()
			for _, host := range tune.hosts {
				host.BatchBytes *= 2
			}
			close(tune.changed)
			tune.changed = make(chan struct{})
			tune.mu.Unlock()
			http.Error(w, "single metric exceeds current budget", 413)
			return
		}
		w.Header().Set("X-Transfer-Bytes", "3000000")
		_ = json.NewEncoder(w).Encode([]chunkstore.TransferProof{{Source: chunkstore.TransferVersion{Name: "large"}, Target: chunkstore.TransferVersion{Name: "large"}, Digest: "digest"}})
	}))
	defer server.Close()
	tune = newTuner([]string{"source", server.URL}, capacity{1, 1, maximum, 1}, true, 0, 0)
	j, err := openJournal(filepath.Join(t.TempDir(), "state"), "plan")
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	m := mover{client: server.Client(), tuner: tune, journal: j}
	if err := m.execute(context.Background(), task{Stream: "stream", End: 1, Source: "source", Target: server.URL, Names: []string{"large"}}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(budgets, []int64{1 << 20, 2 << 20, 4 << 20}) {
		t.Fatal("unexpected retry budgets", budgets)
	}
}

func TestSingleMetricBudgetWaitHonorsCeilingAndCancellation(t *testing.T) {
	for _, serverLimit := range []int64{0, 2 << 20} {
		tune := newTuner([]string{"source", "target"}, capacity{1, 1, 4 << 20, 1}, true, 0, 0)
		tune.hosts["source"].storePrevious = &storageSample{MaxBodyBytes: serverLimit}
		ceiling := int64(4 << 20)
		if serverLimit > 0 {
			ceiling = serverLimit
		}
		if larger, err := tune.waitLargerBatch(context.Background(), "source", "target", ceiling); larger || err != nil {
			t.Fatal("exceeded configured ceiling", larger, err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if larger, err := tune.waitLargerBatch(ctx, "source", "target", 1<<20); larger || !errors.Is(err, context.Canceled) {
			t.Fatal("ignored cancellation", larger, err)
		}
	}
}
