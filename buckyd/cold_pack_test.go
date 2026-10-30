package buckyd

import (
	"bytes"
	"context"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestColdPackTransferNegotiation(t *testing.T) {
	db, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: &whisperio.ContinuationCodec{}, Now: func() time.Time { return time.Unix(1700000000, 0) }, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.StopBackground()
	cfg := store.MetricConfig{Name: "transfer.cold", AggregationMethod: store.Sum, Retentions: []store.Retention{{Step: 60, Count: 1440}}}
	if _, err := db.Create(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	svc, err := New(Config{TmpDir: t.TempDir(), MaxBodyBytes: 1 << 20}, db)
	if err != nil {
		t.Fatal(err)
	}

	svc.SetTransferFlush(func(context.Context, []string) error {
		return db.ConfigureColdPack(store.ColdPackOptions{Mode: "exact"})
	})
	// Admit on v7, then upgrade while the pending cache is flushed.
	req := httptest.NewRequest(http.MethodPost, "/v1/transfer/export?store_format=7", bytes.NewBufferString(`["transfer.cold"]`))
	out := httptest.NewRecorder()
	svc.Handler().ServeHTTP(out, req)
	if out.Code != 409 {
		t.Fatalf("source upgraded during export but old peer received %d", out.Code)
	}
	svc.SetTransferFlush(nil)
	for _, query := range []string{"", "?store_format=7", "?store_format=9&wire=2"} {
		r := httptest.NewRequest(http.MethodPost, "/v1/transfer/export"+query, bytes.NewBufferString(`["transfer.cold"]`))
		w := httptest.NewRecorder()
		svc.Handler().ServeHTTP(w, r)
		want := 409
		if query == "?store_format=9&wire=2" {
			want = 200
		}
		if w.Code != want {
			t.Fatalf("%q: %d %s", query, w.Code, w.Body.String())
		}
	}
}
