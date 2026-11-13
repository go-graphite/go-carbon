package carbonserver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	protov2 "github.com/go-graphite/protocol/carbonapi_v2_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestClosedStoreReadEndpointsUnavailable(t *testing.T) {
	db, err := store.Open("/store", store.Options{}.WithFS(vfs.NewMem()))
	if err != nil {
		t.Fatal(err)
	}
	l := NewCarbonserverListener(nil)
	l.SetMetricStore(db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for _, h := range []struct {
		name    string
		handler http.HandlerFunc
	}{
		{"render", l.renderHandler}, {"find", l.findHandler}, {"info", l.infoHandler},
	} {
		t.Run(h.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			h.handler(w, httptest.NewRequest(http.MethodGet, "/", nil))
			if w.Code != http.StatusServiceUnavailable {
				t.Fatalf("HTTP status = %d, want 503", w.Code)
			}
		})
	}
	if _, err := l.Find(context.Background(), &protov2.GlobRequest{}); status.Code(err) != codes.Unavailable {
		t.Fatalf("Find: %v", err)
	}
	if _, err := l.Info(context.Background(), &protov2.InfoRequest{}); status.Code(err) != codes.Unavailable {
		t.Fatalf("Info: %v", err)
	}
	// Render must reject before accessing the stream or query caches.
	if err := l.Render(&protov2.MultiFetchRequest{}, nil); status.Code(err) != codes.Unavailable {
		t.Fatalf("Render: %v", err)
	}
}
