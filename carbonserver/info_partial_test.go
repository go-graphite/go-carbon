package carbonserver

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
)

func TestInfoPartialMetadata(t *testing.T) {
	t.Parallel()
	const now = 1_700_000_000
	s, err := store.Open(t.TempDir(), store.Options{Now: func() time.Time { return time.Unix(now, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	populateMetricStoreCatalog(context.Background(), t, s, now)
	l := newMetricStoreTestListener(t, cache.New(), s, false)
	request := func(names []string, partial bool) (int, []byte) {
		t.Helper()
		data, err := (&protov3.MultiGlobRequest{Metrics: names}).MarshalVT()
		if err != nil {
			t.Fatal(err)
		}
		path := "/info/?format=carbonapi_v3_pb"
		if partial {
			path += "&allowPartial=1"
		}
		r := httptest.NewRecorder()
		l.infoHandler(r, httptest.NewRequest(http.MethodPost, path, bytes.NewReader(data)))
		return r.Code, r.Body.Bytes()
	}
	present := []string{"servers.api.cpu.system", "servers.api.cpu.user"}
	code, want := request(present, false)
	if code != http.StatusOK {
		t.Fatalf("reference status %d", code)
	}
	for _, names := range [][]string{
		present,
		{"missing.first", present[0], "missing.middle", present[1], "missing.last"},
	} {
		code, got := request(names, true)
		if code != http.StatusOK || !bytes.Equal(got, want) {
			t.Fatalf("partial changed existing metadata: code=%d got=%x want=%x", code, got, want)
		}
	}
	code, data := request([]string{"missing.first", "missing.last"}, true)
	var empty protov3.MultiMetricsInfoResponse
	if code != http.StatusOK || empty.UnmarshalVT(data) != nil || len(empty.Metrics) != 0 {
		t.Fatalf("all absent: code=%d data=%x", code, data)
	}
	for _, names := range [][]string{{"missing"}, {present[0], "missing", present[1]}} {
		if code, _ := request(names, false); code != http.StatusNotFound {
			t.Fatalf("legacy absence status changed: %d", code)
		}
	}
	// The opt-in affects only v3: legacy JSON clients still get a 404.
	r := httptest.NewRecorder()
	l.infoHandler(r, httptest.NewRequest(http.MethodGet, "/info/?format=json&allowPartial=1&target=missing", http.NoBody))
	if r.Code != http.StatusNotFound {
		t.Fatalf("JSON absence status changed: %d", r.Code)
	}
	// Repetition and order have the same wire representation as individual
	// metadata requests; no sorting or deduplication changes the response.
	code, data = request([]string{present[1], "missing", present[1], present[0]}, true)
	var got protov3.MultiMetricsInfoResponse
	if code != http.StatusOK || got.UnmarshalVT(data) != nil {
		t.Fatalf("repeated names: status=%d", code)
	}
	var order []string
	for _, m := range got.Metrics {
		order = append(order, m.Name)
	}
	if !reflect.DeepEqual(order, []string{present[1], present[1], present[0]}) {
		t.Fatalf("order changed: %v", order)
	}
}
