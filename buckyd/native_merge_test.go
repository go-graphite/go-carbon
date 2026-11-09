package buckyd

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestNativeMergeHTTPBoundaryAndCombineFence(t *testing.T) {
	source, donor := newNativeTestService(t)
	destination, target := newNativeTestService(t)
	ctx := context.Background()
	cfg := chunkstore.MetricConfig{Name: "merge.http", AggregationMethod: chunkstore.Sum, Retentions: []chunkstore.Retention{{Step: 1, Count: 10000}}}
	if _, err := donor.Create(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	if err := donor.UpdateMany(ctx, cfg.Name, []chunkstore.Point{{Timestamp: 9900, Value: 42}}); err != nil {
		t.Fatal(err)
	}
	donor.SetNativePendingSnapshot(func() (map[string]struct{}, error) { return map[string]struct{}{cfg.Name: {}}, nil })
	root := token(t, source.secret, []string{"*"}, []string{"*"})
	invoke := func(service *Service, path string, body any) *httptest.ResponseRecorder {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		r := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(data))
		r.Header.Set(authHeader, root)
		w := httptest.NewRecorder()
		service.Handler().ServeHTTP(w, r)
		return w
	}
	r := invoke(source, "/v1/native/source", map[string]string{"MergeSession": "http-history"})
	if r.Code != 200 {
		t.Fatal(r.Code, r.Body.String())
	}
	var pin struct {
		ID, MergeSession                    string
		SnapshotStartedAt, SnapshotPinnedAt time.Time
		BackfillFromUnix                    int64
		BackfillFullRetention               bool
	}
	if err := json.Unmarshal(r.Body.Bytes(), &pin); err != nil {
		t.Fatal(err)
	}
	if pin.MergeSession != "http-history" || pin.SnapshotStartedAt.IsZero() || pin.SnapshotPinnedAt.Before(pin.SnapshotStartedAt) || pin.BackfillFromUnix != 0 || !pin.BackfillFullRetention {
		t.Fatal(pin)
	}
	remote := httptest.NewServer(source.Handler())
	defer remote.Close()
	request := nativePullRequest{Session: pin.ID, Range: chunkstore.NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: strings.Repeat("3", 64), Source: remote.URL, Names: []string{cfg.Name}, End: 1}}
	for _, mode := range []struct {
		merge   string
		combine bool
	}{{"", true}, {"http-history", true}, {"", false}, {"wrong-history", false}} {
		request.Range.MergeSession, request.Range.Combine = mode.merge, mode.combine
		if r := invoke(destination, "/v1/native/pull", request); r.Code == 200 {
			t.Fatal("unsafe mode accepted", mode, r.Body.String())
		}
		if _, err := target.Metadata(ctx, cfg.Name); err == nil {
			t.Fatal("unsafe request published metric")
		}
	}
	request.Range.MergeSession, request.Range.Combine = "http-history", false
	if r := invoke(destination, "/v1/native/pull", request); r.Code != 200 {
		t.Fatal(r.Code, r.Body.String())
	}
	want, err := donor.Snapshot(ctx, cfg.Name)
	if err != nil {
		t.Fatal(err)
	}
	got, err := target.Snapshot(ctx, cfg.Name)
	if err != nil {
		t.Fatal(err)
	}
	if err := chunkstore.EqualSnapshots(want, got); err != nil {
		t.Fatal(err)
	}
}
