package buckyd

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/lomik/zapwriter"
)

func TestLongNamesNativeHTTPRoundTrip(t *testing.T) {
	defer zapwriter.Test()()
	source, donor := newNativeTestService(t)
	destination, target := newNativeTestService(t)
	ctx := context.Background()
	var names []string
	for _, size := range []int{300, 4096} {
		name := "long." + strings.Repeat("x", size) + ".value"
		names = append(names, name)
		if _, err := donor.Create(ctx, chunkstore.MetricConfig{Name: name, AggregationMethod: chunkstore.Sum, Retentions: []chunkstore.Retention{{Step: 1, Count: 120}}}); err != nil {
			t.Fatal(err)
		}
		if err := donor.UpdateMany(ctx, name, []chunkstore.Point{{Timestamp: 9998, Value: 42}}); err != nil {
			t.Fatal(err)
		}
	}
	remote := httptest.NewServer(source.Handler())
	defer remote.Close()
	root := token(t, source.secret, []string{"*"}, []string{"*"})
	invoke := func(s *Service, path string, body any) *httptest.ResponseRecorder {
		t.Helper()
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		r := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(data))
		r.Header.Set(authHeader, root)
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		return w
	}
	w := invoke(source, "/v1/native/source", struct{}{})
	var session struct{ ID string }
	if err := json.Unmarshal(w.Body.Bytes(), &session); err != nil || w.Code != http.StatusOK {
		t.Fatalf("source pin: %d %v", w.Code, err)
	}
	pull := nativePullRequest{Range: chunkstore.NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: strings.Repeat("3", 64), Source: remote.URL, End: int64(len(names)), Names: names}, Session: session.ID}
	w = invoke(destination, "/v1/native/pull", pull)
	var receipt chunkstore.NativeReceipt
	if err := json.Unmarshal(w.Body.Bytes(), &receipt); err != nil || w.Code != http.StatusOK || receipt.CopiedCount != 2 || receipt.DeferredCount != 0 {
		t.Fatalf("native pull: %d %s, %v", w.Code, w.Body.String(), err)
	}
	for _, name := range names {
		want, err := donor.Snapshot(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		got, err := target.Snapshot(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		if err := chunkstore.EqualSnapshots(want, got); err != nil {
			t.Fatal(err)
		}
	}
	if destination.NativeRejectedRequests() != 0 || source.NativeRejectedRequests() != 0 {
		t.Fatal("valid long names counted as rejected")
	}
	// Byte budgets still fail before contacting the donor, and are observable.
	pull.Range.Names = []string{strings.Repeat("x", (1<<20)+1)}
	pull.Range.End = 1
	w = invoke(destination, "/v1/native/pull", pull)
	if w.Code != http.StatusRequestEntityTooLarge || destination.NativeRejectedRequests() != 1 {
		t.Fatalf("silent oversized request: status=%d rejected=%d", w.Code, destination.NativeRejectedRequests())
	}
	if logs := zapwriter.TestString(); !strings.Contains(logs, "ERROR") || !strings.Contains(logs, "native request rejected") {
		t.Fatal("missing native rejection error log", logs)
	}
}
