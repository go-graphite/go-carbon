package downsize

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"reflect"
	"testing"
	"time"

	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
)

func TestSyncInfoLegacyBodyTimeoutKeepsBatch(t *testing.T) {
	t.Parallel()
	b := newSyncLiveBackend(syncLiveOptions{carbonPort: 8080})
	b.retryTick = time.Millisecond
	var requests [][]string
	b.client.Transport = syncInfoRoundTrip(func(r *http.Request) (*http.Response, error) {
		if r.URL.RequestURI() != "/info/?format=carbonapi_v3_pb&allowPartial=1" || r.Method != http.MethodPost {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL)
		}
		body, _ := io.ReadAll(r.Body)
		var req protov3.MultiGlobRequest
		if err := req.UnmarshalVT(body); err != nil {
			t.Fatal(err)
		}
		requests = append(requests, append([]string(nil), req.Metrics...))
		// The first timeout halves the current batch. A singleton timeout
		// retries that singleton, without discarding it or restarting verify.
		if len(requests) <= 2 {
			return &http.Response{StatusCode: 400, Body: io.NopCloser(bytes.NewBufferString("Bad request (unsupported format)\n"))}, nil
		}
		var out protov3.MultiMetricsInfoResponse
		for _, name := range req.Metrics {
			out.Metrics = append(out.Metrics, &protov3.MetricsInfoResponse{Name: name, Retentions: []*protov3.Retention{{SecondsPerPoint: 60, NumberOfPoints: 10000}}})
		}
		data, _ := out.MarshalVT()
		return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(data))}, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := b.Info(ctx, "http://source:4242", []string{"a", "b", "c"})
	if err != nil || len(got) != 3 {
		t.Fatalf("info=%v error=%v", got, err)
	}
	if want := [][]string{{"a", "b", "c"}, {"a"}, {"a"}, {"b"}, {"c"}}; !reflect.DeepEqual(requests, want) {
		t.Fatalf("requests=%v want=%v", requests, want)
	}
}

func TestSyncInfoLegacyTimeoutCancellationAndValidation(t *testing.T) {
	t.Parallel()
	for _, body := range []string{"Bad request (unsupported format)", "invalid metric"} {
		t.Run(body, func(t *testing.T) {
			b := newSyncLiveBackend(syncLiveOptions{carbonPort: 8080})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			b.client.Transport = syncInfoRoundTrip(func(*http.Request) (*http.Response, error) {
				calls++
				if body == "Bad request (unsupported format)" {
					cancel()
				}
				return &http.Response{StatusCode: 400, Body: io.NopCloser(bytes.NewBufferString(body))}, nil
			})
			got, err := b.Info(ctx, "http://source:4242", []string{"a"})
			if err == nil || len(got) != 0 || calls != 1 {
				t.Fatalf("info=%v error=%v calls=%d", got, err, calls)
			}
			if body == "invalid metric" {
				var h *syncHTTPError
				if !errors.As(err, &h) || h.status != 400 || h.body != body {
					t.Fatalf("validation error changed: %v", err)
				}
			}
		})
	}
}
