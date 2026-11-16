package downsize

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
)

func TestSyncInfoPartialMatchesLegacy(t *testing.T) {
	t.Parallel()
	for _, presentEvery := range []int{0, 3, 1} {
		t.Run(fmt.Sprint(presentEvery), func(t *testing.T) {
			names := make([]string, 500)
			present := map[string]bool{}
			for i := range names {
				names[i] = fmt.Sprintf("metadata.%04d", i)
				present[names[i]] = presentEvery > 0 && i%presentEvery == 0
			}
			var want map[string][]syncArchive
			for _, partial := range []bool{false, true} {
				calls := 0
				b := newSyncLiveBackend(syncLiveOptions{carbonPort: 8080})
				b.client.Transport = syncInfoRoundTrip(func(r *http.Request) (*http.Response, error) {
					calls++
					if r.URL.Query().Get("allowPartial") != "1" {
						t.Fatal("client did not request partial metadata")
					}
					data, err := io.ReadAll(r.Body)
					if err != nil {
						t.Fatal(err)
					}
					var req protov3.MultiGlobRequest
					if err := req.UnmarshalVT(data); err != nil {
						t.Fatal(err)
					}
					var reply protov3.MultiMetricsInfoResponse
					for _, name := range req.Metrics {
						if !present[name] {
							if !partial {
								return &http.Response{StatusCode: 404, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("Metric not found"))}, nil
							}
							continue
						}
						reply.Metrics = append(reply.Metrics, &protov3.MetricsInfoResponse{Name: name, Retentions: []*protov3.Retention{{SecondsPerPoint: 60, NumberOfPoints: 10080}, {SecondsPerPoint: 3600, NumberOfPoints: 8760}}})
					}
					data, err = reply.MarshalVT()
					if err != nil {
						t.Fatal(err)
					}
					return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(data))}, nil
				})
				got, err := b.Info(context.Background(), "http://source:4242", names)
				if err != nil {
					t.Fatal(err)
				}
				if !partial {
					want = got
				} else if calls != 1 || !reflect.DeepEqual(got, want) {
					t.Fatalf("partial calls=%d, metadata changed=%v", calls, !reflect.DeepEqual(got, want))
				}
				if !partial && presentEvery == 0 && calls != 999 {
					t.Fatalf("legacy all-absent request count %d, want 999", calls)
				}
			}
		})
	}
}
