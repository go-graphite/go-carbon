package downsize

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestSyncWindowCountsPipelineAndResume(t *testing.T) {
	source := nativeControllerStore(t)
	donor := httptest.NewServer(nativeControllerService(t, source))
	defer donor.Close()
	names := copyNames("metric.", store.NativeMaxRangeNames*3)
	var pulls, digests atomic.Int64
	dst := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/native/capabilities":
			_ = json.NewEncoder(w).Encode(map[string]bool{"NativeTimeWindow": true, "NativeExactCombine": true, "NativeDeferredCombine": true})
		case "/v1/transfer/digest":
			var req struct{ Names []string }
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Error(err)
				return
			}
			n := digests.Add(1)
			// With one worker the first range must be copied and measured before
			// any later range's baseline; whole-catalog scans violate this ordering.
			if n > 2 && pulls.Load() == 0 {
				t.Error("whole-catalog measurement blocked the first pull")
			}
			if len(req.Names) > store.NativeMaxRangeNames {
				t.Error("unbounded digest")
			}
			count := int64(10)
			if n%2 == 0 {
				count = 17
			}
			out := make([]syncDigestWire, len(req.Names))
			for i, name := range req.Names {
				out[i] = syncDigestWire{Name: name, Found: true, Archives: []syncDigest{{Count: count}}}
			}
			_ = json.NewEncoder(w).Encode(out)
		case "/v1/native/pull":
			pulls.Add(1)
			var req struct{ Range store.NativeRange }
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Error(err)
				return
			}
			id, hash, err := req.Range.Identity()
			if err != nil {
				t.Error(err)
				return
			}
			count := len(req.Range.Names)
			bits := make([]byte, (count+7)/8)
			for i := range count {
				bits[i/8] |= 1 << uint(i%8)
			}
			_ = json.NewEncoder(w).Encode(store.NativeReceipt{Identity: id, NamesSHA256: hash, Count: count, CopiedCount: count, Copied: bits, Digest: fmt.Sprintf("%064d", 0)})
		default:
			http.NotFound(w, r)
		}
	}))
	defer dst.Close()
	b := newSyncLiveBackend(syncLiveOptions{copyStreams: 1})
	state := t.TempDir()
	for range 2 {
		stop := b.beginWindowCopy()
		res, err := b.CopyWindow(context.Background(), donor.URL, dst.URL, names, state+"/copy", syncRanges{{From: testMetricNow - 10, Until: testMetricNow}})
		stop()
		if err != nil || res.Copied != len(names) {
			t.Fatal(res, err)
		}
	}
	if pulls.Load() != 3 || digests.Load() != 6 {
		t.Fatal("completed range receipts/counts repeated", pulls.Load(), digests.Load())
	}
	got, err := collectWindowPointCounts(state)
	if err != nil || got.Added == nil || *got.Added != int64(7*len(names)) || got.Transfers != 3 || got.Unknown != 0 {
		t.Fatal(got, err)
	}
}

func TestWindowRangeCountsLegacyTotals(t *testing.T) {
	for _, complete := range []bool{false, true} {
		t.Run(fmt.Sprint(complete), func(t *testing.T) {
			state := t.TempDir()
			path := state + "/copy.window-points.json"
			before, after := int64(10), int64(17)
			c := windowPointCount{Before: &before}
			if complete {
				c.After = &after
			}
			if err := atomicStateJSON(path, c); err != nil {
				t.Fatal(err)
			}
			measure, err := prepareWindowRangeCounts(path, windowPointCount{})
			if err != nil || measure == complete {
				t.Fatal(measure, err)
			}
			if measure {
				if err := atomicStateJSON(state+"/copy.range-0-4.window-points.json", windowPointCount{Before: &before, After: &after}); err != nil {
					t.Fatal(err)
				}
			}
			got, err := collectWindowPointCounts(state)
			if err != nil || got.Added == nil || *got.Added != 7 || got.Transfers != 1 || got.Unknown != 0 {
				t.Fatal(got, err)
			}
		})
	}
}

// A recovered partial receipt can already have applied points. Taking a new
// baseline would turn an unknown original delta into a misleading smaller total.
func TestWindowRangeCountsAmbiguousReceipt(t *testing.T) {
	var requests atomic.Int64
	dst := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		_ = json.NewEncoder(w).Encode([]syncDigestWire{{Name: "metric", Found: true, Archives: []syncDigest{{Count: 17}}}})
	}))
	defer dst.Close()
	b := newSyncLiveBackend(syncLiveOptions{})
	state := t.TempDir()
	path := state + "/range.window-points.json"
	window := syncRange{From: testMetricNow - 10, Until: testMetricNow}
	measurement, err := b.beginWindowRangeCount(context.Background(), dst.URL, []string{"metric"}, window, path, true, "source")
	if err != nil || measurement.Before != nil || requests.Load() != 0 {
		t.Fatal(measurement, err, requests.Load())
	}
	if err := b.finishWindowRangeCount(context.Background(), dst.URL, []string{"metric"}, window, path, measurement); err != nil {
		t.Fatal(err)
	}
	got, err := collectWindowPointCounts(state)
	if err != nil || got.Added != nil || got.Unknown != 1 || requests.Load() != 1 {
		t.Fatal(got, err, requests.Load())
	}
}
