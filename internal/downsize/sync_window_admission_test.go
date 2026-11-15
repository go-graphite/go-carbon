package downsize

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// Exercise real checksum-validated exporters with concurrent selection and compare
// every digest and copied point bit with the serial client.
func TestSyncWindowSelectionCapacityDifferential(t *testing.T) {
	for _, advertised := range []int{0, 1, 2, 8, 1000} {
		t.Run(fmt.Sprint(advertised), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			source := nativeControllerStore(t)
			names := []string{"zero", "tiny", "negative", "empty"}
			values := []float64{math.Copysign(0, -1), math.SmallestNonzeroFloat64, -3.125, 42}
			for i, n := range names {
				addPoints(t, source, n, values[i])
			}
			windows := syncRanges{{From: testMetricNow - 20, Until: testMetricNow}}
			handler := nativeControllerService(t, source)
			gate := make(chan struct{})
			entered := make(chan struct{}, 16)
			var closeGate sync.Once
			defer closeGate.Do(func() { close(gate) })
			donor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/native/capabilities" {
					_ = json.NewEncoder(w).Encode(map[string]any{"NativeExportConcurrency": advertised, "NativeTimeWindow": true})
					return
				}
				if r.URL.Path == "/v1/native/export" {
					entered <- struct{}{}
					select {
					case <-gate:
					case <-r.Context().Done():
						return
					}
				}
				handler.ServeHTTP(w, r)
			}))
			defer donor.Close()
			b := newSyncLiveBackend(syncLiveOptions{})
			finish := b.beginWindowCopy()
			defer finish()
			count := max(1, min(8, advertised)-1)
			results := make(chan map[string][]syncDigest, count+1)
			errs := make(chan error, count+1)
			for range count + 1 {
				go func() { v, e := b.WindowPresenceRanges(ctx, donor.URL, names, windows); results <- v; errs <- e }()
			}
			for range count {
				select {
				case <-entered:
				case <-ctx.Done():
					t.Fatal("advertised exports were serialized")
				}
			}
			select {
			case <-entered:
				t.Fatal("selection exceeded donor capacity minus copy slot")
			case <-time.After(30 * time.Millisecond):
			}
			closeGate.Do(func() { close(gate) })
			var oracle map[string][]syncDigest
			for range count + 1 {
				got := <-results
				if err := <-errs; err != nil {
					t.Fatal(err)
				}
				if oracle == nil {
					oracle = got
				} else if !reflect.DeepEqual(oracle, got) {
					t.Fatal("concurrent selection changed digests")
				}
			}
			// A separate serial client is the differential oracle for the same source pin.
			serial := newSyncLiveBackend(syncLiveOptions{})
			serial.windowSelectionSlots.Store(donor.URL, &windowSelectionCapacity{load: func() (chan struct{}, error) { return make(chan struct{}, 1), nil }})
			want, err := serial.WindowPresenceRanges(ctx, donor.URL, names, windows)
			if err != nil || !reflect.DeepEqual(want, oracle) {
				t.Fatal("serial/concurrent export differs", err, want, oracle)
			}
			var expected []store.Snapshot
			for i, client := range []*syncLiveBackend{serial, b} {
				target := nativeControllerStore(t)
				addPoints(t, target, "negative", 99) // existing target values must win
				server := httptest.NewServer(nativeControllerService(t, target))
				defer server.Close()
				res, err := client.CopyWindow(ctx, donor.URL, server.URL, names, filepath.Join(t.TempDir(), "copy.jsonl"), windows)
				if err != nil || res.Copied != len(names) || len(res.Failed) != 0 {
					t.Fatal(res, err)
				}
				for ni, n := range names {
					snap, err := target.Snapshot(ctx, n)
					if err != nil {
						t.Fatal(err)
					}
					if i == 0 {
						expected = append(expected, snap)
						continue
					}
					want := expected[ni]
					if !reflect.DeepEqual(want.Metadata.MetricConfig, snap.Metadata.MetricConfig) || len(want.Archives) != len(snap.Archives) {
						t.Fatal("metadata differs", n)
					}
					for ai, a := range want.Archives {
						got := snap.Archives[ai]
						if a.Retention != got.Retention || len(a.Points) != len(got.Points) {
							t.Fatal("archive differs", n)
						}
						for pi, p := range a.Points {
							q := got.Points[pi]
							if p.Timestamp != q.Timestamp || math.Float64bits(p.Value) != math.Float64bits(q.Value) {
								t.Fatal("point bits differ", n)
							}
						}
					}
				}
			}
			// Confirm the underlying snapshots preserve the tricky fixture bits.
			for i, n := range names {
				snap, err := source.Snapshot(ctx, n)
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for _, a := range snap.Archives {
					for _, p := range a.Points {
						if math.Float64bits(p.Value) == math.Float64bits(values[i]) {
							found = true
						}
					}
				}
				if !found {
					t.Fatal("fixture value bits missing", n)
				}
			}
		})
	}
}
