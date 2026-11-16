package downsize

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

func TestSyncClosedSparseBoundaryRepair(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		existing, later, recent, gap bool
		status, failures             int
	}{
		{later: true}, {existing: true, later: true}, {}, {existing: true},
		{existing: true, later: true, recent: true}, {later: true, recent: true}, {later: true, gap: true},
		{later: true, status: http.StatusGone, failures: 1},
		{later: true, status: http.StatusGone, failures: 2},
		{later: true, status: http.StatusServiceUnavailable, failures: 1},
	} {
		existing := tc.existing
		t.Run(map[bool]string{false: "hole", true: "target-wins"}[existing]+map[bool]string{false: "/trailing", true: "/historical"}[tc.later]+map[bool]string{false: "", true: "/recent"}[tc.recent]+map[bool]string{false: "", true: "/gap"}[tc.gap]+fmt.Sprintf("/status%d-failures%d", tc.status, tc.failures), func(t *testing.T) {
			ctx := context.Background()
			// The repair path intentionally uses real time. Put the recent
			// source clock beyond the test deadline so its incomplete bucket
			// cannot become historical while unrelated fixtures are running.
			fixtureNow := time.Unix(testMetricNow, 0)
			if tc.recent {
				fixtureNow = time.Now().Add(time.Hour)
				if deadline, ok := t.Deadline(); ok {
					fixtureNow = deadline.Add(time.Minute)
				}
			}
			clock := func() time.Time { return fixtureNow }
			open := func() *store.Store {
				db, e := store.Open("/db", store.Options{Now: clock, ExactBlocks: true, TimeBuckets: true, Continuation: &whisperio.ContinuationCodec{}, CacheSize: 1 << 20, MemTableSize: 1 << 20}.WithFS(vfs.NewMem()))
				if e != nil {
					t.Fatal(e)
				}
				t.Cleanup(func() { _ = db.Close() })
				return db
			}
			source, target := open(), open()
			seed := func(db *store.Store, snap store.Snapshot) {
				snap.Metadata.Continuation = true
				mem := policyMemory{}
				if e := (&whisperio.ContinuationCodec{}).RestoreArchives(ctx, mem, snap, clock); e != nil {
					t.Fatal(e)
				}
				record := store.TransferMetric{Config: snap.Metadata.MetricConfig, Version: store.TransferVersion{Name: snap.Metadata.Name, ID: 1, Generation: 1, Revision: 1}, Continuation: true}
				if e := mem.Visit(nil, func(k, v []byte) error {
					record.Keys = append(record.Keys, store.RelayoutKV{Key: bytes.Clone(k), Value: bytes.Clone(v)})
					return nil
				}); e != nil {
					t.Fatal(e)
				}
				if _, e := db.ImportTransfer(ctx, []store.TransferMetric{record}); e != nil {
					t.Fatal(e)
				}
			}
			base := testMetricNow/60*60 - 240
			if tc.recent {
				base = clock().Unix()/60*60 - 60
			}
			ret := []store.Retention{{Step: 10, Count: 128}, {Step: 60, Count: 128}}
			snap := store.Snapshot{Metadata: store.Metadata{MetricConfig: store.MetricConfig{Name: "sparse", AggregationMethod: store.Average, Retentions: ret, XFilesFactor: 0}}, Archives: []store.Archive{
				{Retention: ret[0], Points: []store.Point{{Timestamp: base + 10, Value: 1}, {Timestamp: base + 70, Value: 2}}},
				{Retention: ret[1], Points: []store.Point{{Timestamp: base, Value: math.Copysign(0, -1)}, {Timestamp: base + 60, Value: 7}}},
			}}
			if tc.gap {
				snap.Archives[1].Points[1].Timestamp = base + 180
			}
			if !tc.later {
				snap.Archives[1].Points = snap.Archives[1].Points[:1]
			}
			seed(source, snap)
			want := snap
			want.Archives = []store.Archive{
				{Retention: ret[0], Points: []store.Point{{Timestamp: base - 10, Value: 9}, {Timestamp: base + 10, Value: 42}}},
				{Retention: ret[1], Points: []store.Point{{Timestamp: base - 60, Value: 11}}},
			}
			if existing {
				want.Archives[1].Points = append(want.Archives[1].Points, store.Point{Timestamp: base, Value: 43})
			}
			seed(target, want)
			sourceHandler := nativeControllerService(t, source)
			var boundaryCalls atomic.Int64
			var exports, imports, verifies atomic.Int64
			var boundaryMu sync.Mutex
			var expiredID string
			var boundaryBody []byte
			donor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/native/export" {
					exports.Add(1)
					raw, e := io.ReadAll(r.Body)
					if e != nil {
						t.Error(e)
						return
					}
					r.Body = io.NopCloser(bytes.NewReader(raw))
					var request struct {
						Session string
						Names   []string
						Window  *store.NativeWindow
					}
					if e := json.Unmarshal(raw, &request); e != nil {
						t.Error(e)
						return
					}
					if request.Window != nil && request.Window.Until > base+20 {
						boundaryMu.Lock()
						defer boundaryMu.Unlock()
						calls := int(boundaryCalls.Add(1))
						if len(request.Names) != 1 || request.Names[0] != "sparse" {
							t.Errorf("expanded boundary names: %s", raw)
						}
						if calls == 1 {
							expiredID = request.Session
							boundaryBody = raw
						} else {
							var first map[string]any
							var next map[string]any
							_ = json.Unmarshal(boundaryBody, &first)
							_ = json.Unmarshal(raw, &next)
							delete(first, "Session")
							delete(next, "Session")
							a, _ := json.Marshal(first)
							b, _ := json.Marshal(next)
							if !bytes.Equal(a, b) || request.Session == expiredID {
								t.Errorf("retry changed scope or reused expired pin: %s / %s", boundaryBody, raw)
							}
						}
						if calls <= tc.failures {
							if tc.status == http.StatusGone {
								sourceHandler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodDelete, "/v1/native/source/"+request.Session, nil))
								// Let the real donor return 410 for the vanished pin.
							} else {
								http.Error(w, "unavailable", tc.status)
								return
							}
						}
					}
				}
				sourceHandler.ServeHTTP(w, r)
			}))
			defer donor.Close()
			targetHandler := nativeControllerService(t, target)
			survivor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/transfer/import":
					imports.Add(1)
				case "/v1/transfer/verify":
					verifies.Add(1)
				}
				targetHandler.ServeHTTP(w, r)
			}))
			defer survivor.Close()
			path := filepath.Join(t.TempDir(), "copy")
			for attempt := range 2 {
				before := [3]int64{exports.Load(), imports.Load(), verifies.Load()}
				res, err := newSyncLiveBackend(syncLiveOptions{}).CopyWindow(ctx, donor.URL, survivor.URL, []string{"sparse"}, path, syncRanges{{From: base, Until: base + 20}})
				if tc.status != 0 && (tc.status != http.StatusGone || tc.failures > 1) {
					if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("boundary export: HTTP %d", tc.status)) {
						t.Fatalf("expected bounded export failure: %+v %v", res, err)
					}
					wantCalls := 1
					if tc.status == http.StatusGone {
						wantCalls = 2
					}
					if int(boundaryCalls.Load()) != wantCalls {
						t.Fatalf("boundary attempts: got%d want%d", boundaryCalls.Load(), wantCalls)
					}
					break
				}
				if !existing && (!tc.later || tc.recent) {
					if err != nil || res.Copied != 0 || len(res.Failed) != 1 {
						t.Fatalf("unproven trailing bucket accepted: %+v %v", res, err)
					}
					continue
				}
				if attempt == 1 {
					after := [3]int64{exports.Load(), imports.Load(), verifies.Load()}
					if after != before {
						t.Fatalf("completed window repeated export/import/verify: %v -> %v", before, after)
					}
				}
				if err != nil || res.Copied != 1 || len(res.Failed) != 0 {
					t.Fatalf("closed sparse boundary: %+v %v", res, err)
				}
			}
			if tc.status == http.StatusGone && tc.failures == 1 && boundaryCalls.Load() != 2 {
				t.Fatalf("boundary exports including lossless resume: %d", boundaryCalls.Load())
			}
			if !existing && tc.later && !tc.recent && (tc.status == 0 || tc.failures == 1 && tc.status == http.StatusGone) {
				want.Archives[1].Points = append(want.Archives[1].Points, store.Point{Timestamp: base, Value: math.Copysign(0, -1)})
			}
			got, err := target.Snapshot(ctx, "sparse")
			if err != nil {
				t.Fatal(err)
			}
			if err := store.EqualSnapshots(want, got); err != nil {
				t.Fatal("repair changed points outside exact coarse hole or target values:", err)
			}
		})
	}
}
