package downsize

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

func TestCopyPolicyConflictPreservesArchivesAndTargetPolicy(t *testing.T) {
	ctx := context.Background()
	for _, xff := range []float32{0, 0.5} {
		t.Run(string(rune('a'+int(xff*2))), func(t *testing.T) {
			clock := func() time.Time { return time.Unix(1000080, 0) }
			open := func() *store.Store {
				s, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: &whisperio.ContinuationCodec{}, Now: clock})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = s.Close() })
				s.StopBackground()
				return s
			}
			source, target := open(), open()
			cfg := store.MetricConfig{Name: "policy.metric", AggregationMethod: store.Average, XFilesFactor: xff, Retentions: []store.Retention{{Step: 10, Count: 120}, {Step: 60, Count: 120}}}
			if _, err := source.Create(ctx, cfg); err != nil {
				t.Fatal(err)
			}
			if err := source.UpdateMany(ctx, cfg.Name, []store.Point{{Timestamp: 1000060, Value: 17}, {Timestamp: 1000070, Value: 42}}); err != nil {
				t.Fatal(err)
			}
			cfg.XFilesFactor = 0.5 - xff
			if _, err := target.Create(ctx, cfg); err != nil {
				t.Fatal(err)
			}
			if err := target.UpdateMany(ctx, cfg.Name, []store.Point{{Timestamp: 1000070, Value: 99}}); err != nil {
				t.Fatal(err)
			}
			donorHandler := nativeControllerService(t, source)
			donor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/transfer/export" && r.URL.Query().Get("store_format") != "12" {
					http.Error(w, "export requires peer store format 12", 409)
					return
				}
				donorHandler.ServeHTTP(w, r)
			}))
			defer donor.Close()
			destination := httptest.NewServer(nativeControllerService(t, target))
			defer destination.Close()
			o := copyTestOptions(t, donor.URL, destination.URL)
			o.names = filepath.Join(t.TempDir(), "names")
			if err := os.WriteFile(o.names, []byte(cfg.Name+"\n"), 0600); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if err := planFreeCopy(ctx, o); err != nil {
					t.Fatal(err)
				}
			}
			got, err := target.Snapshot(ctx, cfg.Name)
			if err != nil {
				t.Fatal(err)
			}
			if got.Metadata.XFilesFactor != cfg.XFilesFactor {
				t.Fatal("target policy changed")
			}
			coarse := 29.5
			if xff == 0.5 {
				coarse = 99
			}
			want := store.Snapshot{Metadata: got.Metadata, Archives: []store.Archive{{Retention: cfg.Retentions[0], Points: []store.Point{{Timestamp: 1000060, Value: 17}, {Timestamp: 1000070, Value: 99}}}, {Retention: cfg.Retentions[1], Points: []store.Point{{Timestamp: 1000020, Value: coarse}}}}}
			if err := store.EqualSnapshots(want, got); err != nil {
				t.Fatalf("%v; got=%+v", err, got.Archives)
			}
			records, err := source.ExportTransfer(ctx, []string{cfg.Name})
			if err != nil {
				t.Fatal(err)
			}
			bad := cfg
			bad.AggregationMethod = store.Sum
			if _, err := copyWithTargetPolicy(ctx, records[0], bad); err == nil {
				t.Fatal("accepted aggregation conflict")
			}
			bad = cfg
			bad.Retentions = []store.Retention{{Step: 10, Count: 121}, {Step: 60, Count: 120}}
			if _, err := copyWithTargetPolicy(ctx, records[0], bad); err == nil {
				t.Fatal("accepted retention conflict")
			}
		})
	}
}
