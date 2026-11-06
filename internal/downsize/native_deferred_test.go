package downsize

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestNativeDeferredSameJournalRecovery(t *testing.T) {
	for _, fault := range []string{"none", "before-pull", "lost-ack"} {
		t.Run(fault, func(t *testing.T) {
			ctx := context.Background()
			source, target, oracle := nativeControllerStore(t), nativeControllerStore(t), nativeControllerStore(t)
			var names []string
			for i := range 5 {
				name := fmt.Sprintf("deferred.controller.%02d", i)
				names = append(names, name)
				addPoints(t, source, name, float64(i))
				if err := source.UpdateMany(ctx, name, []store.Point{{Timestamp: testMetricNow - 30, Value: 77}}); err != nil {
					t.Fatal(err)
				}
				if i%2 == 1 {
					addPoints(t, target, name, 42)
					addPoints(t, oracle, name, 42)
				}
			}
			records, err := source.ExportTransfer(ctx, names)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = oracle.ImportTransfer(ctx, records); err != nil {
				t.Fatal(err)
			}
			var phase2, fail, unavailable atomic.Bool
			var exported, metricRPCs atomic.Int64
			donorHandler := nativeControllerService(t, source)
			donor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if unavailable.Load() && r.URL.Path != "/v1/native/capabilities" {
					http.Error(w, "donor unavailable", 503)
					return
				}
				if phase2.Load() && r.URL.Path == "/v1/native/export" {
					var body struct {
						Session string
						Names   []string
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
						return
					}
					if len(body.Names) != 2 || body.Names[0] != names[1] || body.Names[1] != names[3] {
						t.Errorf("exported completed ordinals: %v", body.Names)
					}
					exported.Add(int64(len(body.Names)))
					data, _ := json.Marshal(body)
					r.Body = io.NopCloser(bytes.NewReader(data))
				}
				donorHandler.ServeHTTP(w, r)
			}))
			defer donor.Close()
			targetHandler := nativeControllerService(t, target)
			survivor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/transfer/pull" {
					metricRPCs.Add(1)
				}
				if r.URL.Path == "/v1/native/pull" && phase2.Load() && fail.CompareAndSwap(true, false) {
					if fault == "lost-ack" {
						rec := httptest.NewRecorder()
						targetHandler.ServeHTTP(rec, r)
						if rec.Code != 200 {
							t.Errorf("pull failed: %s", rec.Body.String())
						}
					}
					http.Error(w, "simulated interrupted controller response", 503)
					return
				}
				targetHandler.ServeHTTP(w, r)
			}))
			defer survivor.Close()
			o := testOptions(fixturePlan(t, donor.URL, survivor.URL, names))
			o.delete, o.bulkOnly = false, true
			o.deleteMode, o.engine = "auto", "bulk"
			if err := run(ctx, o); err != nil {
				t.Fatal(err)
			}
			p, err := readPlan(o.plan)
			if err != nil {
				t.Fatal(err)
			}
			load := func() task {
				t.Helper()
				j, err := openJournal(o.state, p.PlanID+":delete=false:mode=auto")
				if err != nil {
					t.Fatal(err)
				}
				defer j.close()
				rows := j.ranges(p.Streams[0].SHA256)
				if len(rows) != 1 {
					t.Fatalf("overlapping journal ranges: %d", len(rows))
				}
				return rows[0]
			}
			before := load()
			oldReceipt, _ := json.Marshal(before.Native.Receipt)
			prefix, err := os.ReadFile(o.state)
			if err != nil {
				t.Fatal(err)
			}
			if before.Native.Receipt.CopiedCount != 3 || before.Native.Receipt.DeferredCount != 2 {
				t.Fatal("bad phase1 fixture")
			}
			versions := make(map[string]store.Metadata)
			for i, name := range names {
				if i%2 == 0 {
					versions[name], _ = target.Metadata(ctx, name)
				}
			}
			phase2.Store(true)
			o.bulkCombine, o.deferredOnly = true, true
			o.deferredRange = taskKey(before)
			o.maxBatches = 1
			fail.Store(fault != "none")
			err = run(ctx, o)
			if fault != "none" {
				if err == nil {
					t.Fatal("interruption not reported")
				}
				pending := load()
				if len(pending.Native.Combines) != 1 || pending.Native.Combines[0].Receipt != nil || !bytes.Equal(pending.Native.Done, before.Native.Done) {
					t.Fatal("lost durable selection intent")
				}
				// Simulate termination halfway through the following journal row.
				f, err := os.OpenFile(o.state, os.O_APPEND|os.O_WRONLY, 0600)
				if err != nil {
					t.Fatal(err)
				}
				_, err = f.WriteString("{\"Operation\":")
				if err != nil {
					t.Fatal(err)
				}
				f.Close()
				unavailable.Store(fault == "lost-ack")
				if err = run(ctx, o); err != nil {
					t.Fatal("resume", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			after := load()
			gotReceipt, _ := json.Marshal(after.Native.Receipt)
			if !bytes.Equal(oldReceipt, gotReceipt) || after.Phase != "done" || !after.Native.complete() || len(after.Native.Combines) != 1 || after.Native.Combines[0].Receipt == nil || after.Native.Combines[0].Receipt.CopiedCount != 2 {
				t.Fatal("receipt/completion not preserved")
			}
			data, _ := os.ReadFile(o.state)
			if !bytes.HasPrefix(data, prefix) {
				t.Fatal("rewrote phase1 journal")
			}
			for name, before := range versions {
				after, _ := target.Metadata(ctx, name)
				if before.ID != after.ID || before.Generation != after.Generation || before.Revision != after.Revision {
					t.Fatal("re-copied completed ordinal", name)
				}
			}
			for _, name := range names {
				want, _ := oracle.Snapshot(ctx, name)
				got, _ := target.Snapshot(ctx, name)
				if err := store.EqualSnapshots(want, got); err != nil {
					t.Fatal(name, err)
				}
			}
			stats := target.NativeStats()
			if err := run(ctx, o); err != nil {
				t.Fatal("completed resume", err)
			}
			if target.NativeStats().CopiedNames != stats.CopiedNames || exported.Load() != 2 || metricRPCs.Load() != 0 {
				t.Fatal("re-export or metric fallback", exported.Load(), metricRPCs.Load())
			}
		})
	}
}
