package downsize

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestNativeBoundedRetryCountsPendingIntentBeforeNewGap(t *testing.T) {
	for _, lostAck := range []bool{false, true} {
		t.Run(fmt.Sprintf("lost_ack=%t", lostAck), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			source, target := nativeControllerStore(t), nativeControllerStore(t)
			names := []string{"scope.0", "scope.1", "scope.2", "scope.3", "scope.4", "scope.5"}
			for i, name := range names {
				addPoints(t, source, name, float64(i))
			}
			var exports atomic.Int32
			sourceHandler := nativeControllerService(t, source)
			donor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/native/export" {
					exports.Add(1)
				}
				sourceHandler.ServeHTTP(w, r)
			}))
			defer donor.Close()
			targetHandler := nativeControllerService(t, target)
			var pulls atomic.Int32
			destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/native/pull" && pulls.Add(1) == 1 {
					if !lostAck {
						http.Error(w, "store recovery is not finished", 500)
						return
					}
					recorder := httptest.NewRecorder()
					targetHandler.ServeHTTP(recorder, r)
					if recorder.Code != 200 {
						t.Error(recorder.Body.String())
					}
					connection, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
					} else {
						connection.Close()
					}
					return
				}
				targetHandler.ServeHTTP(w, r)
			}))
			defer destination.Close()
			plan := fixturePlan(t, donor.URL, destination.URL, names)
			p, err := readPlan(plan)
			if err != nil {
				t.Fatal(err)
			}
			o := testOptions(plan)
			o.delete = false
			o.deleteMode = "auto"
			o.engine = "bulk"
			o.bulkOnly = true
			o.maxBatches = 1
			j, err := openJournal(o.state, p.PlanID+":delete=false:mode=auto")
			if err != nil {
				t.Fatal(err)
			}
			op := j.operation
			// Two names already acknowledged by the prior metric controller.
			records, err := source.ExportTransfer(ctx, names[:2])
			if err != nil {
				t.Fatal(err)
			}
			if _, err := target.ImportTransfer(ctx, records); err != nil {
				t.Fatal(err)
			}
			if err := j.save(task{Stream: p.Streams[0].SHA256, Start: 0, End: 2, Source: donor.URL, Target: destination.URL, Phase: "done"}); err != nil {
				t.Fatal(err)
			}
			// Simulate the real pre-existing native intent and a subsequent gap.
			request := chunkstore.NativeRange{Operation: op, Plan: p.PlanID, Stream: p.Streams[0].SHA256, Source: donor.URL, Start: 2, End: 4, Names: names[2:4]}
			identity, hash, err := request.Identity()
			if err != nil {
				t.Fatal(err)
			}
			if err := j.save(task{Kind: "native-range", Stream: request.Stream, Start: 2, End: 4, Source: donor.URL, Target: destination.URL, Phase: "verified", Native: &nativeRangeState{Intent: true, Receipt: chunkstore.NativeReceipt{Identity: identity, NamesSHA256: hash, Count: 2}}}); err != nil {
				t.Fatal(err)
			}
			j.close()
			if err := run(ctx, o); err == nil {
				t.Fatal("first recovery refusal/lost ack not reported")
			}
			if _, err := target.NativeReceipt(ctx, request); !lostAck && err == nil {
				t.Fatal("refused range obtained false receipt")
			}
			if err := run(ctx, o); err != nil {
				t.Fatal("pending bounded retry", err)
			}
			if pulls.Load() != 2 {
				t.Fatal("max1 retry also imported another gap", pulls.Load())
			}
			if exports.Load() != 1 {
				t.Fatal("pending range reexported or additional gap admitted", exports.Load())
			}
			for _, name := range names[4:] {
				if _, err := target.Metadata(ctx, name); err == nil {
					t.Fatal("extra gap copied by max1", name)
				}
			}
			j, err = openJournal(o.state, p.PlanID+":delete=false:mode=auto")
			if err != nil {
				t.Fatal(err)
			}
			if j.operation != op {
				t.Fatal("operation changed")
			}
			pending := j.tasks[taskKey(task{Stream: request.Stream, Start: 2, End: 4})]
			j.close()
			if pending.Phase != "done" || pending.Native != nil {
				t.Fatal("pending range not durably acknowledged", pending)
			}
			if err := run(ctx, o); err != nil {
				t.Fatal("next bounded gap", err)
			}
			if pulls.Load() != 3 || exports.Load() != 2 {
				t.Fatal("next run did not copy exactly one gap", pulls.Load(), exports.Load())
			}
			for _, name := range names {
				if _, err := target.Metadata(ctx, name); err != nil {
					t.Fatal(name, err)
				}
			}
			if err := run(ctx, o); err != nil {
				t.Fatal(err)
			}
			if pulls.Load() != 3 || exports.Load() != 2 {
				t.Fatal("completed rerun exported again")
			}
		})
	}
}
