package downsize

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/buckyd"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func nativeControllerStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open("/db", store.Options{ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20, SyncInterval: time.Hour}.WithFS(vfs.NewMem()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}
func nativeControllerService(t *testing.T, s *store.Store) http.Handler {
	t.Helper()
	service, err := buckyd.New(buckyd.Config{TmpDir: t.TempDir(), MaxTransfers: 2, MaxBodyBytes: 1 << 20, TransferSources: []string{"127.0.0.1"}}, s)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Stop() })
	return service.Handler()
}
func TestNativeControllerSharedJournalAndReviewedFallback(t *testing.T) {
	ctx := context.Background()
	source, target, reference := nativeControllerStore(t), nativeControllerStore(t), nativeControllerStore(t)
	var names []string
	for i := range 19 {
		name := fmt.Sprintf("native.controller.%02d", i)
		names = append(names, name)
		addPoints(t, source, name, float64(i))
		if i%3 == 0 {
			for _, s := range []*store.Store{target, reference} {
				addPoints(t, s, name, 100)
			}
		}
	}
	var exports atomic.Int64
	sourceHandler := nativeControllerService(t, source)
	donor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/native/export" {
			exports.Add(1)
		}
		sourceHandler.ServeHTTP(w, r)
	}))
	defer donor.Close()
	destination := httptest.NewServer(nativeControllerService(t, target))
	defer destination.Close()
	plan := fixturePlan(t, donor.URL, destination.URL, names)
	p, err := readPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	// A range already completed by the metric engine is never bulk-copied.
	records, err := source.ExportTransfer(ctx, names[:1])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.ImportTransfer(ctx, records); err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(plan, "state")
	j, err := openJournal(state, p.PlanID+":delete=false:mode=auto")
	if err != nil {
		t.Fatal(err)
	}
	// A running metric controller already has a durable admission envelope.
	baseline := &mover{options: testOptions(plan), journal: j, planData: p, client: http.DefaultClient, tuner: newTuner([]string{donor.URL, destination.URL}, capacity{2, 8, 1 << 20, 100000}, false, 0, 0)}
	baseline.tuner.backlog = defaultBacklogPolicy()
	if err := baseline.prepareAdmission(ctx, []string{donor.URL, destination.URL}); err != nil {
		t.Fatal(err)
	}
	if err := j.save(task{Stream: p.Streams[0].SHA256, End: 1, Source: donor.URL, Target: destination.URL, Phase: "done"}); err != nil {
		t.Fatal(err)
	}
	j.close()
	o := testOptions(plan)
	o.state = state
	o.delete = false
	o.deleteMode = "auto"
	o.engine = "bulk"
	o.bulkOnly = true
	if err := run(ctx, o); err != nil {
		t.Fatal(err)
	}
	j, err = openJournal(state, p.PlanID+":delete=false:mode=auto")
	if err != nil {
		t.Fatal(err)
	}
	var native task
	for _, task := range j.tasks {
		if task.Native != nil {
			native = task
		}
	}
	j.close()
	if native.Native == nil || native.Native.Intent || native.Native.Receipt.CopiedCount != 12 || native.Native.Receipt.DeferredCount != 6 {
		t.Fatal("native bitmap", native.Native)
	}
	if exports.Load() != 1 {
		t.Fatal("unexpected native exports", exports.Load())
	}
	// Native retry skips the acknowledged range, including its deferred names.
	if err := run(ctx, o); err != nil {
		t.Fatal(err)
	}
	if exports.Load() != 1 {
		t.Fatal("completed native range exported twice")
	}
	before, err := target.Snapshot(ctx, names[1])
	if err != nil {
		t.Fatal(err)
	}
	version, err := target.Metadata(ctx, names[1])
	if err != nil {
		t.Fatal(err)
	}
	o.engine = "metric"
	o.bulkOnly = false
	if err := run(ctx, o); err != nil {
		t.Fatal("reviewed fallback", err)
	}
	after, err := target.Snapshot(ctx, names[1])
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EqualSnapshots(before, after); err != nil {
		t.Fatal("native name copied again", err)
	}
	current, err := target.Metadata(ctx, names[1])
	if err != nil {
		t.Fatal(err)
	}
	if current.ID != version.ID || current.Generation != version.Generation {
		t.Fatal("native identity replaced by fallback")
	}
	records, err = source.ExportTransfer(ctx, names)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reference.ImportTransfer(ctx, records); err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		want, _ := reference.Snapshot(ctx, name)
		got, _ := target.Snapshot(ctx, name)
		if err := store.EqualSnapshots(want, got); err != nil {
			t.Fatal(name, err)
		}
	}
	j, err = openJournal(state, p.PlanID+":delete=false:mode=auto")
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	for _, task := range j.tasks {
		if task.Phase != "done" {
			t.Fatal("unfinished range", task)
		}
	}
}

func TestNativeControllerLostAckCannotReplayThroughMetricPath(t *testing.T) {
	ctx := context.Background()
	source, target := nativeControllerStore(t), nativeControllerStore(t)
	names := []string{"native.lost.ack"}
	addPoints(t, source, names[0], 42)
	donor := httptest.NewServer(nativeControllerService(t, source))
	defer donor.Close()
	var lost atomic.Bool
	targetHandler := nativeControllerService(t, target)
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/native/pull" && !lost.Swap(true) {
			recorder := httptest.NewRecorder()
			targetHandler.ServeHTTP(recorder, r)
			if recorder.Code != 200 {
				t.Error(recorder.Body.String())
			}
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
			} else {
				conn.Close()
			}
			return
		}
		targetHandler.ServeHTTP(w, r)
	}))
	defer destination.Close()
	plan := fixturePlan(t, donor.URL, destination.URL, names)
	o := testOptions(plan)
	o.delete = false
	o.deleteMode = "auto"
	o.engine = "bulk"
	o.bulkOnly = true
	if err := run(ctx, o); err == nil {
		t.Fatal("lost native ack not reported")
	}
	p, err := readPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	j, err := openJournal(o.state, p.PlanID+":delete=false:mode=auto")
	if err != nil {
		t.Fatal(err)
	}
	pending := j.ranges(p.Streams[0].SHA256)[0]
	j.close()
	if pending.Native == nil || !pending.Native.Intent || pending.Phase != "verified" || len(pending.Proofs) != 0 {
		t.Fatal("durable intent missing")
	}
	// A newer live point must survive recovery through the metric-mode handoff.
	if err := target.UpdateMany(ctx, names[0], []store.Point{{Timestamp: testMetricNow, Value: 123}}); err != nil {
		t.Fatal(err)
	}
	before, _ := target.Snapshot(ctx, names[0])
	version, _ := target.Metadata(ctx, names[0])
	o.engine = "metric"
	o.bulkOnly = false
	if err := run(ctx, o); err != nil {
		t.Fatal("receipt recovery", err)
	}
	after, _ := target.Snapshot(ctx, names[0])
	current, _ := target.Metadata(ctx, names[0])
	if err := store.EqualSnapshots(before, after); err != nil {
		t.Fatal(err)
	}
	if current.ID != version.ID {
		t.Fatal("lost ack caused a second import")
	}
}

func TestNativeCompletedStreamDoesNotReadPlanRows(t *testing.T) {
	// A fully covered stream needs neither its rows nor any network access.
	// Deliberately omit planData/client: attempting the old walk would fail.
	c := &nativeController{mover: &mover{journal: &journal{tasks: map[string]task{
		"first":  {Stream: "stream", Start: 0, End: 5, Phase: "done"},
		"second": {Stream: "stream", Start: 5, End: 10, Kind: "native-range", Phase: "verified", Native: &nativeRangeState{}},
	}}}}
	if err := c.route(context.Background(), route{SHA256: "stream", Metrics: 10}); err != nil {
		t.Fatal(err)
	}
	if nativeStreamCovered([]task{{Start: 0, End: 5}, {Start: 6, End: 10}}, 10) {
		t.Fatal("skipped ordinal gap")
	}
	if nativeStreamCovered([]task{{Start: 0, End: 10, Native: &nativeRangeState{Intent: true}}}, 10) {
		t.Fatal("skipped unresolved native acknowledgement")
	}
}
