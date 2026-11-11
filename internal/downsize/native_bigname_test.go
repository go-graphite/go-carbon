package downsize

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestNativeControllerJournalsSizeSkipsAndResumes(t *testing.T) {
	ctx := context.Background()
	source, target := nativeControllerStore(t), nativeControllerStore(t)
	names := []string{"skip.a", "skip.large", "skip.z"}
	for _, name := range names {
		count := 1000
		if name == names[1] {
			count = 1 << 26
		}
		cfg := store.MetricConfig{Name: name, AggregationMethod: store.Sum, Retentions: []store.Retention{{Step: 1, Count: count}}}
		if _, err := source.Create(ctx, cfg); err != nil {
			t.Fatal(err)
		}
		if err := source.UpdateMany(ctx, name, []store.Point{{Timestamp: 99900, Value: 42}}); err != nil {
			t.Fatal(err)
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
	o := testOptions(plan)
	o.state = filepath.Join(plan, "state")
	o.delete = false
	o.deleteMode = "auto"
	o.engine = "bulk"
	o.bulkOnly = true
	for attempt := range 2 {
		if err := run(ctx, o); err != nil {
			t.Fatal(err)
		}
		j, err := openJournal(o.state, p.PlanID+":delete=false:mode=auto")
		if err != nil {
			t.Fatal(err)
		}
		var found bool
		for _, task := range j.tasks {
			if task.Native == nil {
				continue
			}
			r := task.Native.Receipt
			if len(r.Skipped) != 1 || r.Skipped[0].Name != names[1] || r.Skipped[0].ArchiveSlots != 1<<26 || r.CopiedCount != 2 || task.Native.done(1) {
				t.Fatal("journal lost size skip", r)
			}
			found = true
		}
		j.close()
		if !found {
			t.Fatal("missing journaled skip", attempt)
		}
	}
	if exports.Load() != 1 {
		t.Fatal("resume reexported acknowledged skip", exports.Load())
	}
	for _, name := range []string{names[0], names[2]} {
		if _, err := target.Metadata(ctx, name); err != nil {
			t.Fatal(err)
		}
	}
}
