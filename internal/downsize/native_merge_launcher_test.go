package downsize

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// The merge launcher adopts this original journal domain and supplies precisely
// engine=bulk, bulk-only, delete=false: existing archives must remain untouched.
func TestMergeLauncher100ExistingNamesAndInterruptedResume(t *testing.T) {
	ctx := context.Background()
	source, target := nativeControllerStore(t), nativeControllerStore(t)
	names := make([]string, 100)
	want := make(map[string]store.Snapshot)
	existing := make(map[string]store.Snapshot)
	for i := range names {
		names[i] = fmt.Sprintf("merge.launcher.%03d", i)
		addPoints(t, source, names[i], float64(i))
		var err error
		want[names[i]], err = source.Snapshot(ctx, names[i])
		if err != nil {
			t.Fatal(err)
		}
		if i%10 == 0 {
			addPoints(t, target, names[i], -float64(i+1))
			existing[names[i]], err = target.Snapshot(ctx, names[i])
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	var forbidden, pulls atomic.Int32
	guard := func(handler http.Handler, interrupt bool) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodDelete && !strings.HasPrefix(r.URL.Path, "/v1/native/source/") {
				forbidden.Add(1)
				t.Errorf("data DELETE: %s", r.URL.Path)
			}
			if r.URL.Path == "/v1/native/pull" && interrupt && pulls.Add(1) == 1 {
				// Lose the response after the target durably publishes the range.
				recorder := httptest.NewRecorder()
				handler.ServeHTTP(recorder, r)
				if recorder.Code != http.StatusOK {
					t.Errorf("first pull: %d %s", recorder.Code, recorder.Body.String())
				}
				http.Error(w, "interrupted", http.StatusServiceUnavailable)
				return
			}
			handler.ServeHTTP(w, r)
		})
	}
	donor := httptest.NewServer(guard(nativeControllerService(t, source), false))
	defer donor.Close()
	destination := httptest.NewServer(guard(nativeControllerService(t, target), true))
	defer destination.Close()
	plan := nativeMergeGeneratedPlan(t, donor.URL, destination.URL, names)
	parsed, err := readPlanStructure(plan, false)
	if err != nil {
		t.Fatal(err)
	}
	o := testOptions(plan)
	o.engine = "bulk"
	o.bulkOnly = true
	o.delete = false
	o.deleteMode = "auto"
	o.state = filepath.Join(plan, "original-state.jsonl")
	header := journalRow{Operation: strings.Repeat("b", 64), Plan: parsed.PlanID + ":delete=false:mode=auto"}
	data, err := json.Marshal(header)
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, '\n')
	if err = os.WriteFile(o.state, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err = run(ctx, o); err == nil {
		t.Fatal("interruption did not abort")
	}
	before, err := os.ReadFile(o.state)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(before), string(data)) {
		t.Fatal("original header changed")
	}
	if err = run(ctx, o); err != nil {
		t.Fatal("resume", err)
	}
	for _, name := range names {
		got, err := target.Snapshot(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		expected := want[name]
		if old, ok := existing[name]; ok {
			expected = old
		}
		if err = store.EqualSnapshots(expected, got); err != nil {
			t.Fatal(name, err)
		}
	}
	count := pulls.Load()
	if err = run(ctx, o); err != nil {
		t.Fatal("second resume", err)
	}
	if pulls.Load() != count {
		t.Fatal("completed absent pass was replayed")
	}
	after, err := os.ReadFile(o.state)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(after), string(before)) {
		t.Fatal("resume changed journal history")
	}
	if forbidden.Load() != 0 {
		t.Fatal("data deletion issued")
	}
}
