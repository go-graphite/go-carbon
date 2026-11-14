package downsize

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestNativeWindowControllerResume(t *testing.T) {
	ctx := context.Background()
	source, target := nativeControllerStore(t), nativeControllerStore(t)
	names := []string{"window.client"}
	addPoints(t, source, names[0], 1)
	before, err := source.Snapshot(ctx, names[0])
	if err != nil {
		t.Fatal(err)
	}
	ts := before.Archives[0].Points[0].Timestamp
	before.Archives[0].Points[0].Value = 42
	if _, err := target.CreateFromSnapshot(ctx, before); err != nil {
		t.Fatal(err)
	}
	var pulls atomic.Int32
	donor := httptest.NewServer(nativeControllerService(t, source))
	defer donor.Close()
	handler := nativeControllerService(t, target)
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/native/pull" && pulls.Add(1) == 1 {
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, r)
			if recorder.Code != 200 {
				t.Errorf("pull: %d %s", recorder.Code, recorder.Body.String())
			}
			http.Error(w, "lost acknowledgement", 503)
			return
		}
		handler.ServeHTTP(w, r)
	}))
	defer destination.Close()
	plan := fixturePlan(t, donor.URL, destination.URL, names)
	o := testOptions(plan)
	o.engine = "bulk"
	o.bulkOnly = true
	o.bulkCombine = true
	o.delete = false
	o.deleteMode = "auto"
	o.state = filepath.Join(plan, "window.jsonl")
	o.from = strconv.FormatInt(ts, 10)
	o.until = strconv.FormatInt(ts+60, 10)
	if err := run(ctx, o); err == nil {
		t.Fatal("lost ACK must interrupt")
	}
	data, err := os.ReadFile(o.state)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), ":window="+o.from+","+o.until) {
		t.Fatal("window not journaled")
	}
	changed := o
	changed.until = strconv.FormatInt(ts+120, 10)
	if err := run(ctx, changed); err == nil {
		t.Fatal("changed window accepted")
	}
	o.from = ""
	o.until = "" // resume must inherit the saved endpoints
	if err := run(ctx, o); err != nil {
		t.Fatal(err)
	}
	got, err := target.Snapshot(ctx, names[0])
	if err != nil {
		t.Fatal(err)
	}
	if got.Archives[0].Points[0].Value != 42 {
		t.Fatal("target precedence lost", got)
	}
	count := pulls.Load()
	if err := run(ctx, o); err != nil {
		t.Fatal(err)
	}
	if pulls.Load() != count {
		t.Fatal("completed range replayed")
	}
}

func TestNativeWindowParsingAndPlanBinding(t *testing.T) {
	for _, v := range [][2]string{{"1", ""}, {"2", "1"}, {"-1", "1"}, {"2026-01-01T00:00:00.1Z", "2026-01-02T00:00:00Z"}} {
		if _, err := parseNativeWindow(v[0], v[1]); err == nil {
			t.Fatal(v)
		}
	}
	w, err := parseNativeWindow("2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z")
	if err != nil || w.Until-w.From != 3600 {
		t.Fatal(w, err)
	}
	p := &manifest{Spec: specification{NativeWindow: &store.NativeWindow{From: 100, Until: 200}}}
	o := options{}
	if err := resolveNativeWindow(&o, p); err != nil || !store.SameNativeWindow(o.nativeWindow, p.Spec.NativeWindow) {
		t.Fatal(o.nativeWindow, err)
	}
	o.from = "100"
	o.until = "201"
	if err := resolveNativeWindow(&o, p); err == nil {
		t.Fatal("changed plan window accepted")
	}
}

func TestNativeWindowCommittedPlan(t *testing.T) {
	root := t.TempDir()
	names := filepath.Join(root, "names.txt")
	if err := os.WriteFile(names, []byte("window.plan\n"), 0600); err != nil {
		t.Fatal(err)
	}
	node := func(id, endpoint string) map[string]any {
		return map[string]any{"hash": "jump_fnv1a", "replicas": 1, "nodes": []map[string]string{{"id": id, "node": id + ":2003=000", "endpoint": endpoint}}}
	}
	spec := map[string]any{"version": 1, "cluster": "source", "target_cluster": "target", "site": "test", "old": node("old", "http://127.0.0.1:4242"), "new": node("new", "http://127.0.0.1:4243"), "inputs": []map[string]string{{"source": "old", "path": names, "format": "lines"}}}
	data, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "spec.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	plan := filepath.Join(root, "plan")
	if err := Main([]string{"plan", "--spec", path, "--out", plan, "--apply", "--from", "100", "--until", "200"}); err != nil {
		t.Fatal(err)
	}
	p, err := readPlanStructure(plan, false)
	if err != nil {
		t.Fatal(err)
	}
	if p.Spec.NativeWindow == nil || *p.Spec.NativeWindow != (store.NativeWindow{From: 100, Until: 200}) {
		t.Fatal("plan lost endpoints", p.Spec.NativeWindow)
	}
	o := testOptions(plan)
	o.engine = "metric"
	if err := run(context.Background(), o); err == nil {
		t.Fatal("metric fallback accepted window plan")
	}
	manifestPath := filepath.Join(plan, "manifest.json")
	data, err = os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	altered := strings.Replace(string(data), `"Until": 200`, `"Until": 201`, 1)
	if altered == string(data) {
		altered = strings.Replace(string(data), `"Until":200`, `"Until":201`, 1)
	}
	if altered == string(data) {
		t.Fatal("missing serialized window")
	}
	if err := os.WriteFile(manifestPath, []byte(altered), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readPlanStructure(plan, false); err == nil {
		t.Fatal("changed window digest accepted")
	}
}

func TestNativeWindowRefusesPinnedMergeSession(t *testing.T) {
	names := []string{"window.merge.refusal"}
	plan := nativeMergeGeneratedPlan(t, "http://127.0.0.1:4242", "http://127.0.0.1:4243", names)
	o := testOptions(plan)
	o.engine, o.bulkOnly, o.mergeSession = "bulk", true, "history"
	o.delete = false
	o.from, o.until = "1970-01-01T00:00:01Z", "1970-01-01T00:00:02Z"
	if err := run(context.Background(), o); err == nil || !strings.Contains(err.Error(), "windowed plans") {
		t.Fatal("windowed pinned merge was not refused before contacting peers", err)
	}
}
