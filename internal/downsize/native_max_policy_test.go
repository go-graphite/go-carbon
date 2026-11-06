package downsize

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func nativeMaximumApproval(t *testing.T, plan string) {
	t.Helper()
	path, sum := maxDecisionFixture(t)
	t.Setenv("RESIZE_MAX_SPEED_DECISION", path)
	t.Setenv("RESIZE_MAX_SPEED_DECISION_SHA256", sum)
	manifest, err := os.ReadFile(filepath.Join(plan, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var p struct {
		PlanID string `json:"plan_id"`
	}
	if err := json.Unmarshal(manifest, &p); err != nil {
		t.Fatal(err)
	}
	sha := sha256.Sum256(manifest)
	raw, _ := json.Marshal(map[string]any{"approved_plan_id": p.PlanID, "approved_plan_sha256": hex.EncodeToString(sha[:]), "phase": "copy-only", "delete": false})
	approval := filepath.Join(plan, "MAX-WRITE-OK.json")
	if err := os.WriteFile(approval, raw, 0600); err != nil {
		t.Fatal(err)
	}
	sha = sha256.Sum256(raw)
	t.Setenv("RESIZE_MAX_SPEED_WRITE_OK", approval)
	t.Setenv("RESIZE_MAX_SPEED_WRITE_OK_SHA256", hex.EncodeToString(sha[:]))
}

func TestNativeMixedMAXOnlyDeferredPreservesNormalsAndUnknownAudit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	source, target, reference := nativeControllerStore(t), nativeControllerStore(t), nativeControllerStore(t)
	var names []string
	for i := range 9 {
		name := fmt.Sprintf("native.max.%02d", i)
		names = append(names, name)
		addPoints(t, source, name, float64(i))
		if i%3 == 0 {
			addPoints(t, target, name, 100)
			addPoints(t, reference, name, 100)
		}
	}
	// A real missing source name is deferred by native and requires both fresh
	// authenticated HEAD404 and exact-prefix catalog absence in MAX mode.
	names = append(names, "native.max.absent")
	donor := httptest.NewServer(nativeControllerService(t, source))
	defer donor.Close()
	targetHandler := nativeControllerService(t, target)
	var mu sync.Mutex
	imported := map[string]int{}
	var failVerify atomic.Bool
	failVerify.Store(true)
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/transfer/pull" {
			raw, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
				return
			}
			var req struct{ Names []string }
			if err := json.Unmarshal(raw, &req); err != nil {
				t.Error(err)
				return
			}
			recorder := httptest.NewRecorder()
			r.Body = io.NopCloser(bytes.NewReader(raw))
			targetHandler.ServeHTTP(recorder, r)
			if recorder.Code == 200 {
				mu.Lock()
				for _, name := range req.Names {
					imported[name]++
				}
				mu.Unlock()
			}
			for key, values := range recorder.Header() {
				for _, value := range values {
					w.Header().Add(key, value)
				}
			}
			w.WriteHeader(recorder.Code)
			_, _ = w.Write(recorder.Body.Bytes())
			return
		}
		if r.URL.Path == "/v1/transfer/verify" && failVerify.Swap(false) {
			http.Error(w, "temporary verify transport boundary", 503)
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
	state := filepath.Join(plan, "state")
	id := p.PlanID + ":delete=false:mode=auto"
	j, err := openJournal(state, id)
	if err != nil {
		t.Fatal(err)
	}
	operation := j.operation
	frozen := &backlogBaseline{Start: 100, End: 400, Samples: 60, Min: 111, Median: 122, Max: 123}
	admission := &admissionState{Policy: defaultBacklogPolicy(), Hosts: map[string]hostAdmission{}}
	for _, host := range []string{donor.URL, destination.URL} {
		admission.Hosts[host] = hostAdmission{Baseline: frozen, DebtBaseline: &backlogBaseline{Max: 456}, LatencyBaseline: &backlogBaseline{Max: 789}, ReadBaseline: 22.7}
	}
	if err := j.saveAdmission(admission); err != nil {
		t.Fatal(err)
	}
	records, err := source.ExportTransfer(ctx, names[:1])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.ImportTransfer(ctx, records); err != nil {
		t.Fatal(err)
	}
	if err := j.save(task{Stream: p.Streams[0].SHA256, End: 1, Source: donor.URL, Target: destination.URL, Phase: "done"}); err != nil {
		t.Fatal(err)
	}
	unknown := []byte(fmt.Sprintf("{\"Operation\":%q,\"Plan\":%q,\"Task\":{},\"UnknownFuturePolicy\":{\"decision\":\"preserve exact bytes\"},\"GuardPolicyUpdate\":{\"sha\":\"old\"}}\n", operation, id))
	if _, err := j.file.Write(unknown); err != nil {
		t.Fatal(err)
	}
	if err := j.file.Sync(); err != nil {
		t.Fatal(err)
	}
	j.close()
	prefix, err := os.ReadFile(state)
	if err != nil {
		t.Fatal(err)
	}
	o := testOptions(plan)
	o.delete = false
	o.deleteMode = "auto"
	o.engine = "bulk"
	o.bulkOnly = true
	o.state = state
	// Native remains governor-free even with metric-mode environment present.
	t.Setenv("RESIZE_MAX_SPEED_DECISION", "does-not-exist")
	t.Setenv("RESIZE_HOST_JWT_REFRESH", "1")
	if err := run(ctx, o); err != nil {
		t.Fatal("native unexpectedly entered metric policy", err)
	}
	t.Setenv("RESIZE_HOST_JWT_REFRESH", "")
	j, err = openJournal(state, id)
	if err != nil {
		t.Fatal(err)
	}
	var native task
	for _, task := range j.tasks {
		if task.Native != nil {
			native = task
		}
	}
	if native.Native == nil || native.Native.Receipt.CopiedCount != 6 || native.Native.Receipt.DeferredCount != 3 {
		t.Fatal("wrong mixed receipt", native.Native)
	}
	var protected []string
	for i := range native.Native.Receipt.Count {
		if native.Native.Receipt.IsCopied(i) {
			protected = append(protected, names[int(native.Start)+i])
		}
	}
	j.close()
	before := map[string]store.Snapshot{}
	for _, name := range protected {
		snapshot, err := target.Snapshot(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		before[name] = snapshot
	}
	nativeMaximumApproval(t, plan)
	o.engine = "metric"
	o.bulkOnly = false
	o.tick = time.Second
	if err := run(ctx, o); err != nil {
		t.Fatal("MAX native handoff", err)
	}
	for _, name := range protected {
		after, err := target.Snapshot(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.EqualSnapshots(before[name], after); err != nil {
			t.Fatal("native name replayed", name, err)
		}
	}
	mu.Lock()
	got := map[string]int{}
	for name, n := range imported {
		got[name] = n
	}
	mu.Unlock()
	if !reflect.DeepEqual(got, map[string]int{names[3]: 1, names[6]: 1}) {
		t.Fatal("only native deferrals may import; verify retry must use its saved proof", got)
	}
	records, err = source.ExportTransfer(ctx, names[:9])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reference.ImportTransfer(ctx, records); err != nil {
		t.Fatal(err)
	}
	for _, name := range names[:9] {
		want, err := reference.Snapshot(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		actual, err := target.Snapshot(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.EqualSnapshots(want, actual); err != nil {
			t.Fatal(name, err)
		}
	}
	if err := run(ctx, o); err != nil {
		t.Fatal("completed MAX resume", err)
	}
	mu.Lock()
	if !reflect.DeepEqual(got, imported) {
		t.Error("completed resume imported again", imported)
	}
	mu.Unlock()
	j, err = openJournal(state, id)
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	if j.operation != operation || j.maxSpeedDecisionSHA == "" {
		t.Fatal("identity/policy lost")
	}
	for _, host := range []string{donor.URL, destination.URL} {
		h := j.admission.Hosts[host]
		if !reflect.DeepEqual(h.Baseline, frozen) || h.ReadBaseline != 22.7 || h.DebtBaseline.Max != 456 || h.LatencyBaseline.Max != 789 {
			t.Fatal("old normal replaced", h)
		}
	}
	for _, task := range j.tasks {
		if task.Phase != "done" || task.Native != nil {
			t.Fatal("unfinished native bitmap", task)
		}
	}
	if len(j.absent) != 1 {
		t.Fatal("absence not durably accounted", j.absent)
	}
	for _, skip := range j.absent {
		if !reflect.DeepEqual(skip.CatalogConfirmedNames, []string{names[9]}) {
			t.Fatal("missing fresh exact-name proof", skip)
		}
	}
	if err := j.compact(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(state)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(raw, prefix) || !bytes.Contains(raw, unknown) {
		t.Fatal("unknown policy/original journal rows rewritten")
	}
}

func TestNativeMAXUncertainMetricChildNeverReimportsOnRetryOrResume(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	source, target := nativeControllerStore(t), nativeControllerStore(t)
	names := []string{"native.max.uncertain"}
	addPoints(t, source, names[0], 1)
	addPoints(t, target, names[0], 100)
	donor := httptest.NewServer(nativeControllerService(t, source))
	defer donor.Close()
	handler := nativeControllerService(t, target)
	var pulls atomic.Int32
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/transfer/pull" {
			pulls.Add(1)
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, r)
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
		handler.ServeHTTP(w, r)
	}))
	defer destination.Close()
	plan := fixturePlan(t, donor.URL, destination.URL, names)
	o := testOptions(plan)
	o.engine = "bulk"
	o.bulkOnly = true
	o.delete = false
	o.deleteMode = "auto"
	if err := run(ctx, o); err != nil {
		t.Fatal(err)
	}
	nativeMaximumApproval(t, plan)
	o.engine = "metric"
	o.bulkOnly = false
	for attempt := range 2 {
		err := run(ctx, o)
		if err == nil || !strings.Contains(err.Error(), "uncertain bulk import") {
			t.Fatal("uncertain child not fail-closed", attempt, err)
		}
		if pulls.Load() != 1 {
			t.Fatal("uncertain deferred import replayed", attempt, pulls.Load())
		}
	}
	p, err := readPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	j, err := openJournal(o.state, p.PlanID+":delete=false:mode=auto")
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	for _, parent := range j.tasks {
		if parent.Native == nil || parent.Native.Pending == nil || parent.Native.Pending.Kind != "bulk-intent" || parent.Native.done(0) {
			t.Fatal("uncertain child incorrectly completed", parent)
		}
	}
}

func TestNativeMAXDefinitive429RetriesWithoutHoldingAdmission(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	source, target := nativeControllerStore(t), nativeControllerStore(t)
	names := []string{"native.max.declined"}
	addPoints(t, source, names[0], 1)
	addPoints(t, target, names[0], 100)
	donor := httptest.NewServer(nativeControllerService(t, source))
	defer donor.Close()
	handler := nativeControllerService(t, target)
	var pulls atomic.Int32
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/transfer/pull" && pulls.Add(1) == 1 {
			http.Error(w, "source export: 429 Too Many Requests", 429)
			return
		}
		handler.ServeHTTP(w, r)
	}))
	defer destination.Close()
	plan := fixturePlan(t, donor.URL, destination.URL, names)
	o := testOptions(plan)
	o.engine = "bulk"
	o.bulkOnly = true
	o.delete = false
	o.deleteMode = "auto"
	if err := run(ctx, o); err != nil {
		t.Fatal(err)
	}
	nativeMaximumApproval(t, plan)
	o.engine = "metric"
	o.bulkOnly = false
	if err := run(ctx, o); err != nil {
		t.Fatal("definitively declined child stopped instead of retrying", err)
	}
	if pulls.Load() != 2 {
		t.Fatal("wrong pull attempts", pulls.Load())
	}
	if err := run(ctx, o); err != nil {
		t.Fatal(err)
	}
	if pulls.Load() != 2 {
		t.Fatal("completed child replayed on resume")
	}
}

func TestNativeDeclinedPullRequiresPreImportEvidence(t *testing.T) {
	for _, tc := range []struct {
		code    int
		message string
		safe    bool
	}{
		{429, "endpoint /v1/transfer/pull: HTTP429", true},
		{403, "forbidden", true},
		{404, "endpoint /v1/transfer/pull: HTTP 404 source export: 404 Not Found\n", true},
		{404, "endpoint /v1/transfer/pull: HTTP 404 metadata not found", false},
		{413, "endpoint /v1/transfer/pull: HTTP 413 source export: 413 Request Entity Too Large", true},
		{413, "endpoint /v1/transfer/pull: HTTP 413 importer rejected data", false},
		{500, "source export: internal failure", false},
	} {
		if got := nativeDefinitivePullRejection(&apiError{tc.code, tc.message}); got != tc.safe {
			t.Fatal(tc, got)
		}
	}
}
