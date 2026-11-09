package downsize

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestAbsentSourceRetryResumeAndJournal(t *testing.T) {
	var absentHeads, pulls atomic.Int32
	var fail atomic.Bool
	fail.Store(true)
	t.Setenv("ABSENT_TEST_TOKEN", "test-token")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Buckyd-Authorization") != "test-token" {
			t.Error("missing authentication")
		}
		if r.Method == http.MethodHead {
			if strings.HasSuffix(r.URL.Path, "/gone") {
				absentHeads.Add(1)
				w.WriteHeader(404)
			}
			return
		}
		if r.URL.Path == "/v1/transfer/list" {
			if r.Method != http.MethodGet || r.URL.Query().Get("prefix") != "gone" || r.URL.Query().Get("limit") != "1" {
				t.Error("missing exact bounded catalog check", r.Method, r.URL.RawQuery)
			}
			_, _ = w.Write([]byte(`[]`))
			return
		}
		if r.URL.Path == "/v1/transfer/verify" {
			_, _ = w.Write([]byte(`{"verified":1}`))
			return
		}
		pulls.Add(1)
		var req struct{ Names []string }
		_ = json.NewDecoder(r.Body).Decode(&req)
		for _, n := range req.Names {
			if n == "gone" {
				http.Error(w, "source export: 404 Not Found", 404)
				return
			}
		}
		if fail.Swap(false) {
			http.Error(w, "interrupted after durable skip", 500)
			return
		}
		// Exercise HTTP413 splitting after the skip, preserving original
		// ordinals and avoiding overlapping parent/child journal ranges.
		if len(req.Names) > 1 {
			http.Error(w, "split", 413)
			return
		}
		var proofs []chunkstore.TransferProof
		for _, name := range req.Names {
			proofs = append(proofs, chunkstore.TransferProof{Source: chunkstore.TransferVersion{Name: name}, Target: chunkstore.TransferVersion{Name: name}, Digest: "digest"})
		}
		_ = json.NewEncoder(w).Encode(proofs)
	}))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "state")
	j, err := openJournal(path, "plan")
	if err != nil {
		t.Fatal(err)
	}
	m := mover{options: options{tokenEnv: "ABSENT_TEST_TOKEN"}, client: server.Client(), tuner: newTestTuner([]string{server.URL}, capacity{1, 100, 1 << 20, 1}, false, 0, 0), journal: j}
	job := task{Stream: "stream", Start: 0, End: 3, Source: server.URL, Target: server.URL, Names: []string{"a", "gone", "b"}}
	if err := m.execute(context.Background(), job); err == nil {
		t.Fatal("expected interruption")
	}
	if len(j.absent) != 1 {
		t.Fatal("skip not recorded", j.absent)
	}
	if err := j.saveAdmission(&admissionState{Policy: defaultBacklogPolicy(), Hosts: map[string]hostAdmission{"test": {Baseline: &backlogBaseline{Min: 10, Median: 20, Max: 30, Samples: 10, Start: 1, End: 301}}}}); err != nil {
		t.Fatal(err)
	}
	if err := j.compact(); err != nil {
		t.Fatal(err)
	}
	j.close()
	j, err = openJournal(path, "plan")
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	m.journal = j
	if j.admission == nil || j.admission.Hosts["test"].Baseline.Max != 30 {
		t.Fatal("compaction lost admission while preserving absent-source skips", j.admission)
	}
	for _, skip := range j.absent {
		if skip.Count != 1 || len(skip.Names) != 1 || skip.Names[0] != "gone" {
			t.Fatal(skip)
		}
	}
	if err := m.execute(context.Background(), job); err != nil {
		t.Fatal("resume", err)
	}
	if absentHeads.Load() != 1 {
		t.Fatal("resume repeated absent HEAD", absentHeads.Load())
	}
	for _, task := range j.ranges("stream") {
		if task.Phase != "done" {
			t.Fatal(task)
		}
	}
	// Directly check completed range coverage: skips never fabricate a
	// parent range that overlaps the HTTP413 children.
	ranges := j.ranges("stream")
	if len(ranges) < 2 {
		t.Fatal("split not exercised", ranges)
	}
	for i := 1; i < len(ranges); i++ {
		if ranges[i-1].End > ranges[i].Start {
			t.Fatal("overlapping resume ranges", ranges)
		}
	}
}

func TestAbsentSourceBoundsPersistAcrossResume(t *testing.T) {
	for _, total := range []bool{false, true} {
		t.Run(fmt.Sprint(total), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/transfer/list" {
					_, _ = w.Write([]byte(`[]`))
					return
				}
				w.WriteHeader(404)
			}))
			defer server.Close()
			path := filepath.Join(t.TempDir(), "state")
			j, err := openJournal(path, "plan")
			if err != nil {
				t.Fatal(err)
			}
			m := mover{client: server.Client(), journal: j}
			if total {
				// Model already persisted skips at the total ceiling.
				j.absent["prior"] = absentSourceSkip{Source: "prior", Stream: "prior", End: 1000000, BatchMetrics: 1000000, Count: 10000, Names: []string{"prior"}}
			}
			names := []string{"gone1", "gone2"}
			batch := task{Stream: "stream", End: 100, Source: server.URL, Names: make([]string, 100)}
			if total {
				names = names[:1]
			}
			err = m.skipAbsentSource(context.Background(), batch, names)
			if err == nil || !strings.Contains(err.Error(), "bound exceeded") {
				t.Fatal("bound did not stop", err)
			}
			if err := j.compact(); err != nil {
				t.Fatal(err)
			}
			j.close()
			j, err = openJournal(path, "plan")
			if err != nil {
				t.Fatal(err)
			}
			defer j.close()
			if err := j.absentError(); err == nil {
				t.Fatal("resume forgot bound")
			}
		})
	}
}

func TestAbsentSourceOtherErrorsAreFatal(t *testing.T) {
	for _, status := range []int{200, 403, 500} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(status) }))
			defer server.Close()
			j, err := openJournal(filepath.Join(t.TempDir(), "state"), "plan")
			if err != nil {
				t.Fatal(err)
			}
			defer j.close()
			m := mover{client: server.Client(), journal: j}
			err = m.skipAbsentSource(context.Background(), task{Source: server.URL, End: 1, Names: []string{"name"}}, []string{"name"})
			if err == nil || len(j.absent) != 0 {
				t.Fatal("silently skipped other error", err, j.absent)
			}
			if status != 200 {
				var api *apiError
				if !errors.As(err, &api) || api.Status != status {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestSingleAbsentSourceCompletesRange(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path == "/v1/transfer/list" {
			_, _ = w.Write([]byte(`[]`))
			return
		}
		w.WriteHeader(404)
	}))
	defer server.Close()
	j, err := openJournal(filepath.Join(t.TempDir(), "state"), "plan")
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	m := mover{client: server.Client(), journal: j, tuner: newTestTuner([]string{server.URL}, capacity{1, 1, 1 << 20, 1}, false, 0, 0)}
	job := task{Stream: "stream", End: 1, Source: server.URL, Target: server.URL, Names: []string{"gone"}}
	if err := m.execute(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 3 || j.tasks[taskKey(job)].Phase != "done" {
		t.Fatal("absent singleton imported or range incomplete", requests.Load(), j.tasks)
	}
}

func TestAbsentSourceRequiresFreshCatalogProof(t *testing.T) {
	t.Setenv("ABSENT_TEST_TOKEN", "catalog-test-token")
	// Exercise path and query escaping independently; these characters must
	// arrive as the exact metric name, not as URL syntax.
	const name = "team.metric+?#% space.β"
	page := func(names ...string) string {
		data, err := json.Marshal(names)
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	for _, tc := range []struct {
		name, body, wantError string
		status                int
	}{
		{"empty", `[]`, "", 200},
		{"longer prefix", page(name + ".child"), "", 200},
		{"longer legacy prefix", page(name + "/legacy"), "", 200},
		{"exact present", page(name), "still lists that exact name", 200},
		{"unrelated prefix", page("other.metric"), "does not honor the exact prefix", 200},
		{"ignored limit", page(name+".a", name+".b"), "invalid bounded JSON array", 200},
		{"null", `null`, "invalid bounded JSON array", 200},
		{"object", `{}`, "invalid bounded JSON array", 200},
		{"null member", `[null]`, "does not honor the exact prefix", 200},
		{"truncated", `[`, "invalid bounded JSON array", 200},
		{"invalid UTF8", `["` + name + string([]byte{0xff}) + `"]`, "invalid bounded JSON array", 200},
		{"trailing response", `[] []`, "invalid bounded JSON array", 200},
		{"oversized", strings.Repeat(" ", (1<<20)+1), "invalid bounded JSON array", 200},
		{"unsupported prefix", `prefix unsupported`, "HTTP 400", 400},
		{"missing endpoint", `not found`, "HTTP 404", 404},
		{"unauthorized", `forbidden`, "HTTP 403", 403},
		{"storage failure", `read failed`, "HTTP 500", 500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var heads, catalogs atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("X-Buckyd-Authorization") != "catalog-test-token" {
					t.Error("missing source authentication")
				}
				if r.Method == http.MethodHead {
					heads.Add(1)
					if r.URL.Path != "/metrics/"+name {
						t.Error("changed HEAD metric name", r.URL.Path)
					}
					w.WriteHeader(http.StatusNotFound)
					return
				}
				catalogs.Add(1)
				if r.Method != http.MethodGet || r.URL.Path != "/v1/transfer/list" || r.URL.Query().Get("prefix") != name || r.URL.Query().Get("limit") != "1" || r.URL.Query().Get("after") != "" {
					t.Error("changed catalog scope", r.Method, r.URL.String())
				}
				if r.Header.Get("Cache-Control") != "no-cache, no-store" {
					t.Error("catalog proof did not require a fresh read")
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			j, err := openJournal(filepath.Join(t.TempDir(), "state"), "plan")
			if err != nil {
				t.Fatal(err)
			}
			defer j.close()
			m := mover{options: options{tokenEnv: "ABSENT_TEST_TOKEN"}, client: server.Client(), journal: j}
			batch := task{Source: server.URL, Stream: "stream", End: 1, Names: []string{name}}
			err = m.skipAbsentSource(context.Background(), batch, batch.Names)
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) || len(j.absent) != 0 {
					t.Fatal("invalid absence proof was accepted", err, j.absent)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				skip := j.absent[absentSourceSkip{Source: batch.Source, Stream: batch.Stream, End: 1}.key()]
				if skip.Count != 1 || len(skip.Names) != 1 || skip.Names[0] != name {
					t.Fatal("lost exact confirmed source name", skip)
				}
			}
			if heads.Load() != 1 || catalogs.Load() != 1 {
				t.Fatal("unexpected proof requests", heads.Load(), catalogs.Load())
			}
		})
	}
}

func TestAbsentSourceRecreatedBetweenHeadAndCatalog(t *testing.T) {
	var recreated atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNotFound)
			recreated.Store(true)
			return
		}
		if !recreated.Load() {
			t.Error("catalog was not read after HEAD")
		}
		_, _ = w.Write([]byte(`["recreated"]`))
	}))
	defer server.Close()
	j, err := openJournal(filepath.Join(t.TempDir(), "state"), "plan")
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	m := mover{client: server.Client(), journal: j}
	err = m.skipAbsentSource(context.Background(), task{Source: server.URL, End: 1, Names: []string{"recreated"}}, []string{"recreated"})
	if err == nil || !strings.Contains(err.Error(), "still lists that exact name") || len(j.absent) != 0 {
		t.Fatal("recreated source was skipped", err, j.absent)
	}
}

func TestAbsentSourceRejectsRedirectsAndPresentHead(t *testing.T) {
	for _, phase := range []string{"HEAD redirect", "catalog redirect", "HEAD present"} {
		t.Run(phase, func(t *testing.T) {
			var catalogs, redirected atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/redirected" {
					redirected.Add(1)
					_, _ = w.Write([]byte(`[]`))
					return
				}
				if r.Method == http.MethodHead {
					if phase == "HEAD present" {
						return
					}
					if phase == "HEAD redirect" {
						http.Redirect(w, r, "/redirected", http.StatusFound)
						return
					}
					w.WriteHeader(http.StatusNotFound)
					return
				}
				catalogs.Add(1)
				http.Redirect(w, r, "/redirected", http.StatusFound)
			}))
			defer server.Close()
			j, err := openJournal(filepath.Join(t.TempDir(), "state"), "plan")
			if err != nil {
				t.Fatal(err)
			}
			defer j.close()
			// server.Client follows redirects by default; the safety request
			// must retain its pool but never follow a redirected proof.
			m := mover{client: server.Client(), journal: j}
			err = m.skipAbsentSource(context.Background(), task{Source: server.URL, End: 1, Names: []string{"name"}}, []string{"name"})
			if err == nil || len(j.absent) != 0 || redirected.Load() != 0 {
				t.Fatal("redirect or present HEAD authorized skip", err, redirected.Load(), j.absent)
			}
			if phase != "catalog redirect" && catalogs.Load() != 0 {
				t.Fatal("checked catalog without a missing HEAD", catalogs.Load())
			}
		})
	}
}
