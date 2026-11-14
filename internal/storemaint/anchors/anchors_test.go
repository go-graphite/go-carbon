package anchors

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-graphite/go-carbon/internal/storemaint/remote"
	"github.com/golang-jwt/jwt/v5"
)

func TestResumeAndIdentity(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, err := jwt.Parse(r.Header.Get("X-Buckyd-Authorization"), func(*jwt.Token) (any, error) { return []byte("test-secret"), nil }, jwt.WithValidMethods([]string{"HS256"}))
		if err != nil || !token.Valid {
			t.Error("invalid auth", err)
		}
		ops := token.Claims.(jwt.MapClaims)["ops"].([]any)
		if len(ops) != 1 || ops[0] != "read" {
			t.Error("dry run has write permission", ops)
		}
		if r.URL.Path == "/v1/transfer/list" {
			if r.URL.Query().Get("after") == "" {
				io.WriteString(w, `["a","b"]`)
			} else {
				io.WriteString(w, `[]`)
			}
			return
		}
		calls++
		var body struct {
			Names []string
			Apply bool
		}
		json.NewDecoder(r.Body).Decode(&body)
		if body.Apply {
			t.Error("unexpected apply")
		}
		json.NewEncoder(w).Encode([]any{map[string]any{"name": body.Names[0], "archives": []any{map[string]any{"changed": true}}}})
	}))
	defer srv.Close()
	c := &remote.Client{Endpoint: srv.URL, Secret: []byte("test-secret"), HTTP: srv.Client()}
	id := identity{Endpoint: srv.URL, Mode: "dry-run", Cohort: "catalog"}
	path := filepath.Join(t.TempDir(), "journal")
	if err := run(c, id, path, nil, false, io.Discard); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatal(calls)
	}
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	f.WriteString(`{"partial":`)
	f.Close()
	if err := run(c, id, path, nil, false, io.Discard); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatal("repeated successful work", calls)
	}
	id.Mode = "apply"
	if err := run(c, id, path, nil, false, io.Discard); err == nil {
		t.Fatal("accepted wrong journal identity")
	}
}
func TestBlockedRetriesAndHTTPFailure(t *testing.T) {
	n := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n++
		if n == 3 {
			w.WriteHeader(401)
			return
		}
		io.WriteString(w, `[{"name":"a","archives":[{"blocked":true}]}]`)
	}))
	defer srv.Close()
	c := &remote.Client{Endpoint: srv.URL, Secret: []byte("test-secret"), HTTP: srv.Client()}
	id := identity{Endpoint: srv.URL}
	path := filepath.Join(t.TempDir(), "journal")
	for i := 0; i < 3; i++ {
		err := run(c, id, path, []string{"a"}, true, io.Discard)
		if err == nil {
			t.Fatal("expected blocked/http failure")
		}
	}
	if n != 3 {
		t.Fatal("blocked rows skipped")
	}
}
func TestApplyApproval(t *testing.T) {
	dir := t.TempDir()
	plan := filepath.Join(dir, "plan")
	ok := filepath.Join(dir, "ok")
	os.WriteFile(plan, []byte("reviewed"), 0600)
	os.WriteFile(ok, []byte("unrelated"), 0600)
	err := Main([]string{"-apply", "-plan", plan, "-write-ok", ok, "-secret-file", "absent", "-journal", filepath.Join(dir, "journal")})
	if err == nil || !strings.Contains(err.Error(), "SHA256") {
		t.Fatal(err)
	}
}
func TestBadCatalog(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, `["b","a"]`) }))
	defer srv.Close()
	c := &remote.Client{Endpoint: srv.URL, Secret: []byte("test-secret"), HTTP: srv.Client()}
	err := run(c, identity{}, filepath.Join(t.TempDir(), "journal"), nil, false, &bytes.Buffer{})
	if err == nil {
		t.Fatal("accepted unordered catalog")
	}
}

// Python time.time() journal timestamps are fractional seconds. A successful
// old row must skip work on resume, including a nullable absent plan digest.
func TestResumePythonJournal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal")
	data := `{"identity":{"endpoint":"http://example.invalid","mode":"dry-run","cohort":"catalog","plan_sha256":null},"at":1794660000.123456,"result":{"name":"a","archives":[]}}` + "\n"
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	c := &remote.Client{}
	id := identity{Endpoint: "http://example.invalid", Mode: "dry-run", Cohort: "catalog"}
	if err := run(c, id, path, []string{"a"}, true, io.Discard); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil || string(after) != data {
		t.Fatalf("resume altered successful journal: %s %v", after, err)
	}
}
