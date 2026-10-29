package buckyd

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/internal/storecopy"
)

func TestStoreCopyFencesBulkMutations(t *testing.T) {
	s, db := testService(t)
	root := t.TempDir()
	logDir := filepath.Join(root, "log")
	log, err := inputlog.Open(logDir, inputlog.Options{SyncInterval: time.Hour, SegmentSize: 16 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	core := cache.New()
	core.EnableInputLog()
	log.Start(core, cache.ShardCount)
	manager := storecopy.New(db, log, logDir, root, nil, nil)
	defer manager.Close()
	s.SetStoreCopy(manager)
	manager.SetMinimumFreeBytes(1)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodPost, storecopy.Endpoint, http.NoBody))
	if w.Code != http.StatusOK || !manager.Active() {
		t.Fatalf("checkpoint: %d %s", w.Code, w.Body.String())
	}
	for _, op := range []string{"import", "pull", "delete", "stage-delete", "retire"} {
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/transfer/"+op, http.NoBody))
		if w.Code != http.StatusConflict {
			t.Fatalf("%s bypassed copy fence: %d %s", op, w.Code, w.Body.String())
		}
	}
	w = httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/transfer/status", http.NoBody))
	if w.Code != http.StatusOK {
		t.Fatalf("status unavailable during copy: %d", w.Code)
	}
}

func TestStoreCopyRequiresWholeStoreAdminACL(t *testing.T) {
	s, db := testService(t)
	s.secret = []byte("secret")
	s.SetStoreCopy(storecopy.New(db, nil, "", t.TempDir(), nil, nil))
	for _, test := range []struct {
		name            string
		namespaces, ops []string
		want            int
	}{
		{"read token", []string{"*"}, []string{"read"}, http.StatusForbidden},
		{"scoped admin", []string{"team.*"}, []string{"admin"}, http.StatusForbidden},
		{"whole-store admin", []string{"*"}, []string{"admin"}, http.StatusConflict},
		{"root", []string{"*"}, []string{"*"}, http.StatusConflict},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, storecopy.Endpoint, http.NoBody)
			r.Header.Set(authHeader, token(t, s.secret, test.namespaces, test.ops))
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, r)
			if w.Code != test.want {
				t.Fatalf("got %d want %d: %s", w.Code, test.want, w.Body.String())
			}
		})
	}
}
