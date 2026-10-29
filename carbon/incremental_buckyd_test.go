package carbon

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/buckyd"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/golang-jwt/jwt/v5"
)

func testIncrementalBuckydPreservesOwnershipAndMixedIndex(t *testing.T) {
	for _, tc := range []struct{ pending, buckets bool }{{}, {true, false}, {false, true}, {true, true}} {
		pending := tc.pending
		name := "v6/reclaimed"
		if pending {
			name = "v6/cleanup-pending"
		}
		if tc.buckets {
			name = "v7/" + name[3:]
		}
		t.Run(name, func(t *testing.T) {
			if isolatedTestProcess(t) {
				return
			}
			path, cfg := incrementalAppFixture(t)
			cfg.Whisper.StoreTimeBuckets = tc.buckets
			cfg.Buckyd.Enabled = true
			secret := []byte("local-incremental-buckyd-test-secret")
			cfg.Buckyd.JWTSecretFile = filepath.Join(filepath.Dir(path), "buckyd.secret")
			if err := os.WriteFile(cfg.Buckyd.JWTSecretFile, secret, 0600); err != nil {
				t.Fatal(err)
			}
			if pending {
				cfg.Whisper.StoreMigrationReclaimDelay = &Duration{time.Hour}
			}
			writeSharedConfig(t, path, cfg)
			app := startIncrementalApp(t, path)
			if !tc.buckets && !pending {
				fastFileIndexRefresher(t, app)
			}
			ctx := context.Background()
			if _, err := app.Persister.ConvertFile(ctx, "first"); err != nil {
				t.Fatal(err)
			}
			wantPhase := store.FileReclaimed
			if pending {
				wantPhase = store.FileCleanupPending
			}
			waitMigration(t, func() bool {
				m, err := app.MetricStore.Metadata(ctx, "first")
				return err == nil && m.FileMigration == wantPhase
			})
			sign := func(metric, op string) string {
				t.Helper()
				token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, buckyd.ACL{
					Namespaces: []string{metric}, Ops: []string{op},
				}).SignedString(secret)
				if err != nil {
					t.Fatal(err)
				}
				return token
			}
			request := func(method, metric, token string) int {
				t.Helper()
				r := httptest.NewRequest(method, "/metrics/"+metric, http.NoBody)
				if token != "" {
					r.Header.Set("X-Buckyd-Authorization", token)
				}
				w := httptest.NewRecorder()
				app.Buckyd.Handler().ServeHTTP(w, r)
				return w.Code
			}
			for _, check := range []struct {
				method, metric, token string
				want                  int
			}{
				{http.MethodHead, "first", "", http.StatusForbidden},
				{http.MethodHead, "first", sign("other", "read"), http.StatusForbidden},
				{http.MethodDelete, "first", sign("first", "read"), http.StatusForbidden},
				{http.MethodPost, "first", sign("first", "update"), http.StatusConflict},
				{http.MethodPut, "first", sign("first", "replace"), http.StatusConflict},
				{http.MethodHead, "first", sign("first", "read"), http.StatusOK},
				{http.MethodDelete, "second", sign("second", "delete"), http.StatusNotFound},
			} {
				if got := request(check.method, check.metric, check.token); got != check.want {
					t.Fatalf("%s %s returned %d, want %d", check.method, check.metric, got, check.want)
				}
			}
			want := http.StatusOK
			if pending {
				want = http.StatusConflict
			}
			if got := request(http.MethodDelete, "first", sign("first", "delete")); got != want {
				t.Fatalf("delete returned %d, want %d", got, want)
			}
			if pending {
				if _, err := app.MetricStore.Metadata(ctx, "first"); err != nil {
					t.Fatal("pending metric was changed:", err)
				}
				if _, err := os.Stat(filepath.Join(cfg.Whisper.DataDir, "first.wsp")); err != nil {
					t.Fatal("pending original was changed:", err)
				}
				return
			}
			if _, err := app.MetricStore.Metadata(ctx, "first"); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("deleted catalog entry remains: %v", err)
			}
			count := func(metric string) int {
				t.Helper()
				url := "http://" + app.Carbonserver.HTTPListener().Addr().String() + "/metrics/list_query/?format=json&target=" + metric + "&leaf_only=true&limit=1"
				response, err := http.Get(url)
				if err != nil {
					t.Fatal(err)
				}
				defer response.Body.Close()
				var result struct{ Count int }
				if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
					t.Fatal(err)
				}
				return result.Count
			}
			// Exercise the coalescer with the production callback, using a short
			// interval so this small fixture does not wait for a production tick.
			app.metricStoreIndex.close()
			app.metricStoreIndex = startFileIndexRefresher(app.Carbonserver, 10*time.Millisecond)
			app.metricStoreIndex.notify()
			// A catalog-only refresh would hide the unconverted second metric.
			if !tc.buckets {
				deadline := time.Now().Add(45 * time.Second)
				for count("first") != 0 || count("second") != 1 {
					if time.Now().After(deadline) {
						t.Fatal("management mutation did not refresh the mixed index")
					}
					time.Sleep(100 * time.Millisecond)
				}
			}
			if _, err := os.Stat(filepath.Join(cfg.Whisper.DataDir, "second.wsp")); err != nil {
				t.Fatal("unconverted original was changed:", err)
			}
			app.Stop()
			app = startIncrementalApp(t, path)
			if got := request(http.MethodHead, "first", sign("first", "read")); got != http.StatusNotFound {
				t.Fatalf("deleted metric resurrected after restart: %d", got)
			}
		})
	}
}

// Keep Buckyd's original callback and the production coalescer, with a shorter
// test cadence. Forward its original notification channel to the fast worker.
func fastFileIndexRefresher(t *testing.T, app *App) {
	t.Helper()
	app.Lock()
	original := app.metricStoreIndex
	original.close()
	fast := startFileIndexRefresher(app.Carbonserver, 5*time.Millisecond)
	app.metricStoreIndex = fast
	app.Unlock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-original.changes:
				fast.notify()
			case <-fast.done:
				return
			}
		}
	}()
	t.Cleanup(func() { app.Stop(); <-done })
}
