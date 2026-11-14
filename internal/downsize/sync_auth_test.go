package downsize

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestSyncJWTRefreshAcrossBackendsBeforeExpiry(t *testing.T) {
	var seconds atomic.Int64
	seconds.Store(1800000000)
	now := func() time.Time { return time.Unix(seconds.Load(), 0) }
	var reads atomic.Int64
	auth := &hostTokenCache{now: now, readSecret: func() ([]byte, error) { reads.Add(1); return []byte("test-only-key"), nil }}
	var tokens sync.Map
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw := r.Header.Get("X-Buckyd-Authorization")
		parsed, err := jwt.Parse(raw, func(*jwt.Token) (any, error) { return []byte("test-only-key"), nil }, jwt.WithTimeFunc(now), jwt.WithValidMethods([]string{"HS256"}))
		if err != nil || !parsed.Valid {
			t.Error("invalid JWT", err)
			w.WriteHeader(401)
			return
		}
		tokens.Store(raw, true)
		w.Write([]byte(`{}`))
	}))
	defer server.Close()
	b := newSyncLiveBackend(syncLiveOptions{auth: auth, authOrigins: []string{server.URL}})
	defer b.client.CloseIdleConnections()
	defer b.m.client.CloseIdleConnections()
	// The live and copy clients share one signer; no environment token is set.
	copyClient := server.Client()
	copyOptions := b.copyOptions(server.URL, server.URL, "", "", 0)
	withSyncJWT(copyClient, copyOptions.auth, server.URL)
	run := func() {
		var wg sync.WaitGroup
		for i := 0; i < 24; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				client := b.client
				if i%2 == 0 {
					client = copyClient
				}
				resp, err := client.Get(server.URL)
				if err != nil {
					t.Error(err)
					return
				}
				resp.Body.Close()
			}(i)
		}
		wg.Wait()
	}
	run()
	if reads.Load() != 1 {
		t.Fatal("duplicate initial mint", reads.Load())
	}
	seconds.Add(61 * 60)
	run()
	if reads.Load() != 2 {
		t.Fatal("token did not refresh before expiry", reads.Load())
	}
	count := 0
	tokens.Range(func(_, _ any) bool { count++; return true })
	if count != 2 {
		t.Fatal("did not use renewed token", count)
	}
	seconds.Add(5 * 3600)
	run()
	if reads.Load() != 3 {
		t.Fatal("token expired across idle phase")
	}
}

func TestSyncJWTDoesNotEscapeStoreOriginsAndFailsClosed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Buckyd-Authorization") != "" {
			t.Error("JWT leaked to read-back origin")
		}
		w.WriteHeader(200)
	}))
	defer server.Close()
	auth := &hostTokenCache{readSecret: func() ([]byte, error) { return nil, errors.New("private secret material") }}
	client := server.Client()
	withSyncJWT(client, auth, "http://different-store:4242")
	resp, err := client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	withSyncJWT(client, auth, server.URL)
	if _, err := client.Get(server.URL); err == nil {
		t.Fatal("missing secret allowed request")
	} else if errors.Is(err, context.Canceled) || err.Error() == "private secret material" {
		t.Fatal("secret reader error leaked")
	}
}

func TestSyncRateCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := (&syncRateLimit{bytesPerSecond: 1}).wait(ctx, 1000); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := (*syncRateLimit)(nil).wait(ctx, 1000); err != nil {
		t.Fatal(err)
	}
}

func syncTestJWTSecret(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "jwt-secret")
	if err := os.WriteFile(path, []byte("test-only-sync-key"), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
