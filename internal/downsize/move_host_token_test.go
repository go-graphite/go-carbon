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

func TestHostTokenRenewsShortLivedCredentialAcrossConcurrentCopyAndProofRequests(t *testing.T) {
	t.Setenv("RESIZE_HOST_JWT_REFRESH", "1")
	now := time.Unix(1800000000, 0)
	var reads atomic.Int32
	m := &mover{tuner: newTuner([]string{"host"}, capacity{8, 32, 1 << 20, 100}, true, 0, 2)}
	m.tuner.enableMaxSpeed([]string{"host"}, "decision")
	m.hostToken.now = func() time.Time { return now }
	m.hostToken.readSecret = func() ([]byte, error) { reads.Add(1); return []byte("fake-test-only-key\n"), nil }
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, err := jwt.Parse(r.Header.Get("X-Buckyd-Authorization"), func(*jwt.Token) (any, error) { return []byte("fake-test-only-key"), nil }, jwt.WithTimeFunc(func() time.Time { return now }), jwt.WithValidMethods([]string{"HS256"}))
		if err != nil || !token.Valid {
			t.Error("host-local credential was not valid")
			http.Error(w, "unauthorized", 401)
			return
		}
		claims := token.Claims.(jwt.MapClaims)
		expires, _ := claims.GetExpirationTime()
		issued, _ := claims.GetIssuedAt()
		if expires.Time.Sub(issued.Time) != 4*time.Hour {
			t.Error("credential lifetime changed")
		}
		ops := claims["ops"].([]any)
		if len(ops) != 2 || ops[0] != "read" || ops[1] != "update" {
			t.Error("copy credential acquired unexpected operations")
		}
		requests.Add(1)
		w.Write([]byte(`{}`))
	}))
	defer server.Close()
	m.client = server.Client()
	run := func() {
		t.Helper()
		var group sync.WaitGroup
		for i := range 32 {
			group.Add(1)
			go func() {
				defer group.Done()
				if i%2 == 0 {
					if _, err := m.requestOnce(context.Background(), server.URL, "/v1/transfer/status", nil, nil); err != nil {
						t.Error(err)
					}
				} else {
					response, err := m.sourceAbsenceRequest(context.Background(), http.MethodHead, server.URL+"/metrics/fixture")
					if err != nil {
						t.Error(err)
						return
					}
					response.Body.Close()
				}
			}()
		}
		group.Wait()
	}
	run()
	first := m.hostToken.token
	if reads.Load() != 1 {
		t.Fatal("concurrent requests minted repeated credentials", reads.Load())
	}
	now = now.Add(61 * time.Minute)
	run()
	if reads.Load() != 2 || m.hostToken.token == first || requests.Load() != 64 {
		t.Fatal("credential did not renew in memory before expiry")
	}
}

func TestHostTokenRequiresExplicitCopyModeAndNeverFallsBackOnSecretFailure(t *testing.T) {
	t.Setenv("RESIZE_HOST_JWT_REFRESH", "")
	t.Setenv("TEST_LEGACY_COPY_TOKEN", "legacy-fake")
	m := &mover{options: options{tokenEnv: "TEST_LEGACY_COPY_TOKEN"}}
	if token, err := m.authorizationToken(); err != nil || token != "legacy-fake" {
		t.Fatal("legacy environment credential changed")
	}
	t.Setenv("RESIZE_HOST_JWT_REFRESH", "1")
	if _, err := m.authorizationToken(); err == nil {
		t.Fatal("renewal without explicit MAX accepted")
	}
	m.tuner = newTuner(nil, capacity{8, 32, 1 << 20, 100}, true, 0, 2)
	m.tuner.enableMaxSpeed(nil, "decision")
	m.hostToken.readSecret = func() ([]byte, error) { return nil, errors.New("fake private material must not be echoed") }
	if _, err := m.authorizationToken(); err == nil || err.Error() != "host-local JWT secret unavailable" {
		t.Fatal("secret failure was unsafe or ignored")
	}
	m.hostToken.readSecret = func() ([]byte, error) { return []byte("\n"), nil }
	if _, err := m.authorizationToken(); err == nil {
		t.Fatal("empty secret accepted")
	}
	m.delete = true
	if _, err := m.authorizationToken(); err == nil {
		t.Fatal("host-renewed copy credential used for deletion")
	}
}

func TestHostTokenUnauthorizedRefreshesBothTransferAndAbsenceCredentials(t *testing.T) {
	t.Setenv("RESIZE_HOST_JWT_REFRESH", "1")
	key := []byte("first-fake-key")
	m := &mover{tuner: newTuner(nil, capacity{8, 32, 1 << 20, 100}, true, 0, 2)}
	m.tuner.enableMaxSpeed(nil, "decision")
	var reads int
	m.hostToken.readSecret = func() ([]byte, error) { reads++; return append([]byte(nil), key...), nil }
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := jwt.Parse(r.Header.Get("X-Buckyd-Authorization"), func(*jwt.Token) (any, error) { return key, nil }, jwt.WithValidMethods([]string{"HS256"}))
		if err != nil {
			http.Error(w, "unauthorized", 401)
			return
		}
		w.Write([]byte(`{}`))
	}))
	defer server.Close()
	m.client = server.Client()
	// Warm a token, then rotate the fake host key without waiting an hour.
	if _, err := m.authorizationToken(); err != nil {
		t.Fatal(err)
	}
	key = []byte("second-fake-key")
	if _, err := m.requestOnce(context.Background(), server.URL, "/v1/transfer/status", nil, nil); err == nil {
		t.Fatal("stale credential unexpectedly accepted")
	}
	if _, err := m.requestOnce(context.Background(), server.URL, "/v1/transfer/status", nil, nil); err != nil {
		t.Fatal("transfer credential did not refresh", err)
	}
	key = []byte("third-fake-key")
	response, err := m.sourceAbsenceRequest(context.Background(), http.MethodHead, server.URL+"/metrics/fixture")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 401 {
		t.Fatal("stale proof credential unexpectedly accepted")
	}
	response, err = m.sourceAbsenceRequest(context.Background(), http.MethodHead, server.URL+"/metrics/fixture")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 200 || reads != 3 {
		t.Fatal("absence credential did not refresh", response.StatusCode, reads)
	}
}

func TestHostTokenConfiguredSecretPath(t *testing.T) {
	t.Setenv("RESIZE_HOST_JWT_REFRESH", "1")
	path := filepath.Join(t.TempDir(), "signing-key")
	if err := os.WriteFile(path, []byte("configured-fake-signing-key\n"), 0600); err != nil {
		t.Fatal(err)
	}
	m := &mover{options: options{hostJWTSecret: path}, tuner: newTuner(nil, capacity{8, 32, 1 << 20, 100}, true, 0, 2)}
	m.tuner.enableMaxSpeed(nil, "decision")
	raw, err := m.authorizationToken()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := jwt.Parse(raw, func(*jwt.Token) (any, error) { return []byte("configured-fake-signing-key"), nil }, jwt.WithValidMethods([]string{"HS256"}))
	if err != nil || !parsed.Valid {
		t.Fatal("configured key was not used", err)
	}
	if hostJWTSecretPath != "/etc/go-carbon/buckyd_api_jwt_secret" {
		t.Fatal("generic default changed")
	}
	m.invalidateHostToken()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err := m.authorizationToken(); err == nil || err.Error() != "host-local JWT secret unavailable" {
		t.Fatal("missing configured key did not fail closed", err)
	}
}
