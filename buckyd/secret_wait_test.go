package buckyd

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestMissingJWTSecretWaitIsOptIn(t *testing.T) {
	_, db := testService(t)
	cfg := Config{TmpDir: t.TempDir(), JWTSecretFile: filepath.Join(t.TempDir(), "late.secret")}
	if _, err := New(cfg, db); err == nil {
		t.Fatal("missing secret accepted without opt-in")
	}
	cfg.WaitForJWTSecret = true
	if err := os.WriteFile(cfg.JWTSecretFile, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(cfg, db); err == nil {
		t.Fatal("initially empty secret accepted")
	}
	cfg.JWTSecretFile = t.TempDir()
	if _, err := New(cfg, db); err == nil {
		t.Fatal("secret read failure accepted")
	}
	cfg.JWTSecretFile = ""
	if _, err := New(cfg, db); err == nil {
		t.Fatal("secret wait accepted without a secret path")
	}
}

func TestMissingJWTSecretKeepsManagementUnavailableUntilAuthenticated(t *testing.T) {
	_, db := testService(t)
	path := filepath.Join(t.TempDir(), "late.secret")
	s, err := New(Config{TmpDir: t.TempDir(), Bind: "127.0.0.1:0", JWTSecretFile: path, WaitForJWTSecret: true}, db)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	url := "http://" + s.listener.Addr().String() + "/metrics"
	request := func(authorization string) (int, error) {
		r, err := http.NewRequest(http.MethodGet, url, http.NoBody)
		if err != nil {
			return 0, err
		}
		r.Header.Set(authHeader, authorization)
		resp, err := http.DefaultClient.Do(r)
		if err != nil {
			return 0, err
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		return resp.StatusCode, nil
	}
	for _, emptyFile := range []bool{false, true} {
		if emptyFile {
			if err := os.WriteFile(path, nil, 0600); err != nil {
				t.Fatal(err)
			}
		}
		if status, err := request(""); err != nil || status != http.StatusServiceUnavailable || s.AuthSecretReady() {
			t.Fatalf("unavailable secret: status=%d error=%v ready=%v", status, err, s.AuthSecretReady())
		}
	}
	secret := []byte("fixture-secret")
	if err := os.WriteFile(path, secret, 0600); err != nil {
		t.Fatal(err)
	}
	read := token(t, secret, []string{"*"}, []string{"read"})
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for i := range 32 {
		wg.Go(func() {
			auth, want := "", http.StatusForbidden
			if i%2 == 0 {
				auth, want = read, http.StatusOK
			}
			status, err := request(auth)
			if err != nil {
				errs <- err
			} else if status != want {
				errs <- fmt.Errorf("published secret: status=%d want=%d", status, want)
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if !s.AuthSecretReady() {
		t.Fatal("secret was not published")
	}
	if err := s.Stop(); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	url = "http://" + s.listener.Addr().String() + "/metrics"
	if status, err := request(""); err != nil || status != http.StatusForbidden {
		t.Fatalf("restart lost authentication: status=%d error=%v", status, err)
	}
}
