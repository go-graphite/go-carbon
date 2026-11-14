package downsize

import (
	"bytes"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const hostJWTSecretPath = "/etc/go-carbon/buckyd_api_jwt_secret"

// The key and short-lived credential stay in the controller host's memory.
// Only non-secret mint/expiry times are reported; no credential file is created.
type hostTokenCache struct {
	mu         sync.Mutex
	token      string
	renewAt    time.Time
	now        func() time.Time
	readSecret func() ([]byte, error)
}

func (c *hostTokenCache) get(secretPath string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	if c.now != nil {
		now = c.now()
	}
	if c.token != "" && now.Before(c.renewAt) {
		return c.token, nil
	}
	reader := c.readSecret
	if reader == nil {
		if secretPath == "" {
			secretPath = hostJWTSecretPath
		}
		reader = func() ([]byte, error) { return os.ReadFile(secretPath) }
	}
	key, err := reader()
	if err != nil {
		return "", errors.New("host-local JWT secret unavailable")
	}
	key = bytes.TrimSpace(key)
	defer clear(key)
	if len(key) == 0 {
		return "", errors.New("host-local JWT secret is empty")
	}
	expires := now.Add(4 * time.Hour)
	claims := jwt.MapClaims{"iat": now.Unix(), "exp": expires.Unix(), "namespaces": []string{"*"}, "ops": []string{"read", "update"}}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(key)
	if err != nil {
		return "", errors.New("cannot mint host-local copy JWT")
	}
	c.token, c.renewAt = token, now.Add(time.Hour)
	event("host_jwt_refreshed", map[string]any{"minted_at": now.UTC().Format(time.RFC3339), "expires_at": expires.UTC().Format(time.RFC3339), "next_refresh_after": c.renewAt.UTC().Format(time.RFC3339), "ops": []string{"read", "update"}, "host_local": true})
	return c.token, nil
}

func (m *mover) authorizationToken() (string, error) {
	enabled := os.Getenv("RESIZE_HOST_JWT_REFRESH")
	if enabled == "" {
		return os.Getenv(m.tokenEnv), nil
	}
	if enabled != "1" || m.delete || m.tuner == nil || m.tuner.maxSpeed == nil {
		return "", errors.New("host-local JWT renewal requires explicit maximum-speed copy-only mode")
	}
	return m.hostToken.get(m.hostJWTSecret)
}

func (m *mover) invalidateHostToken() {
	if os.Getenv("RESIZE_HOST_JWT_REFRESH") != "1" {
		return
	}
	m.hostToken.mu.Lock()
	m.hostToken.renewAt = time.Time{}
	m.hostToken.mu.Unlock()
}
