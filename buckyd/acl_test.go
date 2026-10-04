package buckyd

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func token(t *testing.T, secret []byte, namespaces, ops []string) string {
	t.Helper()
	v, err := jwt.NewWithClaims(jwt.SigningMethodHS256, ACL{Namespaces: namespaces, Ops: ops}).SignedString(secret)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestACLScopesOperationsAndRootToken(t *testing.T) {
	secret := []byte("test-secret")
	s := &Service{secret: secret}
	tests := []struct {
		name, metric, op string
		token            string
		want             bool
	}{
		{"scoped read", "team.cpu", "read", token(t, secret, []string{"team.*"}, []string{"read"}), true},
		{"wrong namespace", "other.cpu", "read", token(t, secret, []string{"team.*"}, []string{"read"}), false},
		{"wrong op", "team.cpu", "delete", token(t, secret, []string{"team.*"}, []string{"read"}), false},
		{"root", "other.cpu", "delete", token(t, secret, []string{"*"}, []string{"*"}), true},
		{"invalid", "team.cpu", "read", "invalid", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/", nil)
			r.Header.Set(authHeader, tt.token)
			got := s.allowed(tt.metric, tt.op, r) == nil
			if got != tt.want {
				t.Fatalf("allowed=%v want %v", got, tt.want)
			}
		})
	}
}

func TestNoAuthAllowsRequests(t *testing.T) {
	if err := (&Service{}).allowed("metric", "delete", httptest.NewRequest("GET", "/", nil)); err != nil {
		t.Fatal(err)
	}
}

func TestJWTRejectsExpiredAndWrongSecret(t *testing.T) {
	secret := []byte("test-secret")
	s := &Service{secret: secret}
	expired, err := jwt.NewWithClaims(jwt.SigningMethodHS256, ACL{
		RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(-time.Hour))},
		Namespaces:       []string{"*"}, Ops: []string{"*"},
	}).SignedString(secret)
	if err != nil {
		t.Fatal(err)
	}
	for _, signed := range []string{expired, token(t, []byte("wrong-secret"), []string{"*"}, []string{"*"})} {
		r := httptest.NewRequest("GET", "/metrics", nil)
		r.Header.Set(authHeader, signed)
		if err := s.allowed("*", "read", r); err == nil {
			t.Fatal("invalid JWT accepted")
		}
	}
}

func TestOffloadTokenGrantsOnlySourceReadAndExpires(t *testing.T) {
	s := &Service{secret: []byte("test-secret")}
	tests := []struct {
		metric, other string
	}{
		{"team.cpu", "other.cpu"},
		{"team.*.cpu", "team.other.cpu"},
		{"team.?.cpu", "team.a.cpu"},
		{"team.[ab].cpu", "team.a.cpu"},
		{`team.\cpu`, "team.cpu"},
	}
	for _, tt := range tests {
		t.Run(tt.metric, func(t *testing.T) {
			signed, err := s.offloadToken(tt.metric)
			if err != nil {
				t.Fatal(err)
			}
			r := httptest.NewRequest("GET", "/", nil)
			r.Header.Set(authHeader, signed)
			if err := s.allowed(tt.metric, "read", r); err != nil {
				t.Fatalf("source read rejected: %v", err)
			}
			if err := s.allowed(tt.other, "read", r); err == nil {
				t.Fatal("offload token grants another metric")
			}
			for _, op := range []string{"update", "replace", "delete"} {
				if err := s.allowed(tt.metric, op, r); err == nil {
					t.Fatalf("offload token grants %s", op)
				}
			}
			claims := &ACL{}
			if _, err := jwt.ParseWithClaims(signed, claims, func(*jwt.Token) (interface{}, error) {
				return s.secret, nil
			}, jwt.WithExpirationRequired()); err != nil {
				t.Fatal(err)
			}
			remaining := time.Until(claims.ExpiresAt.Time)
			if remaining <= 4*time.Minute || remaining > 5*time.Minute {
				t.Fatalf("unexpected token lifetime: %s", remaining)
			}
		})
	}
}
