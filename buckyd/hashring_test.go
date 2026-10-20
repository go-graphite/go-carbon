package buckyd

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestHashringDiscoveryDoesNotRequireMetricAccess(t *testing.T) {
	secret := []byte("test-secret")
	s := &Service{
		secret: secret,
		config: Config{Node: "test-node", Hash: "carbon", Replicas: 2},
		nodes:  []Node{{Server: "carbon-a", Port: 2003, Instance: "a"}},
	}
	tests := []struct {
		name, token string
	}{
		{"no token", ""},
		{"namespace read token", token(t, secret, []string{"team.*"}, []string{"read"})},
		{"invalid token", "invalid"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/hashring", http.NoBody)
			r.Header.Set(authHeader, tt.token)
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, r)
			if w.Code != http.StatusOK {
				t.Fatalf("GET /hashring = %d: %s", w.Code, w.Body.String())
			}
			var got struct {
				Name     string
				Nodes    []Node
				Algo     string
				Replicas int
			}
			if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			if got.Name != s.config.Node || got.Algo != s.config.Hash || got.Replicas != s.config.Replicas || !reflect.DeepEqual(got.Nodes, s.nodes) {
				t.Fatalf("unexpected hashring: %+v", got)
			}
		})
	}
}

func TestMetricEndpointsRequireAuthentication(t *testing.T) {
	s := &Service{secret: []byte("test-secret")}
	tests := []struct {
		method, path string
	}{
		{http.MethodGet, "/metrics"},
		{http.MethodPost, "/metrics"},
		{http.MethodHead, "/metrics/team.cpu"},
		{http.MethodGet, "/metrics/team.cpu"},
		{http.MethodPost, "/metrics/team.cpu"},
		{http.MethodPut, "/metrics/team.cpu"},
		{http.MethodDelete, "/metrics/team.cpu"},
	}
	for _, tt := range tests {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			r := httptest.NewRequest(tt.method, tt.path, http.NoBody)
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, r)
			if w.Code != http.StatusForbidden {
				t.Fatalf("unauthenticated request = %d: %s", w.Code, w.Body.String())
			}
		})
	}
}
