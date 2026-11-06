package downsize

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

func TestMaxSpeedAllowsHealthyWorkPastTheSeparateHeaderDeadline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { time.Sleep(180 * time.Millisecond); w.WriteHeader(200) }))
	defer server.Close()
	// Scale the old deadline to keep the regression bounded: this demonstrates
	// the same cancellation independently of the overall request timeout.
	old := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{ResponseHeaderTimeout: 90 * time.Millisecond}}
	if response, err := old.Get(server.URL); err == nil {
		response.Body.Close()
		t.Fatal("slow healthy work did not reproduce legacy header timeout")
	}
	old.CloseIdleConnections()
	m, _, approval := nativeMaxPolicyFixture(t)
	path, sum := maxDecisionFixture(t)
	t.Setenv("RESIZE_MAX_SPEED_DECISION", path)
	t.Setenv("RESIZE_MAX_SPEED_DECISION_SHA256", sum)
	t.Setenv("RESIZE_MAX_SPEED_WRITE_OK", filepath.Join(m.planData.root, "WRITE-OK"))
	t.Setenv("RESIZE_MAX_SPEED_WRITE_OK_SHA256", approval.ApprovalSHA256)
	m.client = &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{ResponseHeaderTimeout: 90 * time.Millisecond}}
	defer m.client.CloseIdleConnections()
	if enabled, err := m.configureMaximumSpeed(context.Background(), nil); err != nil || !enabled {
		t.Fatal(enabled, err)
	}
	response, err := m.client.Get(server.URL)
	if err != nil {
		t.Fatal("MAX discarded slow healthy work", err)
	}
	response.Body.Close()
	if m.client.Timeout != 5*time.Second {
		t.Fatal("overall request bound changed")
	}
}
