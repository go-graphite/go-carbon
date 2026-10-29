package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/buckyd"
)

func TestPostAndCheckpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Buckyd-Authorization") != "credential" || r.Method != http.MethodPost {
			t.Error("wrong request")
		}
		_ = json.NewEncoder(w).Encode(buckyd.ActivityBackfillResult{Scanned: 1, WouldChange: 1, AgeBuckets: map[string]int{"8d_15d": 1}})
	}))
	defer server.Close()
	result, err := post(context.Background(), server.Client(), server.URL, "credential", []byte(`{}`))
	if err != nil || result.WouldChange != 1 {
		t.Fatalf("post %+v %v", result, err)
	}
	path := filepath.Join(t.TempDir(), "checkpoint.json")
	state := checkpoint{Identity: "host|window|dry-run|baseline-sha256", After: "last.metric", Records: 128, Totals: result}
	if err := save(path, state); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var recovered checkpoint
	if err := json.Unmarshal(data, &recovered); err != nil {
		t.Fatal(err)
	}
	if recovered.Identity != state.Identity || recovered.After != state.After || recovered.Totals.AgeBuckets["8d_15d"] != 1 {
		t.Fatalf("checkpoint %+v", recovered)
	}
}
func TestPressureRetryCanBeInterrupted(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(429) }))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := post(ctx, server.Client(), server.URL, "", []byte(`{}`)); err == nil {
		t.Fatal("pressure retry ignored cancellation")
	}
}
