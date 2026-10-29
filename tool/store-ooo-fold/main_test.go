package main

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

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestFoldToolCheckpointAndDryRun(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cursor.json")
	folds, scans := 0, 0
	fail := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			json.NewEncoder(w).Encode(store.SidecarInventory{})
			return
		}
		if r.Method != http.MethodPost {
			t.Error(r.Method)
		}
		q := r.URL.Query()
		switch q.Get("action") {
		case "scan":
			scans++
			json.NewEncoder(w).Encode(store.SidecarPage{Done: true, Sidecars: []store.SidecarRecord{{Name: "a", SidecarSummary: store.SidecarSummary{Present: true, Points: 3}}}})
		case "fold":
			if q.Get("after") == "" {
				folds++
				json.NewEncoder(w).Encode(store.SidecarFoldPage{After: "a", Count: 1, Folded: 1, Inventory: store.SidecarInventory{Count: 1, Ready: true}})
			} else if fail {
				http.Error(w, "interrupted", 503)
			} else {
				folds++
				json.NewEncoder(w).Encode(store.SidecarFoldPage{After: "b", Done: true, Count: 1, Folded: 1, Inventory: store.SidecarInventory{Ready: true}})
			}
		default:
			t.Error(q)
		}
	}))
	defer server.Close()
	o := options{endpoint: server.URL, checkpoint: path, limit: 1, dry: true}
	if err := run(context.Background(), server.Client(), o); err != nil {
		t.Fatal(err)
	}
	if folds != 0 {
		t.Fatal("dry run folded")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("dry run wrote checkpoint")
	}
	o.dry = false
	if err := run(context.Background(), server.Client(), o); err == nil {
		t.Fatal("expected interrupted request")
	}
	value, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var c checkpoint
	if err := json.Unmarshal(value, &c); err != nil || c.After != "a" {
		t.Fatal("cursor lost", string(value), err)
	}
	fail = false
	scansBefore := scans
	if err := run(context.Background(), server.Client(), o); err != nil {
		t.Fatal(err)
	}
	if scans != scansBefore || folds != 2 {
		t.Fatal("resume repeated completed work", scans, folds)
	}
}

func TestFoldToolUsesReadyCatalog(t *testing.T) {
	gets, folds := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			gets++
			json.NewEncoder(w).Encode(store.SidecarInventory{Ready: true})
			return
		}
		if r.URL.Query().Get("action") != "fold" {
			t.Error("ready catalog was rescanned")
		}
		folds++
		json.NewEncoder(w).Encode(store.SidecarFoldPage{Done: true, Inventory: store.SidecarInventory{Ready: true}})
	}))
	defer server.Close()
	if err := run(context.Background(), server.Client(), options{endpoint: server.URL, limit: 1000}); err != nil {
		t.Fatal(err)
	}
	if gets != 1 || folds != 1 {
		t.Fatal(gets, folds)
	}
}

func TestFoldToolPacesAcrossPages(t *testing.T) {
	folds := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			json.NewEncoder(w).Encode(store.SidecarInventory{Ready: true})
			return
		}
		folds++
		json.NewEncoder(w).Encode(store.SidecarFoldPage{After: "a", Count: 1, Folded: 1, Inventory: store.SidecarInventory{Ready: true, Count: 1}})
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := run(ctx, server.Client(), options{endpoint: server.URL, limit: 1, rate: 1})
	if !errors.Is(err, context.DeadlineExceeded) || folds != 1 {
		t.Fatalf("pacing: folds=%d err=%v", folds, err)
	}
}
