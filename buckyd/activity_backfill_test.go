package buckyd

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/golang-jwt/jwt/v5"
)

func TestActivityBackfillAuthorizationPagingAndWriteFence(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	conversion := time.Unix(1790000100, 0)
	db, err := chunkstore.Open(filepath.Join(root, "store"), chunkstore.Options{ExactBlocks: true, IncrementalSource: source, Now: func() time.Time { return conversion }, SyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	retention := chunkstore.Retention{Step: 1, Count: 3600}
	var baselines []chunkstore.ActivityBaseline
	for _, name := range []string{"a.old", "b.written"} {
		snapshot := chunkstore.Snapshot{Metadata: chunkstore.Metadata{MetricConfig: chunkstore.MetricConfig{Name: name, Retentions: []chunkstore.Retention{retention}, AggregationMethod: chunkstore.Average}, LastUpdate: conversion}, Archives: []chunkstore.Archive{{Retention: retention, Points: []chunkstore.Point{{Timestamp: 1790000001, Value: 1}}}}}
		m, err := db.StageFileSnapshot(context.Background(), snapshot)
		if err != nil {
			t.Fatal(err)
		}
		m, err = db.PublishFileOwnership(context.Background(), snapshot, m)
		if err != nil {
			t.Fatal(err)
		}
		if err := db.CompleteFileCleanup(context.Background(), m); err != nil {
			t.Fatal(err)
		}
		m, _ = db.Metadata(context.Background(), name)
		baselines = append(baselines, chunkstore.ActivityBaseline{Name: name, ID: m.ID, Generation: m.Generation, Revision: m.Revision, LastUpdate: m.LastUpdate})
	}
	s, err := New(Config{TmpDir: t.TempDir()}, db)
	if err != nil {
		t.Fatal(err)
	}
	s.secret = []byte("activity-test")
	token := func(namespaces, ops []string) string {
		token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, ACL{Namespaces: namespaces, Ops: ops}).SignedString(s.secret)
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	read := token([]string{"*"}, []string{"read"})
	update := token([]string{"*"}, []string{"read", "update"})
	invoke := func(request ActivityBackfillRequest, credential string) (int, ActivityBackfillResult) {
		body, _ := json.Marshal(request)
		r := httptest.NewRequest(http.MethodPost, "/v1/activity-backfill", bytes.NewReader(body))
		r.Header.Set(authHeader, credential)
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		var result ActivityBackfillResult
		if w.Code == 200 {
			if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
		}
		return w.Code, result
	}
	request := ActivityBackfillRequest{Start: conversion.Add(-time.Minute), End: conversion.Add(time.Minute), Limit: 1}
	if code, _ := invoke(request, ""); code != 403 {
		t.Fatalf("unauthenticated %d", code)
	}
	if code, _ := invoke(request, token([]string{"a.*"}, []string{"read", "update"})); code != 403 {
		t.Fatalf("scoped token %d", code)
	}
	code, result := invoke(request, read)
	if code != 200 || result.Next != "a.old" || result.Scanned != 1 || result.WouldChange != 1 || result.Changed != 0 {
		t.Fatalf("dry-run %d %+v", code, result)
	}
	request.After = result.Next
	if code, result := invoke(request, read); code != 200 || result.Next != "b.written" {
		t.Fatalf("resume %d %+v", code, result)
	}
	request.Apply = true
	if code, _ := invoke(request, update); code != 400 {
		t.Fatalf("apply without baseline %d", code)
	}
	request.Baselines = baselines
	if code, _ := invoke(request, read); code != 403 {
		t.Fatalf("read token mutation %d", code)
	}
	if err := db.UpdateMany(context.Background(), "b.written", []chunkstore.Point{{Timestamp: 1790000002, Value: 2}}); err != nil {
		t.Fatal(err)
	}
	code, result = invoke(request, update)
	if code != 200 || result.Changed != 1 || result.Conflicts != 1 {
		t.Fatalf("apply %d %+v", code, result)
	}
	m, _ := db.Metadata(context.Background(), "a.old")
	if m.LastUpdate.Unix() != 1790000001 {
		t.Fatalf("activity %s", m.LastUpdate)
	}
	code, result = invoke(request, update)
	if code != 200 || result.Changed != 0 || result.Conflicts != 2 {
		t.Fatalf("repeated page %d %+v", code, result)
	}
}
