package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestOfflineCatalogDecisions(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "catalog.json")
	data := `{"Metrics":[{"Scope":{"PlacementGroup":"service","VShard":1},"Name":"service.cpu","Retentions":[{"Step":1,"Count":60}],"Generation":1,"Revision":1,"LastUpdate":"2026-01-01T00:00:00Z"}],"Queries":["service.*"],"Events":[{"Name":"service.cpu","Points":1,"InCache":true}]}`
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := run([]string{"-input", path, "-now", "2026-01-02T00:00:00Z", "-expiration-default", "1h"}, &buf); err != nil {
		t.Fatal(err)
	}
	var got output
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Expiration.Delete) != 1 || len(got.Find["service.*"]) != 1 || got.Throttled[0] {
		t.Fatal(buf.String())
	}
	if err := run(nil, &buf); err == nil {
		t.Fatal("missing deterministic input accepted")
	}
}

func TestOfflineStorageQuotaFiles(t *testing.T) {
	dir := t.TempDir()
	files := map[string]string{
		"catalog.json": `{"Metrics":[{"Name":"service.cpu","Retentions":[{"Step":1,"Count":60}]}],"Events":[{"Name":"service.cpu","Points":1,"InCache":true},{"Name":"service.cpu","Points":1,"InCache":true},{"Name":"service.cpu","Points":1,"InCache":true}]}`,
		"schemas.conf": "[default]\npattern = .*\nretentions = 1s:1m\n",
		"quotas.conf":  "[/]\nmetrics = 1\nthroughput = 2\ndropping-policy = new\n",
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	var buf bytes.Buffer
	if err := run([]string{"-input", filepath.Join(dir, "catalog.json"), "-now", "2026-01-02T00:00:00Z", "-quotas", filepath.Join(dir, "quotas.conf"), "-schemas", filepath.Join(dir, "schemas.conf")}, &buf); err != nil {
		t.Fatal(err)
	}
	var got output
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Throttled) != 3 || got.Throttled[0] || got.Throttled[1] || !got.Throttled[2] {
		t.Fatal(buf.String())
	}
}
