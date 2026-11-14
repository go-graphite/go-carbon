package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestNativeCapturedDuplicateModelsPreserveMultisetsAndFetchEveryUniquePoint(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "internal", "whisperio", "testdata", "anchor-duplicate-layouts.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixtures []struct {
		Label string `json:"label"`
		Wire  string `json:"wire_base64"`
	}
	if err := json.Unmarshal(raw, &fixtures); err != nil {
		t.Fatal(err)
	}
	total, queried := 0, 0
	for _, f := range fixtures {
		t.Run(f.Label, func(t *testing.T) {
			wire, err := base64.StdEncoding.DecodeString(f.Wire)
			if err != nil {
				t.Fatal(err)
			}
			out, err := evaluate(wire, time.Unix(1794273800, 0))
			if err != nil {
				t.Fatal(err)
			}
			if out.Status != "MODEL_VERIFIED" || out.Applied || out.RuntimeQualified || out.BeforeSHA != out.AfterSHA || out.BeforeSHA == "" {
				t.Fatalf("unqualified model %+v", out)
			}
			if out.Nonselected != 0 || out.ExpiredOrFuture != 0 {
				t.Fatalf("actual fixture has excluded points %+v", out)
			}
			total += out.Points
			queried += out.FetchPoints
			corrupted := bytes.Clone(wire)
			corrupted[len(corrupted)/2] ^= 1
			if _, err := evaluate(corrupted, time.Unix(1794273800, 0)); err == nil {
				t.Fatal("changed checksummed native wire accepted")
			}
		})
	}
	if total != 3812 || queried != 3771 {
		t.Fatalf("points=%d unique fetched=%d", total, queried)
	}
}

func TestNativeModelRefusesAbsentOrFlatRecords(t *testing.T) {
	for _, r := range []store.TransferMetric{{}, {Config: store.MetricConfig{Name: "n"}, Version: store.TransferVersion{ID: 1}, Continuation: false}} {
		var wire bytes.Buffer
		if err := store.WriteTransfer(&wire, []store.TransferMetric{r}); err != nil {
			t.Fatal(err)
		}
		if _, err := evaluate(wire.Bytes(), time.Unix(1000, 0)); err == nil {
			t.Fatal("missing/flat native record admitted")
		}
	}
}
