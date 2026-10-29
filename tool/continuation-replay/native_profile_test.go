package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

// Profiles the actual final-state queries only after the paired correctness
// gate passes. Each mode runs in a separate process; production is never opened.
func TestNativeContinuationReadProfile(t *testing.T) {
	root, mode := os.Getenv("GRAPHITE_CONTINUATION_PROFILE_CORPUS"), os.Getenv("GRAPHITE_CONTINUATION_PROFILE_MODE")
	if root == "" {
		t.Skip("requires an already verified private paired production corpus")
	}
	if !filepath.IsAbs(root) || !strings.Contains(filepath.Clean(root), "/production-replay-lab-v1/") || mode != "reference" && mode != "candidate" {
		t.Fatal("private corpus and explicit reference/candidate mode required")
	}
	data, err := os.ReadFile(filepath.Join(root, "paired-v1", "RESULT.json"))
	if err != nil {
		t.Fatal(err)
	}
	var receipt struct {
		Correct bool `json:"correctness_passed"`
	}
	if err := json.Unmarshal(data, &receipt); err != nil || !receipt.Correct {
		t.Fatal("paired correctness gate did not pass")
	}
	planPath, operations := filepath.Join(root, "plan.json"), filepath.Join(root, "operations.jsonl")
	if err := childMarker(filepath.Join(root, "paired-v1"), planPath, operations); err != nil {
		t.Fatal(err)
	}
	plan, err := loadPlan(planPath)
	if err != nil {
		t.Fatal(err)
	}
	e := &replayEngine{mode: mode, root: filepath.Join(root, "paired-v1", mode), plan: plan, clock: time.Unix(0, plan.EndNS)}
	if mode == "candidate" {
		e.db, err = store.Open(filepath.Join(e.root, "shared"), store.Options{Continuation: &whisperio.ContinuationCodec{}, ExactBlocks: true,
			IncrementalSource: filepath.Join(e.root, "originals"), CacheSize: plan.CacheBytes, MemTableSize: plan.MemtableBytes, Now: func() time.Time { return e.clock }})
		if err != nil {
			t.Fatal(err)
		}
		defer e.db.Close()
	}
	var queries []replayOperation
	if err := eachOperation(operations, plan, func(_ int, op replayOperation) error {
		if op.Kind == "read" {
			queries = append(queries, op)
		}
		return nil
	}); err != nil || len(queries) == 0 {
		t.Fatal("missing or invalid query trace", err)
	}
	// Warm the exact same final-state queries before starting the profile.
	for _, q := range queries {
		got, err := e.read(q)
		if err != nil {
			t.Fatal(err)
		}
		if mode == "candidate" {
			reference := &replayEngine{mode: "reference", root: filepath.Join(root, "paired-v1", "reference"), plan: plan, clock: e.clock}
			want, err := reference.read(q)
			if err != nil || seriesHash(want) != seriesHash(got) {
				t.Fatal("profile query differs from CWhisper", err)
			}
		}
	}
	output := filepath.Join(root, os.Getenv("GRAPHITE_CONTINUATION_PROFILE_OUTPUT"))
	if filepath.Dir(output) != filepath.Clean(root) || filepath.Base(output) == "." {
		t.Fatal("profile output must be a new direct child of the private corpus")
	}
	if err := os.Mkdir(output, 0700); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(filepath.Join(output, "cpu.pb.gz"))
	if err != nil {
		t.Fatal(err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		t.Fatal(err)
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	start := time.Now()
	reads, cells := 0, 0
	for time.Since(start) < 15*time.Second {
		for _, q := range queries {
			s, err := e.read(q)
			if err != nil {
				pprof.StopCPUProfile()
				f.Close()
				t.Fatal(err)
			}
			reads++
			if s != nil {
				cells += len(s.Values)
			}
		}
	}
	seconds := time.Since(start).Seconds()
	runtime.ReadMemStats(&after)
	pprof.StopCPUProfile()
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(filepath.Join(output, "RESULT.json"), map[string]any{"mode": mode, "reads": reads, "returned_cells": cells, "wall_seconds": seconds,
		"allocated_bytes": after.TotalAlloc - before.TotalAlloc, "performance_qualified": false, "scope": "Repeated warm final-state queries from a verified private production corpus; profiler diagnostic, not production request tails or steady-state read/write replay."}); err != nil {
		t.Fatal(err)
	}
}
