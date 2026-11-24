package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func dumpBits(v float64) string { return fmt.Sprintf("%016x", math.Float64bits(v)) }
func dumpFixture(t *testing.T, rows []dumpRow, compressed bool) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "source.jsonl")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if compressed {
		gz := gzip.NewWriter(f)
		for _, r := range rows {
			if err := json.NewEncoder(gz).Encode(r); err != nil {
				t.Fatal(err)
			}
		}
		if err := gz.Close(); err != nil {
			t.Fatal(err)
		}
	} else {
		for _, r := range rows {
			if err := json.NewEncoder(f).Encode(r); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}
func sourceRow(name string, step, count int64, values map[int64]float64) dumpRow {
	a := dumpArchive{Step: step, Count: count}
	for ts, v := range values {
		a.Points = append(a.Points, dumpPoint{ts, dumpBits(v)})
	}
	return dumpRow{Name: name, Archives: []dumpArchive{a}}
}

func TestDumpHTTPDifferential(t *testing.T) {
	t.Parallel()
	donor := newFake(t)
	donor.fill("a.gaps", base, func(ts int64) float64 { return float64(ts-base) / 7 }, nil)
	donor.fill("b.missing-local", base, func(int64) float64 { return -0.25 }, nil)
	// Both absent records and explicit reader "not found" responses are ordinary
	// no-source outcomes. Existing local values deliberately disagree with donor.
	donor.set("c.no-source", base-60, 1)
	rows := []dumpRow{
		sourceRow("b.missing-local", step, 1000, donor.data["b.missing-local"]),
		sourceRow("a.gaps", step, 1000, donor.data["a.gaps"]),
		{Name: "d.not-found", Error: "not found"},
	}
	var previousLines []string
	var previousState map[string]map[int64]float64
	var previousMetrics map[string]int64
	for _, mode := range []string{"http", "plain", "gzip"} {
		t.Run(mode, func(t *testing.T) {
			local := newFake(t)
			local.fill("a.gaps", base, func(int64) float64 { return -99 }, func(ts int64) bool { return ts >= base+3*step && ts <= base+6*step })
			local.set("c.no-source", base, 5)
			local.set("d.not-found", base, 6)
			in := newIngest(t, local.set)
			o := testOptions(local, in.ln.Addr().String(), donor)
			o.lookback = 0
			o.workers = 1
			o.batch = 2
			o.namesFile = filepath.Join(t.TempDir(), "names")
			if err := os.WriteFile(o.namesFile, []byte("a.gaps\nb.missing-local\nc.no-source\nd.not-found\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if mode != "http" {
				o.replicas = nil
				o.sourceDumps = dumpPaths{dumpFixture(t, rows, mode == "gzip")}
			}
			r := runOK(t, o)
			if got := len(in.wait(t, 15)); got != 15 {
				t.Fatalf("sent %d, want 15", got)
			}
			in.mu.Lock()
			sent := append([]string(nil), in.lines...)
			in.mu.Unlock()
			local.mu.Lock()
			state := make(map[string]map[int64]float64)
			for name, pts := range local.data {
				state[name] = make(map[int64]float64)
				for ts, v := range pts {
					state[name][ts] = v
				}
			}
			local.mu.Unlock()
			if state["a.gaps"][base] != -99 {
				t.Fatal("overwrote existing local value")
			}
			if mode == "http" {
				previousLines = sent
				previousState = state
				previousMetrics = r.Metrics
			} else {
				if strings.Join(sent, "\n") != strings.Join(previousLines, "\n") {
					t.Fatalf("plaintext differs:\n%v\n%v", sent, previousLines)
				}
				if !reflect.DeepEqual(state, previousState) {
					t.Fatal("final local state differs")
				}
				if !reflect.DeepEqual(r.Metrics, previousMetrics) {
					t.Fatalf("metrics differ: %v vs %v", r.Metrics, previousMetrics)
				}
			}
		})
	}
}

func TestDumpArchivesAndRanges(t *testing.T) {
	t.Parallel()
	fine := dumpArchive{Step: 10, Count: 60, Points: []dumpPoint{{base + 600, dumpBits(99)}}}
	coarse := dumpArchive{Step: 60, Count: 1000, Points: []dumpPoint{
		{base - 120, dumpBits(1.25)}, {base - 60, dumpBits(0)}, {base, dumpBits(-3.5)}, {base + 60, dumpBits(7)},
	}}
	// Coarse points predate the entire finest retention, and archive order is
	// deliberately reversed: selection must use steps, not array position.
	path := dumpFixture(t, []dumpRow{{Name: "metric", Archives: []dumpArchive{coarse, fine}}}, true)
	d, err := openDump(t.Context(), path, base+1200)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	c := &client{dump: d}
	for _, wantedStep := range []int64{0, 60} {
		got, err := c.fetchSource(t.Context(), []string{"metric", "absent"}, base-121, base, map[string]gap{"metric": {step: wantedStep}})
		if err != nil {
			t.Fatal(err)
		}
		s := got["metric"]
		if s.Start != base-120 || s.Step != 60 || !reflect.DeepEqual(s.Values, []float64{1.25, 0, -3.5}) || len(got) != 1 {
			t.Fatalf("got %+v", got)
		}
	}
	got, err := c.fetchSource(t.Context(), []string{"metric"}, base-120, base-60, map[string]gap{"metric": {step: 60}})
	if err != nil || got["metric"].Start != base-60 || len(got["metric"].Values) != 1 {
		t.Fatalf("range got %v, %v", got, err)
	}
	got, err = c.fetchSource(t.Context(), []string{"metric"}, base-121, base, map[string]gap{"metric": {step: 30}})
	if err != nil || got["metric"].Step == 30 {
		t.Fatalf("incompatible archive: %v %v", got, err)
	}
	e := &engine{cfg: engineConfig{from: base - 120, until: base}}
	if _, ok, mismatch := e.fill("metric", gap{step: 30, slots: []int64{base}}, got["metric"]); ok || !mismatch {
		t.Fatal("incompatible step was not rejected")
	}
}

func TestDumpFallbackAndCheckpoint(t *testing.T) {
	t.Parallel()
	local := newFake(t)
	local.fill("metric", base, func(int64) float64 { return -1 }, func(ts int64) bool { return ts == base+step })
	in := newIngest(t, local.set)
	o := testOptions(local, in.ln.Addr().String())
	o.sourceDumps = dumpPaths{
		dumpFixture(t, []dumpRow{{Name: "metric", Error: "not found"}}, false),
		dumpFixture(t, []dumpRow{sourceRow("metric", step, 1000, map[int64]float64{base + step: 42})}, true),
	}
	o.checkpoint = filepath.Join(t.TempDir(), "checkpoint")
	r := runOK(t, o)
	if r.Metrics["fallback_metrics"] != 1 || len(in.wait(t, 1)) != 1 {
		t.Fatalf("fallback: %+v", r)
	}
	r = runOK(t, o)
	if r.Metrics["skipped_checkpoint"] != 1 || r.Write["points_sent"] != 0 {
		t.Fatalf("checkpoint: %+v", r)
	}
}

func TestDumpValidation(t *testing.T) {
	t.Parallel()
	valid := `{"name":"m","archives":[{"step":60,"count":10,"points":[{"t":60,"bits":"3ff0000000000000"}]}]}`
	for _, input := range []string{
		`{`, `{}`, `{"name":"m","error":"disk failed"}`, valid + "\n" + valid,
		strings.Replace(valid, `"step":60`, `"step":0`, 1),
		strings.Replace(valid, `"bits":"3ff0000000000000"`, `"bits":"invalid"`, 1),
		strings.Replace(valid, `"t":60`, `"t":61`, 1),
	} {
		path := filepath.Join(t.TempDir(), "bad")
		if err := os.WriteFile(path, []byte(input), 0600); err != nil {
			t.Fatal(err)
		}
		if d, err := openDump(t.Context(), path, base); err == nil {
			d.Close()
			t.Fatalf("accepted %s", input)
		}
	}
	path := dumpFixture(t, []dumpRow{{Name: "m", Error: "not found"}}, true)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data[:len(data)-4], 0600); err != nil {
		t.Fatal(err)
	}
	if d, err := openDump(t.Context(), path, base); err == nil {
		d.Close()
		t.Fatal("accepted truncated gzip")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	path = dumpFixture(t, []dumpRow{{Name: "m"}}, false)
	if d, err := openDump(ctx, path, base); err == nil {
		d.Close()
		t.Fatal("ignored cancellation")
	}
}

func TestSourceDumpFlags(t *testing.T) {
	for _, args := range [][]string{
		{"--source-dump", "one,plain", "--source-dump", "two.gz", "--since", "1h"},
		{"--replica", "http://replica", "--since", "1h"},
	} {
		o, err := parseFlags(args)
		if err != nil {
			t.Fatal(err)
		}
		if len(o.sourceDumps) > 0 && !reflect.DeepEqual(o.sourceDumps, dumpPaths{"one,plain", "two.gz"}) {
			t.Fatal(o.sourceDumps)
		}
	}
	if _, err := parseFlags([]string{"--source-dump", "one", "--replica", "http://replica", "--since", "1h"}); err == nil {
		t.Fatal("accepted mixed source modes")
	}
}

// Opt-in RSS probe: the parent can measure this test process with /usr/bin/time
// without including compilation or synthetic-file generation in peak RSS.
// The measured process includes the name list, dump index, local HTTP render
// server and the dry-run engine (no ingestion).
func TestDumpMillionNameRSS(t *testing.T) {
	path := os.Getenv("BACKFILL_RSS_DUMP")
	if path == "" {
		t.Skip("set BACKFILL_RSS_DUMP to a synthetic 1M-name dump")
	}
	began := time.Now()
	d, err := openDump(t.Context(), path, time.Now().Unix())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if len(d.index) != 1_000_000 {
		t.Fatalf("indexed %d names", len(d.index))
	}
	local := syntheticServer(true)
	defer local.Close()
	names := make([]string, 1_000_000)
	for i := range names {
		names[i] = fmt.Sprintf("synthetic.metric.%07d", i)
	}
	e := &engine{
		cfg:      engineConfig{from: base, until: base + 120, batch: 500, workers: 4, dryRun: true},
		local:    newClient(local.URL, &options{timeout: time.Minute, workers: 4}, nil),
		replicas: []*client{{dump: d}},
	}
	if err := e.run(t.Context(), names); err != nil {
		t.Fatal(err)
	}
	if e.stats.scanned.Load() != 1_000_000 || e.stats.fillable.Load() != 2_000_000 {
		t.Fatalf("incomplete scan: %d / %d", e.stats.scanned.Load(), e.stats.fillable.Load())
	}
	t.Logf("indexed, locally rendered and compared %d names in %s", len(d.index), time.Since(began))
}
