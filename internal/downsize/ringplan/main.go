// ring plans ingestion ownership changes without changing a store or a relay.
package ringplan

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"
	"time"
)

func Main(args []string) error {
	flags := flag.NewFlagSet("resize plan", flag.ContinueOnError)
	mode := flags.String("mode", "plan", "plan, compare, or config (local artifacts only)")
	specPath := flags.String("spec", "", "JSON specification (see FORMAT.md)")
	out := flags.String("out", "", "local output directory")
	apply := flags.Bool("apply", false, "write local plan/config files; default only calculates and prints")
	workers := flags.Int("workers", min(4, runtime.GOMAXPROCS(0)), "parallel inventory readers, 1..64")
	perSource := flags.Int("per-source-workers", 1, "max readers per physical source, 1..16")
	segment := flags.Int("segment-metrics", 100000, "records per durable resume segment")
	rate := flags.Float64("names-per-second", 0, "per-source inventory rate ceiling (0 unlimited)")
	timeout := flags.Duration("http-timeout", 30*time.Minute, "whole buckyd inventory request timeout")
	target := flags.Int("target-nodes", 0, "compare: number of surviving members")
	samples := flags.Int("samples", 100000, "compare: deterministic synthetic names if no inventories")
	maxCandidates := flags.Int("max-candidates", 1000, "compare: reject larger subset searches")
	cpuProfile := flags.String("cpuprofile", "", "write local CPU profile")
	memProfile := flags.String("memprofile", "", "write local heap profile after completion")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *specPath == "" {
		return fmt.Errorf("--spec is required")
	}
	s, old, next, id, err := loadSpec(*specPath)
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			return err
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			f.Close()
			return err
		}
		defer func() { pprof.StopCPUProfile(); f.Close() }()
	}
	if *memProfile != "" {
		defer func() {
			f, err := os.Create(*memProfile)
			if err == nil {
				runtime.GC()
				_ = pprof.WriteHeapProfile(f)
				_ = f.Close()
			}
		}()
	}
	fmt.Fprintf(os.Stderr, "mode=%s local_write=%t old=%d/%s new=%d/%s inventories=%d out=%q plan_id=%s\n", *mode, *apply, len(s.Old.Nodes), s.Old.Hash, len(s.New.Nodes), s.New.Hash, len(s.Inputs), *out, id)
	for _, item := range []struct {
		label   string
		members []member
	}{{"old", old.members}, {"new", next.members}} {
		for index, m := range item.members {
			fmt.Fprintf(os.Stderr, "%s[%d] id=%q node=%q buckyd=%q\n", item.label, index, m.ID, m.Node, m.Endpoint)
		}
	}
	for _, in := range s.Inputs {
		location := in.Path
		if in.Format == "buckyd" {
			location = in.URL
		}
		fmt.Fprintf(os.Stderr, "read source=%q format=%s input=%q estimated_bytes=%d token_env=%q\n", in.Source, in.Format, location, in.EstimatedBytes, in.TokenEnv)
	}
	opt := options{Out: *out, Apply: *apply, Workers: *workers, PerSourceWorkers: *perSource, SegmentMetrics: *segment, Rate: *rate, HTTPTimeout: *timeout}
	switch *mode {
	case "plan":
		started := time.Now()
		m, err := plan(ctx, s, old, next, id, opt)
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "plan ready: %d records, %d copies, %d estimated bytes; elapsed %s (includes checkpoint reuse)\n", m.Summary.Metrics, m.Summary.Copies, m.Summary.EstimatedBytes, time.Since(started))
		return json.NewEncoder(os.Stdout).Encode(m.Summary)
	case "compare":
		result, err := compare(ctx, s, old, *target, *samples, *maxCandidates, *timeout)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(result)
	case "config":
		return generateConfig(s, old, next, id, opt, os.Stdout)
	default:
		return fmt.Errorf("unknown mode %q", *mode)
	}
}
