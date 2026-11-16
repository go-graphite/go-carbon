// v2convert repacks materialized core parts or an exclusively owned coherent
// native checkpoint. Loose native SSTs support physical scan diagnostics only.
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2part"

	"github.com/go-graphite/go-carbon/internal/v2convert"
)

func main() {
	nativePlan := flag.String("native-plan", "", "certified coherent checkpoint plan JSON; enables native baseline materialization")
	nativeWorkers := flag.Int("native-workers", 1, "native snapshot materializer workers (1..64)")
	nativePrefix := flag.String("native-prefix", "", "catalog prefix if the native plan omits its bounded name selection")
	nativeLimit := flag.Int("native-limit", 128, "maximum names to discover when the native plan omits names (1..10000)")
	source := flag.String("source", "", "owned immutable SST directory")
	output := flag.String("output", "", "repack materialized GCP3 SSTs to this directory; omit for scan only")
	limit := flag.Int("limit", 0, "maximum number of source SSTs (0 = all)")
	workers := flag.Int("scan-workers", 1, "parallel forward SST readers in scan-only mode")
	memory := flag.Int("part-mib", 256, "maximum decoded input table size in MiB")
	incarnation := flag.String("source-incarnation", "", "original source incarnation, 32 hex digits; required for output")
	stream := flag.String("source-stream", "", "original ordering stream; required for output")
	flag.Parse()
	if *workers < 1 || (*output != "" && *workers != 1) {
		fail(fmt.Errorf("scan-workers must be positive and may exceed one only in scan-only mode"))
	}
	var provenance v2part.Provenance
	if *output != "" && *nativePlan == "" {
		b, e := hex.DecodeString(*incarnation)
		if e != nil || len(b) != 16 || *stream == "" {
			fail(fmt.Errorf("original source-incarnation and source-stream required"))
		}
		copy(provenance.Incarnation[:], b)
		provenance.Stream = *stream
	}
	if *source == "" {
		fmt.Fprintln(os.Stderr, "-source is required")
		os.Exit(2)
	}
	if *nativePlan != "" {
		if *output == "" {
			fail(fmt.Errorf("native materialization requires -output"))
		}
		data, err := os.ReadFile(*nativePlan)
		if err != nil {
			fail(err)
		}
		var plan v2convert.NativeCheckpointPlan
		if err := json.Unmarshal(data, &plan); err != nil {
			fail(err)
		}
		begin := time.Now()
		ctx := context.Background()
		reader, err := v2convert.OpenNativeCheckpoint(ctx, *source, plan)
		if err != nil {
			fail(err)
		}
		if len(plan.Names) == 0 {
			if *nativeLimit < 1 || *nativeLimit > 10000 {
				reader.Close()
				fail(fmt.Errorf("native-limit must be 1..10000"))
			}
			page, e := reader.ListCatalogPage(ctx, *nativePrefix, "", *nativeLimit)
			if e != nil {
				reader.Close()
				fail(e)
			}
			for _, m := range page {
				plan.Names = append(plan.Names, m.Name)
			}
		}
		receipt, err := v2convert.ConvertNativeCheckpoint(ctx, reader, plan, *output, *memory<<20, *nativeWorkers)
		closeErr := reader.Close()
		if err == nil {
			err = closeErr
		}
		if err != nil {
			fail(err)
		}
		if err := json.NewEncoder(os.Stdout).Encode(map[string]any{"native": true, "workers": *nativeWorkers, "seconds": time.Since(begin).Seconds(), "names": len(plan.Names), "receipt": receipt}); err != nil {
			fail(err)
		}
		return
	}
	paths, err := filepath.Glob(filepath.Join(*source, "*.sst"))
	if err != nil {
		fail(err)
	}
	if len(paths) == 0 {
		fail(fmt.Errorf("no SST inputs"))
	}
	if *limit > 0 && len(paths) > *limit {
		paths = paths[:*limit]
	}
	begin := time.Now()
	ctx := context.Background()
	type result struct {
		File    string
		Stats   v2convert.ScanStats
		Seconds float64
		Error   string             `json:",omitempty"`
		Receipt *v2convert.Receipt `json:",omitempty"`
	}
	encoder := json.NewEncoder(os.Stdout)
	var total int64
	var scans []v2convert.ScanResult
	if *output == "" {
		scans, err = v2convert.ScanFiles(ctx, paths, *workers)
		if err != nil {
			fail(err)
		}
	}
	failed := false
	for i, path := range paths {
		start := time.Now()
		r := result{File: filepath.Base(path)}
		if *output == "" {
			r.Stats, err = scans[i].Stats, scans[i].Err
			r.Seconds = scans[i].Seconds
		} else {
			receipt, e := v2convert.ConvertSST(ctx, path, v2convert.Options{Directory: *output, PartBytes: *memory << 20, Adapter: v2convert.CoreRecords{}, Writer: v2convert.CoreWriter{Source: provenance}})
			err = e
			r.Receipt = &receipt
			r.Stats.Bytes = receipt.InputBytes
		}
		if *output != "" {
			r.Seconds = time.Since(start).Seconds()
		}
		total += r.Stats.Bytes
		if err != nil {
			r.Error = err.Error()
		}
		if e := encoder.Encode(r); e != nil {
			fail(e)
		}
		if err != nil {
			if *output != "" {
				os.Exit(1)
			}
			failed = true
		}
	}
	if err := encoder.Encode(map[string]any{"summary": true, "files": len(paths), "bytes": total, "seconds": time.Since(begin).Seconds(), "gomaxprocs": runtime.GOMAXPROCS(0), "scan_workers": *workers, "failed": failed}); err != nil {
		fail(err)
	}
	if failed {
		os.Exit(1)
	}
}
func fail(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
