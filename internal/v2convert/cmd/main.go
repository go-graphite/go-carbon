// v2convert is an offline SST diagnostic and current-core repacker. Native
// daemon SSTs are scan-only until a certified source adapter is installed.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2convert"
)

func main() {
	source := flag.String("source", "", "owned immutable SST directory")
	output := flag.String("output", "", "repack materialized GCP3 SSTs to this directory; omit for scan only")
	limit := flag.Int("limit", 0, "maximum number of source SSTs (0 = all)")
	memory := flag.Int("part-mib", 256, "maximum decoded input table size in MiB")
	flag.Parse()
	if *source == "" {
		fmt.Fprintln(os.Stderr, "-source is required")
		os.Exit(2)
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
	for _, path := range paths {
		start := time.Now()
		r := result{File: filepath.Base(path)}
		if *output == "" {
			r.Stats, err = v2convert.ScanSST(ctx, path, nil)
		} else {
			receipt, e := v2convert.ConvertSST(ctx, path, v2convert.Options{Directory: *output, PartBytes: *memory << 20, Adapter: v2convert.CoreRecords{}, Writer: v2convert.CoreWriter{}})
			err = e
			r.Receipt = &receipt
			r.Stats.Bytes = receipt.InputBytes
		}
		r.Seconds = time.Since(start).Seconds()
		total += r.Stats.Bytes
		if err != nil {
			r.Error = err.Error()
		}
		if e := encoder.Encode(r); e != nil {
			fail(e)
		}
		if err != nil {
			os.Exit(1)
		}
	}
	if err := encoder.Encode(map[string]any{"summary": true, "files": len(paths), "bytes": total, "seconds": time.Since(begin).Seconds(), "gomaxprocs": runtime.GOMAXPROCS(0)}); err != nil {
		fail(err)
	}
}
func fail(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
