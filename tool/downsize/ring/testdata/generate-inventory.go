//go:build ignore

// Generate immutable name inventories for planner benchmarks; no store access.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	hashing "github.com/go-graphite/go-carbon/internal/downsize/buckyhash"
)

func main() {
	n := flag.Int("metrics", 1000000, "total unique names")
	dir := flag.String("dir", "", "new output directory")
	template := flag.String("template", "tool/downsize/ring/testdata/metrics-region-a-6-to-3.json", "six-to-three specification template")
	bytesPerMetric := flag.Int64("bytes", 600, "estimated stored bytes per metric")
	flag.Parse()
	if *n < 1 || *dir == "" {
		panic("metrics>0 and dir required")
	}
	blob, err := os.ReadFile(*template)
	must(err)
	var spec map[string]json.RawMessage
	must(json.Unmarshal(blob, &spec))
	var old struct{ Nodes []struct{ ID string } }
	must(json.Unmarshal(spec["old"], &old))
	if len(old.Nodes) != 6 {
		panic("template must have six old members in index order")
	}
	must(os.Mkdir(*dir, 0700))
	abs, err := filepath.Abs(*dir)
	must(err)
	type input struct {
		Source         string `json:"source"`
		Path           string `json:"path"`
		Format         string `json:"format"`
		EstimatedBytes int64  `json:"estimated_bytes"`
	}
	inputs := make([]input, 6)
	files := make([]*os.File, 6)
	writers := make([]*bufio.Writer, 6)
	for i := range files {
		path := filepath.Join(abs, fmt.Sprintf("inventory-%03d.txt", i))
		files[i], err = os.Create(path)
		must(err)
		writers[i] = bufio.NewWriterSize(files[i], 1<<20)
		inputs[i] = input{old.Nodes[i].ID, path, "lines", *bytesPerMetric}
	}
	started := time.Now()
	for i := 0; i < *n; i++ {
		name := fmt.Sprintf("aggregations.minutely.metric_%012d.by_service.service_%03d.by_host.host_%04d", i, i%997, i%1777)
		owner := hashing.Jump(hashing.Fnv1a64([]byte(name)), 6)
		_, err := writers[owner].WriteString(name + "\n")
		must(err)
	}
	for i := range files {
		must(writers[i].Flush())
		must(files[i].Close())
	}
	spec["inputs"], err = json.Marshal(inputs)
	must(err)
	blob, err = json.MarshalIndent(spec, "", "  ")
	must(err)
	must(os.WriteFile(filepath.Join(abs, "spec.json"), blob, 0600))
	fmt.Printf("generated %d names in %s under %s\n", *n, time.Since(started), abs)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
