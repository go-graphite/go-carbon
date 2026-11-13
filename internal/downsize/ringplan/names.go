package ringplan

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// NamesPlan plans an explicit, byte-sorted unique name list from one source
// to one target. It is the same go-carbon-ring plan that
// `config --mode plan` writes for a one-member old ring (the source), a
// one-member new ring (the target) and a "lines" inventory of the list.
type NamesPlan struct {
	Names, Source, Target, Out string
	Apply                      bool
	SegmentMetrics             int
	// EstimatedBytes is the per-name estimate the importer batches by;
	// zero means DefaultNamesEstimate.
	EstimatedBytes int64
}

// NamesSpecification returns the generated specification for a names plan.
// The names path is made absolute so the plan identity does not depend on
// the working directory.
func NamesSpecification(names, source, target string, estimate int64) ([]byte, error) {
	if names == "" || source == "" || target == "" {
		return nil, errors.New("--names, --source and --target are required together")
	}
	for _, endpoint := range []string{source, target} {
		if err := validURL(endpoint); err != nil {
			return nil, fmt.Errorf("endpoint %q: %w", endpoint, err)
		}
	}
	if strings.TrimRight(source, "/") == strings.TrimRight(target, "/") {
		return nil, errors.New("source and target are the same endpoint")
	}
	path, err := filepath.Abs(names)
	if err != nil {
		return nil, err
	}
	ring := func(id, endpoint string) topology {
		// Single-member rings route every listed name source -> target;
		// distinct instances keep the two hash identities distinct.
		return topology{Hash: "jump_fnv1a", Replicas: 1, Nodes: []member{{ID: id, Node: id + "=" + id, Endpoint: strings.TrimRight(endpoint, "/")}}}
	}
	return json.Marshal(specification{Version: version, Cluster: "names", Site: "names",
		Old: ring("source", source), New: ring("target", target),
		Inputs: []input{{Source: "source", Path: path, Format: "lines", EstimatedBytes: estimate}}})
}

// checkNamesFile fails before planning with an actionable message: the lines
// inventory needs byte order (LC_ALL=C sort -u) and at least one name.
func checkNamesFile(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	var previous []byte
	count := 0
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			return count, fmt.Errorf("names file line %d is empty", count+1)
		}
		if count > 0 && bytes.Compare(line, previous) <= 0 {
			return count, fmt.Errorf("names file line %d: %q after %q; the list must be byte-sorted and unique (LC_ALL=C sort -u)", count+1, line, previous)
		}
		previous = append(previous[:0], line...)
		count++
	}
	if err := scanner.Err(); err != nil {
		return count, err
	}
	if count == 0 {
		return 0, errors.New("names file has no names")
	}
	return count, nil
}

// PlanNames writes (with Apply) or prints a names plan.
func PlanNames(ctx context.Context, p NamesPlan) (Manifest, error) {
	if p.EstimatedBytes == 0 {
		p.EstimatedBytes = DefaultNamesEstimate
	}
	blob, err := NamesSpecification(p.Names, p.Source, p.Target, p.EstimatedBytes)
	if err != nil {
		return manifest{}, err
	}
	if _, err := checkNamesFile(p.Names); err != nil {
		return manifest{}, err
	}
	base, err := os.Getwd()
	if err != nil {
		return manifest{}, err
	}
	s, old, next, id, err := parseSpec(bytes.NewReader(blob), base)
	if err != nil {
		return manifest{}, err
	}
	return plan(ctx, s, old, next, id, options{Out: p.Out, Apply: p.Apply, Workers: 1, PerSourceWorkers: 1, SegmentMetrics: p.SegmentMetrics, HTTPTimeout: 30 * time.Minute})
}
