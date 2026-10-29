// continuation-replay runs a private paired CWhisper/shared-state diagnostic.
// Production files are inputs; every engine writes a newly created private tree.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

type replayReceipt struct{ PlanSHA256, OperationsSHA256 string }

func disjointRoots(source, output string) error {
	for _, pair := range [][2]string{{source, output}, {output, source}} {
		r, err := filepath.Rel(pair[0], pair[1])
		if err != nil {
			return err
		}
		if filepath.IsLocal(r) {
			return errors.New("source and output must be disjoint")
		}
	}
	return nil
}

func runPair(planPath, operations, root string, paced bool, run func(string) error) error {
	plan, err := loadPlan(planPath)
	if err != nil {
		return err
	}
	// Parse the whole trace before creating an engine, so malformed late records
	// cannot produce an apparently complete partial result.
	if err := eachOperation(operations, plan, func(int, replayOperation) error { return nil }); err != nil {
		return err
	}
	source, err := filepath.EvalSymlinks(plan.SourceRoot)
	if err != nil {
		return err
	}
	if !filepath.IsAbs(root) {
		return errors.New("output must be absolute")
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(root))
	if err != nil {
		return err
	}
	if err := disjointRoots(source, filepath.Join(parent, filepath.Base(root))); err != nil {
		return err
	}
	if err := os.Mkdir(root, 0700); err != nil {
		return err
	}
	output, err := filepath.EvalSymlinks(root)
	if err != nil {
		return err
	}
	if err := disjointRoots(source, output); err != nil {
		return err
	}
	planHash, err := fileSHA(planPath)
	if err != nil {
		return err
	}
	opsHash, err := fileSHA(operations)
	if err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(root, "REPLAY-MARKER.json"), replayReceipt{planHash, opsHash}); err != nil {
		return err
	}
	// Hash both inputs and every main/OOO pair around the complete run. A live
	// source advancing between the two engines must invalidate the comparison.
	sourceHashes, err := sourceFingerprints(plan)
	if err != nil {
		return err
	}
	for _, row := range plan.Rows {
		path := filepath.Join(plan.SourceRoot, row.Path)
		if sourceHashes[path] != row.MainSHA256 || sourceHashes[sidecarPath(path)] != row.OOOSHA256 {
			return errors.New("source does not match the pre-replay capture ledger")
		}
	}
	if err := writeJSON(filepath.Join(root, "SOURCE-SHA256.json"), sourceHashes); err != nil {
		return err
	}
	for _, mode := range []string{"reference", "candidate"} {
		if err := run(mode); err != nil {
			return err
		}
	}
	after, err := sourceFingerprints(plan)
	if err != nil {
		return err
	}
	if len(after) != len(sourceHashes) {
		return errors.New("source changed during replay")
	}
	for path, hash := range sourceHashes {
		if after[path] != hash {
			return errors.New("source changed during replay")
		}
	}
	for _, pair := range []struct{ path, want string }{{planPath, planHash}, {operations, opsHash}} {
		got, err := fileSHA(pair.path)
		if err != nil {
			return err
		}
		if got != pair.want {
			return errors.New("replay inputs changed")
		}
	}
	for _, file := range []string{"READS.jsonl", "REOPEN.json"} {
		a, err := fileSHA(filepath.Join(root, "reference", file))
		if err != nil {
			return err
		}
		b, err := fileSHA(filepath.Join(root, "candidate", file))
		if err != nil {
			return err
		}
		if a != b {
			return fmt.Errorf("paired %s differs", file)
		}
	}
	options := store.Options{Continuation: &whisperio.ContinuationCodec{}, ExactBlocks: true, IncrementalSource: filepath.Join(root, "candidate", "originals"),
		CacheSize: plan.CacheBytes, MemTableSize: plan.MemtableBytes, SyncInterval: time.Second, Now: func() time.Time { return time.Unix(0, plan.EndNS) }}
	db, err := store.Open(filepath.Join(root, "candidate", "shared"), options)
	if err != nil {
		return err
	}
	for _, row := range plan.Rows {
		if err := db.VerifyContinuationSource(context.Background(), row.Metric, filepath.Join(root, "reference", "originals", row.Path)); err != nil {
			_ = db.Close()
			return err
		}
	}
	if err := db.Close(); err != nil {
		return err
	}
	return writeJSON(filepath.Join(root, "RESULT.json"), map[string]any{"correctness_passed": true, "performance_qualified": false, "corpus_metrics": len(plan.Rows),
		"sampling": plan.Sampling, "population_bytes": plan.PopulationBytes, "plan_sha256": planHash, "operations_sha256": opsHash, "paced": paced,
		"scope": "Identical captured per-metric write batches and read windows, in arrival order, separate engine processes and private copies. Exact read hashes, forced OOO compaction, reopen, full physical writer-state verification and unchanged source hashes. One writer; no production cache/index/HTTP or fleet capacity claim. Resource figures include validation work; RSS is the process lifetime peak, including conversion."})
}

func sourceFingerprints(plan replayPlan) (map[string]string, error) {
	result := make(map[string]string)
	for _, row := range plan.Rows {
		main := filepath.Join(plan.SourceRoot, row.Path)
		for _, path := range []string{main, sidecarPath(main)} {
			h, err := fileSHA(path)
			if errors.Is(err, os.ErrNotExist) && path != main {
				h = "absent"
			} else if err != nil {
				return nil, err
			}
			result[path] = h
		}
	}
	return result, nil
}

func childMarker(root, planPath, operations string) error {
	b, err := os.ReadFile(filepath.Join(root, "REPLAY-MARKER.json"))
	if err != nil {
		return err
	}
	var receipt replayReceipt
	if err := json.Unmarshal(b, &receipt); err != nil {
		return err
	}
	p, err := fileSHA(planPath)
	if err != nil {
		return err
	}
	o, err := fileSHA(operations)
	if err != nil {
		return err
	}
	if p != receipt.PlanSHA256 || o != receipt.OperationsSHA256 {
		return errors.New("child replay inputs do not match the parent")
	}
	return nil
}

func main() {
	planPath := flag.String("plan", "", "private weighted corpus and replay configuration JSON")
	operations := flag.String("operations", "", "captured read/write operations in JSONL arrival order")
	ingress := flag.String("prepare-ingress", "", "prepare a labelled workload from a private GTREPL01 ingress trace")
	root := flag.String("work-dir", "", "new absolute private output directory")
	engine := flag.String("engine", "", "internal child mode")
	unpaced := flag.Bool("unpaced", false, "functional diagnostic only: omit arrival-time pacing")
	profile := flag.Bool("profile", false, "record CPU and allocation profiles for the measured interval of each engine")
	procs := flag.Int("procs", 2, "maximum Go execution threads per engine")
	flag.Parse()
	if *planPath == "" || *operations == "" || *root == "" && *ingress == "" || *procs < 1 || *procs > 16 {
		fmt.Fprintln(os.Stderr, "explicit plan, operations, work directory and 1..16 procs required")
		os.Exit(2)
	}
	runtime.GOMAXPROCS(*procs)
	var err error
	if *ingress != "" {
		if *engine != "" || *unpaced || *root != "" {
			err = errors.New("ingress preparation uses only plan, operations, prepare-ingress and procs")
		} else {
			err = prepareIngress(*planPath, *ingress, *operations)
		}
	} else if *engine != "" {
		if *engine != "reference" && *engine != "candidate" {
			err = errors.New("invalid child engine")
		} else if err = childMarker(*root, *planPath, *operations); err == nil {
			var plan replayPlan
			plan, err = loadPlan(*planPath)
			if err == nil {
				directory := filepath.Join(*root, *engine)
				err = os.Mkdir(directory, 0700)
				if err == nil {
					err = runEngine(*engine, directory, plan, *operations, !*unpaced, *profile)
				}
			}
		}
	} else {
		exe, e := os.Executable()
		if e != nil {
			err = e
		} else {
			err = runPair(*planPath, *operations, *root, !*unpaced, func(mode string) error {
				args := []string{"-plan", *planPath, "-operations", *operations, "-work-dir", *root, "-engine", mode, "-procs", fmt.Sprint(*procs)}
				if *unpaced {
					args = append(args, "-unpaced")
				}
				if *profile {
					args = append(args, "-profile")
				}
				cmd := exec.Command(exe, args...)
				cmd.Stdout = os.Stderr
				cmd.Stderr = os.Stderr
				return cmd.Run()
			})
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
