package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/whisperio"
)

// Opt-in native qualification on a private XFS fixture, never a live source.
// Uses real retained production captures; fixture timings are not a production
// speed claim. Kill/restart the actual CLI, then force a complete oracle pass.
func TestNativeAdaptiveConversionControlsAndCrashResume(t *testing.T) {
	binary := os.Getenv("GRAPHITE_ADAPTIVE_BINARY")
	corpus := os.Getenv("GRAPHITE_ADAPTIVE_CORPUS")
	root := os.Getenv("GRAPHITE_ADAPTIVE_TEST_ROOT")
	if binary == "" || corpus == "" || root == "" {
		t.Skip("requires explicit native converter, retained corpus and unused private XFS root")
	}
	corpus, err := physicalPath(corpus)
	if err != nil {
		t.Fatal(err)
	}
	root, err = physicalPath(root)
	if err != nil {
		t.Fatal(err)
	}
	if within("/var/lib/carbon/whisper", root) || within("/var/lib/carbon/whisper", corpus) || within(corpus, root) || within(root, corpus) {
		t.Fatal("native fixture must be private and disjoint from production and retained corpus")
	}
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	var sources []string
	err = filepath.WalkDir(corpus, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("native corpus contains symlink: %s", path)
		}
		if !d.IsDir() && strings.HasSuffix(path, ".wsp") {
			sources = append(sources, path)
			if len(sources) > 1000 {
				return fmt.Errorf("native corpus exceeds the 1000-file qualification bound")
			}
		}
		return nil
	})
	if err != nil || len(sources) < 2 || len(sources) > 1000 {
		t.Fatalf("retained corpus: %d files, %v", len(sources), err)
	}
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	const metrics = 2048
	for i := range metrics {
		path := filepath.Join(source, fmt.Sprintf("series%06d.wsp", i))
		if err := whisperio.Capture(context.Background(), sources[i%len(sources)], path, true); err != nil {
			t.Fatalf("clone real capture %d: %v", i, err)
		}
	}
	lock, err := os.OpenFile(filepath.Join(source, "series000000.wsp"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	const sourceID = "native-adaptive-controls-20261020-v1"
	control := filepath.Join(root, "resource-control.json")
	var generation uint64
	lease := func(cpu, workers int) {
		t.Helper()
		generation++
		data, err := json.Marshal(resourceLease{Version: 1, SourceID: sourceID, Generation: generation, ExpiresAt: time.Now().Unix() + 75, CPU: cpu, Workers: workers, MemoryLimitBytes: 2 << 30})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(control+".tmp", data, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(control+".tmp", control); err != nil {
			t.Fatal(err)
		}
	}
	start := func(name string, checkpoints bool, controlled bool) (*exec.Cmd, string) {
		t.Helper()
		log := filepath.Join(root, name+".log")
		f, err := os.Create(log)
		if err != nil {
			t.Fatal(err)
		}
		args := []string{"-source", source, "-source-id", sourceID, "-source-quiesced", "-copy", filepath.Join(root, "copies"), "-destination", filepath.Join(root, "exact"), "-cpu", "8", "-workers", "8", "-max-cpu", "32", "-max-workers", "32", "-verify=true", fmt.Sprintf("-verified-checkpoints=%t", checkpoints)}
		if controlled {
			args = append(args, "-control-file", control)
		}
		cmd := exec.Command(binary, args...)
		cmd.Stdout, cmd.Stderr = f, f
		if err := cmd.Start(); err != nil {
			_ = f.Close()
			t.Fatal(err)
		}
		_ = f.Close()
		t.Cleanup(func() { _ = cmd.Process.Kill() })
		return cmd, log
	}
	waitApplied := func(log string, want uint64) {
		t.Helper()
		deadline := time.Now().Add(6 * time.Second)
		for time.Now().Before(deadline) {
			data, _ := os.ReadFile(log)
			for _, line := range bytes.Split(data, []byte("\n")) {
				prefix := []byte("resource-control applied ")
				if !bytes.HasPrefix(line, prefix) {
					continue
				}
				var got resourceLease
				if json.Unmarshal(line[len(prefix):], &got) == nil && got.Generation == want {
					return
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("native converter did not apply live lease generation %d; log=%s", want, log)
	}
	lease(1, 0)
	killed, killedLog := start("interrupted", true, true)
	waitApplied(killedLog, generation)
	budgets := [][2]int{{4, 4}, {16, 16}, {2, 2}, {1, 0}, {8, 8}}
	for _, budget := range budgets {
		lease(budget[0], budget[1])
		waitApplied(killedLog, generation)
		time.Sleep(250 * time.Millisecond)
	}
	// Keep one private metric blocked, so completion is impossible; let the
	// reporter group-sync successful verification receipts before the crash.
	time.Sleep(12 * time.Second)
	if err := killed.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := killed.Wait(); err == nil {
		t.Fatal("native killed converter unexpectedly completed")
	}
	if _, err := os.Stat(filepath.Join(root, "exact", "CONVERSION-IN-PROGRESS")); err != nil {
		t.Fatalf("interrupted conversion lost its serving guard: %v", err)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	lease(8, 8)
	resumed, resumeLog := start("resumed", true, true)
	if err := resumed.Wait(); err != nil {
		t.Fatalf("native resume: %v; log=%s", err, resumeLog)
	}
	last := func(log string) map[string]any {
		t.Helper()
		data, err := os.ReadFile(log)
		if err != nil {
			t.Fatal(err)
		}
		var result map[string]any
		for _, line := range bytes.Split(data, []byte("\n")) {
			var row map[string]any
			if json.Unmarshal(line, &row) == nil && row["phase"] != nil {
				result = row
			}
		}
		if result["phase"] != "baseline-complete" || result["errors"] != float64(0) || result["completed_metrics"] != float64(metrics) {
			t.Fatalf("native conversion did not completely verify all fixtures: %+v", result)
		}
		return result
	}
	resumeResult := last(resumeLog)
	if resumeResult["checkpoint_skips"].(float64) == 0 {
		t.Fatal("crash/resume did not preserve any verified progress")
	}
	oracle, oracleLog := start("full-oracle", false, false)
	if err := oracle.Wait(); err != nil {
		t.Fatalf("complete checkpoint-independent oracle: %v; log=%s", err, oracleLog)
	}
	oracleResult := last(oracleLog)
	if oracleResult["checkpoint_skips"] != float64(0) || oracleResult["points"] != resumeResult["points"] || oracleResult["resumed"] != float64(metrics) {
		t.Fatalf("full oracle disagrees with resumed conversion: %+v / %+v", resumeResult, oracleResult)
	}
	if _, err := os.Stat(filepath.Join(root, "exact", "CONVERSION-IN-PROGRESS")); !os.IsNotExist(err) {
		t.Fatalf("complete native oracle retained conversion guard: %v", err)
	}
	report := map[string]any{"passed": true, "retained_production_metrics": len(sources), "fixture_metrics": metrics, "live_budget_changes": len(budgets), "leases_issued": generation, "crash": "SIGKILL of private converter while one private metric was flocked", "resume": resumeResult, "full_checkpoint_independent_oracle": oracleResult, "scope": "Native functional/liveness/crash qualification; production speed and serving effects require actual full-node governor observations."}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "RESULT.json"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Log(string(data))
}
