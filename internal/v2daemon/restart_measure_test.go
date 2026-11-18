package v2daemon

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/points"
)

func restartMeasureConfig() Config {
	segment, _ := strconv.ParseInt(os.Getenv("V2_RESTART_MEASURE_SEGMENT"), 10, 64)
	if segment == 0 {
		segment = 1 << 20
	}
	shards, _ := strconv.Atoi(os.Getenv("V2_RESTART_MEASURE_SHARDS"))
	if shards == 0 {
		shards = 8
	}
	return Config{Layer: 3, Producers: 8, Shards: shards, GroupInterval: time.Hour, SealInterval: time.Hour, JournalSegmentBytes: segment}
}

// TestCoreOpenMeasure times core open of a kept measurement store at several
// GOMAXPROCS values; the open limit is min(16, GOMAXPROCS). Env-gated.
func TestCoreOpenMeasure(t *testing.T) {
	root := os.Getenv("V2_CORE_OPEN_DIR")
	if root == "" {
		t.Skip("set V2_CORE_OPEN_DIR")
	}
	for _, procs := range []int{2, 4, 8, 16, 2, 4, 8, 16} {
		dir := t.TempDir()
		copyTree(t, root, dir)
		old := runtime.GOMAXPROCS(procs)
		t0 := time.Now()
		e, err := v2core.Open(filepath.Join(dir, "placement-group-00000001", "core"), coreConfig(restartMeasureConfig()), v2core.Hooks{})
		open := time.Since(t0)
		runtime.GOMAXPROCS(old)
		if err != nil {
			t.Fatal(err)
		}
		if err = e.Close(); err != nil {
			t.Fatal(err)
		}
		t.Logf("gomaxprocs=%d core-open=%.2fs", procs, open.Seconds())
		_ = os.RemoveAll(dir)
	}
}

// TestRestartPhaseMeasure times kill -> open phases on a fixed store. It only
// runs with V2_RESTART_MEASURE=<rounds>; the result is a log line, not a gate.
func TestRestartPhaseMeasure(t *testing.T) {
	const names = 4096
	if root := os.Getenv("V2_RESTART_MEASURE_CHILD"); root != "" {
		rounds, _ := strconv.Atoi(os.Getenv("V2_RESTART_MEASURE"))
		tail, _ := strconv.Atoi(os.Getenv("V2_RESTART_MEASURE_TAIL"))
		if tail == 0 {
			tail = 8
		}
		s, err := Open(root, restartMeasureConfig())
		if err != nil {
			t.Fatal(err)
		}
		policies := make([][]byte, names*len(s.Producers))
		for j := range policies {
			policies[j], err = v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: partsformat.ID(fmt.Sprintf("m.%d.%d", j/names, j%names)), Group: "test", Config: sealrollup.Config{Method: sealrollup.Average, Archives: []sealrollup.Archive{{Step: 10, Count: 360}, {Step: 60, Count: 1440}}}})
			if err != nil {
				t.Fatal(err)
			}
		}
		for round := 0; round < rounds; round++ {
			for i, p := range s.Producers {
				apps := make([]Application, names)
				for j := range apps {
					apps[j] = Application{Name: fmt.Sprintf("m.%d.%d", i, j), Policy: policies[i*names+j], Order: uint64(round*names + j + 1), Clock: int64(1_000_000 + round*10), Points: []points.Point{{Timestamp: int64(1_000_000 + round*10), Value: float64(j*round) / 3}}, Sequence: []uint64{uint64(round + 1)}}
				}
				for k := 0; k < names; k += 1024 {
					if err = p.AppendApplications(apps[k : k+1024]); err != nil {
						t.Fatal(err)
					}
				}
				// Seal every 8 rounds and before the unsealed tail.
				if (round%8 == 7 || round == rounds-tail-1) && round < rounds-tail {
					if err = p.Seal(); err != nil {
						t.Fatal(err)
					}
				}
				if err = p.Drain(); err != nil {
					t.Fatal(err)
				}
			}
		}
		fmt.Println("CUT")
		select {}
	}
	rounds := os.Getenv("V2_RESTART_MEASURE")
	if rounds == "" {
		t.Skip("set V2_RESTART_MEASURE=<rounds>")
	}
	root := t.TempDir()
	start := time.Now()
	cmd := exec.Command(os.Args[0], "-test.run=^TestRestartPhaseMeasure$", "-test.timeout=20m")
	cmd.Env = append(os.Environ(), "V2_RESTART_MEASURE_CHILD="+root)
	cmd.Stderr = os.Stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill() }()
	scan := bufio.NewScanner(out)
	for scan.Scan() && scan.Text() != "CUT" {
		t.Log("child:", scan.Text())
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	t.Logf("write %.1fs", time.Since(start).Seconds())
	if keep := os.Getenv("V2_RESTART_MEASURE_KEEP"); keep != "" {
		copyTree(t, root, keep)
	}
	for _, full := range []bool{true, false, true, false} {
		dir := t.TempDir()
		copyTree(t, root, dir)
		c := restartMeasureConfig()
		c.fullReplay = full
		t0 := time.Now()
		e, err := v2core.Open(filepath.Join(dir, "placement-group-00000001", "core"), coreConfig(c), v2core.Hooks{})
		if err != nil {
			t.Fatal(err)
		}
		if err = e.Close(); err != nil {
			t.Fatal(err)
		}
		coreOpen := time.Since(t0)
		t1 := time.Now()
		s, err := Open(dir, c)
		if err != nil {
			t.Fatal(err)
		}
		ready := time.Since(t1)
		skipped, replayed := 0, 0
		var journal int64
		for _, p := range s.Producers {
			skipped += p.RecoverySkippedSegments
			replayed += p.RecoveryReplayedSegments
		}
		_ = filepath.Walk(filepath.Join(dir, "placement-group-00000001", "journal"), func(_ string, info os.FileInfo, err error) error {
			if err == nil && !info.IsDir() && filepath.Ext(info.Name()) == ".pdc4" {
				journal += info.Size()
			}
			return nil
		})
		if err = s.Close(); err != nil {
			t.Fatal(err)
		}
		t.Logf("full=%v journal=%.1fMB segments skipped=%d replayed=%d core-open=%.2fs open->ready=%.2fs journal+reseal=%.2fs",
			full, float64(journal)/1e6, skipped, replayed, coreOpen.Seconds(), ready.Seconds(), (ready - coreOpen).Seconds())
		_ = os.RemoveAll(dir)
	}
}
