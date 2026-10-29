package main

import (
	"encoding/json"
	"errors"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

func TestMain(m *testing.M) {
	if os.Getenv("GO_CARBON_REPLAY_TEST_HELPER") == "1" {
		main()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func replayFixture(t *testing.T) (string, string, replayPlan) {
	t.Helper()
	dir := t.TempDir()
	source := filepath.Join(dir, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	const base = int64(1792432631)
	p := replayPlan{Version: 1, SourceRoot: source, Sampling: "synthetic harness fixture; no production evaluation", StartNS: base * int64(time.Second), WarmupUntilNS: (base + 2) * int64(time.Second), EndNS: (base + 55) * int64(time.Second), CacheBytes: 1 << 20, MemtableBytes: 1 << 20}
	for _, name := range []string{"integers", "floats"} {
		path := filepath.Join(source, name+".wsp")
		w, err := whisper.CreateWithOptions(path, whisper.MustParseRetentionDefs("1s:20s,5s:2m,30s:10m"), whisper.Average, .5, &whisper.Options{Compressed: true, OutOfOrder: true, PointsPerBlock: 16, Now: func() time.Time { return time.Unix(base, 0) }})
		if err != nil {
			t.Fatal(err)
		}
		var points []*whisper.TimeSeriesPoint
		for age := 18; age > 0; age-- {
			v := float64(age)
			if name == "floats" {
				v += .1
			}
			points = append(points, &whisper.TimeSeriesPoint{Time: int(base) - age, Value: v})
		}
		if err := w.UpdateMany(points); err != nil {
			t.Fatal(err)
		}
		if err := w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: int(base) - 10, Value: 12.25}}); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		var allocated int64
		for _, file := range []string{path, sidecarPath(path)} {
			info, err := os.Stat(file)
			if err != nil {
				t.Fatal(err)
			}
			allocated += info.Sys().(*syscall.Stat_t).Blocks * 512
		}
		mainHash, err := fileSHA(path)
		if err != nil {
			t.Fatal(err)
		}
		oooHash, err := fileSHA(sidecarPath(path))
		if err != nil {
			t.Fatal(err)
		}
		p.Rows = append(p.Rows, replayRow{Metric: name, Path: name + ".wsp", AllocatedBytes: allocated, InclusionProbability: 1, CapturedAtNS: (base - 1) * int64(time.Second), MainSHA256: mainHash, OOOSHA256: oooHash})
		p.PopulationBytes += allocated
	}
	planPath := filepath.Join(dir, "plan.json")
	if err := writeJSON(planPath, p); err != nil {
		t.Fatal(err)
	}
	operations := filepath.Join(dir, "ops.jsonl")
	f, err := os.Create(operations)
	if err != nil {
		t.Fatal(err)
	}
	encoder := json.NewEncoder(f)
	for step := int64(1); step <= 50; step++ {
		stamp := base + step
		for index := range p.Rows {
			v := float64(step)
			if index == 1 {
				v += .1
			}
			op := replayOperation{AtNS: stamp * int64(time.Second), Kind: "write", Metric: index, Points: []replayPoint{{Timestamp: stamp - 1, Bits: math.Float64bits(v)}, {Timestamp: stamp - 17, Bits: math.Float64bits(v + .25)}, {Timestamp: stamp - 17, Bits: math.Float64bits(v + .5)}}}
			if err := encoder.Encode(op); err != nil {
				t.Fatal(err)
			}
			for _, age := range []int{8, 20, 100, 240} {
				if err := encoder.Encode(replayOperation{AtNS: op.AtNS, Kind: "read", Metric: index, From: int(stamp) - age, Until: int(stamp) - 1}); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return planPath, operations, p
}

func TestPairedReplayCLI(t *testing.T) {
	plan, ops, _ := replayFixture(t)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(t.TempDir(), "paired")
	cmd := exec.Command(binary, "-plan", plan, "-operations", ops, "-work-dir", root, "-unpaced", "-profile")
	cmd.Env = append(os.Environ(), "GO_CARBON_REPLAY_TEST_HELPER=1")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("paired replay: %v\n%s", err, output)
	}
	data, err := os.ReadFile(filepath.Join(root, "RESULT.json"))
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		CorrectnessPassed    bool `json:"correctness_passed"`
		PerformanceQualified bool `json:"performance_qualified"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	if !result.CorrectnessPassed || result.PerformanceQualified {
		t.Fatal("incorrect qualification scope")
	}
	var pids []int
	for _, mode := range []string{"reference", "candidate"} {
		for _, file := range []string{"READS.jsonl", "REOPEN.json", "RESULT.json"} {
			if _, err := os.Stat(filepath.Join(root, mode, file)); err != nil {
				t.Fatal(err)
			}
		}
		data, err := os.ReadFile(filepath.Join(root, mode, "RESULT.json"))
		if err != nil {
			t.Fatal(err)
		}
		var process struct {
			PID int `json:"pid"`
		}
		if err := json.Unmarshal(data, &process); err != nil {
			t.Fatal(err)
		}
		pids = append(pids, process.PID)
	}
	if pids[0] == pids[1] || pids[0] == os.Getpid() || pids[1] == os.Getpid() {
		t.Fatal("engines were not isolated in separate processes")
	}
	if _, err := os.Stat(filepath.Join(root, "candidate", "originals", "integers.wsp")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("candidate originals were not reclaimed")
	}
	if _, err := os.Stat(filepath.Join(root, "SOURCE-SHA256.json")); err != nil {
		t.Fatal(err)
	}
}

func TestReplayRejectsIncompleteAndChangedInputs(t *testing.T) {
	path, operations, p := replayFixture(t)
	t.Run("capture later than trace", func(t *testing.T) {
		copy := p
		copy.Rows = append([]replayRow(nil), p.Rows...)
		copy.Rows[0].CapturedAtNS = p.StartNS + 1
		path := filepath.Join(t.TempDir(), "plan.json")
		if err := writeJSON(path, copy); err != nil {
			t.Fatal(err)
		}
		if _, err := loadPlan(path); err == nil {
			t.Fatal("accepted a capture made after replay began")
		}
	})
	t.Run("changed allocation", func(t *testing.T) {
		p.Rows[0].AllocatedBytes++
		if _, err := openReplayEngine("reference", t.TempDir(), p); err == nil {
			t.Fatal("silently reweighted the corpus")
		}
		p.Rows[0].AllocatedBytes--
	})
	t.Run("source output overlap", func(t *testing.T) {
		output := filepath.Join(p.SourceRoot, "new-output")
		if err := runPair(path, operations, output, false, func(string) error { return nil }); err == nil {
			t.Fatal("accepted output in source")
		}
		if _, err := os.Stat(output); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("invalid run modified source")
		}
	})
	t.Run("out of order trace", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "bad.jsonl")
		f, err := os.Create(file)
		if err != nil {
			t.Fatal(err)
		}
		d := json.NewEncoder(f)
		for _, at := range []int64{p.StartNS + 100, p.StartNS} {
			if err := d.Encode(replayOperation{AtNS: at, Kind: "read", Metric: 0, From: 1, Until: 2}); err != nil {
				t.Fatal(err)
			}
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		if err := eachOperation(file, p, func(int, replayOperation) error { return nil }); err == nil {
			t.Fatal("reordered captured operations")
		}
	})
	t.Run("source hash mismatch", func(t *testing.T) {
		copy := p
		copy.Rows = append([]replayRow(nil), p.Rows...)
		copy.Rows[0].MainSHA256 = "0000000000000000000000000000000000000000000000000000000000000000"
		path := filepath.Join(t.TempDir(), "plan.json")
		if err := writeJSON(path, copy); err != nil {
			t.Fatal(err)
		}
		calls := 0
		if err := runPair(path, operations, filepath.Join(t.TempDir(), "out"), false, func(string) error { calls++; return nil }); err == nil || calls != 0 {
			t.Fatal("ran an engine against an unverified source")
		}
	})
}
