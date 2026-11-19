package v2core

import (
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// rollupStateDigest lists every installed name's identity, committed scalars,
// residency and closure calendar ends, in ID order.
func rollupStateDigest(r *coreRollups) string {
	ids := make([]uint64, 0, len(r.metrics))
	for id := range r.metrics {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	filed := map[uint64][]int64{}
	for end, slots := range r.calendar.slots {
		for _, s := range slots {
			if id := r.ids[s]; id != deadSlot && !slices.Contains(filed[id], end) {
				filed[id] = append(filed[id], end)
			}
		}
	}
	var b strings.Builder
	for _, id := range ids {
		m := r.metrics[id]
		slices.Sort(filed[id])
		fmt.Fprintf(&b, "%d %s %+v %v calendar=%v name=%v\n", id, m.definitionBytes(), r.meta[r.slotOf(id, m)], m.slots.AppendResident(nil), filed[id], nameIndexed(r, m))
	}
	fmt.Fprintf(&b, "names=%d metrics=%d\n", r.names.Len(), len(r.metrics))
	return b.String()
}

func nameIndexed(r *coreRollups, m *rollupMetric) bool {
	_, ok := r.names.Get(m.fingerprint())
	return ok
}

func rollupScanDigest(t *testing.T, e *Engine) string {
	t.Helper()
	snap := e.DBs()[0].NewSnapshot()
	defer snap.Close()
	var lines []string
	if err := ScanRollups(snap, func(p RollupPolicy, m RollupMetadata, records []sealrollup.Record) error {
		lines = append(lines, fmt.Sprintf("%s %+v %+v", p.Name, m, records))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	slices.Sort(lines)
	return strings.Join(lines, "\n")
}

// checkpointFixture writes rounds of points for single- and multi-archive
// names, sealing after each round, from order base+1.
func checkpointFixture(t *testing.T, e *Engine, policies [][]byte, base, rounds int) {
	t.Helper()
	clock := int64(1794700800)
	for round := range rounds {
		order := uint64(base + round + 1)
		now := clock + int64(base+round)*7
		var batches []Batch
		for i, encoded := range policies {
			id := uint64(i + 1)
			if (base+round+i)%5 == 4 {
				continue // idle rounds, so names diverge in frontier and residency
			}
			batches = append(batches, Batch{ID: id, Order: order, Clock: now, Policy: encoded, Points: []Row{
				{ID: id, Time: now - int64(i%3)*61, Bits: math.Float64bits(float64(round*31 + i)), Seq: order*100 + id},
			}})
		}
		if err := e.AppendBatches(0, batches, order, true); err != nil {
			t.Fatal(err)
		}
	}
}

func copyTree(t *testing.T, src string) string {
	t.Helper()
	dst := filepath.Join(t.TempDir(), "copy")
	if out, err := exec.Command("cp", "-R", src, dst).CombinedOutput(); err != nil {
		t.Fatalf("cp: %v %s", err, out)
	}
	return dst
}

// TestRollupCheckpointMatchesScan: a store reopened from its checkpoint holds
// the same per-name state as one rebuilt by scanRollups, keeps producing the
// same parts, and every damaged or stale checkpoint falls back to the scan.
func TestRollupCheckpointMatchesScan(t *testing.T) {
	cfg, policies := checkpointTestSetup(t)
	root := t.TempDir()
	e, err := Open(root, cfg, Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	checkpointFixture(t, e, policies, 0, 23)
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	owner := filepath.Join(root, "owner-000")
	file := filepath.Join(owner, rollupCheckpointFile)
	if _, err = os.Stat(file); err != nil {
		t.Fatalf("no checkpoint after close: %v", err)
	}
	reopen := func(dir string, wantCheckpoint bool) (*Engine, string) {
		t.Helper()
		e, err := Open(dir, cfg, Hooks{})
		if err != nil {
			t.Fatal(err)
		}
		o := e.owners[0]
		if o.rollups.fromCheckpoint != wantCheckpoint {
			t.Fatalf("%s: fromCheckpoint=%v want %v", dir, o.rollups.fromCheckpoint, wantCheckpoint)
		}
		return e, rollupStateDigest(o.rollups)
	}
	damage := map[string]func(string){
		"missing":     func(dir string) { _ = os.Remove(filepath.Join(dir, "owner-000", rollupCheckpointFile)) },
		"flipped":     func(dir string) { flipByte(t, filepath.Join(dir, "owner-000", rollupCheckpointFile), 0.6) },
		"truncated":   func(dir string) { truncateHalf(t, filepath.Join(dir, "owner-000", rollupCheckpointFile)) },
		"wrong-magic": func(dir string) { flipByte(t, filepath.Join(dir, "owner-000", rollupCheckpointFile), 0) },
	}
	scanDir := copyTree(t, root)
	damage["missing"](scanDir)
	ckptEngine, ckptState := reopen(copyTree(t, root), true)
	scanEngine, scanState := reopen(scanDir, false)
	if ckptState != scanState {
		t.Fatalf("checkpoint state differs from scan:\n--- checkpoint\n%s\n--- scan\n%s", ckptState, scanState)
	}
	for name, hurt := range damage {
		if name == "missing" {
			continue
		}
		dir := copyTree(t, root)
		hurt(dir)
		e, state := reopen(dir, false)
		if state != scanState {
			t.Fatalf("%s: fallback state differs from scan", name)
		}
		if err = e.Close(); err != nil {
			t.Fatal(err)
		}
	}
	// The loaded state keeps sealing exactly like the scanned one.
	checkpointFixture(t, ckptEngine, policies, 23, 9)
	checkpointFixture(t, scanEngine, policies, 23, 9)
	if a, b := rollupStateDigest(ckptEngine.owners[0].rollups), rollupStateDigest(scanEngine.owners[0].rollups); a != b {
		t.Fatalf("state after further seals differs:\n%s\n---\n%s", a, b)
	}
	if a, b := rollupScanDigest(t, ckptEngine), rollupScanDigest(t, scanEngine); a != b {
		t.Fatalf("parts after further seals differ:\n%s\n---\n%s", a, b)
	}
	if err = errors.Join(ckptEngine.Close(), scanEngine.Close()); err != nil {
		t.Fatal(err)
	}
}

func flipByte(t *testing.T, path string, at float64) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[int(float64(len(b))*at)] ^= 0x20
	if err = os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func truncateHalf(t *testing.T, path string) {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Truncate(path, st.Size()/2); err != nil {
		t.Fatal(err)
	}
}

// TestRollupCheckpointOpenMeasure times core open of one store loaded from its
// checkpoint and rebuilt by scanRollups. CKPT_MEASURE_NAMES sets the names.
func TestRollupCheckpointOpenMeasure(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("CKPT_MEASURE_NAMES"))
	if n == 0 {
		t.Skip("set CKPT_MEASURE_NAMES")
	}
	delta, _ := strconv.Atoi(os.Getenv("CKPT_MEASURE_DELTA")) // seals newer than the kill arm's checkpoint
	delta = max(delta, 1)
	cfg := coreTestConfig()
	cfg.Shards, cfg.Producers, cfg.Rollups, cfg.MaxRows = 4, 1, true, 1<<20
	policies := make([][]byte, n)
	for i := range policies {
		def := "10s:6h,5m:7d,1h:1y"
		if i%3 == 0 {
			def = "1m:30d"
		}
		_, policies[i] = rollupTestPolicy(t, fmt.Sprintf("measure.host%04d.cpu.core%03d.user", i/1000, i%1000), def, sealrollup.Average, 0)
	}
	root := t.TempDir()
	e, err := Open(root, cfg, Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	clock := int64(1794700800)
	seal := func(round int) {
		order := uint64(round + 1)
		batches := make([]Batch, n)
		for i := range batches {
			id := uint64(i + 1)
			batches[i] = Batch{ID: id, Order: order, Clock: clock + int64(round)*10, Policy: policies[i], Points: []Row{{ID: id, Time: clock + int64(round)*10, Bits: math.Float64bits(float64(i)), Seq: order}}}
		}
		if err = e.AppendBatches(0, batches, order, true); err != nil {
			t.Fatal(err)
		}
	}
	seal(0)
	seal(1)
	// The kill arm keeps the checkpoint of the first two seals: delta seals
	// touching every name are newer than it, as after a SIGKILL.
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	stale := make([][]byte, cfg.Shards)
	for s := range cfg.Shards {
		if stale[s], err = os.ReadFile(filepath.Join(root, fmt.Sprintf("owner-%03d", s), rollupCheckpointFile)); err != nil {
			t.Fatal(err)
		}
	}
	if e, err = Open(root, cfg, Hooks{}); err != nil {
		t.Fatal(err)
	}
	for round := 2; round < 2+delta; round++ {
		seal(round)
	}
	start := time.Now()
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	closeTime := time.Since(start)
	var size int64
	for s := range cfg.Shards {
		st, err := os.Stat(filepath.Join(root, fmt.Sprintf("owner-%03d", s), rollupCheckpointFile))
		if err != nil {
			t.Fatal(err)
		}
		size += st.Size()
	}
	scanDir := copyTree(t, root)
	for s := range cfg.Shards {
		_ = os.Remove(filepath.Join(scanDir, fmt.Sprintf("owner-%03d", s), rollupCheckpointFile))
	}
	ckptDir := copyTree(t, root)
	killDir := copyTree(t, root)
	for s := range cfg.Shards {
		if err = os.WriteFile(filepath.Join(killDir, fmt.Sprintf("owner-%03d", s), rollupCheckpointFile), stale[s], 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, arm := range []struct {
		name, dir string
	}{{"scan", scanDir}, {fmt.Sprintf("kill(checkpoint+%d seals)", delta), killDir}, {"checkpoint", ckptDir}} {
		start := time.Now()
		e, err := Open(arm.dir, cfg, Hooks{})
		if err != nil {
			t.Fatal(err)
		}
		open := time.Since(start)
		if want := arm.name != "scan"; e.owners[0].rollups.fromCheckpoint != want {
			t.Fatalf("%s: fromCheckpoint=%v", arm.name, !want)
		}
		if err = e.Close(); err != nil {
			t.Fatal(err)
		}
		t.Logf("MEASURE names=%d arm=%s open=%v", n, arm.name, open)
	}
	t.Logf("MEASURE names=%d close(with checkpoint write)=%v checkpoint bytes=%d (%.1f B/name)", n, closeTime, size, float64(size)/float64(n))
}

func checkpointTestSetup(t *testing.T) (Config, [][]byte) {
	cfg := coreTestConfig()
	cfg.Shards, cfg.Producers, cfg.Rollups, cfg.MaxRows = 1, 1, true, 1024
	var policies [][]byte
	for i, def := range []string{"1s:1d", "1s:1d,1m:30d,1h:1y,1d:10y", "1m:30d,1h:1y,1d:10y", "10s:6h,5m:7d,1h:1y", "1m:14d,30m:2y"} {
		for j := range 4 {
			_, encoded := rollupTestPolicy(t, fmt.Sprintf("ckpt.m%d.n%d", i, j), def, sealrollup.Method(1+(i+j)%5), float32(j%2)/2)
			policies = append(policies, encoded)
		}
	}
	return cfg, policies
}

// checkpointVsScan opens a copy of root with its checkpoint and a copy
// without, requires identical state, seals more rounds on both and requires
// identical state and parts again.
func checkpointVsScan(t *testing.T, cfg Config, policies [][]byte, root string, base int, wantCheckpoint bool) {
	t.Helper()
	ckptDir, scanDir := copyTree(t, root), copyTree(t, root)
	_ = os.Remove(filepath.Join(scanDir, "owner-000", rollupCheckpointFile))
	// One fixed clock for both arms: idle closure and expiry read it.
	open := func(dir string) *Engine {
		e, err := Open(dir, cfg, Hooks{Now: func() time.Time { return time.Unix(1794800000, 0) }})
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	a, b := open(ckptDir), open(scanDir)
	if got := a.owners[0].rollups.fromCheckpoint; got != wantCheckpoint {
		t.Fatalf("fromCheckpoint=%v want %v", got, wantCheckpoint)
	}
	if b.owners[0].rollups.fromCheckpoint {
		t.Fatal("scan copy loaded a checkpoint")
	}
	if x, y := rollupStateDigest(a.owners[0].rollups), rollupStateDigest(b.owners[0].rollups); x != y {
		t.Fatalf("checkpoint+delta state differs from scan:\n--- checkpoint\n%s\n--- scan\n%s", x, y)
	}
	checkpointFixture(t, a, policies, base, 7)
	checkpointFixture(t, b, policies, base, 7)
	if x, y := rollupStateDigest(a.owners[0].rollups), rollupStateDigest(b.owners[0].rollups); x != y {
		t.Fatalf("state after further seals differs:\n%s\n---\n%s", x, y)
	}
	if x, y := rollupScanDigest(t, a), rollupScanDigest(t, b); x != y {
		t.Fatalf("parts after further seals differ:\n%s\n---\n%s", x, y)
	}
	if err := errors.Join(a.Close(), b.Close()); err != nil {
		t.Fatal(err)
	}
}

// TestRollupCheckpointStalePlusParts: a checkpoint written many seals ago,
// plus the parts published after it, equals a full scan. The merged arm folds
// a merge output that re-carries rows the checkpoint already reflects, so a
// resident must re-enter with its record Order to keep winning the tie.
func TestRollupCheckpointStalePlusParts(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	for _, merge := range []bool{false, true} {
		t.Run(fmt.Sprintf("merge=%v", merge), func(t *testing.T) {
			cfg, policies := checkpointTestSetup(t)
			root := t.TempDir()
			e, err := Open(root, cfg, Hooks{})
			if err != nil {
				t.Fatal(err)
			}
			checkpointFixture(t, e, policies, 0, 11)
			if err = e.Close(); err != nil {
				t.Fatal(err)
			}
			file := filepath.Join(root, "owner-000", rollupCheckpointFile)
			stale, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			if e, err = Open(root, cfg, Hooks{}); err != nil {
				t.Fatal(err)
			}
			checkpointFixture(t, e, policies, 11, 13)
			if merge && mergeDrain(t, e.owners[0]) == 0 {
				t.Fatal("no merge ran")
			}
			if err = e.Close(); err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(file, stale, 0o644); err != nil {
				t.Fatal(err)
			}
			checkpointVsScan(t, cfg, policies, root, 24, true)
		})
	}
}

// TestRollupCheckpointWriteFailureIsLoud: failed checkpoint writes are counted
// and returned by Close, keep the previous file, and the next open folds the
// parts published since that file.
func TestRollupCheckpointWriteFailureIsLoud(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	cfg, policies := checkpointTestSetup(t)
	root := t.TempDir()
	e, err := Open(root, cfg, Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	checkpointFixture(t, e, policies, 0, 11)
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	// A non-empty directory at the tmp path fails every write.
	blocker := filepath.Join(root, "owner-000", rollupCheckpointFile+".tmp")
	if err = os.MkdirAll(filepath.Join(blocker, "x"), 0o755); err != nil {
		t.Fatal(err)
	}
	if e, err = Open(root, cfg, Hooks{}); err != nil {
		t.Fatal(err)
	}
	checkpointFixture(t, e, policies, 11, 13)
	if err = e.Close(); err == nil {
		t.Fatal("Close hid a failed checkpoint write")
	}
	if got := e.owners[0].ckptFailures.Load(); got != 2 { // first seal after open, and close
		t.Fatalf("checkpoint failures=%d want 2", got)
	}
	if err = os.RemoveAll(blocker); err != nil {
		t.Fatal(err)
	}
	checkpointVsScan(t, cfg, policies, root, 24, true)
}

// TestRollupCheckpointKillChild SIGKILLs a child that seals continuously and
// writes a checkpoint after every seal, at random points (mid-seal,
// mid-checkpoint write), then requires checkpoint+parts == scan.
func TestRollupCheckpointKillChild(t *testing.T) {
	if dir := os.Getenv("CKPT_KILL_CHILD_DIR"); dir != "" {
		cfg, policies := checkpointTestSetup(t)
		rollupCheckpointEvery = 0
		e, err := Open(dir, cfg, Hooks{})
		if err != nil {
			t.Fatal(err)
		}
		for base := 0; ; base++ {
			checkpointFixture(t, e, policies, base, 1)
			fmt.Printf("sealed %d\n", base)
		}
	}
	cfg, policies := checkpointTestSetup(t)
	for kill, after := range []time.Duration{90 * time.Millisecond, 230 * time.Millisecond, 410 * time.Millisecond, 700 * time.Millisecond, 1100 * time.Millisecond} {
		root := t.TempDir()
		cmd := exec.Command(os.Args[0], "-test.run=^TestRollupCheckpointKillChild$", "-test.count=1")
		cmd.Env = append(os.Environ(), "CKPT_KILL_CHILD_DIR="+root)
		out, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err = cmd.Start(); err != nil {
			t.Fatal(err)
		}
		lines := make(chan string, 1024)
		go func() {
			b := make([]byte, 4096)
			var acc []byte
			for {
				n, err := out.Read(b)
				acc = append(acc, b[:n]...)
				for {
					i := slices.Index(acc, '\n')
					if i < 0 {
						break
					}
					lines <- string(acc[:i])
					acc = acc[i+1:]
				}
				if err != nil {
					close(lines)
					return
				}
			}
		}()
		// Wait for two seals, so a checkpoint exists, then kill after a delay.
		seals := 0
		for line := range lines {
			if strings.HasPrefix(line, "sealed ") {
				if seals++; seals == 2 {
					break
				}
			}
		}
		time.Sleep(after)
		_ = cmd.Process.Signal(syscall.SIGKILL)
		_ = cmd.Wait()
		last := 0
		for line := range lines {
			if strings.HasPrefix(line, "sealed ") {
				last, _ = strconv.Atoi(strings.TrimPrefix(line, "sealed "))
			}
		}
		_, statErr := os.Stat(filepath.Join(root, "owner-000", rollupCheckpointFile))
		t.Logf("kill %d after %v: last printed seal %d, checkpoint present=%v", kill, after, last, statErr == nil)
		// Continue above every child order but inside the shortest archive's
		// retention, so no history becomes droppable between the two arms.
		checkpointVsScan(t, cfg, policies, root, 1000, statErr == nil)
	}
}
