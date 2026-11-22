package v2core

import (
	"encoding/json"
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
	if err := r.loadAllCheckpoint(); err != nil {
		panic(err)
	}
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
	// Ten minutes after the fixture clock every name's hourly and daily
	// windows are still open, so the digest compares closure calendar entries.
	hooks := Hooks{Now: func() time.Time { return time.Unix(1794700800+600, 0) }}
	root := t.TempDir()
	e, err := Open(root, cfg, hooks)
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
		e, err := Open(dir, cfg, hooks)
		if err != nil {
			t.Fatal(err)
		}
		o := e.owners[0]
		if o.rollups.fromCheckpoint != wantCheckpoint {
			t.Fatalf("%s: fromCheckpoint=%v want %v", dir, o.rollups.fromCheckpoint, wantCheckpoint)
		}
		state := ownerStateDigest(t, e)
		if len(o.rollups.calendar.slots) == 0 {
			t.Fatalf("%s: no open coarse window on the closure calendar", dir)
		}
		return e, state
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
	if a, b := ownerStateDigest(t, ckptEngine), ownerStateDigest(t, scanEngine); a != b {
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
	cfg.Shards, cfg.Producers, cfg.Rollups, cfg.MaxRows = 4, 1, true, max(1<<20, n)
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
		materialized := 0
		for _, owner := range e.owners {
			materialized += len(owner.rollups.metrics)
		}
		if err = e.Close(); err != nil {
			t.Fatal(err)
		}
		t.Logf("MEASURE names=%d arm=%s open=%v materialized=%d", n, arm.name, open, materialized)
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
// ownerStateDigest takes rollupStateDigest of a live engine on its owner
// goroutine, as an owner operation: the planner state belongs to that
// goroutine, whose maintenance pass loads checkpoint names concurrently.
func ownerStateDigest(t *testing.T, e *Engine) string {
	t.Helper()
	var digest string
	ack := make(chan error, 1)
	o := e.owners[0]
	op := func(o *coreOwner) error { digest = rollupStateDigest(o.rollups); return nil }
	if err := o.queues[0].push(&corePacket{ack: ack, operation: op}, o.dead); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-ack:
		if err != nil {
			t.Fatal(err)
		}
	case <-o.dead:
		t.Fatal("owner stopped")
	}
	return digest
}

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
	if x, y := ownerStateDigest(t, a), ownerStateDigest(t, b); x != y {
		t.Fatalf("checkpoint+delta state differs from scan:\n--- checkpoint\n%s\n--- scan\n%s", x, y)
	}
	checkpointFixture(t, a, policies, base, 7)
	checkpointFixture(t, b, policies, base, 7)
	if x, y := ownerStateDigest(t, a), ownerStateDigest(t, b); x != y {
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
	if coreTestProcess(t) {
		return
	}
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
	if coreTestProcess(t) {
		return
	}
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
	// Exercise the at-seal failure even though a loaded checkpoint is recent.
	e.owners[0].ckptAt = time.Time{}
	checkpointFixture(t, e, policies, 11, 13)
	if err = e.Close(); err == nil {
		t.Fatal("Close hid a failed checkpoint write")
	}
	if got := e.owners[0].ckptFailures.Load(); got != 2 { // forced at-seal write, and close
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
	t.Parallel()
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

// Loading a single ID must preserve its resident tie-breaks and frontier when
// it next seals, without materializing unrelated names. The scan is the eager
// oracle, with the same fixed clock and canonical part encoding.
func TestLazyCheckpointFirstSealMatchesEager(t *testing.T) {
	cfg, policies := checkpointTestSetup(t)
	hooks := Hooks{Now: func() time.Time { return time.Unix(1794700800, 0) }}
	root := t.TempDir()
	e, err := Open(root, cfg, hooks)
	if err != nil {
		t.Fatal(err)
	}
	checkpointFixture(t, e, policies, 0, 23)
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	lazyDir, eagerDir := copyTree(t, root), copyTree(t, root)
	if err = os.Remove(filepath.Join(eagerDir, "owner-000", rollupCheckpointFile)); err != nil {
		t.Fatal(err)
	}
	a, err := Open(lazyDir, cfg, hooks)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := Open(eagerDir, cfg, hooks)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	r := a.owners[0].rollups
	if !r.fromCheckpoint || r.checkpoint == nil || len(r.metrics) != 0 {
		t.Fatalf("open materialized %d names", len(r.metrics))
	}
	// One ordinary write/seal, before the first multi-archive closure is due.
	batch := Batch{ID: 2, Order: 24, Clock: 1794701400, Policy: policies[1], Points: []Row{{ID: 2, Time: 1794700950, Bits: math.Float64bits(123), Seq: 2402}}}
	for _, engine := range []*Engine{a, b} {
		if err = engine.AppendBatches(0, []Batch{batch}, 24, true); err != nil {
			t.Fatal(err)
		}
	}
	if len(r.metrics) != 1 {
		t.Fatalf("first seal materialized %d names, want 1", len(r.metrics))
	}
	if a.owners[0].rollups.metrics[2] == nil {
		t.Fatal("first seal did not materialize its ID")
	}
	if x, y := rollupScanDigest(t, a), rollupScanDigest(t, b); x != y {
		t.Fatal("lazy first seal differs from eager parts")
	}
	// The at-seal checkpoint may serialize remaining names. Verify all state too.
	if x, y := ownerStateDigest(t, a), ownerStateDigest(t, b); x != y {
		t.Fatalf("lazy first seal differs from eager state:\n%s\n---\n%s", x, y)
	}
}

func TestLazyCheckpointIdleClosureMatchesEager(t *testing.T) {
	cfg, policies := checkpointTestSetup(t)
	hooks := Hooks{Now: func() time.Time { return time.Unix(1794700800, 0) }}
	root := t.TempDir()
	e, err := Open(root, cfg, hooks)
	if err != nil {
		t.Fatal(err)
	}
	checkpointFixture(t, e, policies, 0, 23)
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	lazyDir, eagerDir := copyTree(t, root), copyTree(t, root)
	if err = os.Remove(filepath.Join(eagerDir, "owner-000", rollupCheckpointFile)); err != nil {
		t.Fatal(err)
	}
	a, err := Open(lazyDir, cfg, hooks)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := Open(eagerDir, cfg, hooks)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if len(a.owners[0].rollups.metrics) != 0 {
		t.Fatal("idle names loaded at open")
	}
	for _, engine := range []*Engine{a, b} {
		engine.owners[0].rollups.now = func() time.Time { return time.Unix(1794787200, 0) }
		if err = engine.AppendBatches(0, nil, 23, true); err != nil {
			t.Fatal(err)
		}
	}
	if x, y := rollupScanDigest(t, a), rollupScanDigest(t, b); x != y {
		t.Fatal("lazy idle closure differs from eager parts")
	}
	if x, y := ownerStateDigest(t, a), ownerStateDigest(t, b); x != y {
		t.Fatal("lazy idle closure differs from eager state")
	}
}

func TestLazyCheckpointRejectsColdDuplicateName(t *testing.T) {
	cfg, policies := checkpointTestSetup(t)
	hooks := Hooks{Now: func() time.Time { return time.Unix(1794700800, 0) }}
	root := t.TempDir()
	e, err := Open(root, cfg, hooks)
	if err != nil {
		t.Fatal(err)
	}
	checkpointFixture(t, e, policies, 0, 3)
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = Open(root, cfg, hooks)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if len(e.owners[0].rollups.metrics) != 0 {
		t.Fatal("names loaded at open")
	}
	err = e.AppendBatches(0, []Batch{{ID: 1000, Order: 4, Clock: 1794700800, Policy: policies[0], Points: []Row{{ID: 1000, Time: 1794700800, Bits: math.Float64bits(42), Seq: 4}}}}, 4, true)
	if err == nil || !strings.Contains(err.Error(), "multiple producer IDs") {
		t.Fatalf("cold duplicate accepted: %v", err)
	}
}

// More than two blocks, with name order deliberately unrelated to ID order.
// Alternating distant names exercises scratch reuse in both lookup indexes.
func TestLazyCheckpointCompressedBlocks(t *testing.T) {
	cfg, _ := checkpointTestSetup(t)
	var policies [][]byte
	for i := range 521 {
		_, encoded := rollupTestPolicy(t, fmt.Sprintf("block.n%03d", (i*137)%521), "10s:6h,5m:7d,1h:1y", sealrollup.Average, 0)
		policies = append(policies, encoded)
	}
	hooks := Hooks{Now: func() time.Time { return time.Unix(1794700800, 0) }}
	root := t.TempDir()
	e, err := Open(root, cfg, hooks)
	if err != nil {
		t.Fatal(err)
	}
	checkpointFixture(t, e, policies, 0, 3)
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	eagerDir := copyTree(t, root)
	if err = os.Remove(filepath.Join(eagerDir, "owner-000", rollupCheckpointFile)); err != nil {
		t.Fatal(err)
	}
	a, err := Open(root, cfg, hooks)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := Open(eagerDir, cfg, hooks)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	r := a.owners[0].rollups
	if len(r.metrics) != 0 {
		t.Fatal("compressed checkpoint eagerly decoded records")
	}
	for step := range 521 {
		i := (step * 263) % 521
		name := []byte(fmt.Sprintf("block.n%03d", (i*137)%521))
		if err = r.loadCheckpointName(name); err != nil {
			t.Fatal(err)
		}
		if m := r.metrics[uint64(i+1)]; m == nil || !m.definitionEqual(policies[i]) {
			t.Fatalf("wrong name after block reuse: ID %d", i+1)
		}
	}
	if x, y := ownerStateDigest(t, a), ownerStateDigest(t, b); x != y {
		t.Fatal("compressed blocks differ from eager recovery")
	}
}

// The callback has no persisted configuration identity. A restart with a new
// TTL must re-resolve cold names before an idle seal can expire them.
func TestLazyCheckpointChangedExpiryMatchesScan(t *testing.T) {
	cfg, policies := checkpointTestSetup(t)
	clock := time.Unix(1794700800+600, 0)
	hooks := Hooks{Now: func() time.Time { return clock },
		Expire:    func(RollupPolicy, RollupMetadata, time.Time) bool { return true },
		ExpiryTTL: expiryWindow(365 * 24 * time.Hour),
	}
	root := t.TempDir()
	e, err := Open(root, cfg, hooks)
	if err != nil {
		t.Fatal(err)
	}
	checkpointFixture(t, e, policies, 0, 3)
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	scanDir := copyTree(t, root)
	if err = os.Remove(filepath.Join(scanDir, "owner-000", rollupCheckpointFile)); err != nil {
		t.Fatal(err)
	}
	hooks.ExpiryTTL = expiryWindow(time.Nanosecond)
	a, err := Open(root, cfg, hooks)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := Open(scanDir, cfg, hooks)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if len(a.owners[0].rollups.metrics) != len(policies) {
		t.Fatal("changed expiry did not resolve every cold name")
	}
	for _, engine := range []*Engine{a, b} {
		if err = engine.AppendBatches(0, nil, 3, true); err != nil {
			t.Fatal(err)
		}
	}
	if x, y := ownerStateDigest(t, a), ownerStateDigest(t, b); x != y {
		t.Fatal("changed expiry differs from scan")
	}
	for _, meta := range a.owners[0].rollups.meta {
		if !meta.Expired {
			t.Fatal("new TTL did not expire idle name")
		}
	}
}

// Checkpoint indexes contain record ordinals, not producer IDs. Sparse wide
// IDs and escaped names must retain their name/ID binding after a rewrite.
func TestLazyCheckpointSparseIDs(t *testing.T) {
	cfg, _ := checkpointTestSetup(t)
	hooks := Hooks{Now: func() time.Time { return time.Unix(1794700800, 0) }}
	ids := []uint64{1007, 1<<40 + 13, 42, 99, 1<<32 + 7}
	names := []string{"sparse.z", "sparse.\"quoted", "sparse.\nline", "sparse.\\slash", "sparse.a"}
	root := t.TempDir()
	e, err := Open(root, cfg, hooks)
	if err != nil {
		t.Fatal(err)
	}
	policies := make([][]byte, len(ids))
	var batches []Batch
	for i, id := range ids {
		_, policies[i] = rollupTestPolicy(t, names[i], "10s:6h,5m:7d,1h:1y", sealrollup.Average, 0)
		batches = append(batches, Batch{ID: id, Order: 1, Clock: 1794700800, Policy: policies[i], Points: []Row{{ID: id, Time: 1794700800, Bits: math.Float64bits(float64(i) + .25), Seq: 1}}})
	}
	if err = e.AppendBatches(0, batches, 1, true); err != nil {
		t.Fatal(err)
	}
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	scanDir := copyTree(t, root)
	if err = os.Remove(filepath.Join(scanDir, "owner-000", rollupCheckpointFile)); err != nil {
		t.Fatal(err)
	}
	a, err := Open(root, cfg, hooks)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := Open(scanDir, cfg, hooks)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	r := a.owners[0].rollups
	if !r.fromCheckpoint || len(r.metrics) != 0 {
		t.Fatal("checkpoint did not stay lazy")
	}
	// A seal with repeated, sparse/wide IDs restores each required run before
	// planning, while leaving unrelated checkpoint names cold.
	seal := []Batch{batches[1], batches[3], batches[1]}
	for i := range seal {
		seal[i].Order = uint64(i + 2)
	}
	for _, engine := range []*Engine{a, b} {
		if err = engine.AppendBatches(0, seal, 4, true); err != nil {
			t.Fatal(err)
		}
	}
	if len(r.metrics) != 2 || r.metrics[ids[1]] == nil || r.metrics[ids[3]] == nil {
		t.Fatalf("required-ID seal materialized %d names, want exactly its two IDs", len(r.metrics))
	}
	if x, y := rollupScanDigest(t, a), rollupScanDigest(t, b); x != y {
		t.Fatal("sparse-ID first seal differs from eager parts")
	}
	for i, id := range ids {
		encodedName, err := json.Marshal(names[i])
		if err != nil {
			t.Fatal(err)
		}
		name := encodedName[1 : len(encodedName)-1]
		if err = r.loadCheckpointName(name); err != nil {
			t.Fatal(err)
		}
		if m := r.metrics[id]; m == nil || !m.definitionEqual(policies[i]) {
			t.Fatalf("wrong name/ID binding for %d", id)
		}
	}
	if x, y := ownerStateDigest(t, a), ownerStateDigest(t, b); x != y {
		t.Fatal("sparse-ID checkpoint differs from scan")
	}
}
