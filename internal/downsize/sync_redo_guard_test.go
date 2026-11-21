package downsize

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestRedoGuardCountsRangesAcrossChangedBatchesAndRestart(t *testing.T) {
	t.Parallel()
	j, _ := checkpointFixture(t)
	path := j.file.Name()
	identity := j.rows[0].Identity
	g := newSyncRedoGuard(j)
	ctx := redoBatchContext(context.Background(), []string{"a", "b", "c"}, []int64{0, 1, 2})
	for _, phase := range []string{"verify", "fetch"} {
		finish, err := g.begin(ctx, phase, syncD1, []string{"a", "b", "c"}, 100, 200)
		if err != nil {
			t.Fatal(err)
		}
		finish(true)
	}
	row := syncRow{Kind: "verified", First: "a", Last: "c", Names: 3, Work: g.cleanWork(ctx, syncRow{})}
	if err := j.append(row); err != nil {
		t.Fatal(err)
	}
	j.close()
	j, err := openSyncJournal(path, identity)
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	g = newSyncRedoGuard(j)
	// Different batch bounds still overlap stable IDs 1 and 2 exactly.
	ctx = redoBatchContext(context.Background(), []string{"b", "c", "d"}, []int64{1, 2, 3})
	for _, phase := range []string{"verify", "fetch"} {
		finish, err := g.begin(ctx, phase, syncD1, []string{"b", "c", "d"}, 100, 200)
		if err != nil {
			t.Fatal(err)
		}
		finish(true)
	}
	if got := j.progress.RedoNames; got["verify"] != 2 || got["fetch"] != 2 {
		t.Fatalf("redo lost after restart: %v", got)
	}
	// A different range and an explicitly invalidated name are legitimate work.
	redoReason(ctx, "b", "mutation_proof_changed")
	redoReason(ctx, "c", "mutation_proof_changed")
	redoReason(ctx, "d", "mutation_proof_changed")
	finish, err := g.begin(ctx, "verify", syncD1, []string{"b", "c", "d"}, 100, 200)
	if err != nil {
		t.Fatal(err)
	}
	finish(true)
	finish, err = g.begin(ctx, "verify", syncD1, []string{"b", "c", "d"}, 200, 300)
	if err != nil {
		t.Fatal(err)
	}
	finish(true)
	if j.progress.RedoNames["verify"] != 2 {
		t.Fatal("justified recheck counted as redo")
	}
}
func TestRedoGuardFailedAndDirtyWorkNeverBecomesClean(t *testing.T) {
	t.Parallel()
	j, _ := checkpointFixture(t)
	defer j.close()
	g := newSyncRedoGuard(j)
	ctx := redoBatchContext(context.Background(), []string{"a", "b"}, []int64{0, 1})
	finish, err := g.begin(ctx, "fetch", syncD1, []string{"a", "b"}, 100, 200)
	if err != nil {
		t.Fatal(err)
	}
	finish(false)
	if len(g.seen) != 0 {
		t.Fatal("failed call became done")
	}
	finish, err = g.begin(ctx, "fetch", syncD1, []string{"a", "b"}, 100, 200)
	if err != nil {
		t.Fatal(err)
	}
	finish(true)
	row := syncRow{Holes: []syncHole{{Name: "b"}}}
	work := g.cleanWork(ctx, row)
	if len(work) != 1 || !reflect.DeepEqual(work[0].IDs, []syncRedoRange{{0, 0}}) {
		t.Fatalf("dirty name sealed: %+v", work)
	}
}
func TestRedoGuardImportExactMembershipAndFailures(t *testing.T) {
	t.Parallel()
	j, s := checkpointFixture(t)
	defer j.close()
	if err := j.append(syncRow{Kind: "copied", Source: syncD1, First: "a", Last: "z", Names: 3, Copied: 2, Selected: []string{"a", "m", "z"}, Failed: []string{"m"}}); err != nil {
		t.Fatal(err)
	}
	s.redo = newSyncRedoGuard(j)
	if err := s.redoImport(syncD1, []string{"a", "b", "m", "z"}, "missing-batch"); err != nil {
		t.Fatal(err)
	}
	if j.progress.RedoNames["import"] != 2 {
		t.Fatalf("incorrect import redo: %v", j.progress.RedoNames)
	}
	if err := s.redoImport(syncD1, []string{"a"}, "fill"); err != nil {
		t.Fatal(err)
	}
	if j.progress.RedoNames["import"] != 2 {
		t.Fatal("observed repair counted as unexpected import")
	}
}
func TestRedoGuardFreshStartAndLostState(t *testing.T) {
	t.Parallel()
	c, err := parseSyncCommand(syncCLIArgs(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(c.sync.state, 0700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(c.sync.state, "progress.json")
	if err := os.WriteFile(marker, []byte(`{"done":123}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := c.prepareStateIntent(); err == nil || !strings.Contains(err.Error(), "--allow-fresh-start") {
		t.Fatalf("fresh over existing work allowed: %v", err)
	}
	c.resuming = true
	if err := c.prepareStateIntent(); err == nil || !strings.Contains(err.Error(), "lost sync journal") {
		t.Fatalf("lost state silently restarted: %v", err)
	}
	c.resuming = false
	c.freshReason = "operator requested independent scope"
	prior := c.sync.state
	if err := c.prepareStateIntent(); err != nil {
		t.Fatal(err)
	}
	if filepath.Dir(c.sync.state) != prior || c.sync.state == prior {
		t.Fatal("fresh run reused prior state")
	}
	if data, err := os.ReadFile(marker); err != nil || string(data) != `{"done":123}` {
		t.Fatal("old state was changed")
	}
	if c.config.Flags["state"] != c.sync.state {
		t.Fatal("fresh resume points at old state")
	}
}
func TestRedoGuardCountsSurviveReadyReport(t *testing.T) {
	t.Parallel()
	f, o, r := newReadyFixture(t, 100)
	err := runSyncReady(context.Background(), &o, r, f, func() error {
		if err := runSync(context.Background(), o, f); err != nil {
			return err
		}
		path := filepath.Join(o.state, "progress.json")
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var p progressSnapshot
		if err = json.Unmarshal(data, &p); err != nil {
			return err
		}
		p.RedoNames = map[string]int64{"verify": 7, "fetch": 2, "import": 1}
		return atomicStateJSON(path, p)
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := readReadyReport(t, o.state).RedoNames; got["verify"] != 7 || got["fetch"] != 2 || got["import"] != 1 {
		t.Fatalf("READY hides redo: %v", got)
	}
}

func TestRedoGuardMetricsKeepDistinctRecoveryCounters(t *testing.T) {
	t.Parallel()
	// A disabled TCP address makes this observation-only test cheap; running
	// snapshots do not send, and no global metric observer is installed.
	p := &progressMetrics{}
	p.paths = map[string]bool{metricStatePath("root/progress.json"): true}
	p.observe(metricStatePath("root/progress.json"), progressSnapshot{Status: "running", RedoNames: map[string]int64{"verify": 3}})
	p.observe(metricStatePath("root/ready-recovery-one/progress.json"), progressSnapshot{Status: "running", RedoNames: map[string]int64{"verify": 2, "fetch": 1}})
	p.observe(metricStatePath("root/ready-recovery-one/progress.json"), progressSnapshot{Status: "running", RedoNames: map[string]int64{"verify": 2, "fetch": 1}})
	if p.redoNames["verify"] != 5 || p.redoNames["fetch"] != 1 {
		t.Fatalf("counter replay or lost child: %v", p.redoNames)
	}
}
func TestRedoGuardIDRangesStayCompact(t *testing.T) {
	t.Parallel()
	var runs []syncRedoRange
	for i := int64(0); i < 320000; i++ {
		runs = redoMerge(runs, syncRedoRange{i * 500, i*500 + 499})
	}
	if !reflect.DeepEqual(runs, []syncRedoRange{{0, 159999999}}) {
		t.Fatalf("160M clean IDs did not coalesce: %d", len(runs))
	}
}

func TestRedoGuardRejectsMissingJournalHeader(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "sync.jsonl")
	if err := os.WriteFile(path, []byte("{\"kind\":\"verified\",\"names\":50}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if j, err := openSyncJournal(path, "expected"); err == nil {
		j.close()
		t.Fatal("missing header silently adopted")
	}
}

func TestRedoGuardPublishesExactGraphiteMetricNames(t *testing.T) {
	t.Parallel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	got := make(chan string, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			got <- ""
			return
		}
		defer conn.Close()
		b, _ := io.ReadAll(conn)
		got <- string(b)
	}()
	p := &progressMetrics{addr: listener.Addr().String(), prefix: "tools.storectl.host.unit.sync", redoNames: map[string]int64{"verify": 4, "fetch": 2, "import": 1}}
	p.send()
	select {
	case text := <-got:
		for _, expected := range []string{"tools.storectl.host.unit.sync.verify.redo_names 4 ", "tools.storectl.host.unit.sync.fetch.redo_names 2 ", "tools.storectl.host.unit.sync.import.redo_names 1 "} {
			if !strings.Contains(text, expected) {
				t.Fatalf("missing %q in %s", expected, text)
			}
		}
	case <-time.After(time.Second):
		t.Fatal("metric send did not finish")
	}
}

func TestRedoGuardMetricsRestoreCompletedRecovery(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	child := filepath.Join(dir, "ready-recovery-one")
	if err := os.MkdirAll(child, 0700); err != nil {
		t.Fatal(err)
	}
	for path, n := range map[string]int64{dir: 3, child: 2} {
		if err := atomicStateJSON(filepath.Join(path, "progress.json"), progressSnapshot{RedoNames: map[string]int64{"verify": n}}); err != nil {
			t.Fatal(err)
		}
	}
	p := &progressMetrics{}
	p.restoreRedoMetrics(dir)
	if p.redoNames["verify"] != 5 {
		t.Fatalf("restart reset redo metric: %v", p.redoNames)
	}
	if counts, err := syncRedoCounts(dir); err != nil || counts["verify"] != 5 {
		t.Fatalf("READY lost child counts: %v %v", counts, err)
	}
}
func TestRedoGuardInitialImportsRemainDoneAfterCheckpoint(t *testing.T) {
	t.Parallel()
	j, s := checkpointFixture(t)
	defer j.close()
	j.checkpoint = &syncPhaseCheckpoint{}
	s.redo = newSyncRedoGuard(j)
	if err := s.redoImport(syncD1, []string{"a", "b"}, "missing-batch"); err != nil {
		t.Fatal(err)
	}
	if j.progress.RedoNames["import"] != 2 {
		t.Fatal("phase checkpoint hid repeated imports")
	}
	// Avoid writing the synthetic checkpoint from this fixture during close.
	j.checkpoint = nil
}
