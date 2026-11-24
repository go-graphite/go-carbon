package downsize

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestProgressMetricsWireAndSyncPhases(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	lines := make(chan string, 100)
	go func() {
		for {
			c, e := listener.Accept()
			if e != nil {
				return
			}
			s := bufio.NewScanner(c)
			for s.Scan() {
				lines <- s.Text()
			}
			c.Close()
		}
	}()
	p := &progressMetrics{addr: listener.Addr().String(), prefix: "tools.storectl.runner.run.sync", phase: "starting", active: 1, started: 123.5}
	p.version, p.lastSentVersion = "0123456789ab", "fedcba987654"
	p.observeStoreWait(1, true)
	p.observeStoreWait(0, true)
	p.storeWaitStarted = time.Now().Add(-time.Second)
	for index, phase := range []string{"copy", "fill", "verify round 2"} {
		p.observe("state/progress.json", progressSnapshot{Phase: phase, Done: 12, Total: 20, Rate: 3.5, Status: "running", FreshDone: 5, FreshDoneAvailable: true, Quarantined: 2, PolicySkipped: 1, LegacyRevalidated: 9, LegacyAdopted: 7, LegacyReverified: 2, PhaseRegressions: 1, AmbiguousReceiptNames: 2, ReceiptContradictions: 3, OwnershipExcluded: 8})
		// Internal copy progress must not replace parent sync phase or totals.
		p.observe("nested.progress.json", progressSnapshot{Done: 999, Total: 999})
		p.send()
		got := map[string]string{}
		count := 34
		if index == 0 {
			count++
		}
		if index > 0 {
			count += 2
		}
		for i := 0; i < count; i++ {
			select {
			case line := <-lines:
				parts := strings.Fields(line)
				got[parts[0]] = parts[1]
			case <-time.After(time.Second):
				t.Fatal("missing Graphite sample")
			}
		}
		if got[p.prefix+".quarantined"] != "2" || got[p.prefix+".policy_skipped"] != "1" {
			t.Fatal("missing issue counts", got)
		}
		if got[p.prefix+".legacy_observation_adopted"] != "7" || got[p.prefix+".legacy_observation_reverified"] != "2" {
			t.Fatal("missing legacy adoption counters", got)
		}
		if got[p.prefix+".legacy_revalidated_names"] != "9" {
			t.Fatal("missing migration count", got)
		}
		if got[p.prefix+".ambiguous_receipt_names"] != "2" || got[p.prefix+".receipt_contradictions"] != "3" {
			t.Fatal("missing receipt recovery metrics", got)
		}
		if got[p.prefix+".ownership_excluded"] != "8" {
			t.Fatal("missing ownership exclusion gauge", got)
		}
		if got[p.prefix+".phase_regressions"] != "1" {
			t.Fatal("missing refused regression metric", got)
		}
		if got[p.prefix+".store_retries"] != "2" || got[p.prefix+".waiting_for_store"] != "1" || got[p.prefix+".store_wait_seconds"] == "0" {
			t.Fatal("missing restart metrics", got)
		}
		if got[p.prefix+".process_start_time_seconds"] != "123.5" || got[p.prefix+".client.0123456789ab.active"] != "1" {
			t.Fatal(got)
		}
		if index == 0 && got[p.prefix+".client.fedcba987654.active"] != "0" {
			t.Fatal(got)
		}
		prefix := p.prefix + "." + metricToken(phase) + "."
		if got[prefix+"fresh_done"] != "5" || got[p.prefix+".phase_fresh_done"] != "5" || got[p.prefix+".fresh_done_available"] != "1" || got[prefix+"done"] != "12" || got[prefix+"total"] != "20" || got[prefix+"names_per_second"] != "3.5" || got[prefix+"active"] != "1" {
			t.Fatal(got)
		}
	}
	p.observeStoreWait(-1, false)
	if p.storeWaiting != 0 || p.storeWaitSeconds < 1 {
		t.Fatal("wait duration lost", p.storeWaitSeconds)
	}
	p.observe("state/progress.json", progressSnapshot{Phase: "verify round 2", Status: "incomplete", RepairFailedNames: 7})
	for i := 0; i < 34; i++ {
		select {
		case line := <-lines:
			if strings.Contains(line, ".repair_failed_names ") && !strings.Contains(line, ".repair_failed_names 7 ") {
				t.Fatal(line)
			}
			if strings.Contains(line, ".active ") && !strings.Contains(line, ".active 0 ") {
				t.Fatal(line)
			}
		case <-time.After(time.Second):
			t.Fatal("missing final sample")
		}
	}
	p.observe("state/progress.json", progressSnapshot{Phase: "ready", Status: "running"})
	if p.quarantined != 2 || p.policySkipped != 1 {
		t.Fatal("parent finalization erased issue counts")
	}
	if p.repairFailedNames != 7 {
		t.Fatal("parent finalization erased repair failure metric")
	}
}

func TestProgressMetricsTokens(t *testing.T) {
	if got := metricToken("runner.prod\nother path"); got != "runner_prod_other_path" {
		t.Fatal(got)
	}
}

func TestProgressMetricsUsesProgressFileSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "progress.json")
	var observed progressSnapshot
	outputMu.Lock()
	progressObserver = func(gotPath string, p progressSnapshot) {
		if gotPath != path {
			t.Errorf("path = %s", gotPath)
		}
		observed = p
	}
	outputMu.Unlock()
	defer func() { outputMu.Lock(); progressObserver = nil; outputMu.Unlock() }()
	stop := startProgressFile(context.Background(), path, func() progressSnapshot {
		return progressSnapshot{Phase: "copy", Done: 1000015, Total: 2000000, FreshDone: 15, FreshDoneAvailable: true}
	}, nil)
	if err := stop(context.Canceled); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var stored progressSnapshot
	if err := json.Unmarshal(data, &stored); err != nil {
		t.Fatal(err)
	}
	if !stored.Updated.Equal(observed.Updated) {
		t.Fatal("snapshot timestamps differ")
	}
	observed.Updated = stored.Updated
	if !reflect.DeepEqual(stored, observed) {
		t.Fatalf("JSON %+v differs from Graphite snapshot %+v", stored, observed)
	}
	if stored.Status != "interrupted" || stored.Done != 1000015 || stored.Total != 2000000 || stored.FreshDone != 15 || !stored.FreshDoneAvailable {
		t.Fatal(stored)
	}
}

func TestProgressMetricsSourceBatchPreservesControllerPhase(t *testing.T) {
	dir := t.TempDir()
	j, err := openSyncJournal(filepath.Join(dir, "sync.jsonl"), "phase")
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	f := newFakeSync()
	f.forbidden[syncD1] = true
	s := &syncer{o: syncOptions{state: dir, to: syncT, from: []string{syncD1}}, b: f, j: j, forbid: map[string]bool{}}
	s.prog.begin("window retry", 1000)
	s.prog.done.Store(150)
	if _, err := s.copyFrom(context.Background(), "window-retry-immutable-batch", 1, 0, []string{"metric"}); err != nil {
		t.Fatal(err)
	}
	if s.prog.phase != "window retry" || s.prog.total != 1000 || s.prog.done.Load() != 150 {
		t.Fatalf("batch replaced controller progress: %s %d/%d", s.prog.phase, s.prog.done.Load(), s.prog.total)
	}
}

// A real legacy FILE resume must reuse the exact Graphite paths emitted by the
// previous client, even though its journal has grown and concurrency changes.
func TestProgressMetricsLegacyResumeKeepsIdentity(t *testing.T) {
	f, src, dst := newFakeCopy(t, []string{"a", "b", "c"})
	state := filepath.Join(t.TempDir(), "copy.jsonl")
	fixture, err := os.ReadFile("testdata/legacy-copy.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	fixture = []byte(strings.NewReplacer("SOURCE", src, "TARGET", dst).Replace(string(fixture)))
	if err := os.WriteFile(state, fixture, 0600); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	t.Setenv("STORECTL_GRAPHITE_ADDR", listener.Addr().String())
	lines := make(chan string, 200)
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			c, err := listener.Accept()
			if err != nil {
				return
			}
			s := bufio.NewScanner(c)
			for s.Scan() {
				lines <- s.Text()
			}
			c.Close()
		}
	}()
	defer func() { listener.Close(); <-stopped }()
	invoke := func(args []string) map[string]bool {
		t.Helper()
		if err := MainWithMetrics(args, "0123456789abcdef"); err != nil {
			t.Fatal(err)
		}
		// All synchronous sends precede this marker in the accept queue.
		c, err := net.Dial("tcp", listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintln(c, "END")
		c.Close()
		paths := map[string]bool{}
		for {
			select {
			case line := <-lines:
				if line == "END" {
					return paths
				}
				fields := strings.Fields(line)
				if len(fields) != 3 || !strings.HasPrefix(fields[0], "tools.storectl.") {
					t.Fatalf("unexpected series %q", line)
				}
				parts := strings.Split(fields[0], ".")
				if len(parts) != 7 && len(parts) != 6 && len(parts) != 8 {
					t.Fatalf("unexpected metric path %q", fields[0])
				}
				paths[strings.Join(parts[:5], ".")] = true
			case <-time.After(3 * time.Second):
				t.Fatal("missing metric marker")
			}
		}
	}
	args := []string{"sync", "--from", src, "--to", dst, "--state", state, "--jwt-secret", syncTestJWTSecret(t), "--no-verify", "--streams", "1"}
	first := invoke(args)
	args = []string{"sync", "--resume", state + ".sync", "--streams", "2"}
	second := invoke(args)
	if len(first) != 1 || !reflect.DeepEqual(first, second) {
		t.Fatalf("resume split metric identity:\n%v\n%v", first, second)
	}
	if !reflect.DeepEqual(f.imported, map[string]int{"c": 1}) {
		t.Fatal(f.imported)
	}
}

func TestProgressMetricsStateIdentityAndParentTransition(t *testing.T) {
	state := filepath.Join(t.TempDir(), "state")
	prefix, canonical := progressMetricIdentity("runner", []string{"sync", "--state", state})
	for _, args := range [][]string{
		{"sync", "--state=" + filepath.Join(filepath.Dir(state), ".", "state")},
		{"sync", "-state", state, "--streams", "128"},
		{"sync", "--resume", state, "--streams", "128"},
		{"sync", "--state=discard", "--state=" + state},
	} {
		if got, _ := progressMetricIdentity("runner", args); got != prefix {
			t.Fatalf("%v: %s != %s", args, got, prefix)
		}
	}
	if other, _ := progressMetricIdentity("runner", []string{"sync", "--state", state + "-other"}); other == prefix {
		t.Fatal("distinct state collision")
	}
	p := &progressMetrics{prefix: prefix, paths: map[string]bool{canonical + ".progress.json": true, filepath.Join(canonical+".sync", "progress.json"): true}}
	p.observe(state+".progress.json", progressSnapshot{Phase: "copy", Done: 7, Status: "running"})
	p.observe(filepath.Join(state+".sync", "progress.json"), progressSnapshot{Phase: "verify", Done: 8, Status: "running"})
	p.observe(filepath.Join(state+".sync", "nested", "copy.progress.json"), progressSnapshot{Phase: "nested", Done: 999, Status: "running"})
	if p.prefix != prefix || p.phase != "verify" || p.done != 8 {
		t.Fatalf("parent transition lost: %+v", p)
	}
}

func TestProgressMetricHumanLabels(t *testing.T) {
	host := "runner-" + metricToken(t.TempDir())
	cache, _ := os.UserCacheDir()
	t.Cleanup(func() { _ = os.RemoveAll(filepath.Join(cache, "storectl", "metric-labels", host)) })
	dir := t.TempDir()
	state := filepath.Join(dir, "Night Copy")
	prefix, _ := progressMetricIdentity(host, []string{"sync", "--state", state})
	if !strings.Contains(prefix, ".night-copy.sync") {
		t.Fatal(prefix)
	}
	again, _ := progressMetricIdentity(host, []string{"sync", "--resume", state})
	if again != prefix {
		t.Fatal(prefix, again)
	}
	other, _ := progressMetricIdentity(host, []string{"sync", "--state", filepath.Join(t.TempDir(), "Night Copy")})
	if other == prefix || !strings.Contains(other, ".night-copy-") {
		t.Fatal(other)
	}
	block := filepath.Join(dir, "History East.json")
	if err := os.WriteFile(block, []byte(fmt.Sprintf(`{"state":%q}`, filepath.Join(dir, "state"))), 0600); err != nil {
		t.Fatal(err)
	}
	got, canonical := progressMetricIdentity(host, []string{"sync", "--block", block})
	if !strings.Contains(got, ".history-east.sync") || canonical != filepath.Join(dir, "state") {
		t.Fatal(got, canonical)
	}
	resumed, _ := progressMetricIdentity(host, []string{"sync", "--resume", filepath.Join(dir, "state")})
	if resumed != got {
		t.Fatal("block label changed on resume", got, resumed)
	}
	// Saved legacy copy-state changes observation paths, not the resume label.
	resume := filepath.Join(dir, "Resume State")
	if err := os.Mkdir(resume, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(resume, "config.json"), []byte(`{"copy_state":"/legacy/journal"}`), 0600); err != nil {
		t.Fatal(err)
	}
	got, canonical = progressMetricIdentity(host, []string{"sync", "--resume=" + resume})
	if !strings.Contains(got, ".resume-state.sync") || canonical != "/legacy/journal" {
		t.Fatal(got, canonical)
	}
}

func TestProgressPhaseCodes(t *testing.T) {
	for _, tc := range []struct {
		phase, status          string
		waiting                bool
		round, code, wantRound int
	}{
		{"starting", "running", false, 0, 0, 0},
		{"discover", "running", false, 0, 1, 0},
		{"copy", "running", false, 0, 2, 0},
		{"window retry", "running", false, 0, 2, 0},
		{"verify round 12", "running", false, 11, 3, 12},
		{"verify", "running", false, 2, 3, 2},
		{"fill", "running", false, 3, 4, 3},
		{"ready", "complete", false, 5, 5, 0},
		{"copy", "running", true, 2, 9, 0},
		{"fill", "failed", false, 2, 6, 0},
		{"fill", "interrupted", false, 2, 7, 0},
		{"future", "running", false, 0, -1, 0},
	} {
		code, round := progressPhase(progressSnapshot{Phase: tc.phase, Status: tc.status, WaitingForStore: tc.waiting, Round: tc.round})
		if code != tc.code || round != tc.wantRound {
			t.Fatalf("%+v: got %d/%d", tc, code, round)
		}
	}
}

func TestProgressMetricsETA(t *testing.T) {
	now := time.Unix(1000, 0)
	p := &progressMetrics{active: 1, code: 2, total: 1000, done: 100}
	if got := p.eta(now); got != -1 {
		t.Fatal(got)
	}
	p.done = 200
	if got := p.eta(now.Add(time.Minute)); got != 480 {
		t.Fatal(got)
	}
	p.done = 500
	// Smooth 100/min then 300/min into 200/min, rather than the last rate.
	if got := p.eta(now.Add(2 * time.Minute)); got != 150 {
		t.Fatal(got)
	}
	if got := p.eta(now.Add(6 * time.Minute)); got != -1 {
		t.Fatal("stalled", got)
	}
	p.code = 5
	if got := p.eta(now.Add(7 * time.Minute)); got != 0 {
		t.Fatal(got)
	}
}

func TestProgressMetricsETAPhaseSwitch(t *testing.T) {
	p := &progressMetrics{}
	now := time.Now()
	p.observe("progress.json", progressSnapshot{Phase: "verify round 1", Round: 1, Status: "running", Done: 100, Total: 1000})
	p.eta(now)
	p.done = 200
	if got := p.eta(now.Add(time.Minute)); got != 480 {
		t.Fatal(got)
	}
	for _, phase := range []string{"fill", "verify round 2", "waiting"} {
		p.observe("progress.json", progressSnapshot{Phase: phase, Round: 2, Status: "running", Done: 10, Total: 20})
		if got := p.eta(now.Add(2 * time.Minute)); got != -1 {
			t.Fatal(phase, got)
		}
	}
	p.observe("progress.json", progressSnapshot{Phase: "fill", Round: 3, Status: "running", Total: 20})
	p.eta(now)
	p.observe("progress.json", progressSnapshot{Phase: "fill", Round: 4, Status: "running", Done: 10, Total: 20})
	if got := p.eta(now.Add(time.Minute)); got != -1 {
		t.Fatal("round change", got)
	}
}

func TestProgressMetricsReadyObserved(t *testing.T) {
	state := t.TempDir()
	if err := atomicStateJSON(filepath.Join(state, "progress.json"), progressSnapshot{Phase: "verify round 1"}); err != nil {
		t.Fatal(err)
	}
	var got progressSnapshot
	outputMu.Lock()
	progressObserver = func(_ string, snapshot progressSnapshot) { got = snapshot }
	outputMu.Unlock()
	defer func() { outputMu.Lock(); progressObserver = nil; outputMu.Unlock() }()
	if err := readyProgress(state, "complete", nil); err != nil {
		t.Fatal(err)
	}
	if code, _ := progressPhase(got); code != 5 {
		t.Fatal(got)
	}
}

func TestProgressVersion(t *testing.T) {
	const sha = "abcdef0123456789abcdef0123456789abcdef01"
	for _, tc := range []struct {
		name, input, want string
	}{
		{"bare short", "abcdef012345", "abcdef012345"},
		{"bare full", sha, "abcdef012345"},
		{"bare dirty", sha + "-dirty", "abcdef012345-dirty"},
		{"staging", "staging-build-" + sha, "abcdef012345"},
		{"staging suffix", "staging-build-" + sha + "-release", "abcdef012345"},
		{"staging dirty", "staging-build-" + sha + "-dirty", "abcdef012345-dirty"},
		{"staging suffix dirty", "staging-build-" + sha + "-release-dirty", "abcdef012345-dirty"},
		{"git describe", "v0.19.1-42-g" + sha, "abcdef012345"},
		{"git describe short", "v0.19.1-42-gabcdef012345", "abcdef012345"},
		{"git describe dirty", "v0.19.1-42-g" + sha + "-dirty", "abcdef012345-dirty"},
		{"uppercase", "staging-build-ABCDEF0123456789", "abcdef012345"},
		{"uppercase dirty", "ABCDEF0123456789-DIRTY", "abcdef012345-dirty"},
		{"unsafe surrounding text", "some.path/" + sha + "\nother metric", "abcdef012345"},
		{"empty", "", "unknown"},
		{"unknown", "unknown", "unknown"},
		{"development dirty", "devel-dirty", "unknown-dirty"},
		{"too short", "staging-build-abcdef01234", "unknown"},
		{"non hex", "staging-build-abcdef01234z", "unknown"},
		{"discontinuous hex", "abcdef.012345", "unknown"},
		{"tag only", "v0.19.1", "unknown"},
		{"unsafe without sha", "bad.path\nother metric", "unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := progressVersion(tc.input); got != tc.want {
				t.Fatalf("progressVersion(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}
