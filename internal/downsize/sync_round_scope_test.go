package downsize

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func readRoundRows(t *testing.T, state string, round int) []syncRow {
	t.Helper()
	f, err := os.Open(filepath.Join(state, "sync.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []syncRow
	dec := json.NewDecoder(f)
	for dec.More() {
		var row syncRow
		if err := dec.Decode(&row); err != nil {
			t.Fatal(err)
		}
		if row.Kind == "verified" && row.Round == round {
			out = append(out, row)
		}
	}
	return out
}

func TestSyncFinishFirstFrontierRepairScope(t *testing.T) {
	t.Parallel()
	f, o := frontierFixture(t)
	o.rescanAll = false
	o.noCopy, o.maxRounds = false, 0 // includes old saved unlimited defaults
	b := &mutateCleanDuringCopy{proofFixture: f}
	if err := runSync(context.Background(), o, b); err != nil {
		t.Fatal(err)
	}
	rows := readRoundRows(t, o.state, 2)
	if len(rows) != 1 || rows[0].Names != 1 || rows[0].First != "metric.19" || rows[0].Last != "metric.19" {
		t.Fatalf("round 2 expanded outside repair set: %+v", rows)
	}
	if _, ok := f.store(syncT).points["metric.02"][3600][syncTestNow-6*3600]; ok {
		t.Fatal("unrelated mutation was included in repair scope")
	}
	// A wrapper's second resume must not turn a completed pass into a rescan.
	f.seen = map[string]bool{}
	if err := runSync(context.Background(), o, b); err != nil {
		t.Fatal(err)
	}
	if len(f.seen) != 0 {
		t.Fatalf("completed pass reopened: %v", f.seen)
	}
}

type continuouslyDirtyProofs struct {
	*proofFixture
	revisions atomic.Int64
}

func (b *continuouslyDirtyProofs) Proofs(_ context.Context, store string, names []string) ([]string, error) {
	revision := b.revisions.Add(1)
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = fmt.Sprintf("%s/%s/%d", store, n, revision)
	}
	return out, nil
}
func TestSyncFinishFirstContinuouslyDirtyTerminates(t *testing.T) {
	t.Parallel()
	f, o := frontierFixture(t)
	o.rescanAll = false
	o.noCopy, o.maxRounds = false, 0
	b := &continuouslyDirtyProofs{proofFixture: f}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runSync(ctx, o, b); !errors.Is(err, ErrSyncResidual) {
		t.Fatalf("want residual report, got %v", err)
	}
	if len(readRoundRows(t, o.state, 2)) == 0 || len(readRoundRows(t, o.state, 3)) != 0 {
		t.Fatal("default did not stop after round 2")
	}
	data, err := os.ReadFile(filepath.Join(o.state, "residual.json"))
	if err != nil {
		t.Fatal(err)
	}
	var residual []syncResidual
	if err := json.Unmarshal(data, &residual); err != nil || len(residual) != 20 {
		t.Fatalf("dirty names not reported: %d %v", len(residual), err)
	}
}

func TestSyncRoundScopeResumeKeepsEnteredUnion(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"verify", "fill"} {
		t.Run(phase, func(t *testing.T) {
			s, _ := phaseResumeFixture(t)
			s.o.rescanAll = false
			s.frontier = &syncFrontier{}
			s.resume = syncPhasePosition{Phase: phase, Round: 4}
			for round := 1; round <= 5; round++ {
				full, err := s.roundUsesUnion(round, false)
				if err != nil || full != (round <= 4) {
					t.Fatalf("round %d: union=%v err=%v", round, full, err)
				}
			}
			// After a crash in the new sparse round, its durable scope wins over
			// legacy inference and the newly dirtied catalog cannot widen it.
			s.resume = syncPhasePosition{Phase: "verify", Round: 5}
			full, err := s.roundUsesUnion(5, false)
			if err != nil || full {
				t.Fatalf("sparse resumed as union: %v %v", full, err)
			}
			if !keepVerificationRow(syncRow{Kind: "verification-round-scope"}) {
				t.Fatal("checkpoint drops round scope")
			}
		})
	}
}

func TestSyncFinishFirstSavedLegacyDefault(t *testing.T) {
	t.Parallel()
	c, err := parseSyncCommand(syncCLIArgs(t))
	if err != nil {
		t.Fatal(err)
	}
	if c.sync.roundLimit() != 2 || c.sync.rescanAll {
		t.Fatal("unbounded default")
	}
	c.config.Flags["max-rounds"] = "0"
	delete(c.config.Flags, "rescan-all")
	if err := c.saveConfig(); err != nil {
		t.Fatal(err)
	}
	resumed, err := parseSyncCommand([]string{"--resume", c.sync.state, "--streams", "64"})
	if err != nil || resumed.sync.roundLimit() != 2 || resumed.sync.rescanAll {
		t.Fatalf("legacy default: %+v %v", resumed.sync, err)
	}
	explicit, err := parseSyncCommand([]string{"--resume", c.sync.state, "--rescan-all", "--max-rounds", "0"})
	if err != nil || explicit.sync.roundLimit() != 0 || !explicit.sync.rescanAll {
		t.Fatalf("explicit convergence: %+v %v", explicit.sync, err)
	}
}

func TestSyncFinishFirstTerminalRetryScope(t *testing.T) {
	t.Parallel()
	s, j := phaseResumeFixture(t)
	s.o.rescanAll = false
	s.verificationBase = 2
	want := []string{"metric.a", "metric.z"}
	if err := j.append(syncRow{Kind: "verification-retry", Round: 2, Selected: want, SelectionComplete: true}); err != nil {
		t.Fatal(err)
	}
	names, scoped, err := s.windowResumeSet()
	if err != nil || !scoped || !reflect.DeepEqual(names, want) {
		t.Fatalf("retry scope=%v scoped=%v err=%v", names, scoped, err)
	}
}

type interruptedUnionSync struct {
	*proofFixture
	cancel  context.CancelFunc
	imports atomic.Int64
}

func (b *interruptedUnionSync) Copy(ctx context.Context, source, target string, names []string, journal string) (syncCopyResult, error) {
	result, err := b.fakeSync.Copy(ctx, source, target, names, journal)
	count := b.imports.Add(1)
	if err == nil && count <= 3 {
		b.mu.Lock()
		delete(b.store(syncT).points[fmt.Sprintf("metric.%02d", count+1)][3600], syncTestNow-6*3600)
		b.mu.Unlock()
	}
	return result, err
}
func (b *interruptedUnionSync) Digest(ctx context.Context, store string, names []string, from, until int64) (map[string][]syncDigest, error) {
	if b.cancel != nil && b.imports.Load() >= 3 {
		b.cancel()
		return nil, context.Canceled
	}
	return b.proofFixture.Digest(ctx, store, names, from, until)
}
func TestSyncFinishFirstResumeBeyondLimit(t *testing.T) {
	t.Parallel()
	f, o := frontierFixture(t)
	o.from = []string{syncD1}
	o.noCopy, o.maxRounds = false, 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := &interruptedUnionSync{proofFixture: f, cancel: cancel}
	if err := runSync(ctx, o, b); !errors.Is(err, context.Canceled) {
		t.Fatalf("want interrupted fourth union round: %v", err)
	}
	if b.imports.Load() != 3 {
		t.Fatalf("imports=%d", b.imports.Load())
	}
	// Remove only the new scheduler receipts to reproduce an old binary's
	// journal. All data/fill receipts remain byte-for-byte intact.
	path := filepath.Join(o.state, "sync.jsonl")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var retained []byte
	for _, line := range bytes.SplitAfter(data, []byte("\n")) {
		if !bytes.Contains(line, []byte(`"kind":"verification-round-scope"`)) {
			retained = append(retained, line...)
		}
	}
	if err := os.WriteFile(path, retained, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path + ".checkpoint.json"); err != nil {
		t.Fatal(err)
	}
	b.cancel = nil
	o.rescanAll = false
	if err := runSync(context.Background(), o, b); err != nil {
		t.Fatalf("want completed repair round: %v", err)
	}
	if b.imports.Load() != 3 {
		t.Fatalf("replayed fills or filled beyond limit: %d", b.imports.Load())
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.HasPrefix(after, retained) {
		t.Fatalf("lost previously journaled observations: %v", err)
	}
	var switched bool
	for _, line := range bytes.Split(after[len(retained):], []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var r syncRow
		if err := json.Unmarshal(line, &r); err != nil {
			t.Fatal(err)
		}
		if r.Kind == "legacy-round-repair" {
			switched = reflect.DeepEqual(r.Selected, []string{"metric.03"})
		}
		if r.Kind == "verified" && r.Round == 4 && (r.Names != 1 || r.First != "metric.03") {
			t.Fatalf("read outside previous repair set: %+v", r)
		}
	}
	if !switched {
		t.Fatal("missing repair scope transition")
	}
	if len(readRoundRows(t, o.state, 5)) != 0 {
		t.Fatal("continued past current round")
	}
}

func TestSyncRescanAllUnfillableTerminates(t *testing.T) {
	t.Parallel()
	f, o := frontierFixture(t)
	o.noCopy, o.maxRounds = false, 0
	f.forbidden[syncD1], f.forbidden[syncD2] = true, true
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runSync(ctx, o, f); !errors.Is(err, ErrSyncResidual) {
		t.Fatalf("want unfillable report, got %v", err)
	}
	if len(readRoundRows(t, o.state, 3)) != 0 {
		t.Fatal("unfillable holes started another no-work round")
	}
}
