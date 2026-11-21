package downsize

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// scopeRecorder records every name a verification read touches.
type scopeRecorder struct {
	*readyFixture
	mu   sync.Mutex
	read map[string]bool
}

func (r *scopeRecorder) note(names []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, n := range names {
		r.read[n] = true
	}
}
func (r *scopeRecorder) Info(ctx context.Context, store string, names []string) (map[string][]syncArchive, error) {
	r.note(names)
	return r.readyFixture.Info(ctx, store, names)
}
func (r *scopeRecorder) Fetch(ctx context.Context, store string, names []string, from, until int64) (map[string]syncSeries, error) {
	r.note(names)
	return r.readyFixture.Fetch(ctx, store, names, from, until)
}
func (r *scopeRecorder) Digest(ctx context.Context, store string, names []string, from, until int64) (map[string][]syncDigest, error) {
	r.note(names)
	return r.readyFixture.Digest(ctx, store, names, from, until)
}

// sourceScopeFixture: the target holds 200 names no source has, 50 names
// donor1 also holds (one with a hole), and lacks 10 names donor2 holds.
func sourceScopeFixture(t *testing.T) (*scopeRecorder, syncOptions, syncReadyOptions, int64) {
	t.Helper()
	base, o, r := newReadyFixture(t, 0)
	o.from = []string{syncD1, syncD2}
	hole := (syncTestNow - 2*86400) / 3600 * 3600
	schema := []syncArchive{{Step: 3600, Points: 24 * 10}}
	for i := range 200 {
		base.put(syncT, fmt.Sprintf("t.only.%03d", i), schema)
	}
	for i := range 50 {
		n := fmt.Sprintf("s.both.%03d", i)
		if i == 7 {
			base.put(syncT, n, schema, hole)
		} else {
			base.put(syncT, n, schema)
		}
		base.put(syncD1, n, schema)
	}
	for i := range 10 {
		base.put(syncD2, fmt.Sprintf("m.missing.%03d", i), schema)
	}
	r.noReady = true
	return &scopeRecorder{readyFixture: base, read: map[string]bool{}}, o, r, hole
}

func round1Verified(t *testing.T, state string) int {
	t.Helper()
	f, err := os.Open(filepath.Join(state, "sync.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	total := 0
	scan := bufio.NewScanner(f)
	scan.Buffer(make([]byte, 1<<20), 16<<20)
	for scan.Scan() {
		var row syncRow
		if json.Unmarshal(scan.Bytes(), &row) == nil && row.Kind == "verified" && row.Round == 1 {
			total += row.Names
		}
	}
	return total
}

func TestSyncNoReadyVerifiesOnlySourceHeldNames(t *testing.T) {
	t.Parallel()
	b, o, r, hole := sourceScopeFixture(t)
	run := func() error {
		return runSyncReady(context.Background(), &o, r, b, func() error { return runSync(context.Background(), o, b) })
	}
	if err := run(); err != nil {
		t.Fatal(err)
	}
	if !o.sourceScope {
		t.Fatal("--no-ready pass did not scope verification to source-held names")
	}
	for n := range b.read {
		if strings.HasPrefix(n, "t.only.") {
			t.Fatalf("target-only name %s was read during verification", n)
		}
	}
	// Scope = union of source names: 50 shared + 10 copied.
	if got := round1Verified(t, o.state); got != 60 {
		t.Fatalf("round 1 verified %d names, want 60 (union of source names)", got)
	}
	dst := b.store(syncT)
	if _, ok := dst.points["s.both.007"][3600][hole]; !ok {
		t.Fatal("hole on a source-held name was not filled")
	}
	for i := range 10 {
		if _, ok := dst.archives[fmt.Sprintf("m.missing.%03d", i)]; !ok {
			t.Fatalf("m.missing.%03d not copied", i)
		}
	}
	// Resume over the finished state repeats no reads.
	b.read = map[string]bool{}
	if err := run(); err != nil {
		t.Fatal(err)
	}
	if len(b.read) != 0 {
		t.Fatalf("resume re-read %d names", len(b.read))
	}
}

// Without --no-ready the scope stays the full union: READY attests every
// discovered name, including names a source may acquire after discovery.
func TestSyncDefaultScopeKeepsTargetOnlyNames(t *testing.T) {
	t.Parallel()
	b, o, _, _ := sourceScopeFixture(t)
	if err := runSync(context.Background(), o, b); err != nil {
		t.Fatal(err)
	}
	if got := round1Verified(t, o.state); got != 260 {
		t.Fatalf("round 1 verified %d names, want 260 (full union)", got)
	}
	if !b.read["t.only.000"] {
		t.Fatal("default scope skipped a target-only name")
	}
}

// The verification plan and summary count only the scoped names.
func TestSyncSourceScopeCounts(t *testing.T) {
	t.Parallel()
	s := &syncer{o: syncOptions{sourceScope: true}}
	s.sum.Discovered, s.sum.OnTarget, s.both = 260, 250, 50
	if got := s.targetOnlySkipped(); got != 200 {
		t.Fatalf("target-only skipped %d, want 200", got)
	}
	if got := s.verifyScopeNames(); got != 60 {
		t.Fatalf("verify scope %d, want 60", got)
	}
	s.o.sourceScope = false
	if s.targetOnlySkipped() != 0 || s.verifyScopeNames() != 260 {
		t.Fatal("default scope must cover every discovered name")
	}
}
