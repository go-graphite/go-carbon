package downsize

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSyncResumeTerminalResidualRechecksStores(t *testing.T) {
	f := newFakeSync()
	o := convergeOptions(t)
	schema := []syncArchive{{60, 10000}}
	f.put(syncT, "hole", schema)
	f.put(syncP, "hole", schema)
	ts := int64(syncTestNow - 6000)
	delete(f.store(syncT).points["hole"][60], ts)
	if err := runSync(context.Background(), o, partialSync{fakeSync: f, stuck: true}); !errors.Is(err, ErrSyncResidual) {
		t.Fatalf("want residual verdict: %v", err)
	}
	p := filepath.Join(o.state, "sync.jsonl")
	before, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := runSync(context.Background(), o, partialSync{fakeSync: f}); err != nil {
		t.Fatalf("resume reused stale verification/fill receipts: %v", err)
	}
	after, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(after, before) || !bytes.Contains(after[len(before):], []byte(`"kind":"verification-retry"`)) {
		t.Fatal("retry did not append to original journal")
	}
	if _, ok := f.store(syncT).points["hole"][60][ts]; !ok {
		t.Fatal("fresh fill never reached target")
	}
}

func TestSyncLegacyResidualRetryKeepsInterruptedGeneration(t *testing.T) {
	dir := t.TempDir()
	j, err := openSyncJournal(filepath.Join(dir, "sync.jsonl"), "test")
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	if err := j.append(syncRow{Kind: "verified", Round: 3}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "progress.json"), []byte(`{"status":"incomplete","error":"sync finished with residual holes"}`), 0600); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		s := &syncer{o: syncOptions{state: dir}, j: j}
		if err := s.prepareVerificationRetry(); err != nil {
			t.Fatal(err)
		}
		if s.verificationBase != 3 {
			t.Fatalf("base=%d", s.verificationBase)
		}
	}
	if rows := j.find(func(r syncRow) bool { return r.Kind == "verification-retry" }); len(rows) != 1 {
		t.Fatalf("interrupted retry created %d generations", len(rows))
	}
	if err := j.append(syncRow{Kind: "verification-incomplete", Round: 5}); err != nil {
		t.Fatal(err)
	}
	s := &syncer{o: syncOptions{state: dir}, j: j}
	if err := s.prepareVerificationRetry(); err != nil || s.verificationBase != 5 {
		t.Fatalf("next terminal verdict: base=%d err=%v", s.verificationBase, err)
	}
}

// A denied transfer is evidence of an attempt, not a permanent source policy.
// Exercise both missing-name and existing-name hole repair through a terminal
// residual retry, retaining the original journal and already successful work.
func TestSyncResumeRetriesRepairedSourceAllowlist(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprint("missing=", missing), func(t *testing.T) {
			f := newFakeSync()
			o := convergeOptions(t)
			schema := []syncArchive{{60, 10000}}
			ts := int64(syncTestNow - 6000)
			f.put(syncD1, "repair", schema)
			if !missing {
				f.put(syncT, "repair", schema, ts)
			}
			f.put(syncD1, "already-good", schema)
			f.put(syncT, "already-good", schema)
			f.forbidden[syncD1] = true
			if err := runSync(context.Background(), o, f); !errors.Is(err, ErrSyncResidual) {
				t.Fatalf("want denied-source residual: %v", err)
			}
			path := filepath.Join(o.state, "sync.jsonl")
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			f.forbidden[syncD1] = false
			if err := runSync(context.Background(), o, f); err != nil {
				t.Fatalf("repaired allowlist did not recover: %v", err)
			}
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.HasPrefix(after, before) {
				t.Fatal("rewrote original journal")
			}
			if _, ok := f.store(syncT).points["repair"][60][ts]; !ok {
				t.Fatal("source points not recovered")
			}
			for _, call := range f.copies {
				if strings.Contains(call, "already-good") {
					t.Fatal("recopied healthy name", call)
				}
			}
		})
	}
}

type cancelDeniedSync struct {
	*fakeSync
	cancel context.CancelFunc
}

func (b cancelDeniedSync) Copy(ctx context.Context, source, target string, names []string, journal string) (syncCopyResult, error) {
	result, err := b.fakeSync.Copy(ctx, source, target, names, journal)
	if result.Forbidden {
		b.cancel()
	}
	return result, err
}

func TestSyncInterruptedResumeRetriesDeniedFill(t *testing.T) {
	f := newFakeSync()
	o := convergeOptions(t)
	o.from = []string{syncD1}
	schema := []syncArchive{{60, 10000}}
	ts := int64(syncTestNow - 6000)
	f.put(syncD1, "repair", schema)
	f.put(syncT, "repair", schema, ts)
	f.forbidden[syncD1] = true
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := runSync(ctx, o, cancelDeniedSync{f, cancel}); !errors.Is(err, context.Canceled) {
		t.Fatalf("want interrupted attempt: %v", err)
	}
	f.forbidden[syncD1] = false
	if err := runSync(context.Background(), o, f); err != nil {
		t.Fatalf("resume retained denied fill receipt: %v", err)
	}
	if _, ok := f.store(syncT).points["repair"][60][ts]; !ok {
		t.Fatal("hole not recovered")
	}
}

func TestSyncVerificationResumePublishesDurableFloorBeforeReplay(t *testing.T) {
	o := convergeOptions(t)
	o.batch = 1
	j, err := openSyncJournal(filepath.Join(o.state, "sync.jsonl"), "floor-test")
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	for _, name := range []string{"a", "b"} {
		if err := j.append(syncRow{Kind: "verified", Round: 1, First: name, Last: name, Names: 1}); err != nil {
			t.Fatal(err)
		}
	}
	s := &syncer{o: o, b: newFakeSync(), j: j}
	s.sum.phases = map[string]*syncPhase{}
	next := 0
	set := syncNameSource{total: 2, close: func() {}, next: func() (string, uint64, error) {
		if next == 0 {
			s.prog.mu.Lock()
			floor, accounted := s.prog.durableFloor, s.prog.done.Load()
			s.prog.mu.Unlock()
			if floor != 2 || accounted != 0 {
				t.Fatalf("before first replay: durable=%d accounted=%d", floor, accounted)
			}
		}
		if next == 2 {
			return "", 0, io.EOF
		}
		name := []string{"a", "b"}[next]
		next++
		return name, 0, nil
	}}
	if _, _, _, err := s.verifyRound(context.Background(), 1, set); err != nil {
		t.Fatal(err)
	}
	if s.prog.done.Load() != 2 || s.prog.replayed.Load() != 2 {
		t.Fatalf("replay counted twice or inflated throughput: done=%d replayed=%d", s.prog.done.Load(), s.prog.replayed.Load())
	}
	s.prog.begin("fill", 1)
	if s.prog.durableFloor != 0 {
		t.Fatal("verification floor leaked into another phase")
	}
}
