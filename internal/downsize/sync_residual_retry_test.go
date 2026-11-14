package downsize

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
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
