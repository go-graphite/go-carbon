package downsize

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// The fixture is the original header/done JSONL wire format, including a
// partially finished copy. Only its ephemeral HTTP origins are substituted.
func TestSyncAdoptsLegacyJournalAndFills(t *testing.T) {
	for _, noCopy := range []bool{false, true} {
		t.Run(map[bool]string{false: "resume", true: "read-only"}[noCopy], func(t *testing.T) {
			f, src, dst := newFakeCopy(t, []string{"a", "b", "c"})
			state := filepath.Join(t.TempDir(), "legacy-journal") // Detect files, not just suffixes.
			fixture, err := os.ReadFile("testdata/legacy-copy.jsonl")
			if err != nil {
				t.Fatal(err)
			}
			fixture = []byte(strings.NewReplacer("SOURCE", src, "TARGET", dst).Replace(string(fixture)))
			if err := os.WriteFile(state, fixture, 0600); err != nil {
				t.Fatal(err)
			}
			args := []string{"--from", src, "--to", dst, "--state", state, "--streams", "4", "--settle", "0s"}
			if noCopy {
				args = append(args, "--no-copy")
			}
			c, err := parseSyncCommand(args)
			if err != nil {
				t.Fatal(err)
			}
			if !c.legacyJournal || c.sync.state != state+".sync" {
				t.Fatal(c)
			}
			c.sync.now = func() time.Time { return time.Unix(syncTestNow, 0) }
			b := &legacyImportSync{fakeSync: newFakeSync(), f: f, target: dst}
			for _, n := range []string{"a", "b", "c"} {
				b.put(src, n, syncTestSchema)
			}
			b.put(dst, "a", syncTestSchema, syncTestNow-60) // Completed copy still needs a gap filled.
			b.put(dst, "b", syncTestSchema)
			err = c.run(context.Background(), b)
			if noCopy {
				if !errors.Is(err, ErrSyncResidual) || f.attempts != 0 || len(b.copies) != 0 {
					t.Fatalf("read-only: %v attempts=%d copies=%v", err, f.attempts, b.copies)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(f.imported, map[string]int{"c": 1}) {
					t.Fatalf("recopied completed ranges: %v", f.imported)
				}
				if b.digested.Load() == 0 || len(b.copies) == 0 {
					t.Fatal("did not continue through verify/fill")
				}
				for _, n := range []string{"a", "b", "c"} {
					if !reflect.DeepEqual(b.store(src).points[n], b.store(dst).points[n]) {
						t.Fatalf("unfilled %s", n)
					}
				}
				// A second invocation must not transfer any finished range again.
				if err := c.run(context.Background(), b); err != nil {
					t.Fatal(err)
				}
				if f.imported["c"] != 1 || len(f.imported) != 1 {
					t.Fatal(f.imported)
				}
			}
			data, err := os.ReadFile(state)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(string(data), string(fixture)) {
				t.Fatal("legacy journal prefix changed")
			}
			c.copy.froms, c.copy.to = []string{dst}, src // Reversed hosts must not adopt these ranges.
			if err := planFreeCopy(context.Background(), c.copy); err == nil || !strings.Contains(err.Error(), "belongs to another copy") {
				t.Fatalf("wrong host pair: %v", err)
			}
		})
	}
}

// Reflect successful HTTP imports into the verification backend when discovery
// starts. Transfer uses the real metric engine, including its journal reader.
type legacyImportSync struct {
	*fakeSync
	f      *fakeCopy
	target string
}

func (b *legacyImportSync) List(ctx context.Context, host, prefix, after string, limit int) ([]string, error) {
	b.f.mu.Lock()
	imported := b.f.imported["c"] > 0
	b.f.mu.Unlock()
	b.fakeSync.mu.Lock()
	if imported && b.store(b.target).archives["c"] == nil {
		b.put(b.target, "c", syncTestSchema)
	}
	b.fakeSync.mu.Unlock()
	return b.fakeSync.List(ctx, host, prefix, after, limit)
}

func TestStorectlRejectsCopyCommand(t *testing.T) {
	if err := Main([]string{"copy"}); err == nil || !strings.Contains(err.Error(), "unknown storectl command") {
		t.Fatal(err)
	}
}

// Missing names are transferred before any digest/fetch comparison. The only
// pre-transfer metadata I/O is the batched donor retention eligibility check.
func TestSyncMissingNamesTransferBeforeCompare(t *testing.T) {
	f := newFakeSync()
	f.put(syncD1, "missing", syncTestSchema)
	f.put(syncD1, "shared", syncTestSchema)
	f.put(syncT, "shared", syncTestSchema)
	b := &precheckBackend{fakeSync: f}
	b.info = func(ctx context.Context, host string, names []string) (map[string][]syncArchive, error) {
		return f.Info(ctx, host, names)
	}
	b.copy = func() {
		if f.digested.Load() != 0 || f.fetches.Load() != 0 {
			t.Error("comparison ran before missing-name transfer")
		}
	}
	o := syncOptions{to: syncT, from: []string{syncD1}, state: t.TempDir(), streams: 2, batch: 500, now: func() time.Time { return time.Unix(syncTestNow, 0) }}
	if err := runSync(context.Background(), o, b); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(f.copies, []string{syncD1 + " missing"}) {
		t.Fatalf("transfer selection %v", f.copies)
	}
}

func TestSyncLegacyJournalOptOutAndWindows(t *testing.T) {
	_, src, dst := newFakeCopy(t, []string{"a", "b", "c"})
	state := filepath.Join(t.TempDir(), "copy.jsonl")
	fixture, err := os.ReadFile("testdata/legacy-copy.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	fixture = []byte(strings.NewReplacer("SOURCE", src, "TARGET", dst).Replace(string(fixture)))
	if err := os.WriteFile(state, fixture, 0600); err != nil {
		t.Fatal(err)
	}
	args := []string{"--from", src, "--to", dst, "--state", state, "--no-ready", "--jwt-secret", syncTestJWTSecret(t), "--no-verify"}
	if err := SyncMain(context.Background(), args); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(state+".sync", "sync.jsonl")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("no-verify created verification journal: %v", err)
	}
	args = args[:len(args)-1]
	if _, err := parseSyncCommand(append(args, "--window", "closed:100:200")); err != nil {
		t.Fatal(err)
	}
	if _, err := parseSyncCommand(append(args, "--window", "100:200", "--no-full-history")); err == nil {
		t.Fatal("legacy history journal accepted window-only copy")
	}
	dir := filepath.Join(t.TempDir(), "state.jsonl")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	c, err := parseSyncCommand([]string{"--from", src, "--to", dst, "--state", dir})
	if err != nil || c.legacyJournal {
		t.Fatalf("directory suffix mistaken for journal: %+v %v", c, err)
	}
}
