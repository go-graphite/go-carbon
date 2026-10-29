package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestParallelPruneBorrowedNamesAndLocks(t *testing.T) {
	root := t.TempDir()
	const jobs = 96 // exceeds the 32-worker queue; no large fixture is needed
	wantDirs, wantLocks := uint64(0), uint64(0)
	var retained, stale []string
	for i := 0; i < jobs; i++ {
		dir := filepath.Join(root, "a", "b", fmt.Sprintf("c%03d", i))
		makeDir(t, filepath.Join(dir, "empty", "leaf"))
		wantDirs += 2
		if i%11 == 0 {
			path := filepath.Join(dir, "keep.log")
			putFile(t, path)
			retained = append(retained, path)
		} else {
			wantDirs++
		}
		if i%7 == 0 {
			name := []string{"old.wsp.lock", "old.wsp.ooo.lock", "old.lock"}[i%3]
			path := filepath.Join(dir, name)
			if err := os.WriteFile(path, nil, 0600); err != nil {
				t.Fatal(err)
			}
			old := time.Now().Add(-2 * time.Hour)
			if err := os.Chtimes(path, old, old); err != nil {
				t.Fatal(err)
			}
			wantLocks++
			stale = append(stale, path)
		}
	}
	protected := filepath.Join(root, "a", "b", ".ns-quota-trim", "empty")
	makeDir(t, protected)
	p := newParallelWalk(32, false, nil)
	if err := p.walk(context.Background(), root, nil); err != nil {
		t.Fatal(err)
	}
	c := p.final()
	if c.Removed != wantDirs || c.LocksRemoved != wantLocks {
		t.Fatalf("counts=%+v wanted dirs=%d locks=%d", c, wantDirs, wantLocks)
	}
	for _, path := range retained {
		if contents, err := os.ReadFile(path); err != nil || string(contents) != "keep" {
			t.Fatalf("file changed: %s %v", path, err)
		}
	}
	if _, err := os.Stat(protected); err != nil {
		t.Fatal("quarantine changed", err)
	}
	for _, path := range stale {
		if _, err := os.Lstat(path); err == nil {
			t.Fatal("stale lock remains")
		}
	}
}

func TestStaleLockEligibility(t *testing.T) {
	now := time.Now()
	cutoff := now.Add(-time.Hour)
	for _, tc := range []struct {
		name                            string
		age                             time.Duration
		size                            int
		target, held, symlink, eligible bool
	}{
		{name: "old.wsp.lock", age: 2 * time.Hour, eligible: true},
		{name: "boundary.wsp.lock", age: 61 * time.Minute, eligible: true},
		{name: "rounded.wsp.lock", age: 60*time.Minute + 59*time.Second, eligible: true},
		{name: "young.wsp.lock", age: time.Minute},
		{name: "near.wsp.lock", age: 59*time.Minute + 59*time.Second},
		{name: "data.wsp.lock", age: 2 * time.Hour, size: 1},
		{name: "present.wsp.lock", age: 2 * time.Hour, target: true},
		{name: "active.wsp.lock", age: 2 * time.Hour, held: true},
		{name: "link.wsp.lock", age: 2 * time.Hour, symlink: true},
		{name: "other.lock", age: 2 * time.Hour, eligible: true},
		{name: "sidecar.wsp.ooo.lock", age: 2 * time.Hour, eligible: true},
		{name: "target.wsp.ooo.lock", age: 2 * time.Hour, target: true},
		{name: "plain.wsp", age: 2 * time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, tc.name)
			if tc.symlink {
				if err := os.Symlink("missing", path); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.WriteFile(path, make([]byte, tc.size), 0600); err != nil {
					t.Fatal(err)
				}
				old := now.Add(-tc.age)
				if err := os.Chtimes(path, old, old); err != nil {
					t.Fatal(err)
				}
			}
			if tc.target {
				putFile(t, strings.TrimSuffix(path, ".lock"))
			}
			if tc.held {
				f, err := os.OpenFile(path, os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				defer f.Close()
				if err := unix.Flock(int(f.Fd()), unix.LOCK_EX); err != nil {
					t.Fatal(err)
				}
			}
			fd, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer unix.Close(fd)
			eligible, removed, err := staleLock(fd, tc.name, cutoff, true)
			if err != nil || eligible != tc.eligible || removed {
				t.Fatalf("dry eligibility=%t removed=%t err=%v", eligible, removed, err)
			}
			eligible, removed, err = staleLock(fd, tc.name, cutoff, false)
			if err != nil || eligible != tc.eligible || removed != tc.eligible {
				t.Fatalf("eligibility=%t removed=%t err=%v", eligible, removed, err)
			}
			_, err = os.Lstat(path)
			if (err == nil) == tc.eligible {
				t.Fatalf("incorrect lock retention: %v", err)
			}
		})
	}
}

func TestUnlimitedDefaultAndWorkerCounts(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"storage":{"migration":{"phase":"shared","in_flight":0}}}`)
	}))
	defer server.Close()
	for _, workers := range []int{1, 4, 32} {
		root := t.TempDir()
		makeDir(t, filepath.Join(root, "a", "b", "c", "d"))
		var out bytes.Buffer
		if err := run(context.Background(), options{root: root, admin: server.URL, metrics: server.URL, workers: workers, interval: time.Second}, &out); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(out.String(), "budget/s=unlimited") {
			t.Fatal(out.String())
		}
		entries, err := os.ReadDir(root)
		if err != nil || len(entries) != 0 {
			t.Fatalf("worker=%d root=%v err=%v", workers, entries, err)
		}
	}
}

func TestDrySuffixesOwnDirectoryBufferNames(t *testing.T) {
	root := t.TempDir()
	const files = 32
	for i := 0; i < files; i++ {
		if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("f%03d.ext%03d", i, i)), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	w := walker{dry: true, buffers: [][]byte{make([]byte, 256)}}
	if err := w.walk(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	if w.counts.Files != files || len(w.counts.Suffixes) != files {
		t.Fatalf("counts=%+v", w.counts)
	}
	for i := 0; i < files; i++ {
		if w.counts.Suffixes[fmt.Sprintf(".ext%03d", i)] != 1 {
			t.Fatal(w.counts.Suffixes)
		}
	}
}
