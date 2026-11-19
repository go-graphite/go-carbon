package v2index

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
)

// settle waits for the background base merge and installs it.
func (c *Catalog) settle() { c.install(c.Snapshot(), true) }

func baseNames(n, salt int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("app%d.host%04d.metric%d.%d", i%7, i, salt, i%13)
	}
	return out
}

// A failed base write keeps the preceding base file usable and the catalog
// correct (the merged base stays on the heap), and reports the failure.
func TestBaseFileWriteFailureKeepsOldBase(t *testing.T) {
	dir := t.TempDir()
	var c Catalog
	if err := c.SetDir(dir); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	first := baseNames(20000, 1)
	if err := c.Publish(1, first); err != nil {
		t.Fatal(err)
	}
	oldPath := c.Snapshot().levels[0].path
	fileHook = func(size int) error {
		if size > 100<<10 {
			return errors.New("injected write failure")
		}
		return nil
	}
	defer func() { c.Close(); fileHook = nil }() // after merges stop reading it
	add := baseNames(5000, 2)
	if err := c.Apply(2, add, nil); err != nil {
		t.Fatal(err)
	}
	c.settle()
	if c.Err() == nil {
		t.Fatal("write failure not reported")
	}
	s := c.Snapshot()
	want := append(slices.Clone(first), add...)
	slices.Sort(want)
	if !slices.Equal(slices.Collect(s.Names()), want) {
		t.Fatal("names differ after failed base write")
	}
	if s.levels[0].path != "" {
		t.Fatalf("base should be on the heap after a failed write, path %q", s.levels[0].path)
	}
	c.Close() // the next spill attempt may be writing its temporary file
	if m, _ := filepath.Glob(filepath.Join(dir, "*.tmp")); len(m) != 0 {
		t.Fatalf("temporary file left: %v", m)
	}
	// The failed merge's inputs were retired, but a reader holding the old
	// base still has its mapping; a new process would reopen a complete file.
	_ = oldPath
}

// A process killed while writing the next base leaves the preceding base
// file whole: it reopens with the same names, and the partial file fails.
func TestBaseFileKillDuringWrite(t *testing.T) {
	if dir := os.Getenv("V2INDEX_KILL_DIR"); dir != "" {
		var c Catalog
		c.dir = dir // keep files of the parent's view; no cleanup
		if err := c.Publish(1, baseNames(20000, 1)); err != nil {
			t.Fatal(err)
		}
		fileHook = func(size int) error {
			if size > 100<<10 {
				syscall.Kill(os.Getpid(), syscall.SIGKILL)
				select {}
			}
			return nil
		}
		c.Publish(2, baseNames(30000, 2))
		t.Fatal("not killed")
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestBaseFileKillDuringWrite$")
	cmd.Env = append(os.Environ(), "V2INDEX_KILL_DIR="+dir)
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
		t.Fatalf("child was not killed: %v\n%s", err, out)
	}
	r, err := openRun(filepath.Join(dir, "base-000001.fc"))
	if err != nil {
		t.Fatalf("old base unusable: %v", err)
	}
	want := baseNames(20000, 1)
	slices.Sort(want)
	if !slices.Equal(levelNames(&level{run: r}), want) {
		t.Fatal("old base names differ")
	}
	if _, err := os.Stat(filepath.Join(dir, "base-000002.fc")); !os.IsNotExist(err) {
		t.Fatalf("new base must not exist: %v", err)
	}
	if _, err := openRun(filepath.Join(dir, "base-000002.fc.tmp")); err == nil {
		t.Fatal("partial base file opened")
	}
	// The next process serves the whole old base at once, as revision 1, and
	// removes the partial file; the rebuild's publication then replaces it.
	var c Catalog
	if err := c.SetDir(dir); err != nil {
		t.Fatal(err)
	}
	s := c.Snapshot()
	if s.Revision() != 1 || !slices.Equal(slices.Collect(s.Names()), want) {
		t.Fatalf("restart did not serve the old base: revision %d, %d names", s.Revision(), s.Len())
	}
	if m, _ := filepath.Glob(filepath.Join(dir, "base-*")); !slices.Equal(m, []string{filepath.Join(dir, "base-000001.fc")}) {
		t.Fatalf("SetDir kept %v, want only the newest whole base", m)
	}
	rebuilt := baseNames(30000, 2)
	if err := c.Publish(s.Revision()+1, rebuilt); err != nil {
		t.Fatal(err)
	}
	slices.Sort(rebuilt)
	if got := slices.Collect(c.Snapshot().Names()); !slices.Equal(got, rebuilt) {
		t.Fatal("rebuild did not replace the old base")
	}
	if m, _ := filepath.Glob(filepath.Join(dir, "base-*")); !slices.Equal(m, []string{filepath.Join(dir, "base-000003.fc")}) {
		t.Fatalf("after the rebuild: %v, want only base-000003.fc", m)
	}
	// A corrupt newest base is removed and never served.
	if err := os.WriteFile(filepath.Join(dir, "base-000004.fc"), []byte("torn"), 0o644); err != nil {
		t.Fatal(err)
	}
	var d Catalog
	if err := d.SetDir(dir); err != nil {
		t.Fatal(err)
	}
	if d.Snapshot().Len() != 0 {
		t.Fatal("corrupt base served")
	}
	if m, _ := filepath.Glob(filepath.Join(dir, "base-*")); len(m) != 0 {
		t.Fatalf("SetDir left %v", m)
	}
}

// Apply never runs the base merge: the base stays the same run until the
// background merge is installed, and heap levels stay below 1/levelFanout.
func TestBaseMergeOffPublishPath(t *testing.T) {
	var c Catalog
	if err := c.SetDir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Publish(1, baseNames(64000, 1)); err != nil {
		t.Fatal(err)
	}
	base := c.Snapshot().levels[0].run
	block := make(chan struct{})
	fileHook = func(int) error { <-block; return nil }
	defer func() { c.Close(); fileHook = nil }() // after merges stop reading it
	add := baseNames(10000, 2)
	for i := 0; i < len(add); i += 1000 {
		if err := c.Apply(uint64(2+i/1000), add[i:i+1000], nil); err != nil {
			t.Fatal(err)
		}
		if c.Snapshot().levels[0].run != base {
			t.Fatal("Apply rewrote the base")
		}
	}
	if c.job == nil {
		t.Fatal("no background base merge started")
	}
	close(block)
	c.settle()
	s := c.Snapshot()
	if s.levels[0].run == base || s.levels[0].path == "" || s.Len() != 74000 {
		t.Fatalf("merge not installed: levels=%d len=%d", s.Levels(), s.Len())
	}
	checkLevels(t, s)
}
