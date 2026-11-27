package v2dict

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// laneDict opens a dictionary that flushes only on Flush and holds a merged
// base of 4000 names, with no merge running. append adds names under
// prefix, flushes them and records their IDs in ids.
func laneDict(t *testing.T, dir string) (d *Dict, ids map[string]uint32, add func(prefix string, n int)) {
	t.Helper()
	d, err := Open(dir, Options{BufferBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	ids = map[string]uint32{}
	add = func(prefix string, n int) {
		t.Helper()
		for i := range n {
			name := fmt.Sprintf("%s.%05d", prefix, i)
			e, _, err := d.Append("g", uint16(i%7), []byte(name), 0)
			if err != nil {
				t.Fatal(err)
			}
			ids[name] = e.ID
		}
		if err := d.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	add("base", 4000)
	d.Merge()
	if n := len(d.snap.Load().levels); n != 1 {
		t.Fatalf("base: %d levels", n)
	}
	return d, ids, add
}

func checkIDs(t *testing.T, d *Dict, ids map[string]uint32) {
	t.Helper()
	for name, id := range ids {
		i := strings.LastIndexByte(name, '.')
		var n int
		fmt.Sscanf(name[i+1:], "%d", &n)
		e, ok := d.Lookup("g", uint16(n%7), []byte(name))
		if !ok || e.ID != id || e.Deleted() {
			t.Fatalf("%s: %+v %v, want ID %d", name, e, ok, id)
		}
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !cond(); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for", what)
		}
	}
}

func busyMergers(d *Dict) int {
	d.mergeMu.Lock()
	defer d.mergeMu.Unlock()
	return d.mergers
}

// hookMerges blocks the merges whose write (counted from 0) is in block,
// with their .tmp files in place, until release[i] closes; started[i]
// closes when merge i blocks. The merge installed failInstall-th (from 0;
// -1: none) fails there, before it removes its inputs, as a kill would.
func hookMerges(t *testing.T, block []int, failInstall int) (started, release map[int]chan struct{}) {
	started, release = map[int]chan struct{}{}, map[int]chan struct{}{}
	for _, i := range block {
		started[i], release[i] = make(chan struct{}), make(chan struct{})
	}
	var writes, installs atomic.Int32
	testHook = func(p string) error {
		switch p {
		case "merge-write":
			if i := int(writes.Add(1)) - 1; started[i] != nil {
				close(started[i])
				<-release[i]
			}
		case "merge-installed":
			if int(installs.Add(1))-1 == failInstall {
				return fmt.Errorf("killed")
			}
		}
		return nil
	}
	t.Cleanup(func() {
		testHook = nil
		for _, c := range release {
			select {
			case <-c:
			default:
				close(c)
			}
		}
	})
	return started, release
}

func waitClosed(t *testing.T, what string, c chan struct{}) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for", what)
	}
}

// A base merge whose time grows with the name count must not stop the
// newer levels merging: while it runs, 60 flushes leave at most 12 levels
// (with one merge at a time they leave 62), and Close waits for both lanes.
func TestMergeLanesBoundLevels(t *testing.T) {
	dir := t.TempDir()
	d, ids, add := laneDict(t, dir)
	started, release := hookMerges(t, []int{0}, -1)
	add("new", 1000) // 1/4 of the base: the base merge is due
	waitClosed(t, "base merge", started[0])
	peak := 0
	for f := range 60 {
		add(fmt.Sprintf("f%02d", f), 20)
		// The base merge holds one lane; wait for the other to finish.
		waitFor(t, "second lane", func() bool { return busyMergers(d) == 1 })
		peak = max(peak, len(d.snap.Load().levels))
	}
	if peak > 12 {
		t.Fatalf("%d levels behind a running base merge, want <= 12", peak)
	}
	t.Logf("peak %d levels behind the base merge", peak)
	checkIDs(t, d, ids)
	closed := make(chan error, 1)
	go func() { closed <- d.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Close returned during a merge: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release[0])
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	d, err := Open(dir, Options{NoMerge: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	checkIDs(t, d, ids)
}

// Producers admitting first-sight names in separate shards write levels
// through size-triggered flushes (shard-local logs, no store-wide lock).
// While the first merge (the base merge) blocks one lane, the other keeps
// those levels merged: after the producers, at most 12 levels remain (with
// one merge at a time every flushed level waits), and no name is lost.
func TestMergeLanesBoundConcurrentAdmission(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, Options{BufferBytes: 8 << 10})
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]uint32{}
	for i := range 400 {
		name := fmt.Sprintf("base.%05d", i)
		e, _, err := d.Append("g", uint16(i%7), []byte(name), 0)
		if err != nil {
			t.Fatal(err)
		}
		ids[name] = e.ID
	}
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := d.MergeAll(); err != nil {
		t.Fatal(err)
	}
	started, release := hookMerges(t, []int{0}, -1)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for g := range 7 { // producer g owns vshard g: n%7 == g
		wg.Go(func() {
			for i := range 600 {
				name := fmt.Sprintf("c.%05d", i*7+g)
				e, _, err := d.Append("g", uint16(g), []byte(name), 0)
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				ids[name] = e.ID
				mu.Unlock()
			}
		})
	}
	waitClosed(t, "first merge", started[0])
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
	// The blocked merge holds one lane; wait for the other to finish.
	waitFor(t, "second lane", func() bool { return busyMergers(d) == 1 })
	if n := len(d.snap.Load().levels); n > 12 {
		t.Fatalf("%d levels behind a running merge, want <= 12", n)
	} else {
		t.Logf("%d levels behind the blocked merge", n)
	}
	checkIDs(t, d, ids)
	close(release[0])
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(dir, Options{NoMerge: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	checkIDs(t, d, ids)
}

// A kill while both lanes write their outputs, after a merge installed its
// output but not yet removed its inputs, loses no name: Open removes both
// lanes' .tmp files and the covered inputs.
func TestMergeLanesKillMidMerge(t *testing.T) {
	dir := t.TempDir()
	d, ids, add := laneDict(t, dir)
	// Merge writes: 0 the base merge, 1 f0's compaction, 2 f0+f1 (installed,
	// inputs left), 3 (f0+f1)+f2.
	started, release := hookMerges(t, []int{0, 3}, 1)
	add("new", 1000)
	waitClosed(t, "base merge", started[0])
	add("f0", 20)
	waitFor(t, "f0 compaction", func() bool { return busyMergers(d) == 1 })
	add("f1", 20)
	waitFor(t, "failed f0+f1 lane to leave", func() bool { return busyMergers(d) == 1 })
	add("f2", 20)
	waitClosed(t, "second lane", started[3])
	// The kill: copy the files as they are now.
	crash := t.TempDir()
	des, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	tmps, lvls := 0, 0
	for _, de := range des {
		b, err := os.ReadFile(filepath.Join(dir, de.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(crash, de.Name()), b, 0o644); err != nil {
			t.Fatal(err)
		}
		switch filepath.Ext(de.Name()) {
		case ".tmp":
			tmps++
		case ".lvl":
			lvls++
		}
	}
	// base, new, f0, f1, f0-f1, f2 and the two lanes' outputs.
	if tmps != 2 || lvls != 6 {
		t.Fatalf("killed with %d .tmp and %d .lvl files, want 2 and 6", tmps, lvls)
	}
	c, err := Open(crash, Options{NoMerge: true})
	if err != nil {
		t.Fatal(err)
	}
	if n := len(c.snap.Load().levels); n != 4 {
		t.Fatalf("reopened %d levels, want 4 (base, new, f0-f1, f2)", n)
	}
	checkIDs(t, c, ids)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	des, _ = os.ReadDir(crash)
	for _, de := range des {
		if filepath.Ext(de.Name()) == ".tmp" {
			t.Fatal("Open left", de.Name())
		}
	}
	close(release[0])
	close(release[3])
	d.Merge()
	if n := len(d.snap.Load().levels); n > 2 {
		t.Fatalf("%d levels after both lanes finished", n)
	}
	checkIDs(t, d, ids)
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if d, err = Open(dir, Options{NoMerge: true}); err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	checkIDs(t, d, ids)
}
