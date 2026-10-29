//go:build linux

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestLocksMatchGNUFindEligibility(t *testing.T) {
	root := t.TempDir()
	for _, tc := range []struct {
		name   string
		age    time.Duration
		target bool
	}{
		{"old.wsp.lock", 2 * time.Hour, false}, {"rounded.wsp.lock", 60*time.Minute + 30*time.Second, false},
		{"young.wsp.lock", time.Minute, false}, {"target.wsp.lock", 2 * time.Hour, true},
		{"ordinary.lock", 2 * time.Hour, false}, {"ooo.wsp.ooo.lock", 2 * time.Hour, false},
	} {
		path := filepath.Join(root, tc.name)
		if err := os.WriteFile(path, nil, 0600); err != nil {
			t.Fatal(err)
		}
		old := time.Now().Add(-tc.age)
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatal(err)
		}
		if tc.target {
			putFile(t, strings.TrimSuffix(path, ".lock"))
		}
	}
	// Use the script's exact candidate/age/missing-target predicates. Every lock
	// here is zero-length; separate tests cover nonzero locks.
	cmd := exec.Command("find", root, "-type", "f", "-name", "*.lock", "-size", "0", "-mmin", "+60", "-exec", "sh", "-c", `! [ -e "${1%.lock}" ]`, "sh", "{}", ";", "-printf", "%f\n")
	output, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Fields(string(output))
	sort.Strings(want)
	fd, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, entry := range entries {
		eligible, _, err := staleLock(fd, entry.Name(), time.Now().Add(-time.Hour), true)
		if err != nil {
			t.Fatal(err)
		}
		if eligible {
			got = append(got, entry.Name())
		}
	}
	sort.Strings(got)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("Go=%v GNU find=%v", got, want)
	}
}
