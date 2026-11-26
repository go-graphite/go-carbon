package v2daemon

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// TestCatalogFoldDictOnlyL0Kill SIGKILLs an ID-keyed store after its L0
// run file (version 2, packed IDs) is renamed and before the seal, removes
// every producer catalog, appends a torn L0 tail file, and requires the
// restart to load the rows through the dictionary alone: the same packed
// IDs, the same points after the seal, and new admissions on the same IDs.
func TestCatalogFoldDictOnlyL0Kill(t *testing.T) {
	v2part.IDKeys.Store(true)
	defer v2part.IDKeys.Store(false)
	names := []string{"fold.a", "fold.b", "fold.c"}
	if root := os.Getenv("V2_FOLD_CHILD_ROOT"); root != "" {
		var armed atomic.Bool
		var once sync.Once
		cfg := admitConfig()
		cfg.Hooks.Stage = func(s string) {
			if armed.Load() && s == "l0-renamed" {
				once.Do(func() {
					fmt.Println("CUT")
					for {
						time.Sleep(time.Second)
					}
				})
			}
		}
		s, err := Open(root, cfg)
		if err != nil {
			t.Fatal(err)
		}
		p := s.Producers[0]
		var apps []Application
		for i, name := range names {
			apps = append(apps, admitApp(t, name, uint64(i+1)))
		}
		if err = p.AppendApplications(apps); err != nil {
			t.Fatal(err)
		}
		for _, name := range names {
			fmt.Printf("ID %s %d\n", name, knownIdentity(nil, p, name).ID())
		}
		armed.Store(true)
		if err = p.Drain(); err != nil {
			t.Fatal(err)
		}
		if err = s.Core.AppendBatches(0, nil, p.cursor, true); err != nil {
			t.Fatal(err)
		}
		t.Fatal("missed l0-renamed")
	}
	root := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCatalogFoldDictOnlyL0Kill$", "-test.timeout=30s")
	cmd.Env = append(os.Environ(), "V2_FOLD_CHILD_ROOT="+root)
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr synchronizedBuffer
	cmd.Stderr = &stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill() }()
	want := map[string]uint64{}
	ready := make(chan bool, 1)
	go func() {
		scan := bufio.NewScanner(pipe)
		for scan.Scan() {
			var name string
			var id uint64
			if n, _ := fmt.Sscanf(scan.Text(), "ID %s %d", &name, &id); n == 2 {
				want[name] = id
			}
			if scan.Text() == "CUT" {
				ready <- true
				return
			}
		}
		ready <- false
	}()
	select {
	case ok := <-ready:
		if !ok {
			_ = cmd.Wait()
			t.Fatal("child did not reach l0-renamed", stderr.String())
		}
	case <-time.After(20 * time.Second):
		t.Fatal("child timeout", stderr.String())
	}
	if err = cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	if err = cmd.Wait(); !errors.As(err, new(*exec.ExitError)) {
		t.Fatalf("not killed: %v", err)
	}
	for _, name := range names {
		if !v2core.IsPackedID(want[name]) {
			t.Fatalf("%s admitted under %#x, not a packed ID", name, want[name])
		}
	}
	journal := filepath.Join(root, "placement-group-00000001", "journal")
	if err = os.RemoveAll(journal); err != nil {
		t.Fatal(err)
	}
	l0s, _ := filepath.Glob(filepath.Join(root, "placement-group-00000001", "core", "*", "l0", "l0-*"))
	if len(l0s) != 1 {
		t.Fatalf("L0 files after the kill: %v", l0s)
	}
	raw, err := os.ReadFile(l0s[0])
	if err != nil || len(raw) < 6 || string(raw[:5]) != "V2L0\x03" {
		t.Fatalf("L0 file is not version 3: %q %v", raw[:min(len(raw), 6)], err)
	}

	// The torn tail of the next run file: a kill during its write, before
	// the rename, leaves half a version-2 image.
	if err = os.WriteFile(l0s[0][:len(l0s[0])-1]+"9.tmp", raw[:len(raw)/2], 0600); err != nil {
		t.Fatal(err)
	}

	cfg := admitConfig()
	s, err := Open(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	for _, name := range names {
		h := s.Core.ApplicationPolicy(want[name])
		if h == nil || h.Name() != name {
			t.Fatalf("%s: restart did not load packed ID %#x from the dictionary", name, want[name])
		}
	}
	p := s.Producers[0]
	if err = s.Core.AppendBatches(0, nil, p.cursor, true); err != nil {
		t.Fatal(err)
	}
	pin := s.Core.DBs()[0].NewSnapshot()
	defer pin.Close()
	snapshot, err := v2core.NewRollupSnapshot(context.Background(), pin, nil, cfg.Hooks.Identities, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i, name := range names {
		_, meta, ok := snapshot.LookupMetadata(name)
		if !ok || meta.ID != want[name] || meta.Frontier.Order != uint64(i+1) {
			t.Fatalf("%s after kill and seal: ok=%v meta.ID=%#x want %#x order=%d", name, ok, meta.ID, want[name], meta.Frontier.Order)
		}
	}
	// Re-admission after the catalogs are gone lands on the same identities.
	if err = p.AppendApplications([]Application{admitApp(t, "fold.b", 10)}); err != nil {
		t.Fatal(err)
	}
	if got := knownIdentity(t, p, "fold.b").ID(); got != want["fold.b"] {
		t.Fatalf("fold.b re-admitted under %#x, want %#x", got, want["fold.b"])
	}
}
