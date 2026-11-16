package v2core

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

func TestSealedPartCrashPublicationAndReplay(t *testing.T) {
	cfg := coreTestConfig()
	cfg.Shards, cfg.Producers, cfg.Rollups = 1, 1, true
	_, policy := rollupTestPolicy(t, "crash.original", "1s:1d", sealrollup.Last, .5)
	batch := []coreBatch{{ID: 7, Order: 1, Clock: 1700000002, Policy: policy,
		Points: []row{{ID: 7, Time: 1700000000, Bits: 0x7ff8000000000042, Seq: 1}}}}
	if root := os.Getenv("V2_PART_CRASH_ROOT"); root != "" {
		phase := os.Getenv("V2_PART_CRASH_PHASE")
		e, err := openCoreWithHooks(root, cfg, coreHooks{Stage: func(stage string) {
			if stage == phase {
				os.Exit(61) // Skip all cleanup, as an abruptly terminated writer does.
			}
		}})
		if err != nil {
			t.Fatal(err)
		}
		if err := e.submitBatches(0, batch, 1, true); err != nil {
			t.Fatal(err)
		}
		t.Fatal("crash boundary not reached")
	}
	for _, phase := range []string{"part-retained", "commit"} {
		t.Run(phase, func(t *testing.T) {
			root := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestSealedPartCrashPublicationAndReplay$")
			cmd.Env = append(os.Environ(), "V2_PART_CRASH_ROOT="+root, "V2_PART_CRASH_PHASE="+phase)
			out, err := cmd.CombinedOutput()
			if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 61 {
				t.Fatalf("child did not reach boundary: %v\n%s", err, out)
			}
			paths, err := filepath.Glob(filepath.Join(root, "owner-000", "parts", "*"))
			if err != nil || len(paths) != 2 {
				t.Fatalf("missing durable originals before recovery: %v %v", paths, err)
			}
			e, err := openCore(root, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer e.close()
			paths, err = filepath.Glob(filepath.Join(root, "owner-000", "parts", "*"))
			want := 0
			if phase == "commit" {
				want = 2
			}
			if err != nil || len(paths) != want {
				t.Fatalf("recovery retained %d files, want %d: %v", len(paths), want, err)
			}
			// Journal replay either fills the interrupted seal or is suppressed by
			// its committed owner cursor; both leave exactly one published part.
			if err := e.submitBatches(0, batch, 1, true); err != nil {
				t.Fatal(err)
			}
			snapshot := e.owners[0].db.NewSnapshot()
			defer snapshot.Close()
			parts, err := (SealedSnapshotReader{Snapshot: snapshot, Directory: filepath.Join(root, "owner-000")}).ReadPartFiles(context.Background())
			if err != nil || len(parts) != 2 {
				t.Fatalf("replay inventory: %v %v", parts, err)
			}
			descriptor, err := os.ReadFile(parts[1].Path)
			if err != nil {
				t.Fatal(err)
			}
			d, err := v2part.ParseDescriptor(descriptor)
			if err != nil {
				t.Fatal(err)
			}
			if err := v2part.ReadExpanded(context.Background(), parts[0].Path, &d, func(v2part.Key, []byte) error { return nil }); err != nil {
				t.Fatal(err)
			}
		})
	}
}
