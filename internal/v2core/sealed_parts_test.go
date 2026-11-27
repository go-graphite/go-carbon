package v2core

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

func TestPartSourcePersistsWithoutAliasingIndependentRoots(t *testing.T) {
	root := t.TempDir()
	// A process may die during initial identity creation, before publication.
	if err := os.WriteFile(filepath.Join(root, ".PARTSOURCE-interrupted"), []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	first, err := openPartSource(root, "shared-journal-label", v2part.Provenance{})
	if err != nil {
		t.Fatal(err)
	}
	again, err := openPartSource(root, "shared-journal-label", v2part.Provenance{})
	if err != nil || first != again {
		t.Fatalf("restart changed original source: %v", err)
	}
	other, err := openPartSource(t.TempDir(), "shared-journal-label", v2part.Provenance{})
	if err != nil || first.Incarnation == other.Incarnation {
		t.Fatalf("independent roots alias one source: %v", err)
	}
	if _, err := openPartSource(root, "shared-journal-label", other); err == nil {
		t.Fatal("source replacement accepted")
	}
}

func TestRollupBlindBatchRetainsRawAndSharedPart(t *testing.T) {
	for _, blind := range []bool{false, true} {
		t.Run(fmt.Sprint(blind), func(t *testing.T) {
			cfg := coreTestConfig()
			cfg.Shards, cfg.Producers, cfg.Rollups, cfg.BlindBatch = 1, 1, true, blind
			cfg.KeepBaseRows = true
			_, policy := rollupTestPolicy(t, "blind.original", "1s:1d", sealrollup.Last, .5)
			root := t.TempDir()
			e, err := openCore(root, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer e.close()
			rows := []row{{ID: 7, Time: 1700000000, Bits: 0x7ff8000000000042, Seq: 1}}
			if err := e.submitBatches(0, []coreBatch{{ID: 7, Order: 1, Clock: 1700000002, Policy: policy, Points: rows}}, 1, true); err != nil {
				t.Fatal(err)
			}
			snapshot := e.owners[0].db.NewSnapshot()
			defer snapshot.Close()
			got, err := coreRead(snapshot, 7)
			if err != nil || !slices.Equal(got, rows) {
				t.Fatalf("raw rows lost in shared archive ingest: %v %v", got, err)
			}
			parts, err := (SealedSnapshotReader{Snapshot: snapshot, Directory: filepath.Join(root, "owner-000")}).ReadPartFiles(context.Background())
			if err != nil || len(parts) != 2 {
				t.Fatalf("shared part missing: %v %v", parts, err)
			}
		})
	}
}

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
	// "ingest" fires after the one parts/ sync, just before the synced publish batch.
	for _, phase := range []string{"part-retained", "ingest", "commit"} {
		t.Run(phase, func(t *testing.T) {
			root := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestSealedPartCrashPublicationAndReplay$")
			cmd.Env = append(os.Environ(), "V2_PART_CRASH_ROOT="+root, "V2_PART_CRASH_PHASE="+phase)
			out, err := cmd.CombinedOutput()
			exit := &exec.ExitError{}
			if !errors.As(err, &exit) || exit.ExitCode() != 61 {
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
			want := 2
			if phase != "commit" {
				g := e.owners[0].gc
				g.mu.Lock()
				g.now = func() time.Time { return time.Now().Add(2 * sealedPartGrace) }
				g.mu.Unlock()
				if _, err = g.sweep(); err != nil {
					t.Fatal(err)
				}
				paths, err = filepath.Glob(filepath.Join(root, "owner-000", "parts", "*"))
				want = 0
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
