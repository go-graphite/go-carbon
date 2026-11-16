//go:build linux || darwin

package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-graphite/go-carbon/internal/partsconvert"
)

func TestIDRangePhysicalReclaimAndResume(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	fixturePath := filepath.Join(root, "fixture")
	fixture, err := compare(ctx, config{dir: fixturePath, shards: 1, metrics: 4, points: 8192, archives: 2, partitionSeconds: 86400, blockPoints: 4096})
	if err != nil {
		t.Fatal(err)
	}
	legacy := filepath.Join(fixturePath, "legacy")
	target := filepath.Join(root, "target")
	before, err := allocatedBytes(legacy)
	if err != nil {
		t.Fatal(err)
	}
	opts := copyOptions{RangeOrder: true}
	r, err := convertLegacyCopy(ctx, legacy, target, 1, 16<<20, opts)
	if err != nil {
		t.Fatal(err)
	}
	if r.Batches != 4 || r.Metrics != 4 || r.Points != fixture.Points {
		t.Fatalf("%+v fixture%+v", r, fixture)
	}
	after, err := allocatedBytes(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if after >= before {
		t.Fatalf("source allocation did not shrink: %d -> %d", before, after)
	}
	measurements, err := filepath.Glob(filepath.Join(target, "ranges", "measurement-*.json"))
	if err != nil || len(measurements) != 4 {
		t.Fatal(measurements, err)
	}
	shrank := false
	for _, path := range measurements {
		var m rangeMeasurement
		b, _ := os.ReadFile(path)
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatal(err)
		}
		shrank = shrank || m.LegacyAfter < m.LegacyBefore
		t.Logf("range%d allocated %d -> %d SST %d -> %d", m.Sequence, m.LegacyBefore, m.LegacyAfter, m.LegacySSTBefore, m.LegacySSTAfter)
	}
	if !shrank {
		t.Fatal("no allocation fell during conversion")
	}
	r, err = convertLegacyCopy(ctx, legacy, target, 1, 16<<20, opts)
	if err != nil || r.Points != 0 || r.Batches != 0 {
		t.Fatal(r, err)
	}
	// ID order does not pretend that an arbitrary coverage set is a name prefix.
	cursor, _, err := partsconvert.NameState(filepath.Join(target, "names"))
	if err != nil || cursor.Done {
		t.Fatal(cursor, err)
	}
}

func TestIDRangeRetryAfterPublishedAndReclaimed(t *testing.T) {
	for _, phase := range []string{"id-published", "id-reclaimed"} {
		t.Run(phase, func(t *testing.T) {
			ctx := context.Background()
			root := t.TempDir()
			fixturePath := filepath.Join(root, "fixture")
			if _, err := compare(ctx, config{dir: fixturePath, shards: 1, metrics: 2, points: 128, archives: 1, partitionSeconds: 86400, blockPoints: 4096}); err != nil {
				t.Fatal(err)
			}
			legacy, target := filepath.Join(fixturePath, "legacy"), filepath.Join(root, "target")
			interrupted := errors.New("interrupted")
			opts := copyOptions{RangeOrder: true, Checkpoint: func(s string) error {
				if s == phase {
					return interrupted
				}
				return nil
			}}
			if _, err := convertLegacyCopy(ctx, legacy, target, 1, 4<<20, opts); !errors.Is(err, interrupted) {
				t.Fatal(err)
			}
			if _, err := convertLegacyCopy(ctx, legacy, target, 1, 4<<20, copyOptions{RangeOrder: true}); err != nil {
				t.Fatal(err)
			}
			var state rangeState
			b, err := os.ReadFile(filepath.Join(target, "ranges", "state.json"))
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(b, &state); err != nil || !state.Done || state.Active != nil {
				t.Fatal(state, err)
			}
		})
	}
}

func TestOriginalDiskAdmissionRejectsBeforeIdentityOrTarget(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "legacy")
	target := filepath.Join(root, "target")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "bytes"), make([]byte, 64<<10), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := convertLegacyCopy(context.Background(), source, target, 1, 1<<20, copyOptions{OriginalAllocated: 1})
	if err == nil {
		t.Fatal("accepted existing over-budget source")
	}
	for _, path := range []string{target, filepath.Join(source, "CONVERSION-SOURCE")} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("mutated before admission", path, err)
		}
	}
}
