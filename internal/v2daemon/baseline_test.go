package v2daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2convert"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2part"
	"github.com/go-graphite/go-carbon/internal/v2read"
	"github.com/go-graphite/go-carbon/internal/v2recv"
	"github.com/go-graphite/go-carbon/points"
)

type singleBaselineSnapshot struct{ snapshot chunkstore.Snapshot }

func (s singleBaselineSnapshot) Snapshot(context.Context, string) (chunkstore.Snapshot, error) {
	return s.snapshot, nil
}

func convertedBaselineFixture(t *testing.T) (string, []byte) {
	t.Helper()
	const now = int64(1794700860)
	snapshot := chunkstore.Snapshot{Metadata: chunkstore.Metadata{ID: 987, Revision: 1, LastUpdate: time.Unix(now, 0), MetricConfig: chunkstore.MetricConfig{Name: "baseline.metric", Retentions: []chunkstore.Retention{{Step: 1, Count: 120}, {Step: 60, Count: 120}}, AggregationMethod: chunkstore.Average}}}
	snapshot.Archives = []chunkstore.Archive{{Retention: snapshot.Metadata.Retentions[0], Points: []chunkstore.Point{{Timestamp: now - 59, Value: 2}, {Timestamp: now - 58, Value: 4}}}, {Retention: snapshot.Metadata.Retentions[1], Points: []chunkstore.Point{{Timestamp: now - 60, Value: 3}}}}
	sum := sha256.Sum256([]byte("immutable-classic-fixture"))
	plan := v2convert.NativeCheckpointPlan{Version: 1, Now: now, Names: []string{"baseline.metric"}, Files: []v2convert.File{{Name: "CHUNKSTORE", SHA256: hex.EncodeToString(sum[:]), Bytes: 1}, {Name: "CURRENT", SHA256: hex.EncodeToString(sum[:]), Bytes: 1}}, Baseline: v2convert.NativeBaseline{Source: v2part.Provenance{Incarnation: v2part.PartID{3}, Stream: "fenced-fixture"}, Group: "local", Part: v2part.PartID{4}, Order: 100}}
	var err error
	plan.Baseline.Snapshot, err = plan.InventoryDigest()
	if err != nil {
		t.Fatal(err)
	}
	output := t.TempDir()
	if _, err := v2convert.ConvertNativeCheckpoint(context.Background(), singleBaselineSnapshot{snapshot}, plan, output, 1<<20, 1); err != nil {
		t.Fatal(err)
	}
	receipts, err := filepath.Glob(filepath.Join(output, "*", "receipt.json"))
	if err != nil || len(receipts) != 1 {
		t.Fatal(receipts, err)
	}
	policy, err := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: "baseline.metric", Group: "local", Config: sealrollup.Config{Method: sealrollup.Average, Archives: []sealrollup.Archive{{Step: 1, Count: 120}, {Step: 60, Count: 120}}}})
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Dir(receipts[0]), policy
}

func copyBaselineTree(t *testing.T, source, target string) {
	t.Helper()
	err := filepath.WalkDir(source, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		dest := filepath.Join(target, relative)
		if d.IsDir() {
			return os.MkdirAll(dest, 0700)
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.Create(dest)
		if err != nil {
			return err
		}
		_, err = io.Copy(out, in)
		return errors.Join(err, out.Sync(), out.Close())
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBaselineInstallReplayAndExpiry(t *testing.T) {
	directory, policy := convertedBaselineFixture(t)
	root := t.TempDir()
	replayRoot := t.TempDir()
	cfg := Config{Layer: 3, Producers: 2, Shards: 2, MaxCoreRows: 128, GroupInterval: time.Hour, BaselinePath: directory}
	// An independently installed baseline has no post-fence applications. Copying
	// only the original journal into it forces actual replay reduction after crash.
	initial, err := Open(replayRoot, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := initial.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := v2read.New(2, 16<<20, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	reader.Now = func() time.Time { return time.Unix(1794700860, 0) }
	cfg.Hooks = v2core.Hooks{PublishBatches: reader.Publish}
	store, err := Open(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Baseline originals participate in the same checksum-bound whole-part
	// hard-link snapshots as all later seals.
	linked := 0
	for owner, db := range store.Core.DBs() {
		snapshot := db.NewSnapshot()
		copied, err := v2core.LinkPartSnapshot(context.Background(), filepath.Join(t.TempDir(), "copy"), v2core.SealedSnapshotReader{Snapshot: snapshot, Directory: filepath.Join(root, "placement-group-00000001", "core", fmt.Sprintf("owner-%03d", owner))})
		_ = snapshot.Close()
		if err != nil {
			t.Fatal(err)
		}
		linked += len(copied.Parts)
	}
	if linked != 2 {
		t.Fatalf("baseline part/descriptor copy inventory: got %d", linked)
	}
	app := Application{Name: "baseline.metric", Policy: policy, Order: 101, Clock: 1794700860, Points: []points.Point{{Timestamp: 1794700803, Value: 12}}, Sequence: []uint64{101}}
	reader.Close()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.BaselinePath = filepath.Join(directory, "source-removed")
	open := func() {
		t.Helper()
		reader, err = v2read.New(2, 16<<20, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		reader.Now = func() time.Time { return time.Unix(1794700860, 0) }
		cfg.Hooks.PublishBatches = reader.Publish
		store, err = Open(replayRoot, cfg)
		if err != nil {
			t.Fatal(err)
		}
	}
	open()
	defer func() {
		if store != nil {
			reader.Close()
			_ = store.Close()
		}
	}()
	// Intake after the installed baseline continues its order.
	p := store.Producers[v2recv.OwnerFor("baseline.metric", 2)]
	if err := p.AppendApplications([]Application{app}); err != nil {
		t.Fatal(err)
	}
	if err := p.Flush(); err != nil {
		t.Fatal(err)
	}
	series, masked, err := reader.FetchWithMask(context.Background(), "baseline.metric", 1794700739, 1794700860)
	if err != nil || !masked || series.Step() != 60 || math.Float64bits(series.Values()[1]) != math.Float64bits(6) {
		t.Fatalf("baseline + intake: series=%+v masked=%t err=%v", series, masked, err)
	}
	// Expiration masks every baseline archive, and readmission cannot resurrect it.
	reader.Close()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = nil
	cfg.Hooks.Expire = func(v2core.RollupPolicy, v2core.RollupMetadata, time.Time) bool { return true }
	cfg.Hooks.ExpiryTTL = func(v2core.RollupPolicy, time.Time) (time.Duration, time.Time) { return time.Nanosecond, time.Time{} }
	open()
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, _, err := reader.FetchWithMask(context.Background(), "baseline.metric", 1794700739, 1794700860)
		if errors.Is(err, v2read.ErrNotFound) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("idle baseline expiration did not publish")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, masked, err := reader.FetchWithMask(context.Background(), "baseline.metric", 1794700739, 1794700860); !errors.Is(err, v2read.ErrNotFound) || !masked {
		t.Fatalf("expired baseline resurrected: mask=%t err=%v", masked, err)
	}
	baseline, err := reader.BaselineMetadata(context.Background(), "baseline.metric")
	if err != nil || baseline.BaselineOrder != 100 {
		t.Fatal("expired baseline lost cutover certificate", err)
	}
	p = store.Producers[v2recv.OwnerFor("baseline.metric", 2)]
	app.Order = 102
	app.Points[0].Timestamp = 1794700804
	app.Points[0].Value = 18
	app.Sequence[0] = 102
	if err := p.AppendApplications([]Application{app}); err != nil {
		t.Fatal(err)
	}
	if err := p.Drain(); err != nil {
		t.Fatal(err)
	}
	reader.Close()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = nil
	cfg.Hooks.Expire, cfg.Hooks.ExpiryTTL = nil, nil
	open()
	series, masked, err = reader.FetchWithMask(context.Background(), "baseline.metric", 1794700739, 1794700860)
	if err != nil || !masked || series.Values()[1] != 18 {
		t.Fatalf("readmitted baseline resurrected: %+v %t %v", series, masked, err)
	}
}

func TestBaselineInstallRejectsPartialAndPostAdmission(t *testing.T) {
	if stage := os.Getenv("V2_BASELINE_CRASH_STAGE"); stage != "" {
		cfg := Config{Layer: 3, Producers: 2, Shards: 2, MaxCoreRows: 128, BaselinePath: os.Getenv("V2_BASELINE_SOURCE")}
		cfg.Hooks.Stage = func(s string) {
			if s == stage {
				os.Exit(87)
			}
		}
		_, _ = Open(os.Getenv("V2_BASELINE_ROOT"), cfg)
		os.Exit(88)
	}
	directory, _ := convertedBaselineFixture(t)
	for _, stage := range []string{"baseline-ready", "baseline-installed"} {
		t.Run(stage, func(t *testing.T) {
			root := t.TempDir()
			cfg := Config{Layer: 3, Producers: 2, Shards: 2, MaxCoreRows: 128, BaselinePath: directory}
			cmd := exec.Command(os.Args[0], "-test.run=^TestBaselineInstallRejectsPartialAndPostAdmission$")
			cmd.Env = append(os.Environ(), "V2_BASELINE_CRASH_STAGE="+stage, "V2_BASELINE_SOURCE="+directory, "V2_BASELINE_ROOT="+root)
			var exit *exec.ExitError
			if err := cmd.Run(); !errors.As(err, &exit) || exit.ExitCode() != 87 {
				t.Fatalf("crash stage: %v", err)
			}
			marker := filepath.Join(root, "placement-group-00000001", "BASELINE.json")
			_, err := os.Stat(marker)
			if stage == "baseline-ready" && !errors.Is(err, os.ErrNotExist) {
				t.Fatal("partial baseline published", err)
			}
			if stage == "baseline-installed" && err != nil {
				t.Fatal("complete install missing", err)
			}
			if stage == "baseline-installed" {
				cfg.BaselinePath = filepath.Join(directory, "source-removed")
			}
			s, err := Open(root, cfg)
			if err != nil {
				t.Fatal("interrupted install did not recover", err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}

		})
	}
	root := t.TempDir()
	cfg := Config{Layer: 3, Producers: 1, Shards: 1, MaxCoreRows: 128}
	s, err := Open(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.BaselinePath = directory
	if s, err := Open(root, cfg); err == nil {
		_ = s.Close()
		t.Fatal("baseline installed into an already opened group")
	}
	// Corrupt original immutable bytes and require rejection before publication.
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".sst") {
			f, err := os.OpenFile(filepath.Join(directory, e.Name()), os.O_WRONLY, 0600)
			if err != nil {
				t.Fatal(err)
			}
			_, err = f.WriteAt([]byte{0xff}, 0)
			err = errors.Join(err, f.Close())
			if err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	cfg.BaselinePath = directory
	if s, err := Open(t.TempDir(), cfg); err == nil {
		_ = s.Close()
		t.Fatal("corrupt converted baseline accepted")
	}
}

func TestBaselineMissingPartNeverPublishes(t *testing.T) {
	directory, _ := convertedBaselineFixture(t)
	paths, err := filepath.Glob(filepath.Join(directory, "*.sst"))
	if err != nil || len(paths) != 1 {
		t.Fatal(paths, err)
	}
	if err := os.Remove(paths[0]); err != nil {
		t.Fatal(err)
	}
	publications := 0
	cfg := Config{Layer: 3, Producers: 1, Shards: 1, MaxCoreRows: 128, BaselinePath: directory, Hooks: v2core.Hooks{PublishBatches: func(_ int, s *pebble.Snapshot, _ []v2core.Batch, _ uint64) error { publications++; return s.Close() }}}
	if store, err := Open(t.TempDir(), cfg); err == nil {
		_ = store.Close()
		t.Fatal("missing baseline part accepted")
	}
	if publications != 0 {
		t.Fatal("partial baseline was published")
	}
}

// Native history spanning a month boundary converts into one part per UTC
// month, each with the metric envelope, exactly as a live seal lays it out.
// Install must accept the repeated envelope and keep every stored sample.
func TestBaselineInstallAcrossMonthPartitions(t *testing.T) {
	const now, october = int64(1794700860), int64(1792454400) // 2026-11-15, 2026-10-20 UTC
	retentions := []chunkstore.Retention{{Step: 60, Count: 120}, {Step: 3600, Count: 2400}}
	snapshot := chunkstore.Snapshot{Metadata: chunkstore.Metadata{ID: 988, Revision: 1, LastUpdate: time.Unix(now, 0), MetricConfig: chunkstore.MetricConfig{Name: "baseline.months", Retentions: retentions, AggregationMethod: chunkstore.Average}}}
	snapshot.Archives = []chunkstore.Archive{{Retention: retentions[0], Points: []chunkstore.Point{{Timestamp: now - 60, Value: 2}}}, {Retention: retentions[1], Points: []chunkstore.Point{{Timestamp: october, Value: 5}, {Timestamp: now - 60 - 3600, Value: 7}}}}
	sum := sha256.Sum256([]byte("immutable-classic-months"))
	plan := v2convert.NativeCheckpointPlan{Version: 1, Now: now, Names: []string{"baseline.months"}, Files: []v2convert.File{{Name: "CHUNKSTORE", SHA256: hex.EncodeToString(sum[:]), Bytes: 1}, {Name: "CURRENT", SHA256: hex.EncodeToString(sum[:]), Bytes: 1}}, Baseline: v2convert.NativeBaseline{Source: v2part.Provenance{Incarnation: v2part.PartID{3}, Stream: "fenced-fixture"}, Group: "local", Part: v2part.PartID{5}, Order: 100}}
	var err error
	if plan.Baseline.Snapshot, err = plan.InventoryDigest(); err != nil {
		t.Fatal(err)
	}
	output := t.TempDir()
	receipt, err := v2convert.ConvertNativeCheckpoint(context.Background(), singleBaselineSnapshot{snapshot}, plan, output, 1<<20, 1)
	if err != nil {
		t.Fatal(err)
	}
	receipts, err := filepath.Glob(filepath.Join(output, "*", "receipt.json"))
	if err != nil || len(receipts) != 1 {
		t.Fatal(receipts, err)
	}
	directory := filepath.Dir(receipts[0])
	partitions := map[int64]bool{}
	for _, f := range receipt.Files {
		if !strings.HasSuffix(f.Name, ".descriptor") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(directory, f.Name))
		if err != nil {
			t.Fatal(err)
		}
		d, err := v2part.ParseDescriptor(b)
		if err != nil {
			t.Fatal(err)
		}
		if d.Coverage.MinExpiry == 0 || d.Coverage.MaxExpiry == 0 || len(d.VShards) != 1 {
			t.Fatalf("converted part lacks live expiry/inventory: %+v", d.Coverage)
		}
		partitions[d.Scope.Partition] = true
	}
	if len(partitions) != 2 || !partitions[v2core.RollupMonthPartition(october)] || !partitions[v2core.RollupMonthPartition(now)] {
		t.Fatalf("month partitions %v", partitions)
	}
	root := t.TempDir()
	store, err := Open(root, Config{Layer: 3, Producers: 2, Shards: 2, MaxCoreRows: 128, GroupInterval: time.Hour, BaselinePath: directory})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	found := map[int64]uint64{}
	names := 0
	for _, db := range store.Core.DBs() {
		snap := db.NewSnapshot()
		err := v2core.ScanRollups(snap, func(p v2core.RollupPolicy, m v2core.RollupMetadata, records []sealrollup.Record) error {
			names++
			if m.BaselineOrder != 100 || m.Frontier.MaxTime != now-60 {
				return fmt.Errorf("baseline metadata %+v", m)
			}
			for _, r := range records {
				found[r.Sample.Time] = r.Sample.Bits
			}
			return nil
		})
		_ = snap.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	if names != 1 || len(found) != 3 || found[october] != math.Float64bits(5) || found[now-60] != math.Float64bits(2) || found[now-60-3600] != math.Float64bits(7) {
		t.Fatalf("installed baseline names=%d samples=%v", names, found)
	}
}
