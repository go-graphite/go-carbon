package inputlog

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func upgradeTestPlan() UpgradePlan {
	return UpgradePlan{Root: "input.generation.tail.bin", Sources: []UpgradeSource{
		{Name: "snapshot.bin", Offset: 32, Size: 64},
		{Name: "raw.tail.bin", Offset: 8, Size: 48},
		{Name: "capture.older.tail.bin", Size: 123},
		{Journal: true},
		{Name: "capture.newer.tail.bin", Offset: 10, Size: 321},
	}, JournalOffsets: map[uint16]ReplayCursor{3: {Segment: 1, Salt: 123, Offset: 64, Records: 2}}}
}

func TestUpgradeCutCrashProcess(t *testing.T) {
	dir, crash := os.Getenv("INPUTLOG_UPGRADE_TEST_DIR"), os.Getenv("INPUTLOG_UPGRADE_TEST_STAGE")
	if dir == "" || crash == "" {
		t.Skip("subprocess helper")
	}
	l, err := Open(dir, withTestDefaults(Options{SegmentSize: 4096}))
	if err != nil {
		t.Fatal(err)
	}
	_, err = l.commitUpgradeCut(upgradeTestPlan(), func(stage string) {
		if stage == crash {
			os.Exit(82)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Fatal("crash stage not reached", crash)
}

func TestUpgradeCutSurvivesEveryCommitBoundary(t *testing.T) {
	for _, stage := range []string{"segment_allocated", "segment_synced", "cut_synced", "cut_renamed", "cut_committed"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			opts := withTestDefaults(Options{SegmentSize: 4096})
			src := newFakeSource()
			l := openStarted(t, dir, opts, src)
			src.add("historical", 42, 7)
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestUpgradeCutCrashProcess$")
			cmd.Env = append(os.Environ(), "INPUTLOG_UPGRADE_TEST_DIR="+dir, "INPUTLOG_UPGRADE_TEST_STAGE="+stage)
			output, err := cmd.CombinedOutput()
			if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 82 {
				t.Fatalf("did not crash at %s: %v %s", stage, err, output)
			}
			l, err = Open(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			cut, err := l.CommitUpgradeCut(upgradeTestPlan())
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(cut.Plan, upgradeTestPlan()) {
				t.Fatal("changed legacy source order/cursors", cut.Plan)
			}
			changed := upgradeTestPlan()
			changed.Sources = nil
			again, err := l.CommitUpgradeCut(changed)
			if err != nil || !reflect.DeepEqual(cut, again) {
				t.Fatal("reinventoried committed upgrade", again, err)
			}
			l.StartAppend()
			var suffix Batch
			suffix.Append(NewStreamRef("new.suffix"), 99, 7)
			receipt, err := l.Append(0, &suffix)
			if err != nil {
				t.Fatal(err)
			}
			if receipt.Tail.Segment < cut.Segment || receipt.Tail.Segment == cut.Segment && receipt.Salt != cut.Salt {
				t.Fatal("append crossed wrong upgrade incarnation", cut, receipt)
			}
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
			l, err = Open(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			got, _ := replayAll(t, l)
			if len(got["historical"]) != 1 || got["historical"][0].Value != 42 || len(got["new.suffix"]) != 1 || got["new.suffix"][0].Value != 99 {
				t.Fatal("upgrade lost historical or acknowledged suffix", got)
			}
		})
	}
}

func TestUpgradeCutIdentitySurvivesJournalRetirement(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, withTestDefaults(Options{SegmentSize: 4096}))
	if err != nil {
		t.Fatal(err)
	}
	cut, err := l.CommitUpgradeCut(upgradeTestPlan())
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Retire(); err != nil {
		t.Fatal(err)
	}
	l, err = Open(dir, withTestDefaults(Options{SegmentSize: 4096}))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	l.StartAppend()
	var batch Batch
	batch.Append(NewStreamRef("after.retirement"), 1, 1)
	receipt, err := l.Append(0, &batch)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Tail.Segment <= cut.Segment {
		t.Fatal("retirement reused upgrade sequence", cut, receipt)
	}
}

func TestUpgradeCutRejectsUnsafeSourceAndTrailingData(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, withTestDefaults(Options{SegmentSize: 4096}))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	for _, bad := range []string{"..", ".", "../outside", ""} {
		plan := upgradeTestPlan()
		plan.Sources[0].Name = bad
		if _, err := l.CommitUpgradeCut(plan); err == nil {
			t.Fatal("accepted unsafe source", bad)
		}
	}
	if _, err := l.CommitUpgradeCut(upgradeTestPlan()); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(filepath.Join(dir, UpgradeCutName), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(" {}"); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadUpgradeCut(dir); err == nil {
		t.Fatal("accepted trailing manifest data")
	}
}
