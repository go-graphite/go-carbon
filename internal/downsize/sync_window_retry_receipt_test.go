package downsize

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// Old selections can retain a pending name already covered by window-missing,
// or a process can stop after copied but before window-resolved is durable.
func TestWindowRetryReconcilesCompletedCopyReceipts(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"window-missing", "window-retry-prior-subset"} {
		t.Run(phase, func(t *testing.T) {
			b := newFakeSync()
			for _, name := range []string{"completed", "unfinished"} {
				b.put(syncD1, name, syncTestSchema)
			}
			b.put(syncT, "completed", syncTestSchema)
			state := t.TempDir()
			path := filepath.Join(state, "sync.jsonl")
			j, err := openSyncJournal(path, "receipt-retry")
			if err != nil {
				t.Fatal(err)
			}
			for _, row := range []syncRow{
				{Kind: "window-selected", Round: 2, Source: syncD1, Skipped: []syncSkip{{"completed", syncWindowSelectionReason}, {"unfinished", syncWindowSelectionReason}}, SelectionComplete: true},
				{Kind: "copied", Phase: phase, Round: 2, Source: syncD1, First: "completed", Last: "completed", Names: 1, Copied: 1, Selected: []string{"completed"}},
			} {
				if err := j.append(row); err != nil {
					t.Fatal(err)
				}
			}
			j.close()
			j, err = openSyncJournal(path, "receipt-retry")
			if err != nil {
				t.Fatal(err)
			}
			defer j.close()
			s := &syncer{j: j, b: b, redo: newSyncRedoGuard(j), forbid: map[string]bool{}, o: syncOptions{
				state: state, to: syncT, from: []string{syncD1}, includeShort: true,
				windows: syncRanges{{From: syncTestNow - 600, Until: syncTestNow}}, now: func() time.Time { return time.Unix(syncTestNow, 0) },
			}}
			if err := s.retryWindowSelections(context.Background()); err != nil {
				t.Fatal(err)
			}
			if want := []string{syncD1 + " unfinished"}; !reflect.DeepEqual(b.copies, want) {
				t.Fatalf("completed import repeated: calls=%v want=%v", b.copies, want)
			}
			if j.progress.RedoNames["import"] != 0 || len(s.sum.residual) != 0 || s.prog.done.Load() != 2 {
				t.Fatalf("redo=%v residual=%v done=%d", j.progress.RedoNames, s.sum.residual, s.prog.done.Load())
			}
			if err := s.retryWindowSelections(context.Background()); err != nil || len(b.copies) != 1 {
				t.Fatalf("resolved replay copied again: %v %v", b.copies, err)
			}
		})
	}
}

func TestWindowRetryCopyReceiptRequiresExactSuccessfulMembership(t *testing.T) {
	t.Parallel()
	j, s := checkpointFixture(t)
	defer j.close()
	for _, source := range []string{syncD1, syncD2} {
		if err := j.append(syncRow{Kind: "window-selected", Round: 2, Source: source, Skipped: []syncSkip{
			{"a", "pending"}, {"b", "pending"}, {"failed", "pending"}, {"resolved", "pending"}, {"short", "pending"}, {"z", "pending"},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	for _, row := range []syncRow{
		{Kind: "copied", Phase: "window-missing", Round: 2, Source: syncD1, Names: 5, Copied: 2, First: "a", Last: "z", Selected: []string{"a", "failed", "resolved", "short", "z"}, Failed: []string{"failed"}, Resolved: []syncSkip{{"resolved", "policy"}}, Short: []string{"short"}},
		{Kind: "copied", Phase: "window-missing", Round: 2, Source: syncD2, Names: 1, Copied: 1, First: "a", Last: "a", Forbidden: true},
		{Kind: "copied", Phase: "window-missing", Round: 3, Source: syncD2, Names: 1, Copied: 1, First: "z", Last: "z"},
		{Kind: "copied", Phase: "fill", Round: 2, Source: syncD2, Names: 1, Copied: 1, First: "z", Last: "z"},
	} {
		if err := j.append(row); err != nil {
			t.Fatal(err)
		}
	}
	got := s.resolvedWindowSelections()
	want := map[syncWindowSelectionKey]map[string]bool{{2, syncD1}: {"a": true, "z": true}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unsafe receipt reconciliation: got=%v want=%v", got, want)
	}
}
