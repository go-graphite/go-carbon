package downsize

import (
	"context"
	"sync/atomic"
	"testing"
)

// A donor list can take hours. Observe its durable sub-batches before the
// donor completes, and do not count a probe or journal replay as fresh work.
func TestSyncLegacyCopyReportsCommittedBatches(t *testing.T) {
	b := newSyncLiveBackend(syncLiveOptions{copyAttempts: 1})
	var calls int
	var reported []int
	b.copy = func(ctx context.Context, o copyOptions) error {
		calls++
		if calls == 1 {
			if o.onCommitted != nil {
				t.Fatal("probe counted in parent progress")
			}
			return nil
		}
		if o.onCommitted == nil {
			t.Fatal("copy has no committed-batch observer")
		}
		o.onCommitted(3)
		o.onCommitted(2)
		return nil
	}
	ctx := context.WithValue(context.Background(), syncCopyProgressKey{}, func(n int) { reported = append(reported, n) })
	// The fixture copy function does not write a journal, so all names remain
	// residual. Progress observation must not turn them into successful names.
	res, err := b.Copy(ctx, syncD1, syncT, []string{"a", "b", "c", "d", "e"}, t.TempDir()+"/copy.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	if len(reported) != 2 || reported[0] != 3 || reported[1] != 2 {
		t.Fatal(reported)
	}
	if len(res.Failed) != 5 {
		t.Fatalf("observer changed receipt outcome: %+v", res)
	}
}

// Receipt replay must not be advertised as newly transferred names. Run the
// real producer and durable copy journal twice against HTTP transfer endpoints.
func TestSyncCopyCommittedObserverExcludesResume(t *testing.T) {
	names := copyNames("metric.", 100)
	f, src, dst := newFakeCopy(t, names)
	o := copyTestOptions(t, src, dst)
	var fresh atomic.Int64
	o.onCommitted = func(n int) { fresh.Add(int64(n)) }
	for run := range 2 {
		fresh.Store(0)
		if err := planFreeCopy(context.Background(), o); err != nil {
			t.Fatal(err)
		}
		want := int64(100)
		if run == 1 {
			want = 0
		}
		if got := fresh.Load(); got != want {
			t.Fatalf("run %d fresh=%d want=%d", run, got, want)
		}
		for _, name := range names {
			if f.imported[name] != 1 {
				t.Fatalf("%s imported %d times", name, f.imported[name])
			}
		}
	}
}
