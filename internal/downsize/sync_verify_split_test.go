package downsize

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

type splitReadBackend struct {
	*redoHarness
	failStore string
	permanent bool
}

func (f *splitReadBackend) Fetch(ctx context.Context, store string, names []string, from, until int64) (map[string]syncSeries, error) {
	if store == f.failStore && (len(names) > 1 || f.permanent && names[0] == "metric.02") {
		return nil, errors.New("response body read timeout")
	}
	return f.redoHarness.Fetch(ctx, store, names, from, until)
}

func TestVerifySplitRetriesOnlyFailedBackendRead(t *testing.T) {
	for _, permanent := range []bool{false, true} {
		t.Run(map[bool]string{false: "transient", true: "bad_name"}[permanent], func(t *testing.T) {
			f, o := newRedoHarness(t)
			o.windows = syncRanges{{Name: "gap", From: syncTestNow - 7200, Until: syncTestNow - 3600}}
			o.includeShort = true
			o.sampleEvery = 0
			names := []string{"metric.00", "metric.01", "metric.02", "metric.03"}
			for _, n := range names {
				f.put(syncT, n, []syncArchive{{3600, 240}}, syncTestNow-7200)
			}
			b := &splitReadBackend{redoHarness: f, failStore: syncT, permanent: permanent}
			j, s := checkpointFixture(t)
			defer j.close()
			s.o = o
			s.b = b
			s.redo = newSyncRedoGuard(j)
			for _, n := range names {
				s.masks.Store(n, uint64(1))
			}
			ctx := redoBatchContext(context.Background(), names, []int64{0, 1, 2, 3})
			row, err := s.verifyBatch(ctx, 1, names)
			if err != nil {
				t.Fatal(err)
			}
			for call, n := range f.snapshot() {
				if n != 1 {
					t.Errorf("completed backend work repeated %d times: %+v", n, call)
				}
			}
			if len(j.progress.RedoNames) != 0 {
				t.Errorf("redo=%v", j.progress.RedoNames)
			}
			got := map[string]bool{}
			for _, h := range row.Holes {
				got[h.Name] = true
			}
			want := map[string]bool{"metric.00": true, "metric.01": true, "metric.02": true, "metric.03": true}
			if permanent {
				delete(want, "metric.02")
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("holes=%v want %v", got, want)
			}
			if permanent && (len(row.Skipped) != 1 || row.Skipped[0].Name != "metric.02" || !strings.Contains(row.Skipped[0].Reason, "timeout")) {
				t.Fatalf("skipped=%v", row.Skipped)
			}
			if !permanent && len(row.Skipped) != 0 {
				t.Fatal(row.Skipped)
			}
			// A later verification is fresh: the split state cannot hide changes.
			before := f.snapshot()
			_, err = s.verifyBatch(redoBatchContext(context.Background(), names, []int64{0, 1, 2, 3}), 1, names)
			if err != nil {
				t.Fatal(err)
			}
			if reflect.DeepEqual(before, f.snapshot()) {
				t.Fatal("read results leaked into a later verification")
			}
		})
	}
}
