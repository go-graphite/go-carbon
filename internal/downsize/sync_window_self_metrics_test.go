package downsize

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

type selfMetricRetryBackend struct {
	*fakeSync
	failed string
}

func (b *selfMetricRetryBackend) WindowPresence(ctx context.Context, source string, names []string, w syncRange) (map[string][]syncDigest, error) {
	return b.Digest(ctx, source, names, w.From, w.Until)
}

func (b *selfMetricRetryBackend) CopyWindow(ctx context.Context, source, target string, names []string, journal string, windows syncRanges) (syncCopyResult, error) {
	var good []string
	for _, n := range names {
		if n != b.failed {
			good = append(good, n)
		}
	}
	out, err := b.fakeSync.CopyWindow(ctx, source, target, good, journal, windows)
	if b.failed != "" {
		out.Failed = []string{b.failed}
	}
	return out, err
}

// Internal-looking names still carry data: only a named failed repair may be
// quarantined. A successful self metric must remain in the verified selection.
func TestWindowSelectionSelfMetricsAreNotPrefixExcluded(t *testing.T) {
	t.Parallel()
	for _, failed := range []string{"tools.storectl.source.sync.client.active", "stats.flog.secondly.event_counts.sum"} {
		t.Run(failed, func(t *testing.T) {
			b := &selfMetricRetryBackend{fakeSync: newFakeSync(), failed: failed}
			path := t.TempDir()
			q, err := openSyncIssues(path)
			if err != nil {
				t.Fatal(err)
			}
			j, err := openSyncJournal(filepath.Join(path, "sync.jsonl"), "self-metrics")
			if err != nil {
				t.Fatal(err)
			}
			defer j.close()
			w := syncRange{From: syncTestNow - 600, Until: syncTestNow - 120}
			names := []string{failed, "tools.storectl.healthy.sync.progress", "stats.flog.healthy.sum"}
			var skipped []syncSkip
			for _, name := range names {
				b.store(syncD1).archives[name] = []syncArchive{{Step: 60, Points: 10000}}
				b.store(syncD1).points[name] = map[int64]map[int64]float64{60: {w.From: 42}}
				skipped = append(skipped, syncSkip{name, syncWindowSelectionReason})
			}
			if err := j.append(syncRow{Kind: "window-selected", Round: 1, Source: syncD1, Skipped: skipped, SelectionComplete: true}); err != nil {
				t.Fatal(err)
			}
			s := &syncer{o: syncOptions{to: syncT, from: []string{syncD1}, state: path, issues: q, windows: syncRanges{w}, includeShort: true, now: func() time.Time { return time.Unix(syncTestNow, 0) }}, b: b, j: j, forbid: map[string]bool{}}
			for _, name := range names {
				s.addResidual(syncHole{Name: name, Source: syncD1}, syncWindowSelectionReason)
			}
			if err := s.waitWindowSelections(context.Background()); !errors.Is(err, ErrSyncResidual) {
				t.Fatal(err)
			}
			if n, _ := q.counts(); n != 0 || q.excluded(failed) {
				t.Fatalf("wrong failed-name quarantine: %+v", q.quarantined)
			}
			want := map[string]bool{names[1]: true, names[2]: true}
			if got := s.resolvedWindowSelections()[syncWindowSelectionKey{1, syncD1}]; !reflect.DeepEqual(got, want) {
				t.Fatalf("healthy self metrics excluded: got=%v want=%v", got, want)
			}
			for _, name := range names[1:] {
				if q.excluded(name) || b.store(syncT).points[name][60][w.From] != 42 {
					t.Fatal("healthy self metric was not repaired", name)
				}
			}
			copies := len(b.copies)
			if err := s.waitWindowSelections(context.Background()); !errors.Is(err, ErrSyncResidual) || len(b.copies) != copies {
				t.Fatal("replayed completed or quarantined selection", err)
			}
		})
	}
}
