package downsize

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/buckyd"
)

type sourceAbsenceRetryBackend struct {
	*retrySelectionBackend
	pending, missing, activity, changed, metadata bool
	evidenceCalls                                 int
	sumError                                      error
}

func (b *sourceAbsenceRetryBackend) ExpirationEvidence(_ context.Context, _ string, names []string, _ bool) (map[string]buckyd.ExpirationEvidence, error) {
	b.evidenceCalls++
	out := map[string]buckyd.ExpirationEvidence{}
	if b.missing {
		return out, nil
	}
	for _, n := range names {
		e := buckyd.ExpirationEvidence{Pending: b.pending || b.changed && b.evidenceCalls > 1}
		if b.activity {
			e.Latest = syncTestNow - 60
		}
		out[n] = e
	}
	return out, nil
}
func (b *sourceAbsenceRetryBackend) SumDigest(ctx context.Context, source string, names []string, _, from, until int64) (map[string][]syncDigest, error) {
	if b.sumError != nil {
		return nil, b.sumError
	}
	got, err := b.fakeSync.Digest(ctx, source, names, from, until)
	if b.metadata {
		got[names[0]] = []syncDigest{{Step: 60, Points: 10000}}
	}
	return got, err
}

func TestSyncWindowRetrySourceAbsentWithUnrelatedPeerHistory(t *testing.T) {
	for _, tc := range []string{"absent", "pending", "missing evidence", "activity", "changed", "metadata", "unsupported"} {
		t.Run(tc, func(t *testing.T) {
			b := &sourceAbsenceRetryBackend{retrySelectionBackend: &retrySelectionBackend{newFakeSync(), true}}
			switch tc {
			case "pending":
				b.pending = true
			case "missing evidence":
				b.missing = true
			case "activity":
				b.activity = true
			case "changed":
				b.changed = true
			case "metadata":
				b.metadata = true
			case "unsupported":
				b.sumError = errSyncNoDigest
			}
			w := syncRange{From: syncTestNow - 600, Until: syncTestNow - 120}
			b.store(syncD2).archives["old.metric"] = []syncArchive{{Step: 60, Points: 10000}}
			b.store(syncD2).points["old.metric"] = map[int64]map[int64]float64{60: {w.From - 86400: 42}}
			path := t.TempDir()
			j, err := openSyncJournal(filepath.Join(path, "sync.jsonl"), "source-absent")
			if err != nil {
				t.Fatal(err)
			}
			defer j.close()
			if err := j.append(syncRow{Kind: "window-selected", Round: 1, Source: syncD1, Skipped: []syncSkip{{"old.metric", syncWindowSelectionReason}}, SelectionComplete: true}); err != nil {
				t.Fatal(err)
			}
			s := &syncer{o: syncOptions{to: syncT, from: []string{syncD1, syncD2}, state: path, windows: syncRanges{w}, includeShort: true, now: func() time.Time { return time.Unix(syncTestNow, 0) }}, b: b, j: j, forbid: map[string]bool{}}
			s.addResidual(syncHole{Name: "old.metric", Source: syncD1}, syncWindowSelectionReason)
			if err := s.retryWindowSelections(context.Background()); err != nil {
				t.Fatal(err)
			}
			resolved := s.resolvedWindowSelections()[syncWindowSelectionKey{1, syncD1}]["old.metric"]
			if resolved != (tc == "absent") {
				t.Fatalf("resolved=%v residual=%v", resolved, s.sum.residual)
			}
			if len(b.copies) != 0 {
				t.Fatal("copied unrelated history", b.copies)
			}
			if tc == "absent" && len(s.sum.residual) != 0 {
				t.Fatal(s.sum.residual)
			}
			if tc != "absent" && len(s.sum.residual) != 1 {
				t.Fatal("lost unresolved source", s.sum.residual)
			}
		})
	}
}
