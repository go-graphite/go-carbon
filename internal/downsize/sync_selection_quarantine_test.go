package downsize

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// pendingNameBackend keeps one source name in a pending snapshot: its window
// selection stays unresolved, and each unresolved answer advances the clock
// past the selection stall budget.
type pendingNameBackend struct {
	*fakeSync
	pending atomic.Bool
	clock   *atomic.Int64
}

func (b *pendingNameBackend) WindowPresence(ctx context.Context, source string, names []string, w syncRange) (map[string][]syncDigest, error) {
	d, err := b.Digest(ctx, source, names, w.From, w.Until)
	if b.pending.Load() {
		for _, n := range names {
			if n == "pending" {
				d[n] = []syncDigest{{Hash: syncWindowUnresolved}}
				b.clock.Add(int64(3 * time.Minute / time.Second))
			}
		}
	}
	return d, err
}

func TestSyncQuarantinesPendingSelectionAndResumeCopiesIt(t *testing.T) {
	t.Parallel()
	clock := &atomic.Int64{}
	clock.Store(syncTestNow)
	b := &pendingNameBackend{fakeSync: newFakeSync(), clock: clock}
	b.pending.Store(true)
	w := syncRange{Name: "gap", From: syncTestNow - 600, Until: syncTestNow - 120}
	names := []string{"healthy.a", "healthy.b", "pending"}
	for _, n := range names {
		b.store(syncD1).archives[n] = []syncArchive{{Step: 60, Points: 10000}}
		b.store(syncD1).points[n] = map[int64]map[int64]float64{60: {w.From: 42, w.From - 60: math.Copysign(0, -1)}}
	}
	o := syncOptions{to: syncT, from: []string{syncD1}, state: t.TempDir(), batch: 2, streams: 1, maxRounds: 2, windows: syncRanges{w}, includeShort: true, now: func() time.Time { return time.Unix(clock.Load(), 0) }}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The run finishes: the healthy names are copied and verified, the pending
	// name is quarantined and reported instead of ending the run.
	err := runSync(ctx, o, b)
	if !errors.Is(err, ErrSyncQuarantined) {
		t.Fatal("pending selection did not finish as a named exception", err)
	}
	for _, n := range names[:2] {
		if b.store(syncT).points[n][60][w.From] != 42 {
			t.Fatal("healthy name not copied", n)
		}
	}
	if _, ok := b.store(syncT).points["pending"]; ok {
		t.Fatal("pending selection copied without a resolved selection")
	}
	var issues []syncIssue
	data, err := os.ReadFile(filepath.Join(o.state, "quarantine.json"))
	if err != nil || json.Unmarshal(data, &issues) != nil || len(issues) != 1 || issues[0].Name != "pending" || issues[0].Reason != syncWindowSelectionReason {
		t.Fatalf("quarantine report: %s %v", data, err)
	}
	var progress progressSnapshot
	data, err = os.ReadFile(filepath.Join(o.state, "progress.json"))
	if err != nil || json.Unmarshal(data, &progress) != nil || progress.Quarantined != 1 {
		t.Fatalf("progress lacks the quarantined name: %s %v", data, err)
	}

	// The source snapshot lands; a resume retries and copies the name and
	// releases its quarantine.
	b.pending.Store(false)
	copies := len(b.copies)
	if err := runSync(ctx, o, b); err != nil {
		t.Fatal("resume after the source resolved", err)
	}
	if b.store(syncT).points["pending"][60][w.From] != 42 {
		t.Fatal("resume did not copy the resolved name")
	}
	for _, c := range b.copies[copies:] {
		if c != syncD1+" pending" {
			t.Fatal("resume recopied healthy names", b.copies[copies:])
		}
	}
	data, err = os.ReadFile(filepath.Join(o.state, "quarantine.json"))
	if err != nil || json.Unmarshal(data, &issues) != nil || len(issues) != 0 {
		t.Fatalf("resolved name still quarantined: %s %v", data, err)
	}
}
