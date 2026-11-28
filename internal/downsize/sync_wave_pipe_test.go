package downsize

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// pipedFakeSync overlaps copy waves like the live backend: it reports a
// pipeline depth, lets Copy calls run concurrently and can cancel the run in
// the middle of a wave, as SIGTERM does.
type pipedFakeSync struct {
	*fakeSync
	active, peak, calls atomic.Int64
	cancelAt            int64
	cancel              context.CancelFunc
}

func (p *pipedFakeSync) copyPipelineDepth(string) int { return 4 }

func (p *pipedFakeSync) Copy(ctx context.Context, source, target string, names []string, journal string) (syncCopyResult, error) {
	n := p.active.Add(1)
	defer p.active.Add(-1)
	for peak := p.peak.Load(); n > peak && !p.peak.CompareAndSwap(peak, n); peak = p.peak.Load() {
	}
	// Hold the first waves until another one arrives (or 2 s pass), so the
	// test sees overlap however slowly the journal fsyncs on a loaded host.
	for deadline := time.Now().Add(2 * time.Second); p.peak.Load() < 2 && time.Now().Before(deadline); {
		time.Sleep(time.Millisecond)
	}
	if p.calls.Add(1) == p.cancelAt && p.cancel != nil {
		p.cancel()
	}
	if ctx.Err() != nil {
		return syncCopyResult{}, ctx.Err() // an aborted wave leaves no receipt
	}
	return p.fakeSync.Copy(ctx, source, target, names, journal)
}

func pipedSyncFixture(t *testing.T, names int) (*pipedFakeSync, syncOptions, []string) {
	t.Helper()
	f := newFakeSync()
	var missing []string
	for i := range names {
		n := fmt.Sprintf("m.%03d", i)
		f.put(syncD1, n, syncTestSchema)
		missing = append(missing, n)
	}
	f.put(syncD1, "a.clean", syncTestSchema)
	f.put(syncT, "a.clean", syncTestSchema)
	o := syncOptions{to: syncT, from: []string{syncD1}, state: t.TempDir(), streams: 2, batch: 2, maxRounds: 4,
		now: func() time.Time { return time.Unix(syncTestNow, 0) }}
	return &pipedFakeSync{fakeSync: f}, o, missing
}

func TestSyncCopyWavesOverlap(t *testing.T) {
	t.Parallel()
	p, o, missing := pipedSyncFixture(t, 60) // 15 waves of 4 names
	if err := runSync(context.Background(), o, p); err != nil {
		t.Fatal(err)
	}
	if p.peak.Load() < 2 {
		t.Fatalf("copy waves did not overlap: peak %d", p.peak.Load())
	}
	if p.peak.Load() > 4 {
		t.Fatalf("more waves in flight than the pipeline depth: %d", p.peak.Load())
	}
	dst := p.store(syncT)
	seen := map[string]int{}
	for _, c := range p.copies {
		_, list, _ := strings.Cut(c, " ")
		for _, n := range strings.Split(list, ",") {
			seen[n]++
		}
	}
	for _, n := range missing {
		if _, ok := dst.archives[n]; !ok {
			t.Fatalf("%s not copied", n)
		}
		if seen[n] != 1 {
			t.Fatalf("%s copied %d times", n, seen[n])
		}
	}
}

// SIGTERM in the middle of overlapping waves: the resumed run never repeats a
// wave that has a receipt and copies every name the first run did not finish.
func TestSyncCopyWavesResumeAfterCancel(t *testing.T) {
	t.Parallel()
	p, o, missing := pipedSyncFixture(t, 60)
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel, p.cancelAt = cancel, 6
	if err := runSync(ctx, o, p); err == nil {
		t.Fatal("interrupted run must fail")
	}
	first := map[string]bool{}
	for _, c := range p.copies {
		_, list, _ := strings.Cut(c, " ")
		for _, n := range strings.Split(list, ",") {
			first[n] = true
		}
	}
	if len(first) == 0 || len(first) >= len(missing) {
		t.Fatalf("the interruption should fall inside the copy phase, copied %d of %d", len(first), len(missing))
	}
	before := len(p.copies)
	p.cancel, p.cancelAt = nil, 0
	if err := runSync(context.Background(), o, p); err != nil {
		t.Fatal(err)
	}
	for _, c := range p.copies[before:] {
		_, list, _ := strings.Cut(c, " ")
		for _, n := range strings.Split(list, ",") {
			if first[n] {
				t.Fatalf("resume recopied %s (copy %q)", n, c)
			}
		}
	}
	dst := p.store(syncT)
	for _, n := range missing {
		if _, ok := dst.archives[n]; !ok {
			t.Fatalf("%s not copied after resume", n)
		}
	}
}
