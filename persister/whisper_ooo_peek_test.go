package persister

import (
	"path/filepath"
	"testing"
	"time"

	whisper "github.com/go-graphite/go-whisper"

	"github.com/go-graphite/go-carbon/helper"
)

// newSidecarHandle stores one diverted point and opens the file with its sidecar.
func newSidecarHandle(tb testing.TB) (*Whisper, *whisper.Whisper, string) {
	tb.Helper()
	dir := tb.TempDir()
	cache := &fakeCache{}
	p := newOOOTestPersister(tb, dir, cache)
	p.outOfOrder.ticker.Stop()
	p.outOfOrder.ticker = &helper.ThrottleTicker{C: make(chan bool, 1)}
	const metric = "test.peek"
	path := filepath.Join(dir, "test", "peek.wsp")
	base := time.Now().Unix() - 3600
	cache.add(metric, base, 1)
	cache.add(metric, base+2, 2)
	p.store(metric)
	cache.add(metric, base+1, 3)
	p.store(metric)
	w, err := p.openWhisper(path, true)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { w.Close() })
	if w.OutOfOrderPath() == "" {
		tb.Fatal("no sidecar")
	}
	return p, w, path
}

// The stat skip only peeks at the rate budget: tokens are consumed exactly
// when a size-eligible sidecar is checked.
func TestOutOfOrderCompactionPeekKeepsTokenUse(t *testing.T) {
	p, w, path := newSidecarHandle(t)
	const metric = "test.peek"

	p.outOfOrder.threshold = 1
	if got := p.compactOutOfOrder(w, metric, path); got != w || p.oooCompactions != 0 {
		t.Fatalf("merged without a token: compactions=%d", p.oooCompactions)
	}

	p.outOfOrder.threshold = 1 << 40
	p.outOfOrder.ticker.C <- true
	if got := p.compactOutOfOrder(w, metric, path); got != w || p.oooCompactions != 0 {
		t.Fatal("merged below the threshold")
	}
	if len(p.outOfOrder.ticker.C) != 1 {
		t.Fatal("token consumed below the threshold")
	}

	p.outOfOrder.threshold = 1
	w = p.compactOutOfOrder(w, metric, path)
	if p.oooCompactions != 1 || p.oooCompactErrors != 0 || len(p.outOfOrder.ticker.C) != 0 {
		t.Fatalf("compactions=%d errors=%d tokens=%d", p.oooCompactions, p.oooCompactErrors, len(p.outOfOrder.ticker.C))
	}
	if w.OutOfOrderPath() != "" {
		t.Fatal("sidecar remains after merge")
	}

	// A zero-rate ticker is closed and never merges.
	p2, w2, path2 := newSidecarHandle(t)
	p2.outOfOrder.ticker = helper.NewHardThrottleTicker(0)
	p2.outOfOrder.threshold = 1
	if got := p2.compactOutOfOrder(w2, metric, path2); got != w2 || p2.oooCompactions != 0 {
		t.Fatal("zero-rate ticker merged")
	}
}

// Writes to a file with a sidecar while the compaction budget is spent.
func BenchmarkCompactOutOfOrderWithoutBudget(b *testing.B) {
	p, w, path := newSidecarHandle(b)
	p.outOfOrder.threshold = 1
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if p.compactOutOfOrder(w, "test.peek", path) != w {
			b.Fatal("handle replaced")
		}
	}
	if p.oooCompactions != 0 {
		b.Fatal("merged without budget")
	}
}
