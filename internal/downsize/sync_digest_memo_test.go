package downsize

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// mixedDigestHarness serves sum-v1 from current stores and only the legacy
// serving-archive SHA from the stores in old, and counts backend requests.
type mixedDigestHarness struct {
	*redoHarness
	old      map[string]bool
	reqMu    sync.Mutex
	requests map[string]int
}

func (f *mixedDigestHarness) request(op, store string) {
	f.reqMu.Lock()
	defer f.reqMu.Unlock()
	f.requests[op+" "+store]++
}

func (f *mixedDigestHarness) Digest(ctx context.Context, store string, names []string, from, until int64) (map[string][]syncDigest, error) {
	f.request("digest", store)
	return f.redoHarness.Digest(ctx, store, names, from, until)
}

func (f *mixedDigestHarness) SumDigest(ctx context.Context, store string, names []string, step, from, until int64) (map[string][]syncDigest, error) {
	got, err := (&sumSyncFixture{fakeSync: f.fakeSync, old: f.old}).SumDigest(ctx, store, names, step, from, until)
	if err == nil {
		f.request("sum", store)
		f.count("sum", store, names, from, until)
	}
	return got, err
}

// Stale names (no point in the last day) take the absence probe, which digests
// every archive's serving window until it finds a point. Full-history
// verification then needs the same legacy digest from the target (because one
// holder is legacy) and from the legacy holder. Those reads must happen once.
func TestSyncVerifyReusesAbsenceProbeDigests(t *testing.T) {
	t.Parallel()
	f, o := newRedoHarness(t)
	o.from = []string{syncD1, syncD2, syncP}
	o.batch, o.streams = 500, 4
	schema := []syncArchive{{60, 2880}, {600, 1008}, {3600, 720}}
	const total = 1000
	var stale []string
	for i := range total {
		n := fmt.Sprintf("metric.%04d", i)
		for _, store := range []string{syncT, syncD1, syncD2, syncP} {
			f.put(store, n, schema)
		}
		if i%2 == 0 {
			stale = append(stale, n)
			for _, store := range []string{syncT, syncD1, syncD2, syncP} {
				for _, pts := range f.store(store).points[n] {
					for ts := range pts {
						if ts >= syncTestNow-3*86400 {
							delete(pts, ts)
						}
					}
				}
			}
		}
		if i%5 == 0 {
			// A directional hole on the target that only the legacy holder fills.
			delete(f.store(syncT).points[n][3600], (syncTestNow-7*86400)/3600*3600)
		}
	}
	delete(f.store(syncT).archives, "metric.05")
	h := &mixedDigestHarness{redoHarness: f, old: map[string]bool{syncD1: true}, requests: map[string]int{}}
	if err := runSync(context.Background(), o, h); err != nil {
		t.Fatal(err)
	}
	p := redoProgress(t, o.state)
	if n := p.RedoNames["verify"]; n != 0 {
		t.Errorf("verify redo_names=%d, want 0", n)
	}
	names := map[string]int{}
	for k, v := range f.snapshot() {
		names[k.Op+" "+k.Store] += v
		var i int
		if _, err := fmt.Sscanf(k.Name, "metric.%04d", &i); err == nil && i%5 == 0 {
			continue // filled names are re-verified in round 2 by design
		}
		if v > 1 && k.Op == "digest" {
			t.Errorf("digest repeated: %+v x%d", k, v)
		}
	}
	// Every hole the legacy holder proves is still filled.
	for i := 0; i < total; i += 5 {
		n := fmt.Sprintf("metric.%04d", i)
		if n == "metric.0005" {
			continue
		}
		for ts, v := range f.store(syncD1).points[n][3600] {
			if got, ok := f.store(syncT).points[n][3600][ts]; ok && got != v {
				t.Fatalf("%s@%d target value replaced", n, ts)
			}
		}
		if len(f.store(syncT).points[n][3600]) < len(f.store(syncD1).points[n][3600]) {
			t.Fatalf("%s: hole not filled", n)
		}
	}
	per1k := func(v int) float64 { return float64(v) * 1000 / total }
	t.Logf("per 1k names (%d stale): target digest requests %.1f, target sum requests %.1f, target digest name-windows %.0f, legacy holder digest name-windows %.0f, verify redo_names %d",
		len(stale), per1k(h.requests["digest "+syncT]), per1k(h.requests["sum "+syncT]), per1k(names["digest "+syncT]), per1k(names["digest "+syncD1]), p.RedoNames["verify"])
}
