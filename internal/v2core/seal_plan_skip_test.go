package v2core

import (
	"math"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

func TestSealPlanSkipRefile(t *testing.T) {
	_, raw := rollupTestPolicy(t, "skip.refile", "1s:1d,1m:30d,1h:1y,1d:10y", sealrollup.Average, 0)
	r := newCoreRollups()
	m, err := r.metric(1, raw)
	if err != nil {
		t.Fatal(err)
	}
	for _, version := range []uint8{1, 3} {
		for _, oldTime := range []int64{0, 3599, 3600, 3601, 3659, 3660} {
			for _, nextTime := range []int64{3601, 3659, 3660, 7200} {
				if nextTime < oldTime {
					continue
				}
				for _, closed := range []int64{0, 3600, 3660, 7200} {
					for _, now := range []int64{0, 3659, 3660, 7200} {
						for _, expired := range []bool{false, true} {
							old := compactMetadata(RollupMetadata{Version: version, Frontier: sealrollup.Frontier{Order: 1, MaxTime: oldTime}, ClosedThrough: closed, Expired: expired})
							next := old
							next.Version, next.Frontier.Order, next.Frontier.MaxTime = 3, 2, nextTime
							next.Expired, next.ClosedThrough = false, max(closed, nextTime)
							r.planNow = now
							var want uint16
							for i, a := range m.schema().Archives[1:] {
								end, prior := rollupOpenEnd(next, a), rollupOpenEnd(old, a)
								if end != math.MaxInt64 && (end != prior || prior <= now) {
									want |= 1 << min(i, 15)
								}
							}
							if got := r.refile(m, old, next); got != want {
								t.Fatalf("old=%+v next=%+v now=%d got=%b want=%b", old, next, now, got, want)
							}
						}
					}
				}
			}
		}
	}
}
