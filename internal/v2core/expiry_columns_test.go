package v2core

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// Exercise the actual slot lifecycle, including adoption leaving a dead dense
// slot, sorting a fresh tail, and enabling expiry on an already populated owner.
func TestExpiryColumnsActivation(t *testing.T) {
	for _, enableAt := range []int{0, 1, 2} {
		t.Run(fmt.Sprint(enableAt), func(t *testing.T) {
			r := newCoreRollups()
			now := time.Unix(1800000000, 0)
			r.now = func() time.Time { return now }
			enable := func() {
				r.expire = func(RollupPolicy, RollupMetadata, time.Time) bool { return true }
				r.setExpiryTTL(expiryWindow(time.Minute))
			}
			if enableAt == 0 {
				enable()
			}
			for phase := 0; phase < 3; phase++ {
				if phase == 1 && enableAt == 1 {
					enable()
				}
				// Reverse arrival order forces rollupTail.Swap on every renumber.
				for i := 19; i >= 0; i-- {
					m := rankTestMetric(phase*20 + i)
					r.install(m.id, m, metricMetadata{LastUpdate: now.Add(-2*time.Minute + time.Duration(m.id)*time.Second).UnixNano()})
				}
				if phase > 0 {
					old := r.dense[0]
					replacement := *old
					r.install(old.id, &replacement, r.metaOf(old.id))
				}
				r.renumber()
				checkRankArray(t, r, phase)
				if enableAt == 2 || enableAt == 1 && phase == 0 {
					if r.ttl != nil || r.expAt != nil {
						t.Fatal("expiry-off owner retained columns")
					}
				} else {
					checkExpiryColumns(t, r)
				}
			}
			if enableAt == 2 {
				enable()
				checkExpiryColumns(t, r)
			}
			if !r.expiryDue(now) {
				t.Fatal("activation did not file existing idle names")
			}
		})
	}
}

func checkExpiryColumns(t *testing.T, r *coreRollups) {
	t.Helper()
	if len(r.ttl) != len(r.ids) || len(r.expAt) != len(r.ids) {
		t.Fatal("expiry columns do not cover slots")
	}
	for s, id := range r.ids {
		if id == deadSlot {
			continue
		}
		want := (r.meta[s].LastUpdate + int64(time.Minute)) / int64(time.Second)
		if r.ttl[s] != int64(time.Minute) || r.expAt[s] != want {
			t.Fatalf("slot %d: ttl=%d at=%d want at=%d", s, r.ttl[s], r.expAt[s], want)
		}
	}
}

// An expiry-off disk image must allocate and file columns when reopened with
// expiry on, while reopening with expiry off retains no per-name columns.
func TestExpiryColumnsRestart(t *testing.T) {
	for _, initiallyOn := range []bool{false, true} {
		t.Run(fmt.Sprint(initiallyOn), func(t *testing.T) {
			t.Setenv("CORE_MERGE_WORKERS", "0")
			root := t.TempDir()
			cfg := adoptConfig()
			now := time.Unix(1010, 0)
			open := func(on bool) *Engine {
				h := Hooks{Now: func() time.Time { return now }}
				if on {
					h.Expire = func(RollupPolicy, RollupMetadata, time.Time) bool { return true }
					h.ExpiryTTL = expiryWindow(time.Minute)
				}
				e, err := Open(root, cfg, h)
				if err != nil {
					t.Fatal(err)
				}
				return e
			}
			e := open(initiallyOn)
			policy := adoptPolicy(t, sealrollup.Average)
			adoptLive(t, e, policy, 1, 2, moveSample(1000, 2))
			if err := e.Close(); err != nil {
				t.Fatal(err)
			}
			now = now.Add(2 * time.Minute)
			e = open(false)
			r := e.owners[0].rollups
			if len(r.ids) == 0 || r.ttl != nil || r.expAt != nil {
				t.Fatal("expiry-off recovery allocated columns or lost names")
			}
			if err := e.Close(); err != nil {
				t.Fatal(err)
			}
			e = open(true)
			defer e.Close()
			r = e.owners[0].rollups
			checkExpiryColumns(t, r)
			if !r.expiryDue(now) {
				t.Fatal("recovery failed to file idle name")
			}
		})
	}
}
