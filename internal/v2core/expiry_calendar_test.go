package v2core

import (
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// expiryWindow is an ExpiryTTL giving every name window d, forever.
func expiryWindow(d time.Duration) func(RollupPolicy, time.Time) (time.Duration, time.Time) {
	return func(RollupPolicy, time.Time) (time.Duration, time.Time) { return d, time.Time{} }
}

// expiryTestRules is a carbon-like rule table: a time-limited rule, a short
// rule, an exclusion that never expires, and a store default.
func expiryTestRules(t0 int64) func(string, time.Time) (time.Duration, time.Time) {
	until := time.Unix(t0+4000, 0)
	return func(name string, now time.Time) (time.Duration, time.Time) {
		i, _ := strconv.Atoi(strings.TrimPrefix(name, "c."))
		u := time.Time{}
		if now.Before(until) {
			u = until
		}
		switch {
		case i%11 == 0:
			return 0, u
		case i%3 == 0 && now.Before(until):
			return 300 * time.Second, u
		case i%7 == 1:
			return 60 * time.Second, u
		}
		return 900 * time.Second, u
	}
}

// The expiry calendar stages exactly the names the full walk stages, and the
// maintenance check agrees with the walk, across writes, revivals, refused
// expiries (fence), a rule whose Until passes, and sub-second deadlines.
func TestExpiryCalendarMatchesWalk(t *testing.T) {
	const n = 400
	t0 := int64(1794700800)
	r := closureTestStore(t, n, t0)
	rules := expiryTestRules(t0)
	allowed := true
	// The walk's own question, which carbon's Expire asked before ExpiryTTL.
	walkExpire := func(p RollupPolicy, m RollupMetadata, now time.Time) bool {
		if !allowed || m.LastUpdate == 0 {
			return false
		}
		ttl, _ := rules(string(p.Name), now)
		return ttl > 0 && now.Sub(time.Unix(0, m.LastUpdate)) >= ttl
	}
	// As carbon's Expire: a gate only, so a name asked before its window
	// ends would expire early.
	r.expire = func(RollupPolicy, RollupMetadata, time.Time) bool { return allowed }
	for id, m := range r.metrics {
		slot := r.slotOf(id, m)
		r.meta[slot].LastUpdate = (t0-int64(id%700))*int64(time.Second) + int64(id)*7919%int64(time.Second)
	}
	clk := time.Unix(t0, 0)
	r.now = func() time.Time { return clk }
	r.setExpiryTTL(func(p RollupPolicy, now time.Time) (time.Duration, time.Time) { return rules(string(p.Name), now) })
	rng := rand.New(rand.NewSource(11))
	staged := 0
	for round := 0; round < 1500; round++ {
		clk = clk.Add(time.Duration(rng.Int63n(int64(40 * time.Second))))
		allowed = rng.Intn(4) != 0
		now := clk
		written := map[uint64]bool{}
		for k := rng.Intn(30); k > 0; k-- {
			written[uint64(1+rng.Intn(n))] = true
		}
		// The walk, as planParts and the maintenance pass ran it.
		want, walkDue := map[uint32]bool{}, false
		for id, m := range r.metrics {
			slot := r.slotOf(id, m)
			if meta := r.meta[slot]; !meta.Expired && (walkExpire(m.rollupPolicy(), m.metadata(meta), now)) {
				walkDue = true
				if !written[id] {
					want[slot] = true
				}
			}
		}
		if due := r.expiryDue(now); due != walkDue {
			t.Fatalf("round %d: maintenance %v, walk %v", round, due, walkDue)
		}
		r.planNow, r.planClock = now.Unix(), now
		r.reresolve(now)
		r.expDrained = r.expiries.due(r.planNow, 0, r.expDrained[:0])
		got := r.expirySlots(func(id uint64) bool { return written[id] }, nil)
		if len(got) != len(want) {
			t.Fatalf("round %d: calendar staged %d slots, walk %d", round, len(got), len(want))
		}
		for _, slot := range got {
			if !want[slot] {
				t.Fatalf("round %d: calendar staged slot %d the walk does not", round, slot)
			}
		}
		staged += len(got)
		// Commit, as planParts' Commit.
		type change struct {
			slot uint32
		}
		var changes []change
		commit := func(slot uint32, next metricMetadata) {
			changes = append(changes, change{slot})
			r.meta[slot] = next
		}
		for _, slot := range got {
			m := r.slotMetric(slot)
			next := r.meta[slot]
			if walkExpire(m.rollupPolicy(), m.metadata(next), now) {
				next.Expired = true
			}
			commit(slot, next)
		}
		for id := range written {
			slot := r.slotOf(id, r.metrics[id])
			next := r.meta[slot]
			next.Expired = false
			next.LastUpdate = max(next.LastUpdate, now.UnixNano())
			next.Frontier.MaxTime = max(next.Frontier.MaxTime, now.Unix()-int64(rng.Intn(120)))
			commit(slot, next)
		}
		r.commitExpiries()
		for _, c := range changes {
			r.fileExpiry(c.slot)
		}
	}
	if staged == 0 {
		t.Fatal("nothing staged")
	}
}

// BenchmarkExpirySelect is one expiry selection (a seal's or the 1 s
// maintenance pass's) with store expiration on and no name due: the walk
// through a regex rule callback, against the calendar; and the per-name
// filing a commit adds.
func BenchmarkExpirySelect(b *testing.B) {
	for _, names := range []int{200000, 1000000} {
		t0 := int64(1794700800)
		r := closureTestStore(b, names, t0+600)
		rule := regexp.MustCompile(`^c\.[0-9]*7$`)
		ttl := func(name string, _ time.Time) time.Duration {
			if rule.MatchString(name) {
				return 7 * 24 * time.Hour
			}
			return 30 * 24 * time.Hour
		}
		r.expire = func(p RollupPolicy, m RollupMetadata, now time.Time) bool {
			return m.LastUpdate != 0 && now.Sub(time.Unix(0, m.LastUpdate)) >= ttl(string(p.Name), now)
		}
		for i := range r.meta {
			r.meta[i].LastUpdate = t0 * int64(time.Second)
		}
		now := time.Unix(t0+300, 0) // before every open window's end
		b.Run("walk/"+strconv.Itoa(names), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				for id, m := range r.metrics {
					if meta := r.meta[r.slotOf(id, m)]; !meta.Expired && (r.expire(m.rollupPolicy(), m.metadata(meta), now)) {
						b.Fatal("due")
					}
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(names), "ns/name")
		})
		r.setExpiryTTL(func(p RollupPolicy, now time.Time) (time.Duration, time.Time) {
			return ttl(string(p.Name), now), time.Time{}
		})
		b.Run("calendar/"+strconv.Itoa(names), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				if r.expiryDue(now) {
					b.Fatal("due")
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(names), "ns/name")
		})
		b.Run("file/"+strconv.Itoa(names), func(b *testing.B) {
			b.ReportAllocs()
			for i := range b.N {
				slot := uint32(i % names)
				r.meta[slot].LastUpdate += int64(time.Second)
				r.fileExpiry(slot)
			}
		})
	}
}

// BenchmarkExpirySelectSpread is an idle pass with idle names' last writes
// spread over 30 d (a 30 d window: distinct deadline seconds ≈ names), and
// filing a name under a second nothing else holds.
func BenchmarkExpirySelectSpread(b *testing.B) {
	for _, names := range []int{200000, 1000000} {
		t0 := int64(1794700800)
		r := closureTestStore(b, names, t0+600)
		rng := rand.New(rand.NewSource(1))
		r.expire = func(RollupPolicy, RollupMetadata, time.Time) bool { return true }
		for i := range r.meta {
			r.meta[i].LastUpdate = (t0 - rng.Int63n(30*86400-1000)) * int64(time.Second)
		}
		clk := time.Unix(t0, 0)
		r.now = func() time.Time { return clk }
		r.setExpiryTTL(expiryWindow(30 * 24 * time.Hour))
		now := time.Unix(t0+300, 0)
		b.Run("calendar/"+strconv.Itoa(names), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				if r.expiryDue(now) {
					b.Fatal("due")
				}
			}
			b.ReportMetric(float64(len(r.expiries.slots)), "keys")
		})
		b.Run("file-new/"+strconv.Itoa(names), func(b *testing.B) {
			b.ReportAllocs()
			for i := range b.N {
				slot := uint32(i % names)
				r.expAt[slot], r.meta[slot].LastUpdate = 0, (t0+int64(i))*int64(time.Second)
				r.fileExpiry(slot)
			}
		})
	}
}
