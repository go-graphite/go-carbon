package downsize

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"testing"
	"time"
)

type boundedProofFixture struct {
	*proofFixture
	incarnation string
	legacy      bool
}

func (f *boundedProofFixture) WindowProofs(_ context.Context, host string, names []string, from, until int64) ([]string, error) {
	if f.legacy {
		return nil, errSyncNoProof
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(names))
	for i, n := range names {
		if f.unknown[n] {
			continue
		}
		hash := sha256.New()
		fmt.Fprintln(hash, f.incarnation, host, n, from, until, f.store(host).archives[n])
		var steps []int64
		for step := range f.store(host).points[n] {
			steps = append(steps, step)
		}
		sort.Slice(steps, func(i, j int) bool { return steps[i] < steps[j] })
		for _, step := range steps {
			var times []int64
			for ts := range f.store(host).points[n][step] {
				if ts >= from && ts < until {
					times = append(times, ts)
				}
			}
			sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
			for _, ts := range times {
				fmt.Fprintln(hash, step, ts, math.Float64bits(f.store(host).points[n][step][ts]))
			}
		}
		out[i] = hex.EncodeToString(hash.Sum(nil))
	}
	return out, nil
}
func TestSyncBoundedProofRetainsCleanNamesAfterRestartAndLaterWrite(t *testing.T) {
	for _, kind := range []string{"clean_restart", "later_write", "historical_write", "unknown_name", "broken_lineage", "old_endpoint"} {
		t.Run(kind, func(t *testing.T) {
			base, o := frontierFixture(t)
			f := &boundedProofFixture{proofFixture: base, incarnation: "durable-store"}
			if e := runSync(context.Background(), o, f); !errors.Is(e, ErrSyncResidual) {
				t.Fatal(e)
			}
			base.seen = map[string]bool{}
			switch kind {
			case "clean_restart":
				base.epoch = "new-process"
			case "later_write":
				base.store(syncT).points["metric.02"][3600][syncTestNow+3600] = 42
			case "historical_write":
				base.store(syncT).points["metric.02"][3600][syncTestNow-5*3600] = 42
			case "unknown_name":
				base.unknown["metric.02"] = true
			case "broken_lineage":
				f.incarnation = "rebuilt-store"
			case "old_endpoint":
				f.legacy = true
			}
			if e := runSync(context.Background(), o, f); !errors.Is(e, ErrSyncResidual) {
				t.Fatal(e)
			}
			want := []string{"metric.19"}
			if kind == "historical_write" || kind == "unknown_name" {
				want = []string{"metric.02", "metric.19"}
			}
			if kind == "broken_lineage" || kind == "old_endpoint" {
				want = nil
				for i := range 20 {
					want = append(want, fmt.Sprintf("metric.%02d", i))
				}
			}
			if got := seenProofNames(base); !reflect.DeepEqual(got, want) {
				t.Fatalf("hashed %v want %v", got, want)
			}
		})
	}
}
func TestSyncBoundedProofRestartDuringVerification(t *testing.T) {
	base, o := frontierFixture(t)
	f := &boundedProofFixture{proofFixture: base, incarnation: "stable"}
	base.mutate = func(string, int64) { base.mu.Lock(); f.incarnation = "unknown-successor"; base.mu.Unlock() }
	if e := runSync(context.Background(), o, f); !errors.Is(e, ErrSyncResidual) {
		t.Fatal(e)
	}
	// A changed lineage between observations must leave every affected receipt
	// unclean. The next run still has to inspect the first verification batch.
	base.mutate = nil
	base.seen = map[string]bool{}
	if e := runSync(context.Background(), o, f); !errors.Is(e, ErrSyncResidual) {
		t.Fatal(e)
	}
	if !base.seen["metric.00"] {
		t.Fatal("restart during verification certified the interrupted batch")
	}
}

func TestSyncBoundedProofHistoryOnlyForSampledNames(t *testing.T) {
	base, _ := frontierFixture(t)
	f := &boundedProofFixture{proofFixture: base, incarnation: "durable"}
	s := &syncer{b: f, o: syncOptions{windows: syncRanges{{Name: "window", From: syncTestNow - 6*3600, Until: syncTestNow}}, fullHistory: true, sampleEvery: 2, now: func() time.Time { return time.Unix(syncTestNow, 0) }}}
	names := []string{"metric.00", "metric.01", "metric.02", "metric.03"}
	before, e := s.windowProofs(context.Background(), syncT, names)
	if e != nil {
		t.Fatal(e)
	}
	for _, n := range names {
		base.store(syncT).points[n][3600][syncTestNow-7*3600] = 42
	}
	after, e := s.windowProofs(context.Background(), syncT, names)
	if e != nil {
		t.Fatal(e)
	}
	for i, n := range names {
		if changed := before[i] != after[i]; changed != s.sampled(n) {
			t.Fatalf("%s changed=%v sampled=%v", n, changed, s.sampled(n))
		}
	}
}
