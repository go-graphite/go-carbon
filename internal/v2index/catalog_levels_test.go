package v2index

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

func randomName(rng *rand.Rand) string {
	parts := make([]string, 1+rng.Intn(4))
	for i := range parts {
		parts[i] = string(rune('a'+rng.Intn(4))) + fmt.Sprint(rng.Intn(3))
	}
	return strings.Join(parts, ".")
}

func randomGlob(rng *rand.Rand) string {
	parts := make([]string, 1+rng.Intn(4))
	for i := range parts {
		switch rng.Intn(5) {
		case 0:
			parts[i] = "*"
		case 1:
			parts[i] = string(rune('a'+rng.Intn(4))) + "*"
		case 2:
			parts[i] = "?" + fmt.Sprint(rng.Intn(3))
		case 3:
			parts[i] = "[ab]" + fmt.Sprint(rng.Intn(3))
		default:
			parts[i] = string(rune('a'+rng.Intn(4))) + fmt.Sprint(rng.Intn(3))
		}
	}
	glob := strings.Join(parts, "/")
	if rng.Intn(2) == 0 {
		glob += ".wsp"
	}
	return glob
}

func matches(t *testing.T, s *Snapshot, glob string) (match, visit []string) {
	t.Helper()
	ctx := context.Background()
	if err := s.MatchPaths(ctx, glob, func(p string) error { match = append(match, p); return nil }); err != nil {
		t.Fatal(err)
	}
	if err := s.VisitPaths(ctx, glob, func(p string) error { visit = append(visit, p); return nil }); err != nil {
		t.Fatal(err)
	}
	return match, visit
}

func checkLevels(t *testing.T, s *Snapshot) {
	t.Helper()
	total := 0
	for i, level := range s.levels {
		if len(level) == 0 || !slices.IsSorted(level) || len(slices.Compact(slices.Clone(level))) != len(level) {
			t.Fatalf("level %d is empty, unsorted or duplicated", i)
		}
		if i > 0 && len(level)*levelFanout >= len(s.levels[i-1]) {
			t.Fatalf("level %d (%d names) not compacted into %d", i, len(level), len(s.levels[i-1]))
		}
		total += len(level)
	}
	all := slices.Collect(s.Names())
	if total != s.Len() || len(all) != s.Len() || !slices.IsSorted(all) || len(slices.Compact(slices.Clone(all))) != len(all) {
		t.Fatalf("levels overlap: total=%d len=%d names=%d", total, s.Len(), len(all))
	}
}

// Incremental publications, including expiry and recreation, must yield the
// same names and the same find candidates as a full rebuild of the same set.
func TestCatalogLevelsMatchFullRebuild(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		rng := rand.New(rand.NewSource(seed))
		var c Catalog
		live := map[string]bool{}
		for revision := uint64(1); revision <= 150; revision++ {
			var add, remove []string
			for range rng.Intn(1 + rng.Intn(40)) {
				add = append(add, randomName(rng))
			}
			for name := range live {
				if rng.Intn(25) == 0 {
					remove = append(remove, name)
				}
			}
			if rng.Intn(10) == 0 && len(add) > 0 {
				remove = append(remove, add[0]) // removal wins within a batch
			}
			before := c.Snapshot()
			called := false
			if err := c.ApplyBeforePublish(revision, add, remove, func(next *Snapshot) error { called = true; checkLevels(t, next); return nil }); err != nil {
				t.Fatal(err)
			}
			want := maps(live, add, remove)
			changed := !slices.Equal(slices.Sorted(keys(live)), want)
			if called != changed || (c.Snapshot().Revision() == before.Revision()) == changed {
				t.Fatalf("seed %d revision %d: changed=%v called=%v", seed, revision, changed, called)
			}
			live = map[string]bool{}
			for _, name := range want {
				live[name] = true
			}
			s := c.Snapshot()
			checkLevels(t, s)
			if !s.EqualNames(want) || s.Len() != len(want) {
				t.Fatalf("seed %d revision %d: names differ", seed, revision)
			}
			var full Catalog
			if err := full.Publish(1, want); err != nil {
				t.Fatal(err)
			}
			for range 8 {
				glob := randomGlob(rng)
				gm, gv := matches(t, s, glob)
				wm, wv := matches(t, full.Snapshot(), glob)
				if !slices.Equal(gm, wm) || !slices.Equal(gv, wv) {
					t.Fatalf("seed %d revision %d glob %q: %v/%v want %v/%v", seed, revision, glob, gm, gv, wm, wv)
				}
			}
		}
	}
}

func keys(m map[string]bool) func(func(string) bool) {
	return func(yield func(string) bool) {
		for k := range m {
			if !yield(k) {
				return
			}
		}
	}
}

func maps(live map[string]bool, add, remove []string) []string {
	next := map[string]bool{}
	for k := range live {
		next[k] = true
	}
	for _, k := range add {
		next[k] = true
	}
	for _, k := range remove {
		delete(next, k)
	}
	return slices.Sorted(keys(next))
}

// Readers pinning any snapshot see exactly the set of one revision.
func TestCatalogLevelsConcurrentSnapshots(t *testing.T) {
	var c Catalog
	name := func(i int) string { return fmt.Sprintf("service.%05d.cpu", (i*7919)%100000) }
	expected := func(revision uint64) []string {
		var out []string
		for i := 1; i <= int(revision)*10; i++ {
			if i%3 != 0 || i > int(revision)*10-30 {
				out = append(out, name(i))
			}
		}
		slices.Sort(out)
		return out
	}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				s := c.Snapshot()
				if s.Revision() == 0 {
					continue
				}
				if want := expected(s.Revision()); !s.EqualNames(want) || s.Len() != len(want) {
					t.Errorf("revision %d is not a consistent snapshot", s.Revision())
					return
				}
				var n int
				if err := s.VisitPaths(context.Background(), "service/*", func(string) error { n++; return nil }); err != nil || n != s.Len() {
					t.Errorf("revision %d: find saw %d of %d", s.Revision(), n, s.Len())
					return
				}
			}
		}()
	}
	for revision := uint64(1); revision <= 300; revision++ {
		var add, remove []string
		for i := int(revision-1)*10 + 1; i <= int(revision)*10; i++ {
			add = append(add, name(i))
		}
		// Names become removable 30 positions after their creation.
		for i := int(revision-1)*10 - 29; i <= int(revision)*10-30; i++ {
			if i > 0 && i%3 == 0 {
				remove = append(remove, name(i))
			}
		}
		if err := c.Apply(revision, add, remove); err != nil {
			t.Fatal(err)
		}
		if revision%50 == 0 {
			time.Sleep(time.Millisecond)
		}
	}
	close(stop)
	wg.Wait()
	checkLevels(t, c.Snapshot())
}

// Publication cost depends on the new names, not the total catalog size:
// ns/op and B/op should be comparable at 1M and 10M names.
func BenchmarkCatalogApplyNewNames(b *testing.B) {
	for _, total := range []int{1_000_000, 10_000_000} {
		b.Run(fmt.Sprintf("names=%d", total), func(b *testing.B) {
			names := make([]string, total)
			for i := range names {
				names[i] = fmt.Sprintf("metric.%09d.value", i*2)
			}
			var c Catalog
			if err := c.Publish(1, names); err != nil {
				b.Fatal(err)
			}
			rng := rand.New(rand.NewSource(1))
			add := make([]string, 100)
			revision := uint64(1)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				for i := range add {
					add[i] = fmt.Sprintf("metric.%09d.value", 2*rng.Intn(total)+1)
				}
				revision++
				if err := c.Apply(revision, add, nil); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(len(c.Snapshot().levels)), "levels")
		})
	}
}

func TestCatalogBackgroundMergeNeverDelaysPublication(t *testing.T) {
	oldMax, oldHook := inlineMergeMax, bgMergeHook
	defer func() { inlineMergeMax, bgMergeHook = oldMax, oldHook }()
	inlineMergeMax = 100
	release := make(chan struct{})
	bgMergeHook = func() { <-release }
	var c Catalog
	var want []string
	batch := func(prefix string, n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = fmt.Sprintf("%s.%05d", prefix, i)
		}
		want = append(want, out...)
		return out
	}
	rev := uint64(0)
	apply := func(add []string) {
		rev++
		done := make(chan error, 1)
		go func() { done <- c.Apply(rev, add, nil) }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("publication blocked by background merge")
		}
	}
	apply(batch("a", 500))
	apply(batch("b", 400)) // 900 > inline bound and not 4x smaller: deferred
	for i := range 20 {
		apply(batch(fmt.Sprintf("c%02d", i), 3))
	}
	slices.Sort(want)
	if s := c.Snapshot(); !s.EqualNames(want) || s.Len() != len(want) {
		t.Fatal("snapshot lost names while merge stalled")
	}
	if c.bg == nil {
		t.Fatal("expected a background merge")
	}
	close(release)
	<-c.bg.done
	apply(batch("d", 1))
	slices.Sort(want)
	s := c.Snapshot()
	if !s.EqualNames(want) || s.Len() != len(want) {
		t.Fatal("snapshot wrong after installing merge")
	}
	if s.Levels() > 3 {
		t.Fatalf("merge not installed: %d levels", s.Levels())
	}
	// A level rewritten by a removal invalidates a pending merge.
	bgMergeHook = func() { <-release }
	release = make(chan struct{})
	apply(batch("e", 600))
	if c.bg == nil {
		t.Fatal("expected another background merge")
	}
	rev++
	if err := c.Apply(rev, nil, []string{"e.00000"}); err != nil {
		t.Fatal(err)
	}
	want = slices.DeleteFunc(want, func(n string) bool { return n == "e.00000" })
	close(release)
	<-c.bg.done
	apply(batch("f", 1))
	slices.Sort(want)
	if s := c.Snapshot(); !s.EqualNames(want) {
		t.Fatal("stale merge installed or names lost")
	}
}
