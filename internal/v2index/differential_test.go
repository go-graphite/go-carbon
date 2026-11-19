package v2index

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"testing"
)

// The front-coded index must answer exactly like the plain-string index it
// replaced (oldindex_test.go): names, EqualNames, MatchPaths and VisitPaths,
// after random adds, removes and recreations, at every block boundary shape.
func TestFrontCodedMatchesOldIndex(t *testing.T) {
	ctx := context.Background()
	collect := func(f func(context.Context, string, func(string) error) error, glob string) []string {
		var out []string
		if err := f(ctx, glob, func(p string) error { out = append(out, p); return nil }); err != nil {
			t.Fatal(err)
		}
		return out
	}
	for seed := int64(1); seed <= 24; seed++ {
		rng := rand.New(rand.NewSource(seed))
		// Even seeds keep the base in a file and reopen it after each install.
		file := seed%2 == 0
		name := func() string {
			// Long shared prefixes and short tails exercise front coding.
			parts := make([]string, 1+rng.Intn(5))
			for i := range parts {
				parts[i] = strings.Repeat(string(rune('a'+rng.Intn(3))), 1+rng.Intn(3)) + fmt.Sprint(rng.Intn(4))
			}
			return strings.Join(parts, ".")
		}
		var nc Catalog
		var oc oldCatalog
		if file {
			if err := nc.SetDir(t.TempDir()); err != nil {
				t.Fatal(err)
			}
		}
		t.Cleanup(nc.Close)
		for revision := uint64(1); revision <= 120; revision++ {
			var add, remove []string
			for range rng.Intn(1 + rng.Intn(200)) {
				add = append(add, name())
			}
			live := slices.Collect(oc.Snapshot().Names())
			for _, n := range live {
				if rng.Intn(20) == 0 {
					remove = append(remove, n)
				}
			}
			for range rng.Intn(3) {
				remove = append(remove, name()) // often not live
			}
			if len(add) > 0 && rng.Intn(4) == 0 {
				remove = append(remove, add[rng.Intn(len(add))]) // removal wins within a batch
			}
			var nerr, oerr error
			if revision%37 == 1 {
				nerr, oerr = nc.Publish(revision, add), oc.Publish(revision, add)
			} else {
				nerr, oerr = nc.Apply(revision, add, remove), oc.Apply(revision, add, remove)
			}
			if (nerr == nil) != (oerr == nil) {
				t.Fatalf("seed %d rev %d: errors differ: %v vs %v", seed, revision, nerr, oerr)
			}
			settled := rng.Intn(3) == 0
			if settled {
				for nc.settle(); nc.job != nil; nc.settle() {
				}
			}
			ns, os := nc.Snapshot(), oc.Snapshot()
			if file && len(ns.levels) > 0 && ns.levels[0].path != "" {
				reopened, err := openRun(ns.levels[0].path)
				if err != nil {
					t.Fatalf("seed %d rev %d: reopen: %v", seed, revision, err)
				}
				base := &level{run: reopened, dead: ns.levels[0].dead}
				if !slices.Equal(levelNames(base), levelNames(ns.levels[0])) {
					t.Fatalf("seed %d rev %d: reopened base differs", seed, revision)
				}
			} else if file && settled && len(ns.levels) > 0 && nc.Err() == nil {
				t.Fatalf("seed %d rev %d: base is not file-backed", seed, revision)
			}
			want := slices.Collect(os.Names())
			if ns.Len() != os.Len() || ns.Revision() != os.Revision() || !slices.Equal(slices.Collect(ns.Names()), want) || !ns.EqualNames(want) {
				t.Fatalf("seed %d rev %d: names differ", seed, revision)
			}
			for li, l := range ns.levels {
				names := levelNames(&level{run: l.run}) // positions include removed names
				for range min(4, len(names)) {
					if i := rng.Intn(len(names)); string(l.at(i, nil)) != names[i] {
						t.Fatalf("seed %d rev %d: level %d at(%d) differs", seed, revision, li, i)
					}
				}
			}
			for range 10 {
				glob := randomGlob(rng)
				if rng.Intn(3) == 0 && len(want) > 0 {
					glob = strings.ReplaceAll(want[rng.Intn(len(want))], ".", "/") // exact find
				}
				if a, b := collect(ns.MatchPaths, glob), collect(os.MatchPaths, glob); !slices.Equal(a, b) {
					t.Fatalf("seed %d rev %d glob %q: match %v, old %v", seed, revision, glob, a, b)
				}
				if a, b := collect(ns.VisitPaths, glob), collect(os.VisitPaths, glob); !slices.Equal(a, b) {
					t.Fatalf("seed %d rev %d glob %q: visit %v, old %v", seed, revision, glob, a, b)
				}
			}
		}
	}
}
