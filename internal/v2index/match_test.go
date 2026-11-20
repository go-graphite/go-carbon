package v2index

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestCatalogMatchDifferential(t *testing.T) {
	rng := rand.New(rand.NewPCG(16, 1201))
	var catalog Catalog
	live := make(map[string]bool)
	for round := uint64(1); round < 12; round++ {
		var add, remove []string
		for i := 0; i < 150; i++ {
			name := fmt.Sprintf("svc%d.host%d.metric%d", rng.IntN(11), rng.IntN(27), rng.IntN(19))
			add = append(add, name)
			live[name] = true
		}
		for name := range live {
			if rng.IntN(5) == 0 {
				remove = append(remove, name)
				delete(live, name)
			}
		}
		if err := catalog.Apply(round, add, remove); err != nil {
			t.Fatal(err)
		}
		paths := make(map[string]bool)
		for name := range live {
			parts := strings.Split(name, ".")
			for i := range parts {
				path := "/" + strings.Join(parts[:i+1], "/")
				if i == len(parts)-1 {
					path += ".wsp"
				}
				paths[path] = true
			}
		}
		for _, glob := range []string{"*", "svc1/*", "svc?/host[12]*", "svc[1-3]/host?/*", "svc*/host*/metric*.wsp", "svc1/host1/metric1.wsp", "missing/*", "svc1/host1*"} {
			var want, got []string
			for path := range paths {
				if yes, _ := filepath.Match("/"+glob, path); yes {
					want = append(want, path)
				}
			}
			slices.Sort(want)
			err := catalog.Snapshot().MatchPaths(t.Context(), glob, func(path string) error { got = append(got, path); return nil })
			if err != nil {
				t.Fatal(err)
			}
			slices.Sort(got)
			if !slices.Equal(got, want) {
				t.Fatalf("round %d glob %s: got %v want %v", round, glob, got, want)
			}
		}
	}
}
func TestCatalogMatchCancellation(t *testing.T) {
	var c Catalog
	if err := c.Publish(1, []string{"a.metric", "b.metric", "c.metric"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	err := c.Snapshot().MatchPaths(ctx, "*", func(string) error { calls++; cancel(); return nil })
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("err=%v visits=%d", err, calls)
	}
}
