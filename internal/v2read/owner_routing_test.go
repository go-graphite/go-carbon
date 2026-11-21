package v2read

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

type fetched struct {
	from, until, step int64
	bits              []uint64
}

func fetchAll(r *Reader, names []string, from, until, now int64) (map[string]fetched, error) {
	out := map[string]fetched{}
	err := r.FetchMany(context.Background(), names, from, until, now, func(n string, s *Series) error {
		f := fetched{}
		if s != nil {
			f = fetched{s.From, s.Until, s.Step, make([]uint64, len(s.Values))}
			for i, v := range s.Values {
				f.bits[i] = math.Float64bits(v)
			}
		}
		out[n] = f
		return nil
	})
	return out, err
}

// TestOwnerRoutingMatchesAllOwnerScan compares owner-routed lookups with the
// all-owner scan over random name sets: names only pending, only sealed, in
// both, never written, and spread over every owner.
func TestOwnerRoutingMatchesAllOwnerScan(t *testing.T) {
	const owners = 4
	r, err := New(owners, 256<<20, 16<<20)
	if err != nil {
		t.Fatal(err)
	}
	r.Identities = &v2core.PolicyCatalog{}
	cfg := testConfig()
	cfg.Shards, cfg.Owners = owners, owners
	e, err := v2core.Open(t.TempDir(), cfg, v2core.Hooks{Identities: r.Identities, PublishPending: r.PublishPending})
	if err != nil {
		r.Close()
		t.Fatal(err)
	}
	defer func() {
		r.Close()
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	}()
	const n = 120
	names := make([]string, n)
	policies := make([][]byte, n)
	perOwner := make([]int, owners)
	for i := range names {
		names[i] = fmt.Sprintf("route.metric.%03d.leaf", i)
		perOwner[OwnerFor(names[i], owners)]++
		policies[i], err = v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: v2part.ID(names[i]), Group: "test", Config: sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 1, Count: 1000}}}})
		if err != nil {
			t.Fatal(err)
		}
	}
	for o, c := range perOwner {
		if c == 0 {
			t.Fatalf("owner %d holds no test name", o)
		}
	}
	order, cursor := uint64(0), uint64(0)
	write := func(from, to int, seal bool, time int64) {
		var batches []v2core.Batch
		for i := from; i < to; i++ {
			id := uint64(i + 1)
			order++
			batches = append(batches, v2core.Batch{ID: id, Order: order, Clock: 2000, Policy: policies[i], Points: []v2core.Row{{ID: id, Time: time, Bits: math.Float64bits(float64(i) + float64(time)), Seq: order}}})
		}
		cursor++
		if err := e.AppendBatches(0, batches, cursor, seal); err != nil {
			t.Fatal(err)
		}
	}
	write(0, 80, true, 1990)    // sealed
	write(40, 100, false, 1995) // 40..80 in both, 80..100 pending only
	// Wait for every owner's publication to include its pending names.
	await(t, func() bool {
		got, err := fetchAll(r, names[:100], 1900, 2000, 2000)
		return err == nil && len(got) == 100
	})
	rng := rand.New(rand.NewSource(1547))
	for iter := 0; iter < 200; iter++ {
		k := 1 + rng.Intn(12)
		pick := make([]string, k)
		for i := range pick {
			pick[i] = names[rng.Intn(100)]
		}
		if iter%7 == 0 {
			pick = append(pick, names[100+rng.Intn(20)]) // never written
		}
		r.scanAllOwners = false
		a, aerr := fetchAll(r, pick, 1900, 2000, 2000)
		r.scanAllOwners = true
		b, berr := fetchAll(r, pick, 1900, 2000, 2000)
		r.scanAllOwners = false
		if !errors.Is(aerr, berr) && !(aerr == nil && berr == nil) {
			t.Fatalf("errors differ: routed=%v scan=%v for %v", aerr, berr, pick)
		}
		if len(a) != len(b) {
			t.Fatalf("result count differs %d %d", len(a), len(b))
		}
		for name, fa := range a {
			fb := b[name]
			if fa.from != fb.from || fa.until != fb.until || fa.step != fb.step || fmt.Sprint(fa.bits) != fmt.Sprint(fb.bits) {
				t.Fatalf("%s differs", name)
			}
		}
		m := pick[rng.Intn(len(pick))]
		ma, ea := r.Metadata(context.Background(), m)
		r.scanAllOwners = true
		mb, eb := r.Metadata(context.Background(), m)
		r.scanAllOwners = false
		if !errors.Is(ea, eb) && !(ea == nil && eb == nil) || fmt.Sprint(ma) != fmt.Sprint(mb) {
			t.Fatalf("metadata %s differs: %v %v", m, ea, eb)
		}
	}
}

func TestSplitByOwnerUsesIntakeRouting(t *testing.T) {
	for i := 0; i < 1000; i++ {
		name := fmt.Sprintf("x.%d.y", i)
		for _, owners := range []int{1, 4, 96} {
			if got, want := OwnerFor(name, owners), int(v2part.Route(v2part.ID(name)))%owners; got != want {
				t.Fatalf("%s owners=%d got %d want %d", name, owners, got, want)
			}
		}
	}
}
