package v2part

import (
	"bytes"
	"cmp"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// TestDensePlannedRankOrder checks the planned writer's rank order check: ranked
// tuples write the same bytes as unranked (name-compared) tuples, and a rank
// out of order, a wrong vshard or a duplicate slot under ranks is detected.
func TestDensePlannedRankOrder(t *testing.T) {
	spec := fixtureSpec()
	spec.Scope.VShard = GroupVShard
	spec.Encoding = DenseEncoding
	type metric struct {
		name   ID
		vshard uint16
		rank   uint32
	}
	// Several names per vshard, including length ties and shared prefixes.
	byShard := map[uint16][]ID{}
	for n := 0; n < 20000; n++ {
		name := ID(fmt.Sprintf("svc.%d.m%d", n%97, n))
		if v := Route(name); v < 8 {
			byShard[v] = append(byShard[v], name)
		}
	}
	var metrics []metric
	for v := uint16(0); v < 8; v++ {
		for _, name := range byShard[v] {
			metrics = append(metrics, metric{name: name, vshard: v})
		}
	}
	slices.SortFunc(metrics, func(a, b metric) int {
		return cmp.Or(cmp.Compare(a.vshard, b.vshard), cmp.Compare(len(a.name), len(b.name)), cmp.Compare(a.name, b.name))
	})
	for i := range metrics {
		metrics[i].rank = uint32(i + 1)
	}
	write := func(path string, ms []metric, ranked bool, slots int) ([]byte, error) {
		w, err := NewDenseWriter(path, spec)
		if err != nil {
			t.Fatal(err)
		}
		defer w.Abort()
		for _, m := range ms {
			rank := m.rank
			if !ranked {
				rank = 0
			}
			for s := 0; s < slots; s++ {
				r := sealrollup.Record{Delta: sealrollup.Delta{Archive: 0, Slot: int64(s), Order: 8, Sample: sealrollup.Sample{Time: 1700000000 + int64(s), Bits: math.Float64bits(float64(s))}}}
				if err = w.SetPlannedRecord(m.vshard, rank, m.name, &r); err != nil {
					return nil, err
				}
			}
			if err = w.SetPlannedMetadata(m.vshard, rank, m.name, []byte("envelope")); err != nil {
				return nil, err
			}
		}
		if _, err = w.Close(); err != nil {
			return nil, err
		}
		return os.ReadFile(path)
	}
	dir := t.TempDir()
	want, err := write(filepath.Join(dir, "names.sst"), metrics, false, 3)
	if err != nil {
		t.Fatal(err)
	}
	got, err := write(filepath.Join(dir, "ranks.sst"), metrics, true, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(want) == 0 || !bytes.Equal(got, want) {
		t.Fatalf("ranked part bytes differ from name-ordered part (%d vs %d)", len(got), len(want))
	}
	// A same-vshard neighbour pair with ranks swapped, keeping names in order.
	const i = 1
	if metrics[2].vshard != metrics[0].vshard {
		t.Fatal("fixture needs three names in one vshard")
	}
	mutations := map[string]func([]metric) ([]metric, int){
		"swapped ranks": func(ms []metric) ([]metric, int) { ms[i].rank, ms[i+1].rank = ms[i+1].rank, ms[i].rank; return ms, 3 },
		"equal ranks":   func(ms []metric) ([]metric, int) { ms[i+1].rank = ms[i].rank; return ms, 3 },
		"wrong vshard":  func(ms []metric) ([]metric, int) { ms[i+1].vshard ^= 1; return ms, 3 },
		"duplicate":     func(ms []metric) ([]metric, int) { ms[i+1] = ms[i]; return ms, 3 },
	}
	for name, mutate := range mutations {
		ms, slots := mutate(slices.Clone(metrics))
		b, err := write(filepath.Join(dir, "mutated.sst"), ms, true, slots)
		if err == nil && bytes.Equal(b, want) {
			t.Errorf("mutation %q not detected", name)
		}
	}
}
