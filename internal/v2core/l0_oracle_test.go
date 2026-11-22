package v2core

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// The L0 publication oracle (ported from the B3-L0 model in sealrollup) runs
// publications through the real L0 writer and loader: a pendingHead takes the
// applications, every publication writes its suffix with writeL0 (encodeL0
// from the packed rows), a seal reduces the head and unlinks the files it
// covers, and a kill drops memory and restarts from openL0 and readL0 with
// LoadL0's frontier rule. Every cut compares each metric's archive image
// with the classic reducer fed exactly the surviving applications.

type l0OracleCorpus struct {
	name   string
	cfg    map[uint64]sealrollup.Config
	lookup map[uint64]*ApplicationPolicy
	pubs   [][][]coreBatch // publications, each a list of producer packets
}

var l0OracleConfigs = []sealrollup.Config{
	{Archives: []sealrollup.Archive{{Step: 1, Count: 600}, {Step: 60, Count: 120}}, Method: sealrollup.Average},
	{Archives: []sealrollup.Archive{{Step: 10, Count: 360}, {Step: 60, Count: 240}}, Method: sealrollup.Sum, XFilesFactor: 0.5},
	{Archives: []sealrollup.Archive{{Step: 1, Count: 300}}, Method: sealrollup.Last},
	{Archives: []sealrollup.Archive{{Step: 5, Count: 240}, {Step: 60, Count: 120}, {Step: 600, Count: 60}}, Method: sealrollup.Max, XFilesFactor: 0.3},
	{Archives: []sealrollup.Archive{{Step: 60, Count: 60}}, Method: sealrollup.Min},
	{Archives: []sealrollup.Archive{{Step: 1, Count: 120}, {Step: 10, Count: 120}}, Method: sealrollup.First},
}

// newL0OracleCorpus builds 30 publications of row applications (some with
// wide IDs, several points, explicit sequences, corrections of earlier
// slots, points beyond the finest retention, clock regressions and order
// deltas beyond 32 bits) mixed with policy-byte (cold) applications. Shape
// "tcp" reshapes each publication the way the TCP receiver admits it: one
// single-point application per point under one drain clock; "tcp-split"
// gives the second half the next clock, as when a publication spans drains.
func newL0OracleCorpus(seed uint64, shape string) l0OracleCorpus {
	rng := rand.New(rand.NewPCG(seed, 0x10))
	co := l0OracleCorpus{name: fmt.Sprintf("seed%d-%s", seed, shape), cfg: map[uint64]sealrollup.Config{}, lookup: map[uint64]*ApplicationPolicy{}}
	var rowIDs, coldIDs []uint64
	for i := range 16 {
		id := uint64(i*7 + 1)
		if i%7 == 6 {
			id = math.MaxUint32 + uint64(i) // row escapes
		}
		rowIDs = append(rowIDs, id)
		co.lookup[id] = &ApplicationPolicy{}
		co.cfg[id] = l0OracleConfigs[i%len(l0OracleConfigs)]
	}
	for i := range 3 {
		id := uint64(5000 + i) // no catalog entry: policy bytes make them cold
		coldIDs = append(coldIDs, id)
		co.cfg[id] = l0OracleConfigs[(i+2)%len(l0OracleConfigs)]
	}
	order, clock := uint64(rng.IntN(1000)+1), int64(1_700_000_000)
	last := map[uint64]int64{}
	for range 30 {
		clock++
		if rng.IntN(6) == 0 {
			clock -= int64(1 + rng.IntN(3)) // clock regression
		}
		var apps []coreBatch
		for range 1 + rng.IntN(12) {
			order++
			if rng.IntN(40) == 0 {
				order += math.MaxUint32
			}
			b := coreBatch{Order: order, Clock: clock}
			if rng.IntN(8) == 0 {
				b.ID = coldIDs[rng.IntN(len(coldIDs))]
				b.Policy = []byte{'p', byte(b.ID)}
			} else {
				b.ID = rowIDs[rng.IntN(len(rowIDs))]
				b.Registered = co.lookup[b.ID]
			}
			explicit := rng.IntN(4) == 0
			for j := range 1 + rng.IntN(6) {
				ts := clock - int64(rng.IntN(900))
				if prev, ok := last[b.ID]; ok && rng.IntN(4) == 0 {
					ts = prev // correction of an earlier slot
				}
				last[b.ID] = ts
				seq := order
				if explicit {
					seq = order*8 + uint64(j)
				}
				b.Points = append(b.Points, row{ID: b.ID, Time: ts, Bits: math.Float64bits(float64(rng.IntN(1000)) / 4), Seq: seq})
			}
			apps = append(apps, b)
		}
		if shape != "apps" {
			var flat []coreBatch
			for _, a := range apps {
				for _, p := range a.Points {
					flat = append(flat, coreBatch{ID: a.ID, Policy: a.Policy, Registered: a.Registered, Clock: clock, Points: []row{p}})
				}
			}
			for i := range flat {
				order++
				flat[i].Order, flat[i].Points[0].Seq = order, order
				if shape == "tcp-split" && i >= len(flat)/2 {
					flat[i].Clock = clock + 1
				}
			}
			apps = flat
		}
		// One to three producer packets per publication.
		var packets [][]coreBatch
		for len(apps) > 0 {
			n := 1 + rng.IntN(len(apps))
			packets, apps = append(packets, apps[:n]), apps[n:]
		}
		co.pubs = append(co.pubs, packets)
	}
	return co
}

func (co *l0OracleCorpus) find(id uint64) *ApplicationPolicy { return co.lookup[id] }

type l0OracleImage map[uint64]map[[2]int64]sealrollup.Sample

// reduce applies applications in order, per metric, like the seal.
func (co *l0OracleCorpus) reduce(img l0OracleImage, batches []coreBatch) error {
	byID := map[uint64][]sealrollup.Batch{}
	var ids []uint64
	for _, b := range batches {
		if _, ok := byID[b.ID]; !ok {
			ids = append(ids, b.ID)
		}
		sb := sealrollup.Batch{Order: b.Order, Now: b.Clock}
		for _, p := range b.Points {
			sb.Points = append(sb.Points, sealrollup.Sample{Time: p.Time, Bits: p.Bits, Sequence: p.Seq})
		}
		byID[b.ID] = append(byID[b.ID], sb)
	}
	for _, id := range ids {
		state := img[id]
		if state == nil {
			state = map[[2]int64]sealrollup.Sample{}
			img[id] = state
		}
		deltas, err := sealrollup.Reduce(co.cfg[id], func(a int, s int64) (sealrollup.Sample, bool) {
			v, ok := state[[2]int64{int64(a), s}]
			return v, ok
		}, byID[id])
		if err != nil {
			return fmt.Errorf("metric %d: %w", id, err)
		}
		for _, d := range deltas {
			state[[2]int64{int64(d.Archive), d.Slot}] = d.Sample
		}
	}
	return nil
}

func l0OracleLogical(img l0OracleImage) map[uint64]map[[2]int64][2]uint64 {
	out := map[uint64]map[[2]int64][2]uint64{}
	for id, state := range img {
		if len(state) == 0 {
			continue
		}
		m := map[[2]int64][2]uint64{}
		for k, s := range state {
			m[k] = [2]uint64{uint64(s.Time), s.Bits}
		}
		out[id] = m
	}
	return out
}

var errL0OracleKill = errors.New("killed at an L0 fence")

// run replays the corpus through the L0 writer and loader with one kill after
// publication k (k = -1: an empty store). Modes name the kill point inside
// publication k+1's write: "before" (no file), "temp" (temporary closed, not
// renamed), "renamed" (renamed, not yet visible); "stale" kills before
// publication k+1 right after a seal's commit, before its unlink; "frontier"
// is the mutation in which the restarted producer continues its order from
// the seal frontier instead of above the loaded rows. It returns the store's
// image and the classic image of the surviving applications.
func (co *l0OracleCorpus) run(t *testing.T, k int, mode string) (got, want l0OracleImage, err error) {
	dir := t.TempDir()
	open := func() (*coreOwner, []string) {
		o := &coreOwner{dir: dir}
		files, next, err := o.openL0()
		if err != nil {
			t.Fatal(err)
		}
		o.l0Next = next
		return o, files
	}
	o, _ := open()
	got, want = l0OracleImage{}, l0OracleImage{}
	var h pendingHead
	written := 0
	var head, deferred []string
	var frontier uint64
	restarted := false
	seal := func(p int) error {
		if err := co.reduce(got, h.rawView(co.find).Batches()); err != nil {
			return err
		}
		frontier = max(frontier, co.pubs[p][len(co.pubs[p])-1][len(co.pubs[p][len(co.pubs[p])-1])-1].Order)
		// "stale" defers each unlink to the next seal, so the kill leaves the
		// files of the last seal before it.
		unlink := head
		if mode == "stale" && !restarted {
			unlink, deferred = deferred, head
		}
		if err := unlinkL0(unlink); err != nil {
			t.Fatal(err)
		}
		h, written, head = pendingHead{}, 0, nil
		return nil
	}
	publish := func(p int, shift uint64) error {
		for _, packet := range co.pubs[p] {
			if shift != 0 {
				packet = append([]coreBatch(nil), packet...)
				for i := range packet {
					packet[i].Order -= shift
				}
			}
			h.add(packet, co.find)
		}
		path, err := o.writeL0(h.rawView(nil).Suffix(written))
		if err != nil {
			return err
		}
		head, written = append(head, path), h.n
		if p%3 == 2 {
			return seal(p)
		}
		return nil
	}
	surviving := func(p int) {
		for _, packet := range co.pubs[p] {
			if err := co.reduce(want, packet); err != nil {
				t.Fatal(err)
			}
		}
	}
	for p := 0; p <= k; p++ {
		if err := publish(p, 0); err != nil {
			t.Fatal(err)
		}
		surviving(p)
	}
	// Kill inside publication k+1's write at the mode's fence.
	if p := k + 1; p < len(co.pubs) {
		fence := map[string]string{"temp": "l0-written", "renamed": "l0-renamed"}[mode]
		if fence != "" {
			o.stage = func(s string) {
				if s == fence {
					panic(errL0OracleKill)
				}
			}
			func() {
				defer func() {
					if r := recover(); r != errL0OracleKill {
						panic(r)
					}
				}()
				_ = publish(p, 0)
			}()
			o.stage = nil
			if mode == "renamed" {
				surviving(p)
			}
		}
	}
	// kill -9: memory is gone; restart loads every file present.
	o, files := open()
	restarted = true
	loaded, err := readL0(files)
	if err != nil {
		t.Fatal(err)
	}
	var kept []coreBatch
	for _, b := range loaded {
		if b.Order > frontier { // LoadL0's keep: a durable seal covers the rest
			b.Registered = co.find(b.ID)
			kept = append(kept, b)
		}
	}
	h = pendingHead{}
	h.add(kept, co.find)
	head, written = files, h.n
	var shift uint64
	if mode == "frontier" && k+2 < len(co.pubs) {
		if first := co.pubs[k+2][0][0].Order; first > frontier+1 {
			shift = first - frontier - 1
		}
	}
	for p := k + 2; p < len(co.pubs); p++ {
		if err := publish(p, shift); err != nil {
			return got, want, err
		}
		surviving(p)
	}
	if err := co.reduce(got, h.rawView(co.find).Batches()); err != nil {
		return got, want, err
	}
	if err := unlinkL0(head); err != nil {
		t.Fatal(err)
	}
	if left, _ := filepath.Glob(filepath.Join(dir, "l0", "*")); len(left) != 0 {
		t.Fatalf("%s %s cut%d: %d L0 files left after the final seal", co.name, mode, k, len(left))
	}
	return got, want, nil
}

func TestL0PublicationOracle(t *testing.T) {
	seeds := uint64(2)
	if testing.Short() {
		seeds = 1
	}
	modes := []string{"before", "temp", "renamed", "stale", "frontier"}
	totals := map[string][2]int{}
	for seed := uint64(1); seed <= seeds; seed++ {
		for _, shape := range []string{"apps", "tcp", "tcp-split"} {
			co := newL0OracleCorpus(seed, shape)
			for k := -1; k < len(co.pubs)-1; k++ {
				for _, mode := range modes {
					got, want, err := co.run(t, k, mode)
					same := err == nil && reflect.DeepEqual(l0OracleLogical(got), l0OracleLogical(want))
					r := totals[mode]
					r[0]++
					if !same {
						r[1]++
						if mode != "frontier" {
							t.Fatalf("%s %s cut%d: err %v, store differs from classic", co.name, mode, k, err)
						}
					}
					totals[mode] = r
				}
			}
		}
	}
	if totals["frontier"][1] == 0 {
		t.Fatal("ordinal-frontier mutation escaped every cut")
	}
	for _, mode := range modes {
		r := totals[mode]
		if mode == "frontier" {
			t.Logf("RESULT %s mutation caught %d/%d cuts", mode, r[1], r[0])
		} else {
			t.Logf("RESULT %s exact %d/%d cuts", mode, r[0]-r[1], r[0])
		}
	}
}
