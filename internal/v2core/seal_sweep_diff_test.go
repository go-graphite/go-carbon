package v2core

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// sealPlanner is the seam between seal planner implementations: the same
// signature as the owner's plan hook (o.plan = r.planParts). The differential
// below drives two implementations over independent planner states with the
// same seals; a later planner (the seal sweep) plugs in as one side.
type sealPlanner func(r *coreRollups, in coreSealInput) (coreSealPlan, error)

// todaySealPlanner is the production planner.
func todaySealPlanner(r *coreRollups, in coreSealInput) (coreSealPlan, error) { return r.planParts(in) }

// sealSweepConfigs cover single- and multi-archive ladders, XFF 0 and 0.5, and
// a short finest retention so late points route to coarse archives or drop.
var sealSweepConfigs = []sealrollup.Config{
	{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 10, Count: 8640}}},
	{Method: sealrollup.Average, Archives: []sealrollup.Archive{{Step: 10, Count: 8640}, {Step: 60, Count: 10080}, {Step: 600, Count: 52560}}},
	{Method: sealrollup.Max, Archives: []sealrollup.Archive{{Step: 60, Count: 1440}, {Step: 3600, Count: 24 * 400}}},
	{Method: sealrollup.Average, XFilesFactor: 0.5, Archives: []sealrollup.Archive{{Step: 10, Count: 360}, {Step: 60, Count: 1440}}},
}

var sealSweepGroups = []v2part.Group{"test", "other", "test2"}

const (
	sealSweepSeals   = 30
	sealSweepEvery   = 300  // seconds between seals
	sealSweepExpiry  = 1800 // names idle longer than this expire
	sealSweepAdopt1  = 9
	sealSweepAbort   = 15
	sealSweepAdopt2  = 21
	sealSweepMoveOne = 12
	sealSweepMoveTwo = 25
)

// sealSweepBase is one hour before a month (and year) boundary, so seals and
// late points cover several month partitions.
var sealSweepBase = time.Date(2026, 12, 31, 23, 0, 0, 0, time.UTC).Unix()

type sealSweepName struct {
	name   v2part.ID
	group  v2part.Group
	policy []byte
	cfg    sealrollup.Config
	birth  int
	// class 0: active most seals; 1: stops at stop (idle closure, expiry);
	// 2: alternates long idle stretches with bursts (expiry, then revival).
	class, stop int
}

type sealSweepBatch struct {
	id         uint64
	order      uint64
	clock      int64
	points     []row
	registered bool
}

type sealSweepSeal struct {
	clock   int64
	batches []sealSweepBatch
}

// sealSweepCoverage counts the generated (script) and observed (plan) mix.
type sealSweepCoverage struct {
	RepeatNames, MultiPoint, DupSlots, LatePoints, Fresh, RegisteredFresh int
	Idle, Expired, Coarse, MultiPartition, MultiSegment, Groups           int
	Lingered, Renumbered, Adoptions, AbortedAdoptions, Moves              int
}

type sealSweepScript struct {
	names []sealSweepName
	seals []sealSweepSeal
	cov   sealSweepCoverage
}

// newSealSweepScript generates one randomized, planner-independent seal
// sequence: repeat names and multi-point batches, cross-batch duplicate
// slots (last write wins), same-time Seq ties, late points (older than the
// finest retention, across months), multi-archive names that close windows on
// later seals and when idle, expiry and revival, and staggered births so fresh
// names stay unranked across seals until a renumber.
func newSealSweepScript(seed uint64) sealSweepScript {
	rng := rand.New(rand.NewPCG(seed, 0x5eed))
	s := sealSweepScript{names: make([]sealSweepName, sealSweepNames)}
	for i := range s.names {
		n := &s.names[i]
		n.name = v2part.ID(fmt.Sprintf("%s.s%d", rankTestName(rng.IntN(1<<20)), i))
		n.cfg = sealSweepConfigs[rng.IntN(len(sealSweepConfigs))]
		n.group = sealSweepGroups[rng.IntN(len(sealSweepGroups))]
		raw, err := EncodeRollupPolicy(RollupPolicy{Name: n.name, Group: n.group, Config: n.cfg})
		if err != nil {
			panic(err)
		}
		n.policy = raw
		if rng.IntN(10) == 0 {
			n.birth = 1 + rng.IntN(sealSweepSeals-2)
		}
		switch c := rng.IntN(20); {
		case c < 12:
			n.class = 0
		case c < 17:
			n.class, n.stop = 1, n.birth+1+rng.IntN(sealSweepSeals/2)
		default:
			n.class = 2
		}
	}
	order := uint64(0)
	for seal := 0; seal < sealSweepSeals; seal++ {
		clock := sealSweepBase + int64(seal)*sealSweepEvery
		var batches []sealSweepBatch
		for i := range s.names {
			n := &s.names[i]
			if seal < n.birth {
				continue
			}
			switch n.class {
			case 0:
				if rng.IntN(3) == 0 {
					continue
				}
			case 1:
				if seal >= n.stop {
					continue
				}
			case 2:
				if (seal+i)%11 >= 3 {
					continue
				}
			}
			step := n.cfg.Archives[0].Step
			retention := step * n.cfg.Archives[0].Count
			k := 1
			if rng.IntN(3) == 0 {
				k += 1 + rng.IntN(2)
				s.cov.RepeatNames++
			}
			var seen []int64
			for range k {
				pts := make([]row, 1+rng.IntN(4))
				if len(pts) > 1 {
					s.cov.MultiPoint++
				}
				for j := range pts {
					offset := int64(rng.IntN(30)) * step
					switch rng.IntN(10) {
					case 0: // late: days back, older than a short finest retention, maybe last month
						offset += int64(1+rng.IntN(40)) * 86400
					case 1: // into an already closed coarse window
						offset += int64(1+rng.IntN(4)) * 600
					}
					t := clock - offset - int64(rng.IntN(int(step)))
					if len(seen) > 0 && rng.IntN(3) == 0 { // same slot as an earlier batch of this name
						t = seen[rng.IntN(len(seen))]
						s.cov.DupSlots++
					}
					if j > 0 && rng.IntN(4) == 0 { // equal time within a batch: Seq breaks the tie
						t = pts[j-1].Time
					}
					if clock-t >= retention {
						s.cov.LatePoints++
					}
					pts[j] = row{ID: uint64(i + 1), Time: t, Bits: rng.Uint64(), Seq: uint64(1 + rng.IntN(3))}
				}
				for _, p := range pts {
					seen = append(seen, p.Time)
				}
				batches = append(batches, sealSweepBatch{id: uint64(i + 1), clock: clock - int64(rng.IntN(5)), points: pts, registered: rng.IntN(2) == 0})
			}
		}
		// Interleave names, then assign strictly increasing apply orders.
		rng.Shuffle(len(batches), func(i, j int) { batches[i], batches[j] = batches[j], batches[i] })
		for i := range batches {
			order++
			batches[i].order = order
		}
		s.seals = append(s.seals, sealSweepSeal{clock: clock, batches: batches})
	}
	return s
}

// sealSweepWorld is one planner state driven by one implementation.
type sealSweepWorld struct {
	r     *coreRollups
	plan  sealPlanner
	clock int64
	lanes int
}

func newSealSweepWorld(plan sealPlanner, lanes int) *sealSweepWorld {
	w := &sealSweepWorld{plan: plan, lanes: lanes}
	w.r = w.planner()
	return w
}

func (w *sealSweepWorld) planner() *coreRollups {
	r := newCoreRollups()
	r.parallelism = w.lanes
	r.now = func() time.Time { return time.Unix(w.clock, 0) }
	r.expire = func(_ RollupPolicy, m RollupMetadata, now time.Time) bool {
		return m.LastUpdate > 0 && now.Unix()-m.LastUpdate/1e9 > sealSweepExpiry
	}
	r.setExpiryTTL(expiryWindow((sealSweepExpiry + 1) * time.Second))
	return r
}

// adopt hands every installed metric to a new planner, as a move adoption
// does: the objects are shared and keep the donor's rank (rankOwner), so the
// adopter treats them as unranked until its own renumber.
func (w *sealSweepWorld) adopt() *coreRollups {
	next := w.planner()
	next.identities = w.r.identities
	ids := make([]uint64, 0, len(w.r.metrics))
	for id := range w.r.metrics {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		m := w.r.metrics[id]
		next.install(id, m, w.r.metaOf(id))
	}
	return next
}

// move replaces every 7th installed metric with a struct copy in place, so
// the stale dense entry must drop at the next renumber.
func (w *sealSweepWorld) move(seal int) {
	for id, m := range w.r.metrics {
		if id%7 == uint64(seal%7) {
			c := *m
			w.r.install(id, &c, w.r.metaOf(id))
		}
	}
}

func (w *sealSweepWorld) input(s *sealSweepScript, seal int) (coreSealInput, int, int) {
	var in coreSealInput
	fresh, registeredFresh := 0, 0
	for _, b := range s.seals[seal].batches {
		n := &s.names[b.id-1]
		cb := coreBatch{ID: b.id, Order: b.order, Clock: b.clock, Policy: n.policy, Points: slices.Clone(b.points)}
		installed := w.r.metrics[b.id] != nil
		if !installed {
			fresh++
		}
		if b.registered {
			h, err := w.r.identities.intern(b.id, "", n.policy)
			if err != nil {
				panic(err)
			}
			cb.Registered, cb.Policy = h, nil
			if !installed {
				registeredFresh++
			}
		}
		in.Batches = append(in.Batches, cb)
	}
	return in, fresh, registeredFresh
}

// sealSweepTuple is one planned tuple with its record by value.
type sealSweepTuple struct {
	Name   v2part.ID
	VShard uint16
	Record *sealrollup.Record
	Value  string
}

// sealSweepOutput is the planner-independent view of one seal plan: tuples
// per part, specs (random part UUID masked), part bytes and descriptors.
type sealSweepOutput struct {
	err      string
	names    int
	scopes   []v2part.Scope
	segments int
	specs    []string
	order    [][]string
	records  [][]sealSweepTuple
	envs     [][]sealSweepTuple
	bytes    [][]byte
	descs    []string
}

// sealSweepBytes reports whether a seal also writes and compares part files.
// Every part write fsyncs, so by default only seal 0, every 5th and the last
// write files; tuples, specs and order are compared on every seal, and
// SEAL_SWEEP_ALL_BYTES=1 writes every seal.
func sealSweepBytes(seal int) bool {
	return seal%5 == 0 || seal == sealSweepSeals-1 || os.Getenv("SEAL_SWEEP_ALL_BYTES") != ""
}

func sealSweepCapture(plan coreSealPlan, err error, dir, side string, seal int) (sealSweepOutput, error) {
	var o sealSweepOutput
	if err != nil {
		o.err = err.Error()
		return o, nil
	}
	o.names = plan.Names
	for i, part := range plan.Parts {
		spec := part.Spec
		spec.Scope.Part = v2part.PartID{}
		spec.Policies = slices.Clone(spec.Policies)
		slices.SortFunc(spec.Policies, func(a, b v2part.ArchivePolicy) int {
			if c := bytes.Compare(a.Digest[:], b.Digest[:]); c != 0 {
				return c
			}
			return int(a.Archive) - int(b.Archive)
		})
		o.specs, o.scopes = append(o.specs, fmt.Sprintf("%+v", spec)), append(o.scopes, spec.Scope)
		nonEmpty := 0
		for _, seg := range part.Segments {
			if len(seg) > 0 {
				nonEmpty++
			}
		}
		if nonEmpty > 1 {
			o.segments++
		}
		var names []string
		var recs, envs []sealSweepTuple
		for _, kv := range slices.Concat(part.Segments...) {
			if len(names) == 0 || names[len(names)-1] != string(kv.name) {
				names = append(names, string(kv.name))
			}
			t := sealSweepTuple{Name: kv.name, VShard: kv.vshard}
			if kv.record != nil {
				r := *kv.record
				t.Record = &r
				recs = append(recs, t)
			} else {
				t.Value = string(kv.value)
				envs = append(envs, t)
			}
		}
		o.order, o.records, o.envs = append(o.order, names), append(o.records, recs), append(o.envs, envs)
		if !sealSweepBytes(seal) {
			continue
		}
		// Both sides write with one fixed part UUID and source, so equal
		// tuples must give byte-identical part files.
		part.Spec.Scope.Part = v2part.PartID{byte(seal), byte(i), 0x5e}
		part.Spec.Source.Incarnation, part.Spec.Source.Stream = v2part.PartID{1}, "sweep"
		path := filepath.Join(dir, fmt.Sprintf("%s-%d-%d.sst", side, seal, i))
		d, werr := writePlannedRollupTuples(path, part)
		if werr != nil {
			o.descs = append(o.descs, "write error: "+werr.Error())
			o.bytes = append(o.bytes, nil)
			continue
		}
		b, rerr := os.ReadFile(path)
		if rerr != nil {
			return o, rerr
		}
		_ = os.Remove(path)
		o.bytes, o.descs = append(o.bytes, b), append(o.descs, fmt.Sprintf("%+v", d))
	}
	return o, nil
}

// sealSweepPlannerState is the post-commit planner state compared between
// sides: every installed metric's metadata and the dense rank order.
type sealSweepPlannerState struct {
	IDs      []uint64
	Meta     []RollupMetadata
	Dense    []v2part.ID
	Unranked int
}

func sealSweepState(r *coreRollups) sealSweepPlannerState {
	var st sealSweepPlannerState
	for id := range r.metrics {
		st.IDs = append(st.IDs, id)
	}
	slices.Sort(st.IDs)
	for _, id := range st.IDs {
		st.Meta = append(st.Meta, r.metadataOf(id, r.metrics[id]))
	}
	for _, m := range r.dense {
		st.Dense = append(st.Dense, m.policy.Name)
	}
	st.Unranked = len(r.unranked)
	return st
}

// sealSweepChecks are the comparison surfaces, in report order.
var sealSweepChecks = []string{"error", "spec", "order", "records", "envelopes", "bytes", "state"}

func sealSweepCompare(a, b sealSweepOutput) []string {
	var bad []string
	if a.err != b.err {
		bad = append(bad, "error")
	}
	if a.names != b.names || !slices.Equal(a.specs, b.specs) {
		bad = append(bad, "spec")
	}
	if !reflect.DeepEqual(a.order, b.order) {
		bad = append(bad, "order")
	}
	if !reflect.DeepEqual(a.records, b.records) {
		bad = append(bad, "records")
	}
	if !reflect.DeepEqual(a.envs, b.envs) {
		bad = append(bad, "envelopes")
	}
	if !reflect.DeepEqual(a.bytes, b.bytes) || !slices.Equal(a.descs, b.descs) {
		bad = append(bad, "bytes")
	}
	return bad
}

// sealSweepDiff is the first seal at which two planners disagree.
type sealSweepDiff struct {
	Seal   int
	Checks []string
}

func (d *sealSweepDiff) String() string {
	return fmt.Sprintf("seal %d: %s", d.Seal, strings.Join(d.Checks, ","))
}

// sealSweepDigest folds part i's spec and tuples into h, so runs with
// different planner parallelism can be compared on every seal.
func sealSweepDigest(h io.Writer, o sealSweepOutput, i int) {
	var b []byte
	b = append(b, o.specs[i]...)
	for _, set := range [][]sealSweepTuple{o.records[i], o.envs[i]} {
		for _, t := range set {
			b = binary.AppendUvarint(b, uint64(len(t.Name)))
			b = append(b, t.Name...)
			b = binary.BigEndian.AppendUint16(b, t.VShard)
			if t.Record != nil {
				b = binary.AppendVarint(b, int64(t.Record.Archive))
				b = binary.AppendVarint(b, t.Record.Slot)
				b = sealrollup.AppendRecord(b, *t.Record)
			}
			b = append(b, t.Value...)
		}
	}
	_, _ = h.Write(b)
}

// runSealSweepDifferential drives planners a and b with the same script and
// returns the first disagreement (nil when every seal matches), the observed
// coverage, and a digest over every spec and tuple planner a planned.
func runSealSweepDifferential(t testing.TB, s sealSweepScript, lanes int, a, b sealPlanner) (*sealSweepDiff, sealSweepCoverage, string) {
	t.Helper()
	dir := t.TempDir()
	cov := s.cov
	wa, wb := newSealSweepWorld(a, lanes), newSealSweepWorld(b, lanes)
	digest := sha256.New()
	groups := map[v2part.Group]bool{}
	for seal := range s.seals {
		for _, w := range []*sealSweepWorld{wa, wb} {
			w.clock = s.seals[seal].clock
			switch seal {
			case sealSweepAdopt1, sealSweepAdopt2:
				w.r = w.adopt()
			case sealSweepMoveOne, sealSweepMoveTwo:
				w.move(seal)
			}
		}
		switch seal {
		case sealSweepAdopt1, sealSweepAdopt2:
			cov.Adoptions++
		case sealSweepMoveOne, sealSweepMoveTwo:
			cov.Moves++
		}
		if ra := wa.r; len(ra.dense) > 0 && len(ra.unranked) > 0 {
			if len(ra.unranked)*rankSlack < len(ra.dense) {
				cov.Lingered++
			} else {
				cov.Renumbered++
			}
		}
		if seal == sealSweepAbort {
			// A planner sharing every metric plans this seal and is
			// discarded, as a failed move would be. Its renumber takes rank
			// ownership of the shared metrics, so from here the donor plans
			// them through its exact unranked path (foreign rankOwner).
			var outs [2]sealSweepOutput
			for i, w := range []*sealSweepWorld{wa, wb} {
				next := w.adopt()
				in, _, _ := w.input(&s, seal)
				plan, err := w.plan(next, in)
				out, cerr := sealSweepCapture(plan, err, dir, fmt.Sprintf("abort%d", i), seal)
				if cerr != nil {
					t.Fatal(cerr)
				}
				if err == nil {
					plan.Abort()
				}
				outs[i] = out
			}
			if bad := sealSweepCompare(outs[0], outs[1]); len(bad) > 0 {
				return &sealSweepDiff{seal, bad}, cov, ""
			}
			cov.AbortedAdoptions++
		}
		var outs [2]sealSweepOutput
		var plans [2]coreSealPlan
		for i, w := range []*sealSweepWorld{wa, wb} {
			in, fresh, registeredFresh := w.input(&s, seal)
			if i == 0 {
				cov.Fresh += fresh
				cov.RegisteredFresh += registeredFresh
			}
			plan, err := w.plan(w.r, in)
			out, cerr := sealSweepCapture(plan, err, dir, fmt.Sprintf("side%d", i), seal)
			if cerr != nil {
				t.Fatal(cerr)
			}
			outs[i], plans[i] = out, plan
		}
		bad := sealSweepCompare(outs[0], outs[1])
		for i := range plans {
			if outs[i].err == "" {
				if len(bad) == 0 {
					plans[i].Commit()
				} else {
					plans[i].Abort()
				}
			}
		}
		if len(bad) == 0 && !reflect.DeepEqual(sealSweepState(wa.r), sealSweepState(wb.r)) {
			bad = append(bad, "state")
		}
		if len(bad) > 0 {
			return &sealSweepDiff{seal, bad}, cov, ""
		}
		if outs[0].err != "" {
			t.Fatalf("seal %d: both planners failed: %s", seal, outs[0].err)
		}
		// Observed coverage, from planner a's (equal) output.
		inSeal := map[v2part.ID]bool{}
		for _, b := range s.seals[seal].batches {
			inSeal[s.names[b.id-1].name] = true
		}
		partitions := map[int64]bool{}
		for i, scope := range outs[0].scopes {
			sealSweepDigest(digest, outs[0], i)
			partitions[scope.Partition], groups[scope.Group] = true, true
			for _, env := range outs[0].envs[i] {
				if !inSeal[env.Name] {
					cov.Idle++
				}
				m, err := DecodeRollupMetadata(env.Name, scope.Group, []byte(env.Value))
				if err != nil {
					t.Fatal(err)
				}
				if m.Expired {
					cov.Expired++
				}
			}
			for _, rec := range outs[0].records[i] {
				if rec.Record.Archive > 0 {
					cov.Coarse++
				}
			}
		}
		if len(partitions) > 1 {
			cov.MultiPartition++
		}
		cov.MultiSegment += outs[0].segments
	}
	cov.Groups = len(groups)
	return nil, cov, hex.EncodeToString(digest.Sum(nil))[:16]
}

// sealSweepSeeds returns the fixed seeds, or SEAL_SWEEP_SEEDS: a count N
// (seeds 1..N) or a comma-separated list, for longer runs.
func sealSweepSeeds(t *testing.T) []uint64 {
	v := os.Getenv("SEAL_SWEEP_SEEDS")
	if v == "" {
		return []uint64{1, 2, 3, 4}
	}
	if n, err := strconv.Atoi(v); err == nil && !strings.Contains(v, ",") {
		seeds := make([]uint64, n)
		for i := range seeds {
			seeds[i] = uint64(i + 1)
		}
		return seeds
	}
	var seeds []uint64
	for _, f := range strings.Split(v, ",") {
		n, err := strconv.ParseUint(strings.TrimSpace(f), 10, 64)
		if err != nil {
			t.Fatalf("SEAL_SWEEP_SEEDS: %v", err)
		}
		seeds = append(seeds, n)
	}
	return seeds
}

const sealSweepNames = 1000

// TestSealSweepPlanDifferential runs two seal planner implementations behind
// the sealPlanner seam on the same randomized seals and requires identical
// errors, part specs, rank (key) order, records, envelopes, part bytes and
// descriptors, and post-commit metadata and rank state. Side a is the legacy
// positions-map planner (test-only oracle), side b the production seal sweep.
// Part bytes must also be independent of planner parallelism (1 and 3 lanes).
func TestSealSweepPlanDifferential(t *testing.T) {
	t.Parallel()
	var total sealSweepCoverage
	for _, seed := range sealSweepSeeds(t) {
		s := newSealSweepScript(seed)
		var digests []string
		for _, lanes := range []int{1, 3} {
			diff, cov, digest := runSealSweepDifferential(t, s, lanes, legacySealPlanner, todaySealPlanner)
			if diff != nil {
				t.Fatalf("seed %d lanes %d: planners differ at %s", seed, lanes, diff)
			}
			digests = append(digests, digest)
			a, c := reflect.ValueOf(&total).Elem(), reflect.ValueOf(cov)
			for i := 0; i < a.NumField(); i++ {
				a.Field(i).SetInt(a.Field(i).Int() + c.Field(i).Int())
			}
			t.Logf("seed %d lanes %d: parts digest %s coverage %+v", seed, lanes, digest, cov)
		}
		if digests[0] != digests[1] {
			t.Errorf("seed %d: part bytes depend on planner lanes: %s vs %s", seed, digests[0], digests[1])
		}
	}
	v := reflect.ValueOf(total)
	for i := 0; i < v.NumField(); i++ {
		if v.Field(i).Int() == 0 {
			t.Errorf("coverage: %s = 0", v.Type().Field(i).Name)
		}
	}
}

// sealSweepUnstableRun models an unstable sort within one name's run: each
// name's batch payloads (points, clock) are reversed while the apply orders
// stay ascending by position, as a sweep deriving order from position would.
func sealSweepUnstableRun(r *coreRollups, in coreSealInput) (coreSealPlan, error) {
	batches := slices.Clone(in.Batches)
	runs := map[uint64][]int{}
	for i, b := range batches {
		runs[b.ID] = append(runs[b.ID], i)
	}
	for _, run := range runs {
		for i, j := 0, len(run)-1; i < j; i, j = i+1, j-1 {
			x, y := &batches[run[i]], &batches[run[j]]
			x.Points, y.Points = y.Points, x.Points
			x.Clock, y.Clock = y.Clock, x.Clock
		}
	}
	in.Batches = batches
	return r.planParts(in)
}

// sealSweepDropLastWrite drops a point when an earlier batch of the same name
// in this seal already wrote that timestamp: first write wins instead of last.
func sealSweepDropLastWrite(r *coreRollups, in coreSealInput) (coreSealPlan, error) {
	seen := map[[2]uint64]bool{}
	batches := make([]coreBatch, 0, len(in.Batches))
	for _, b := range in.Batches {
		kept := make([]row, 0, len(b.Points))
		for _, p := range b.Points {
			if !seen[[2]uint64{b.ID, uint64(p.Time)}] {
				kept = append(kept, p)
			}
		}
		for _, p := range b.Points {
			seen[[2]uint64{b.ID, uint64(p.Time)}] = true
		}
		if len(kept) > 0 {
			b.Points = kept
			batches = append(batches, b)
		}
	}
	in.Batches = batches
	return r.planParts(in)
}

// TestSealSweepPlanDifferentialCatchesMutants proves the harness bites: each
// deliberately broken planner behind the seam must be reported, by at least
// the listed check, against the production planner.
func TestSealSweepPlanDifferentialCatchesMutants(t *testing.T) {
	mutants := []struct {
		name string
		plan sealPlanner
		want string
	}{
		{"unstable run order", sealSweepUnstableRun, "records"},
		{"drop duplicate-slot last write", sealSweepDropLastWrite, "records"},
	}
	s := newSealSweepScript(1)
	for _, m := range mutants {
		t.Run(m.name, func(t *testing.T) {
			for _, lanes := range []int{1, 3} {
				diff, _, _ := runSealSweepDifferential(t, s, lanes, todaySealPlanner, m.plan)
				if diff == nil {
					t.Fatalf("lanes %d: mutant not detected", lanes)
				}
				if !slices.Contains(diff.Checks, m.want) {
					t.Fatalf("lanes %d: caught at %s, want check %q", lanes, diff, m.want)
				}
				t.Logf("lanes %d: caught at %s", lanes, diff)
			}
		})
	}
}
