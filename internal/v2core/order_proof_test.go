package v2core

// Offline check: do read and merge winners equal
// "newest part sequence wins, one record per slot within a part"?
//
// ORDER_PROOF=<file> turns collection on. Planner corpora feed every committed
// plan (one plan = one seal = one part sequence number); engine corpora feed
// every newly published part of the live inventory. Each collector compares,
// per (name, archive, physical slot) and after every part:
//
//	current : visible = Order > ResetOrder; winner = max (Time, Order)  (newerOrigin, recordVisible)
//	time+seq: visible = seq > ResetSeq;     winner = max (Time, part seq, order of a colliding name)
//	pure seq: visible = seq > ResetSeq;     winner = max part seq      (ReplacingMergeTree)
//
// and appends one JSON line of counts and examples per collector.

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
	"github.com/go-graphite/go-carbon/persister"
)

type proofKey struct {
	name    string
	archive int
	slot    int64
}

type proofEntry struct {
	seq int
	rec sealrollup.Record
}

type proofName struct {
	resetOrder uint64
	resetSeq   int
	expired    bool
	metaSeen   bool
	keys       []proofKey
}

type orderProofStats struct {
	Label                   string
	Parts, Records, Meta    int
	Keys, MultiKeys         int
	TimeTieKeys             int // keys holding >1 record with one Time
	InPartDuplicates        int // one part holding two records for one slot
	ExceptionNames          int // (part, name) pairs with an in-part duplicate
	ExceptionRecords        int // records of those pairs: they keep explicit orders
	Evaluations             int
	DisagreeTimeSeq         int // current winner (Time,Bits) != time+seq winner
	DisagreePureSeq         int // current winner != pure-seq winner
	DisagreeVisible         int // reset mask by order != reset mask by part seq
	TieConflicts            int // equal (Time, Order) across parts with different bits
	OrderInversionsSameTime int // newer part, same Time, lower Order than an older part's record
	InversionsCoarse        int // ... of which coarse archives
	OlderTimeInNewerPart    int // newer part carries an older epoch for the slot
	PartRangeOverlaps       int // part MinOrder <= the previous part's MaxOrder
	MinOrderSortInversions  int // part pairs whose MinOrder order disagrees with creation order
	MergesSeen, PartsGone   int
	Examples                []string
}

type orderProof struct {
	stats    orderProofStats
	seq      int
	entries  map[proofKey][]proofEntry
	names    map[string]*proofName
	touched  map[proofKey]bool
	reset    map[string]bool
	partMin  []uint64
	partMax  []uint64
	min, max uint64
	seen     map[v2part.Digest]int // engine collectors: live part -> seq
	// Records per name in the current part, and the names with an
	// intra-part collision: only those keep explicit orders (exceptions).
	partNames map[string]int
	partColl  map[string]bool
}

var (
	orderProofMu  sync.Mutex
	orderProofs   []*orderProof
	orderProofKey = map[any]*orderProof{}
)

// ORDER_PROOF_STRICT=1 also collects, and fails the package on any winner or
// reset-mask disagreement (the gate for keeping orders only for names with
// an intra-part collision).
func orderProofEnabled() bool {
	return os.Getenv("ORDER_PROOF") != "" || os.Getenv("ORDER_PROOF_STRICT") != ""
}

// orderProofFor returns the collector for key, labelled with the calling test.
func orderProofFor(key any, label string) *orderProof {
	orderProofMu.Lock()
	defer orderProofMu.Unlock()
	if p := orderProofKey[key]; p != nil {
		return p
	}
	if label == "" {
		label = callerTestName()
	}
	p := &orderProof{entries: map[proofKey][]proofEntry{}, names: map[string]*proofName{}, seen: map[v2part.Digest]int{}}
	p.stats.Label = fmt.Sprintf("%s#%d", label, len(orderProofs))
	orderProofKey[key] = p
	orderProofs = append(orderProofs, p)
	return p
}

func callerTestName() string {
	pcs := make([]uintptr, 64)
	n := runtime.Callers(2, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	for {
		f, more := frames.Next()
		if i := strings.Index(f.Function, ".Test"); i >= 0 {
			name := f.Function[i+1:]
			if j := strings.Index(name, "."); j >= 0 {
				name = name[:j]
			}
			return name
		}
		if !more {
			return "unknown"
		}
	}
}

// orderProofFlush appends every collector's line to $ORDER_PROOF.
func orderProofFlush() (failures int) {
	if !orderProofEnabled() {
		return 0
	}
	orderProofMu.Lock()
	defer orderProofMu.Unlock()
	f := os.Stderr
	if path := os.Getenv("ORDER_PROOF"); path != "" {
		var err error
		if f, err = os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644); err != nil {
			panic(err)
		}
		defer f.Close()
	}
	for _, p := range orderProofs {
		p.stats.Keys = len(p.entries)
		p.stats.MultiKeys, p.stats.TimeTieKeys = 0, 0
		for _, es := range p.entries {
			if len(es) < 2 {
				continue
			}
			p.stats.MultiKeys++
			times := map[int64]bool{}
			for _, e := range es {
				if times[e.rec.Sample.Time] {
					p.stats.TimeTieKeys++
					break
				}
				times[e.rec.Sample.Time] = true
			}
		}
		p.stats.MinOrderSortInversions = sortInversions(p.partMin)
		b, _ := json.Marshal(p.stats)
		f.Write(append(b, '\n'))
		failures += p.stats.DisagreeTimeSeq + p.stats.DisagreeVisible
	}
	orderProofs, orderProofKey = nil, map[any]*orderProof{}
	return failures
}

func TestMain(m *testing.M) {
	code := m.Run()
	if n := orderProofFlush(); n > 0 && os.Getenv("ORDER_PROOF_STRICT") != "" && code == 0 {
		fmt.Fprintf(os.Stderr, "order proof: %d disagreements with part sequence\n", n)
		code = 1
	}
	os.Exit(code)
}

// sortInversions counts pairs i<j (creation order) with min[i] > min[j].
func sortInversions(v []uint64) int {
	n := 0
	for i := range v {
		for j := i + 1; j < len(v) && j < i+64; j++ {
			if v[i] > v[j] {
				n++
			}
		}
	}
	return n
}

func (p *orderProof) example(format string, args ...any) {
	if len(p.stats.Examples) < 12 {
		p.stats.Examples = append(p.stats.Examples, fmt.Sprintf(format, args...))
	}
}

func (p *orderProof) begin() {
	p.seq++
	p.stats.Parts++
	p.touched, p.reset = map[proofKey]bool{}, map[string]bool{}
	p.partNames, p.partColl = map[string]int{}, map[string]bool{}
	p.min, p.max = math.MaxUint64, 0
}

func (p *orderProof) name(n string) *proofName {
	s := p.names[n]
	if s == nil {
		s = &proofName{}
		p.names[n] = s
	}
	return s
}

func (p *orderProof) record(name string, r sealrollup.Record) {
	p.stats.Records++
	p.partNames[name]++
	p.min, p.max = min(p.min, r.Order), max(p.max, r.Order)
	k := proofKey{name, r.Archive, r.Slot}
	es := p.entries[k]
	if len(es) == 0 {
		s := p.name(name)
		s.keys = append(s.keys, k)
	}
	if n := len(es); n > 0 {
		last := es[n-1]
		if last.seq == p.seq {
			p.stats.InPartDuplicates++
			p.partColl[name] = true
			p.example("in-part duplicate %q a%d s%d seq%d", name, r.Archive, r.Slot, p.seq)
		}
		for _, e := range es {
			if e.rec.Sample.Time == r.Sample.Time && e.rec.Order > r.Order {
				p.stats.OrderInversionsSameTime++
				if r.Archive > 0 {
					p.stats.InversionsCoarse++
				}
				p.example("order inversion %q a%d s%d t%d: seq%d order%d bits%x then seq%d order%d bits%x corr%v", name, r.Archive, r.Slot, r.Sample.Time, e.seq, e.rec.Order, e.rec.Sample.Bits, p.seq, r.Order, r.Sample.Bits, r.Correction)
				break
			}
		}
		if last.rec.Sample.Time > r.Sample.Time {
			p.stats.OlderTimeInNewerPart++
			p.example("older epoch in newer part %q a%d s%d: seq%d t%d then seq%d t%d", name, r.Archive, r.Slot, last.seq, last.rec.Sample.Time, p.seq, r.Sample.Time)
		}
	}
	p.entries[k] = append(es, proofEntry{p.seq, r})
	p.touched[k] = true
}

func (p *orderProof) meta(name string, m RollupMetadata) {
	p.stats.Meta++
	p.min, p.max = min(p.min, m.Frontier.Order), max(p.max, m.Frontier.Order)
	s := p.name(name)
	first := !s.metaSeen
	s.metaSeen = true
	if m.ResetOrder != s.resetOrder {
		if m.ResetOrder < s.resetOrder {
			p.example("reset order went back %q %d -> %d", name, s.resetOrder, m.ResetOrder)
		}
		// ResetSeq is the expiry seal. A collector that starts after it (a
		// planner opened mid-history) places the reset before its first part.
		s.resetOrder, s.resetSeq = m.ResetOrder, p.seq
		if !m.Expired && first {
			s.resetSeq = p.seq - 1
		}
		p.reset[name] = true
	}
	if m.Expired != s.expired {
		s.expired = m.Expired
		p.reset[name] = true
	}
}

func (p *orderProof) end() {
	if n := len(p.partMax); n > 0 && p.min != math.MaxUint64 && p.min <= p.partMax[n-1] {
		p.stats.PartRangeOverlaps++
	}
	p.partMin, p.partMax = append(p.partMin, p.min), append(p.partMax, p.max)
	for name := range p.partColl {
		p.stats.ExceptionNames++
		p.stats.ExceptionRecords += p.partNames[name]
	}
	for name := range p.reset {
		for _, k := range p.names[name].keys {
			p.touched[k] = true
		}
	}
	for k := range p.touched {
		p.evaluate(k)
	}
}

func (p *orderProof) evaluate(k proofKey) {
	s := p.names[k.name]
	if s == nil || s.expired {
		return
	}
	p.stats.Evaluations++
	es := p.entries[k]
	var cur, seq, pure *proofEntry
	for i := range es {
		e := &es[i]
		visOrder, visSeq := e.rec.Order > s.resetOrder, e.seq > s.resetSeq
		if visOrder != visSeq {
			p.stats.DisagreeVisible++
			p.example("visibility %q a%d s%d seq%d order%d resetOrder%d resetSeq%d", k.name, k.archive, k.slot, e.seq, e.rec.Order, s.resetOrder, s.resetSeq)
		}
		if visOrder {
			if cur == nil || e.rec.Sample.Time > cur.rec.Sample.Time || e.rec.Sample.Time == cur.rec.Sample.Time && e.rec.Order > cur.rec.Order {
				cur = e
			} else if e.rec.Sample.Time == cur.rec.Sample.Time && e.rec.Order == cur.rec.Order && e.rec.Sample.Bits != cur.rec.Sample.Bits {
				p.stats.TieConflicts++
			}
		}
		if visSeq {
			// Within one part only a colliding name's records compete, and
			// they keep their explicit order (the exception column).
			if seq == nil || e.rec.Sample.Time > seq.rec.Sample.Time || e.rec.Sample.Time == seq.rec.Sample.Time && (e.seq > seq.seq || e.seq == seq.seq && e.rec.Order > seq.rec.Order) {
				seq = e
			}
			if pure == nil || e.seq > pure.seq {
				pure = e
			}
		}
	}
	same := func(a, b *proofEntry) bool {
		if a == nil || b == nil {
			return a == b
		}
		return a.rec.Sample.Time == b.rec.Sample.Time && a.rec.Sample.Bits == b.rec.Sample.Bits
	}
	if !same(cur, seq) {
		p.stats.DisagreeTimeSeq++
		p.example("time+seq differs %q a%d s%d: current %s, seq rule %s", k.name, k.archive, k.slot, entryString(cur), entryString(seq))
	}
	if !same(cur, pure) {
		p.stats.DisagreePureSeq++
		p.example("pure seq differs %q a%d s%d: current %s, pure %s", k.name, k.archive, k.slot, entryString(cur), entryString(pure))
	}
}

func entryString(e *proofEntry) string {
	if e == nil {
		return "none"
	}
	return fmt.Sprintf("seq%d order%d t%d bits%x corr%v", e.seq, e.rec.Order, e.rec.Sample.Time, e.rec.Sample.Bits, e.rec.Correction)
}

// orderProofPlanned wraps a planner plan so its parts enter the planner's
// collector when (and only if) the plan commits.
func orderProofPlanned(r *coreRollups, out *coreSealPlan) {
	if !orderProofEnabled() {
		return
	}
	p := orderProofFor(r, "")
	parts, commit := out.Parts, out.Commit
	out.Commit = func() {
		p.begin()
		for _, part := range parts {
			for _, segment := range part.Segments {
				for _, kv := range segment {
					if kv.record != nil {
						p.record(string(kv.name), *kv.record)
						continue
					}
					m, _, err := decodeRollupMetadataHeader(kv.value)
					if err != nil {
						panic(err)
					}
					p.meta(string(kv.name), m)
				}
			}
		}
		p.end()
		commit()
	}
}

// orderProofObserve feeds every part published since the previous call, in
// file modification order, to the collector named label. Parts that left the
// inventory are merge inputs or retired parts and are only counted.
func orderProofObserve(t testing.TB, label string, e *Engine) {
	if !orderProofEnabled() {
		return
	}
	t.Helper()
	p := orderProofFor(label, label)
	if err := orderProofObserveDirs(p, e.ownerDirs()); err != nil {
		t.Fatal(err)
	}
}

func (e *coreEngine) ownerDirs() []proofOwner {
	var out []proofOwner
	for _, o := range e.owners {
		out = append(out, proofOwner{dir: o.dir, snapshot: func() (*pebble.Snapshot, error) { return o.db.NewSnapshot(), nil }})
	}
	return out
}

type proofOwner struct {
	dir      string
	snapshot func() (*pebble.Snapshot, error)
}

type livePart struct {
	d    v2part.Descriptor
	path string
	mod  int64
}

func orderProofObserveDirs(p *orderProof, owners []proofOwner) error {
	live := map[v2part.Digest]livePart{}
	for _, o := range owners {
		snap, err := o.snapshot()
		if err != nil {
			return err
		}
		it, err := snap.NewIter(&pebble.IterOptions{LowerBound: sealedPartPrefix, UpperBound: v2part.PrefixEnd(sealedPartPrefix)})
		if err != nil {
			snap.Close()
			return err
		}
		for valid := it.First(); valid; valid = it.Next() {
			d, err := v2part.ParseDescriptor(bytes.Clone(it.Value()))
			if err != nil {
				it.Close()
				snap.Close()
				return err
			}
			path := filepath.Join(o.dir, "parts", hex.EncodeToString(d.SHA256[:])+".sst")
			st, err := os.Stat(path)
			if err != nil {
				it.Close()
				snap.Close()
				return err
			}
			live[d.SHA256] = livePart{d, path, st.ModTime().UnixNano()}
		}
		if err = it.Close(); err != nil {
			snap.Close()
			return err
		}
		if err = snap.Close(); err != nil {
			return err
		}
	}
	var fresh []livePart
	for id, part := range live {
		if _, ok := p.seen[id]; !ok {
			fresh = append(fresh, part)
		}
	}
	gone := 0
	for id := range p.seen {
		if _, ok := live[id]; !ok {
			gone++
			delete(p.seen, id)
		}
	}
	p.stats.PartsGone += gone
	if gone > 0 && len(fresh) > 0 {
		p.stats.MergesSeen++
	}
	slices.SortFunc(fresh, func(a, b livePart) int {
		if a.mod != b.mod {
			if a.mod < b.mod {
				return -1
			}
			return 1
		}
		return strings.Compare(a.path, b.path)
	})
	for _, part := range fresh {
		if err := p.readPart(part); err != nil {
			return err
		}
	}
	return nil
}

func (p *orderProof) readPart(part livePart) error {
	v2part.RegisterPolicies(part.d.Coverage.Policies)
	p.begin()
	p.seen[part.d.SHA256] = p.seq
	err := v2part.Read(context.Background(), part.path, &part.d, func(k v2part.Key, value []byte) error {
		name := string(k.Metric)
		if k.Archive == v2part.MetadataArchive {
			m, _, err := decodeRollupMetadataHeader(value)
			if err != nil {
				return err
			}
			p.meta(name, m)
			return nil
		}
		if len(value) >= 4 && bytes.Equal(value[:4], []byte{'G', 'C', 'D', 1}) {
			records, err := v2part.DecodeSlots(k.Archive, k.BlockStart, value)
			if err != nil {
				return err
			}
			for _, r := range records {
				p.record(name, r)
			}
			return nil
		}
		r, err := sealrollup.DecodeRecord(int(k.Archive), k.BlockStart, value)
		if err != nil {
			return err
		}
		p.record(name, r)
		return nil
	})
	p.end()
	return err
}

// orderProofEngine opens a merge-free engine whose hook clock is *clock, so
// idle closure runs at simulated seal times.
func orderProofEngine(t *testing.T, root string, clock *int64, shards int) *Engine {
	t.Helper()
	cfg := coreTestConfig()
	cfg.Shards, cfg.Producers, cfg.Rollups = shards, 1, true
	cfg.MaxRows = 1 << 16
	e, err := Open(root, cfg, Hooks{Now: func() time.Time { return time.Unix(*clock, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// TestOrderProofRealNames replays the bench kit's 401 real names with its
// storage-schemas and aggregation rules: one point per name per 10 s seal, the
// bench's 4 h lag, late rewrites, same-timestamp duplicates and idle names; a
// second stream lags each name by about its fine retention, so points cross
// the retention edge and land directly in coarse archives.
func TestOrderProofRealNames(t *testing.T) {
	if !orderProofEnabled() {
		t.Skip("ORDER_PROOF unset")
	}
	kit := os.Getenv("ORDER_PROOF_KIT")
	if kit == "" {
		t.Skip("ORDER_PROOF_KIT unset")
	}
	t.Setenv("CORE_MERGE_WORKERS", "0")
	raw, err := os.ReadFile(filepath.Join(kit, "names.chk"))
	if err != nil {
		t.Fatal(err)
	}
	schemas, err := persister.ReadWhisperSchemas(filepath.Join(kit, "storage-schemas.conf"))
	if err != nil {
		t.Fatal(err)
	}
	aggr, err := persister.ReadWhisperAggregation(filepath.Join(kit, "storage-aggregation.conf"))
	if err != nil {
		t.Fatal(err)
	}
	type metric struct {
		policy []byte
		cfg    sealrollup.Config
	}
	var metrics []metric
	// The 401 check names all match 1s:1d. Real token swaps reach the kit's
	// other ladders: ten_secondly (10s:2d,1m:14d,30m:2y), minutely
	// (1m:30d,1h:1y,1d:10y) and the catch-all (1m:14d,30m:2y).
	var names []string
	seen := map[string]bool{}
	for _, name := range strings.Fields(string(raw)) {
		for _, n := range []string{name, strings.Replace(name, ".secondly.", ".ten_secondly.", 1), strings.Replace(strings.Replace(name, ".secondly.", ".minutely.", 1), "capacityservice", "capacity", 1), strings.Replace(name, "aggregations.secondly.", "servers.", 1)} {
			if !seen[n] {
				seen[n] = true
				names = append(names, n)
			}
		}
	}
	ladders := map[string]int{}
	for _, name := range names {
		s, ok := schemas.Match(name)
		if !ok {
			t.Fatalf("no schema for %q", name)
		}
		a := aggr.Match(name)
		p := RollupPolicy{Name: v2part.ID(name), Group: "test", Config: sealrollup.Config{Method: sealrollup.Method(a.AggregationMethod()), XFilesFactor: float32(a.XFilesFactor())}}
		for _, r := range s.Retentions {
			p.Config.Archives = append(p.Config.Archives, sealrollup.Archive{Step: int64(r.SecondsPerPoint()), Count: int64(r.NumberOfPoints())})
		}
		encoded, err := EncodeRollupPolicy(p)
		if err != nil {
			t.Fatal(err)
		}
		metrics = append(metrics, metric{encoded, p.Config})
		ladders[s.RetentionStr]++
	}
	t.Logf("%d names, ladders %v", len(metrics), ladders)
	for _, edge := range []bool{false, true} {
		t.Run(fmt.Sprintf("edge=%v", edge), func(t *testing.T) {
			rng := rand.New(rand.NewSource(1401))
			clock := int64(1794700800)
			e := orderProofEngine(t, t.TempDir(), &clock, 4)
			defer e.Close()
			order := uint64(0)
			idle := make([]int, len(metrics))
			for step := 0; step < 360; step++ {
				clock += 10
				var batches []Batch
				for i, m := range metrics {
					if idle[i] > 0 {
						idle[i]--
						continue
					}
					if rng.Intn(400) == 0 {
						idle[i] = 30 + rng.Intn(150)
					}
					fine := m.cfg.Archives[0]
					lag := int64(14400)
					if edge {
						lag = fine.Step*fine.Count - 30*fine.Step + int64(rng.Intn(int(60*fine.Step)))
					}
					ts := clock - lag
					rows := []Row{{ID: uint64(i + 1), Time: ts, Bits: math.Float64bits(float64(rng.Intn(1000)) / 8), Seq: 1}}
					if rng.Intn(50) == 0 { // late rewrite up to 2 h back
						rows = append(rows, Row{ID: uint64(i + 1), Time: ts - int64(rng.Intn(7200)), Bits: math.Float64bits(float64(rng.Intn(1000))), Seq: 2})
					}
					if rng.Intn(100) == 0 { // same timestamp twice in one batch
						rows = append(rows, Row{ID: uint64(i + 1), Time: ts, Bits: math.Float64bits(float64(-rng.Intn(1000))), Seq: 3})
					}
					order++
					batches = append(batches, Batch{ID: uint64(i + 1), Policy: m.policy, Order: order, Clock: clock, Points: rows})
				}
				if err := e.AppendBatches(0, batches, order, true); err != nil {
					t.Fatal(err)
				}
				orderProofObserve(t, t.Name(), e)
			}
		})
	}
}

// TestOrderProofBackgroundCorpus replays TestBackgroundSealRendersMatchClassic's
// stream (seed, names, policies, point generator) with explicit seals in place
// of its held threshold seals.
func TestOrderProofBackgroundCorpus(t *testing.T) {
	if !orderProofEnabled() {
		t.Skip("ORDER_PROOF unset")
	}
	t.Setenv("CORE_MERGE_WORKERS", "0")
	rng := rand.New(rand.NewSource(1559))
	now := int64(1794700800)
	clock := now
	const metrics = 10
	policies := make([][]byte, metrics)
	for i := range policies {
		_, policies[i] = rollupTestPolicy(t, fmt.Sprintf("bg.%d", i), "1s:1d,1m:30d,1h:1y", sealrollup.Method(1+i%int(sealrollup.First)), .5)
	}
	e := orderProofEngine(t, t.TempDir(), &clock, 1)
	defer e.Close()
	var cursor, seq uint64
	appendOne := func(i int) {
		var rows []Row
		for k := 1 + rng.Intn(5); k > 0; k-- {
			seq++
			ts := clock - int64(rng.Intn(600))
			rows = append(rows, Row{ID: uint64(i + 1), Time: ts, Bits: math.Float64bits(float64(rng.Intn(1000)) / 8), Seq: seq})
		}
		cursor++
		if err := e.AppendBatches(0, []Batch{{ID: uint64(i + 1), Policy: policies[i], Order: cursor, Clock: clock, Points: rows}}, cursor, false); err != nil {
			t.Fatal(err)
		}
	}
	seal := func() {
		if err := e.AppendBatches(0, nil, cursor, true); err != nil {
			t.Fatal(err)
		}
		orderProofObserve(t, t.Name(), e)
	}
	for i := range metrics {
		appendOne(i)
	}
	seal()
	for round := 0; round < 120; round++ {
		clock += 3
		appendOne(rng.Intn(metrics))
		if round%3 == 2 {
			seal()
		}
	}
	for step := 0; step < 12; step++ {
		clock = now + 400 + int64(step)*7
		for _, i := range rng.Perm(metrics)[:1+rng.Intn(4)] {
			appendOne(i)
		}
		seal()
	}
}
