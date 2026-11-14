package downsize

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

// Verification cost model. Holes come from restarts, cutovers and copy
// windows, so every name is checked over the given --window ranges and a
// --sample-every fraction of names over their full history (every archive).
// Each store digests a name's archives server-side (/v1/transfer/digest:
// finite point count + hash per archive over the window); only names whose
// digests differ are read point by point. A store without the endpoint is
// read point by point through carbonserver multi-fetch instead.

// syncRange is a named [From, Until) window in Unix seconds.
type syncRange struct {
	Name        string
	From, Until int64
}

type syncRanges []syncRange

func (r *syncRanges) String() string {
	var out []string
	for _, w := range *r {
		out = append(out, fmt.Sprintf("%s:%d:%d", w.Name, w.From, w.Until))
	}
	return strings.Join(out, " ")
}

// Set accepts "[name:]from:until" (Unix seconds), several separated by
// spaces or commas.
func (r *syncRanges) Set(v string) error {
	for _, item := range strings.FieldsFunc(v, func(c rune) bool { return c == ' ' || c == ',' }) {
		parts := strings.Split(item, ":")
		name := ""
		if len(parts) == 3 {
			name, parts = parts[0], parts[1:]
		}
		if len(parts) != 2 {
			return fmt.Errorf("window %q: want [name:]from:until", item)
		}
		from, err1 := strconv.ParseInt(parts[0], 10, 64)
		until, err2 := strconv.ParseInt(parts[1], 10, 64)
		if err1 != nil || err2 != nil || from < 0 || until <= from {
			return fmt.Errorf("window %q: want from < until in Unix seconds", item)
		}
		if name == "" {
			name = fmt.Sprintf("w%d", len(*r))
		}
		*r = append(*r, syncRange{name, from, until})
	}
	return nil
}

// syncDigest is one archive's digest over a window (the buckyd wire shape).
type syncDigest struct {
	Step        int64
	Points      int64
	Count       int64
	Hash        string
	From, Until int64
}

var (
	// errSyncNoDigest: the store has no digest endpoint (older build).
	errSyncNoDigest = errors.New("store has no digest endpoint")
	// errSyncStoreDown: the store stayed unavailable for --host-wait.
	errSyncStoreDown = errors.New("store unavailable")
)

type syncSkip struct {
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

// syncProgress is the live counter set the progress ticker reports.
type syncProgress struct {
	copyActive   int64 // fresh committed names in the unfinished source list
	copyFresh    int64 // fresh committed names over this phase
	copyObserved bool
	mu           sync.Mutex
	phase        string
	total        int
	started      time.Time
	done         atomic.Int64
	replayed     atomic.Int64 // journal progress, excluded from live throughput
	skipped      atomic.Int64
	digests      atomic.Int64 // names compared by digest
	reads        atomic.Int64 // names read point by point
	batches      atomic.Int64
	batchNanos   atomic.Int64
	compareNanos atomic.Int64
}

func (p *syncProgress) begin(phase string, total int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.phase, p.total, p.started = phase, total, time.Now()
	p.copyActive, p.copyFresh, p.copyObserved = 0, 0, false
	p.done.Store(0)
	p.replayed.Store(0)
}

func (s *syncer) sampled(name string) bool {
	if len(s.o.windows) == 0 {
		return true // no windows: every name over its full history
	}
	return !s.windowOnly() && s.o.sampleEvery > 0 && crc32.ChecksumIEEE([]byte(name))%uint32(s.o.sampleEvery) == 0
}

func (s *syncer) isDown(store string) bool {
	_, down := s.down.Load(store)
	return down
}

func (s *syncer) markDown(store string, err error) {
	if _, loaded := s.down.LoadOrStore(store, err.Error()); !loaded {
		event("sync_store_down", map[string]any{"level": "error", "store": store, "error": err.Error(), "hint": "comparisons against this store are skipped for the rest of the run"})
	}
}

// verifyBatch verifies names; a failure that is not the target being down is
// retried on halves down to single names, which are skipped with a reason so
// one bad metric never stalls the run.
func (s *syncer) verifyBatch(ctx context.Context, round int, names []string) (syncRow, error) {
	started := time.Now()
	defer func() { s.prog.batches.Add(1); s.prog.batchNanos.Add(int64(time.Since(started))) }()
	row := syncRow{Kind: "verified", Round: round, First: names[0], Last: names[len(names)-1], Names: len(names)}
	err := s.verifyNames(ctx, &row, names)
	if err == nil || ctx.Err() != nil || errors.Is(err, errSyncStoreDown) && s.isDown(s.o.to) {
		return row, err
	}
	if len(names) == 1 {
		row = syncRow{Kind: "verified", Round: round, First: names[0], Last: names[0], Names: 1, Skipped: []syncSkip{{names[0], err.Error()}}}
		s.prog.skipped.Add(1)
		event("sync_name_skipped", map[string]any{"level": "warn", "round": round, "metric": names[0], "reason": err.Error()})
		return row, nil
	}
	event("sync_batch_split", map[string]any{"round": round, "first": names[0], "names": len(names), "error": err.Error()})
	left, err := s.verifyBatch(ctx, round, names[:len(names)/2])
	if err != nil {
		return row, err
	}
	right, err := s.verifyBatch(ctx, round, names[len(names)/2:])
	if err != nil {
		return row, err
	}
	row = syncRow{Kind: "verified", Round: round, First: names[0], Last: names[len(names)-1], Names: len(names)}
	for _, part := range []syncRow{left, right} {
		row.Holes = append(row.Holes, part.Holes...)
		row.Unindexed = append(row.Unindexed, part.Unindexed...)
		row.Short = append(row.Short, part.Short...)
		row.Skipped = append(row.Skipped, part.Skipped...)
		row.Resolved = append(row.Resolved, part.Resolved...)
	}
	return row, nil
}

func (s *syncer) verifyNames(ctx context.Context, row *syncRow, names []string) error {
	info, err := s.b.Info(ctx, s.o.to, names)
	if errors.Is(err, errSyncStoreDown) {
		s.markDown(s.o.to, err)
	}
	if err != nil {
		return fmt.Errorf("target info: %w", err)
	}
	if err := s.resolveAbsent(ctx, row, names, info); err != nil {
		return err
	}
	resolved := map[string]bool{}
	for _, r := range row.Resolved {
		resolved[r.Name] = true
	}
	var eligible, full []string
	schemas := map[string][]syncArchive{}
	for _, n := range names {
		if resolved[n] {
			continue
		}
		archives, ok := info[n]
		switch {
		case !ok:
			row.Unindexed = append(row.Unindexed, n)
		case !s.o.includeShort && syncShort(archives):
			row.Short = append(row.Short, n)
		default:
			eligible = append(eligible, n)
			schemas[n] = archives
			if s.sampled(n) {
				full = append(full, n)
			}
		}
	}
	now := s.o.now().Unix()
	horizon := now - int64(s.o.settle.Seconds())
	for _, w := range s.o.windows {
		w.Until = min(w.Until, horizon)
		if w.From >= w.Until || len(eligible) == 0 {
			continue
		}
		if err := s.checkWindow(ctx, row, eligible, schemas, w); err != nil {
			return err
		}
	}
	if len(full) > 0 {
		if err := s.checkFull(ctx, row, full, schemas, now); err != nil {
			return err
		}
	}
	return nil
}

// holders returns the subset of names that source src holds.
func (s *syncer) holders(src int, names []string) []string {
	var subset []string
	for _, n := range names {
		if mask, ok := s.masks.Load(n); ok && (mask.(uint64)&(1<<uint(src)) != 0 || s.o.namesFile != "") {
			subset = append(subset, n)
		}
	}
	return subset
}

func (s *syncer) fetch(ctx context.Context, store string, names []string, from, until int64) (map[string]syncSeries, error) {
	got, err := s.b.Fetch(ctx, store, names, from, until)
	if errors.Is(err, errSyncStoreDown) {
		s.markDown(store, err)
	}
	return got, err
}

// noDigest reports a store already known not to serve digests.
func (s *syncer) noDigest(store string) bool {
	v, ok := s.digests.Load(store)
	return ok && !v.(bool)
}

// digest returns errSyncNoDigest once a store is known not to serve digests.
func (s *syncer) digest(ctx context.Context, store string, names []string, w syncRange) (map[string][]syncDigest, error) {
	if s.noDigest(store) {
		return nil, errSyncNoDigest
	}
	got, err := s.b.Digest(ctx, store, names, w.From, w.Until)
	if errors.Is(err, errSyncNoDigest) {
		if _, loaded := s.digests.LoadOrStore(store, false); !loaded {
			event("sync_digest_unavailable", map[string]any{"store": store, "fallback": "carbonserver multi-fetch"})
		}
		return nil, err
	}
	if errors.Is(err, errSyncStoreDown) {
		s.markDown(store, err)
	}
	if err == nil {
		s.digests.Store(store, true)
	}
	return got, err
}

// checkWindow compares every name over one window: by digest where both
// stores serve it, otherwise (and for digest mismatches) point by point at
// the archive carbonserver serves for the window. The target and every
// source are digested concurrently: the calls are independent round trips,
// so a window costs its slowest call (a cross-site replica) instead of the
// sum of all of them. Point reads for mismatches and older stores use the
// same fan-out, with results consumed in source order.
func (s *syncer) checkWindow(ctx context.Context, row *syncRow, names []string, _ map[string][]syncArchive, w syncRange) error {
	if s.isDown(s.o.to) {
		return fmt.Errorf("target: %w", errSyncStoreDown)
	}
	type sourceCheck struct {
		source    string
		subset    []string
		digest    map[string][]syncDigest
		err       error
		detail    []string
		points    map[string]syncSeries
		fetchErr  error
		preloaded bool
		coarse    []syncHole
	}
	var checks []*sourceCheck
	for src, source := range s.o.from {
		subset := s.holders(src, names)
		if len(subset) == 0 {
			continue
		}
		if s.isDown(source) {
			for _, n := range subset {
				row.Skipped = append(row.Skipped, syncSkip{n, "source down: " + source})
			}
			continue
		}
		checks = append(checks, &sourceCheck{source: source, subset: subset})
	}
	if len(checks) == 0 {
		return nil
	}
	targetDigest, targetErr := map[string][]syncDigest(nil), errSyncNoDigest
	if !s.noDigest(s.o.to) {
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			targetDigest, targetErr = s.digest(ctx, s.o.to, names, w)
		}()
		for _, c := range checks {
			if s.noDigest(c.source) {
				c.err = errSyncNoDigest
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				c.digest, c.err = s.digest(ctx, c.source, c.subset, w)
			}()
		}
		wg.Wait()
	}
	if targetErr != nil && !errors.Is(targetErr, errSyncNoDigest) {
		return fmt.Errorf("target digest: %w", targetErr)
	}
	byDigest := targetErr == nil // otherwise every source is read point by point
	for _, c := range checks {
		source, subset := c.source, c.subset
		detail := subset
		if byDigest {
			switch err := c.err; {
			case err == nil:
				detail = detail[:0:0]
				for _, n := range subset {
					if mismatch, coarse := syncDigestMismatch(c.digest[n], targetDigest[n]); mismatch {
						detail = append(detail, n)
						for _, h := range coarse {
							h.Name, h.Source, h.Window = n, source, w.Name
							h.First, h.Last = w.From, w.Until-1
							c.coarse = append(c.coarse, h)
						}
					}
				}
				s.prog.digests.Add(int64(len(subset)))
			case errors.Is(err, errSyncNoDigest):
			case errors.Is(err, errSyncStoreDown):
				for _, n := range subset {
					row.Skipped = append(row.Skipped, syncSkip{n, "source down: " + source})
				}
				continue
			default:
				return fmt.Errorf("source %s digest: %w", source, err)
			}
		}
		c.detail = detail
	}
	// Older sources cannot digest remotely. Read them first, then use the
	// same finite-point wire digest to avoid materializing matching target
	// series. Keep mismatches on the original point-by-point comparison path.
	var legacy sync.WaitGroup
	for _, c := range checks {
		if !byDigest || !errors.Is(c.err, errSyncNoDigest) || len(c.detail) == 0 {
			continue
		}
		legacy.Add(1)
		go func() {
			defer legacy.Done()
			c.preloaded = true
			s.prog.reads.Add(int64(len(c.detail)))
			c.points, c.fetchErr = s.fetch(ctx, c.source, c.detail, w.From, w.Until)
			if c.fetchErr != nil {
				return
			}
			detail := make([]string, 0, len(c.detail))
			for _, n := range c.detail {
				if !syncWindowCovered(c.points[n], targetDigest[n], w) {
					detail = append(detail, n)
				}
			}
			c.detail = detail
		}()
	}
	legacy.Wait()
	needed := map[string]bool{}
	for _, c := range checks {
		for _, n := range c.detail {
			needed[n] = true
		}
	}
	if len(needed) == 0 {
		return nil
	}
	var targetDetail []string
	for _, n := range names {
		if needed[n] {
			targetDetail = append(targetDetail, n)
		}
	}
	var targetPoints map[string]syncSeries
	var targetFetchErr error
	var reads sync.WaitGroup
	reads.Add(1)
	go func() {
		defer reads.Done()
		targetPoints, targetFetchErr = s.fetch(ctx, s.o.to, targetDetail, w.From, w.Until)
	}()
	for _, c := range checks {
		if len(c.detail) == 0 || c.preloaded {
			continue
		}
		s.prog.reads.Add(int64(len(c.detail)))
		reads.Add(1)
		go func() {
			defer reads.Done()
			c.points, c.fetchErr = s.fetch(ctx, c.source, c.detail, w.From, w.Until)
		}()
	}
	reads.Wait()
	if targetFetchErr != nil {
		return fmt.Errorf("target fetch: %w", targetFetchErr)
	}
	compareStart := time.Now()
	defer func() { s.prog.compareNanos.Add(int64(time.Since(compareStart))) }()
	for _, c := range checks {
		source, detail, got, err := c.source, c.detail, c.points, c.fetchErr
		row.Holes = append(row.Holes, c.coarse...)
		if errors.Is(err, errSyncStoreDown) {
			for _, n := range detail {
				row.Skipped = append(row.Skipped, syncSkip{n, "source down: " + source})
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("source %s fetch: %w", source, err)
		}
		for _, n := range detail {
			sv, ok := got[n]
			tv := targetPoints[n]
			if !ok || sv.Step <= 0 || (tv.Step != 0 && tv.Step != sv.Step) {
				if ok && sv.Step > 0 {
					row.Skipped = append(row.Skipped, syncSkip{n, "schema_step_mismatch"})
				}
				continue // retention differs: not comparable at one step
			}
			from := (w.From + sv.Step - 1) / sv.Step * sv.Step
			if h, ok := syncCompare(n, source, sv, tv, sv.Step, from, w.Until); ok {
				h.Window = w.Name
				row.Holes = append(row.Holes, h)
			}
		}
	}
	return nil
}

// syncWindowCovered uses the digest endpoint's bit-exact wire algorithm.
// Empty source windows cannot contain a hole. Nonempty windows require an
// identical finite timestamp/value sequence at the same step; any uncertainty
// falls back to the normal target fetch and point comparison.
func syncWindowCovered(source syncSeries, target []syncDigest, w syncRange) bool {
	if source.Step <= 0 {
		return true
	}
	d := syncSeriesSHA(source, w)
	if d.Count == 0 {
		return true
	}
	for _, t := range target {
		if t.Step == d.Step && t.Count == d.Count && t.Hash == d.Hash {
			return true
		}
	}
	return false
}

func syncSeriesSHA(source syncSeries, w syncRange) syncDigest {
	h := sha256.New()
	var buf [16]byte
	var count int64
	for i, v := range source.Values {
		ts := source.Start + int64(i)*source.Step
		if ts < w.From || ts >= w.Until || math.IsNaN(v) || math.IsInf(v, 0) {
			continue
		}
		binary.BigEndian.PutUint64(buf[:8], uint64(ts))
		binary.BigEndian.PutUint64(buf[8:], math.Float64bits(v))
		_, _ = h.Write(buf[:])
		count++
	}
	sum := h.Sum(nil)
	return syncDigest{Step: source.Step, Count: count, Hash: hex.EncodeToString(sum[:8])}
}

// syncDigestMismatch reports whether a name needs a point-by-point read, and
// holes provable from counts alone in archives coarser than the one
// carbonserver serves for the window (the first archive).
func syncDigestMismatch(source, target []syncDigest) (bool, []syncHole) {
	steps := map[int64]syncDigest{}
	for _, d := range target {
		steps[d.Step] = d
	}
	mismatch := false
	var coarse []syncHole
	for i, d := range source {
		if d.Count == 0 {
			continue
		}
		t, ok := steps[d.Step]
		if ok && t.Hash == d.Hash && t.Count == d.Count {
			continue
		}
		mismatch = true
		if i > 0 && (!ok || t.Count < d.Count) {
			coarse = append(coarse, syncHole{Step: d.Step, Points: int(d.Count - t.Count)})
		}
	}
	return mismatch, coarse
}

// checkFull compares every archive of the names over the range only that
// archive serves (carbonserver picks the archive by "from").
func (s *syncer) checkFull(ctx context.Context, row *syncRow, names []string, schemas map[string][]syncArchive, now int64) error {
	groups := map[string][]string{}
	keyed := map[string][]syncArchive{}
	for _, n := range names {
		key := fmt.Sprint(schemas[n])
		groups[key] = append(groups[key], n)
		keyed[key] = schemas[n]
	}
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		group := groups[key]
		for _, a := range keyed[key] {
			from, until := syncWindow(now, a, s.o.settle)
			if from >= until {
				continue
			}
			if err := s.checkFullArchive(ctx, row, group, a.Step, from, until); err != nil {
				return err
			}
		}
	}
	return nil
}

// fullDigest negotiates cached sums independently of the legacy SHA endpoint.
func (s *syncer) fullDigest(ctx context.Context, store string, names []string, step int64, w syncRange) (map[string][]syncDigest, bool, error) {
	b, ok := s.b.(interface {
		SumDigest(context.Context, string, []string, int64, int64, int64) (map[string][]syncDigest, error)
	})
	key := "sum-v1:" + store
	v, known := s.digests.Load(key)
	if ok && (!known || v.(bool)) {
		got, err := b.SumDigest(ctx, store, names, step, w.From, w.Until)
		if !errors.Is(err, errSyncNoDigest) {
			return got, true, err
		}
		if _, loaded := s.digests.LoadOrStore(key, false); !loaded {
			event("sync_full_digest_unavailable", map[string]any{"store": store, "fallback": "serving-archive SHA digest"})
		}
	}
	got, err := s.digest(ctx, store, names, w)
	return got, false, err
}

type fullArchiveDigest struct {
	legacy    map[string][]syncDigest
	legacyErr error
	digests   map[string][]syncDigest
	sum       bool
	err       error
}

func (s *syncer) fullArchiveDigests(ctx context.Context, names []string, step, from, until int64) map[string]fullArchiveDigest {
	// Digest responses contain only fingerprints. Batch them by names, while
	// materialized fallback reads below retain their independent byte budget.
	stores := append([]string{s.o.to}, s.o.from...)
	results := make([]fullArchiveDigest, len(stores))
	var wg sync.WaitGroup
	for i, store := range stores {
		subset := names
		if i > 0 {
			subset = s.holders(i-1, names)
			if s.isDown(store) {
				continue
			}
		} else if legacy, known := s.digests.Load("sum-v1:" + store); known && !legacy.(bool) {
			continue // the bounded local target read also supplies its legacy SHA
		}
		if len(subset) == 0 {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i].digests, results[i].sum, results[i].err = s.fullDigest(ctx, store, subset, step, syncRange{Name: "full", From: from, Until: until})
		}()
	}
	wg.Wait()
	// Older holders speak the serving-archive SHA protocol. Compare them with
	// the target's SHA for this exact window before materializing any points;
	// fingerprints from different algorithms must never be compared directly.
	if results[0].sum && results[0].err == nil {
		for _, d := range results[1:] {
			if !d.sum && d.err == nil && len(d.digests) > 0 {
				results[0].legacy, results[0].legacyErr = s.digest(ctx, s.o.to, names, syncRange{Name: "full", From: from, Until: until})
				break
			}
		}
	}
	out := make(map[string]fullArchiveDigest, len(stores))
	for i, store := range stores {
		out[store] = results[i]
	}
	return out
}

// checkFullArchive uses exactly the same serving-archive window and hole
// comparison as full point reads. Equal count+hash at the expected step proves
// there are no holes; every other result takes the original point comparison.
// Requests to independent stores overlap, but holes retain source/name order.
func (s *syncer) checkFullArchive(ctx context.Context, row *syncRow, names []string, step, from, until int64) error {
	digests := s.fullArchiveDigests(ctx, names, step, from, until)
	// A verification worker can retain a decoded response for the target and
	// every holder. Bound the names by the archive's point count, rather than
	// treating a 500-name week and a 500-name year as the same amount of work.
	// Four copies allow for the wire body, decoded values and decoder growth.
	// This is a working-set budget, not a hard process limit: one name is the
	// minimum request, and callers still need a process memory limit.
	const budget = int64(64 << 20)
	batch := fullVerifyBatchNames(len(names), len(s.o.from)+1, step, from, until, budget)
	if batch >= len(names) {
		return s.checkFullArchiveBatch(ctx, row, names, step, from, until, digests)
	}
	var result syncRow
	for start := 0; start < len(names); start += batch {
		end := min(start+batch, len(names))
		if err := s.checkFullArchiveBatch(ctx, &result, names[start:end], step, from, until, digests); err != nil {
			return err
		}
	}
	// Keep the existing holder/name order of hole verdicts in the journal.
	sources := make(map[string]int, len(s.o.from))
	for i, source := range s.o.from {
		sources[source] = i
	}
	positions := make(map[string]int, len(names))
	for i, name := range names {
		positions[name] = i
	}
	sort.SliceStable(result.Holes, func(i, j int) bool {
		a, b := result.Holes[i], result.Holes[j]
		if sources[a.Source] != sources[b.Source] {
			return sources[a.Source] < sources[b.Source]
		}
		return positions[a.Name] < positions[b.Name]
	})
	row.Holes = append(row.Holes, result.Holes...)
	row.Skipped = append(row.Skipped, result.Skipped...)
	return nil
}

func fullVerifyBatchNames(names, stores int, step, from, until, budget int64) int {
	if names == 0 || step <= 0 || until <= from {
		return names
	}
	// Divide before multiplying so a long retention or many peers cannot
	// overflow the byte estimate and accidentally remove the bound.
	points := (until-from)/step + 2
	limit := budget / 4 / 8 / int64(max(stores, 1)) / points
	return int(min(int64(names), max(int64(1), limit)))
}

func (s *syncer) checkFullArchiveBatch(ctx context.Context, row *syncRow, names []string, step, from, until int64, digests map[string]fullArchiveDigest) error {
	type check struct {
		store   string
		names   []string
		digests map[string][]syncDigest
		sum     bool
		err     error
		detail  []string
		points  map[string]syncSeries
	}
	target := &check{store: s.o.to, names: names}
	legacy, known := s.digests.Load("sum-v1:" + s.o.to)
	localTarget := known && !legacy.(bool)
	checks := []*check{target}
	for src, source := range s.o.from {
		subset := s.holders(src, names)
		if len(subset) == 0 {
			continue
		}
		if s.isDown(source) {
			for _, n := range subset {
				row.Skipped = append(row.Skipped, syncSkip{n, "source down: " + source})
			}
			continue
		}
		checks = append(checks, &check{store: source, names: subset})
	}
	var wg sync.WaitGroup
	w := syncRange{Name: "full", From: from, Until: until}
	for _, c := range checks {
		d := digests[c.store]
		c.digests, c.sum, c.err = d.digests, d.sum, d.err
	}
	if localTarget {
		// The legacy endpoint performs the same expensive target read as Fetch.
		// Read it once locally, reusing its points if any source hash differs.
		needed := map[string]bool{}
		for _, c := range checks[1:] {
			for _, n := range c.names {
				empty := false
				if c.err == nil {
					for _, d := range c.digests[n] {
						if d.Step == step && d.Count == 0 && ((d.From == 0 && d.Until == 0) || (d.From == from && d.Until == until)) {
							empty = true
						}
					}
				}
				if !empty {
					needed[n] = true
				}
			}
		}
		var targetNames []string
		for _, n := range names {
			if needed[n] {
				targetNames = append(targetNames, n)
			}
		}
		// The local read supersedes any endpoint negotiation failure.
		target.err = nil
		target.points = map[string]syncSeries{}
		target.digests = map[string][]syncDigest{}
		if len(targetNames) > 0 {
			var err error
			target.points, err = s.fetch(ctx, target.store, targetNames, from, until)
			if err != nil {
				return fmt.Errorf("target fetch: %w", err)
			}
			for n, series := range target.points {
				target.digests[n] = []syncDigest{syncSeriesSHA(series, w)}
			}
		}
	}
	// Digest failures fall back to the original read path. In particular an
	// unavailable endpoint must never turn an unchecked name into a match.
	// On a mixed fleet, read an older holder once and compare its canonical
	// finite-point sum locally. A matching target digest avoids its point read.
	// Any uncertainty retains the original directional hole comparison.
	if _, sums := s.b.(interface {
		SumDigest(context.Context, string, []string, int64, int64, int64) (map[string][]syncDigest, error)
	}); sums && target.sum && target.err == nil {
		for _, c := range checks[1:] {
			if !c.sum && c.err == nil && digests[s.o.to].legacyErr == nil {
				keep := make([]string, 0, len(c.names))
				for _, n := range c.names {
					if fullArchiveSHAMatch(c.digests[n], digests[s.o.to].legacy[n], step, from, until) {
						s.prog.digests.Add(1)
					} else {
						keep = append(keep, n)
					}
				}
				c.names = keep
			}
			if len(c.names) == 0 {
				continue
			}
			if c.sum && !errors.Is(c.err, errSyncNoDigest) {
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				c.points, c.err = s.fetch(ctx, c.store, c.names, from, until)
				s.prog.reads.Add(int64(len(c.names)))
			}()
		}
		wg.Wait()
	}

	needed := map[string]bool{}
	for _, c := range checks[1:] {
		for _, n := range c.names {
			match := false
			if target.err == nil && c.err == nil {
				for _, sd := range c.digests[n] {
					if sd.Step != step {
						continue
					}
					// A finite-empty source cannot contain a directional hole.
					if sd.Count == 0 && ((sd.From == from && sd.Until == until) || (sd.From == 0 && sd.Until == 0)) {
						match = true
					}
					for _, td := range target.digests[n] {
						if c.sum == target.sum && td.Step == step && (localTarget || sd.Points == td.Points) && sd.From == td.From && sd.Until == td.Until && sd.Count == td.Count && sd.Hash == td.Hash {
							match = true
						}
					}
				}
			}
			if !match && c.points != nil && c.err == nil {
				// A successful source fetch that omitted this name has no
				// directional holes. The point comparison below also skips it;
				// do not materialize a target archive just to reach that verdict.
				series, found := c.points[n]
				match = !found || syncSumCovered(series, target.digests[n], step, from, until)
			}

			if match {
				s.prog.digests.Add(1)
			} else {
				c.detail = append(c.detail, n)
				needed[n] = true
			}
		}
	}
	for _, n := range names {
		if needed[n] {
			target.detail = append(target.detail, n)
		}
	}
	for _, c := range checks {
		if len(c.detail) == 0 || c.points != nil {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.points, c.err = s.fetch(ctx, c.store, c.detail, from, until)
			if c != target {
				s.prog.reads.Add(int64(len(c.detail)))
			}
		}()
	}
	wg.Wait()
	if len(target.detail) > 0 && target.err != nil {
		return fmt.Errorf("target fetch: %w", target.err)
	}
	compareStart := time.Now()
	defer func() { s.prog.compareNanos.Add(int64(time.Since(compareStart))) }()
	for _, c := range checks[1:] {
		if len(c.detail) == 0 {
			continue
		}
		if errors.Is(c.err, errSyncStoreDown) {
			continue
		}
		if c.err != nil {
			return fmt.Errorf("source %s fetch: %w", c.store, c.err)
		}
		for _, n := range c.detail {
			sv, ok := c.points[n]
			if !ok || sv.Step != step {
				if ok {
					row.Skipped = append(row.Skipped, syncSkip{n, "schema_step_mismatch"})
				}
				continue
			}
			if h, ok := syncCompare(n, c.store, sv, target.points[n], step, from, until); ok {
				h.Window = "full"
				row.Holes = append(row.Holes, h)
			}
		}
	}
	return nil
}

// fullArchiveSHAMatch compares only fingerprints of the requested serving
// archive. A missing digest or a different archive never establishes coverage.
func fullArchiveSHAMatch(source, target []syncDigest, step, from, until int64) bool {
	for _, sd := range source {
		if sd.Step != step {
			continue
		}
		if sd.Count == 0 && ((sd.From == 0 && sd.Until == 0) || (sd.From == from && sd.Until == until)) {
			return true
		}
		for _, td := range target {
			if td.Step == step && sd.Points == td.Points && sd.From == td.From && sd.Until == td.Until && sd.Count == td.Count && sd.Hash == td.Hash {
				return true
			}
		}
	}
	return false
}

// syncSumCovered accepts only the exact archive window negotiated above.
func syncSumCovered(source syncSeries, target []syncDigest, step, from, until int64) bool {
	if source.Step != step {
		return false
	}
	var sum chunkstore.PointSum
	for i, v := range source.Values {
		ts := source.Start + int64(i)*source.Step
		if ts >= from && ts < until {
			sum.Add(ts, v)
		}
	}
	for _, d := range target {
		if d.Step == step && d.From == from && d.Until == until && d.Count == int64(sum.Count) && d.Hash == fmt.Sprintf("%016x", sum.Sum) {
			return true
		}
	}
	return false
}

// progressLoop prints one progress line per interval until ctx ends.
func (s *syncer) progressLoop(ctx context.Context, every time.Duration) {
	if every <= 0 {
		return
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.progressLine()
		}
	}
}

func (s *syncer) progressLine() {
	p := &s.prog
	p.mu.Lock()
	phase, total, started := p.phase, p.total, p.started
	done := p.done.Load() + p.copyActive
	fresh, observed := p.copyFresh, p.copyObserved
	p.mu.Unlock()
	elapsed := time.Since(started).Seconds()
	fields := map[string]any{"phase": phase, "done": done, "total": total, "skipped": p.skipped.Load(),
		"compared_by_digest": p.digests.Load(), "read_point_by_point": p.reads.Load(), "limiter": fmt.Sprintf("streams=%d verify_streams=%d", s.o.streams, s.o.verifyConcurrency())}
	fields["verify_batches"] = p.batches.Load()
	fields["batch_seconds"] = float64(p.batchNanos.Load()) / float64(time.Second)
	fields["compare_seconds"] = float64(p.compareNanos.Load()) / float64(time.Second)
	if st, ok := s.b.(interface{ syncTimings() []syncIOTiming }); ok {
		fields["io_timings"] = st.syncTimings()
	}
	if st, ok := s.b.(interface{ syncStats() (int64, int64, int64) }); ok {
		bytes, waits, busy := st.syncStats()
		fields["read_bytes"], fields["waits"], fields["busy_retries"] = bytes, waits, busy
		if elapsed > 0 {
			fields["mb_per_second"] = float64(bytes) / 1e6 / time.Since(s.started).Seconds()
		}
	}
	if elapsed > 0 && done > 0 {
		rate := float64(max(0, done-p.replayed.Load())) / elapsed
		if observed {
			rate = float64(fresh) / elapsed
		}
		fields["names_per_second"] = rate
		if rate > 0 && total > int(done) {
			fields["eta_seconds"] = float64(int64(total)-done) / rate
		}
	}
	event("sync_progress", fields)
}

// Copy receipts stay authoritative; these counters expose committed batches
// while a potentially multi-million-name donor list is still being copied.
func (p *syncProgress) copyCommitted(n int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.copyObserved = true
	p.copyActive += int64(n)
	p.copyFresh += int64(n)
}
func (p *syncProgress) finishCopy(n int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.done.Add(int64(n))
	p.copyActive = 0
}
