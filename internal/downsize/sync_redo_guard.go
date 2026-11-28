package downsize

// The redo guard observes completed backend work; it never skips a read or
// changes a verdict. Receipts use stable catalog IDs, so batch budgets and
// executable versions do not change their identity. Compact ID runs add no
// second name catalog and normally coalesce to one interval per store/window.
import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type syncRedoRange struct{ First, Last int64 }
type syncRedoWork struct {
	Keys   []uint64        `json:"k"`
	IDs    []syncRedoRange `json:"i"`
	Reason string          `json:"r,omitempty"`
}

// Operation fingerprints are observational only: they never authorize reuse.
// Hashing the fixed store/range tuple keeps millions of receipts compact.
func redoKey(phase, store string, from, until int64) uint64 {
	h := fnv.New64a()
	fmt.Fprintf(h, "%s\x00%s\x00%d\x00%d", phase, store, from, until)
	return h.Sum64()
}
func (r syncRedoRange) MarshalJSON() ([]byte, error) {
	b := strconv.AppendInt([]byte{'['}, r.First, 10)
	b = append(b, ',')
	b = strconv.AppendInt(b, r.Last, 10)
	return append(b, ']'), nil
}
func (r *syncRedoRange) UnmarshalJSON(b []byte) error {
	var pair []int64
	if err := json.Unmarshal(b, &pair); err != nil {
		return err
	}
	if len(pair) != 2 || pair[0] < 0 || pair[1] < pair[0] {
		return errors.New("invalid redo receipt ID range")
	}
	r.First, r.Last = pair[0], pair[1]
	return nil
}

type syncRedoGuard struct {
	mu        sync.Mutex
	j         *syncJournal
	seen      map[uint64][]syncRedoRange
	imports   map[string][]syncRow
	importMax map[string][]string
}
type syncRedoBatch struct {
	mu      sync.Mutex
	ids     map[string]int64
	reasons map[string]string
	work    []syncRedoWork
}
type syncRedoContextKey struct{}

func newSyncRedoGuard(j *syncJournal) *syncRedoGuard {
	g := &syncRedoGuard{j: j, seen: map[uint64][]syncRedoRange{}, imports: map[string][]syncRow{}, importMax: map[string][]string{}}
	// Replay appends and merges each key in amortized batches. Inserting row by
	// row moved the whole sorted slice per out-of-order range: quadratic in a
	// key's disjoint ranges, so a long journal took hours to resume.
	merged := map[uint64]int{}
	for _, row := range j.rows {
		for _, w := range row.Work {
			for _, k := range w.Keys {
				g.seen[k] = append(g.seen[k], w.IDs...)
				if len(g.seen[k]) >= 2*merged[k]+redoReplayBatch {
					g.seen[k] = redoNormalize(g.seen[k])
					merged[k] = len(g.seen[k])
				}
			}
		}
		if row.Kind == "copied" {
			g.imports[row.Source] = append(g.imports[row.Source], row)
		}
	}
	for k, r := range g.seen {
		g.seen[k] = redoNormalize(r)
	}
	for source := range g.imports {
		sort.SliceStable(g.imports[source], func(i, j int) bool { return g.imports[source][i].First < g.imports[source][j].First })
		g.reindexImports(source, 0)
	}
	return g
}
func redoBatchContext(ctx context.Context, names []string, ids []int64) context.Context {
	b := &syncRedoBatch{ids: map[string]int64{}, reasons: map[string]string{}}
	for i, n := range names {
		if i < len(ids) {
			b.ids[n] = ids[i]
		}
	}
	return context.WithValue(ctx, syncRedoContextKey{}, b)
}
func redoReason(ctx context.Context, name, reason string) {
	if b, ok := ctx.Value(syncRedoContextKey{}).(*syncRedoBatch); ok {
		b.mu.Lock()
		b.reasons[name] = reason
		b.mu.Unlock()
	}
}
func redoContains(r []syncRedoRange, id int64) bool {
	i := sort.Search(len(r), func(i int) bool { return r[i].Last >= id })
	return i < len(r) && r[i].First <= id
}
func redoMerge(r []syncRedoRange, add syncRedoRange) []syncRedoRange {
	i := sort.Search(len(r), func(i int) bool { return r[i].Last+1 >= add.First })
	end := i
	for end < len(r) && r[end].First <= add.Last+1 {
		add.First = min(add.First, r[end].First)
		add.Last = max(add.Last, r[end].Last)
		end++
	}
	if i == end {
		r = append(r, syncRedoRange{})
		copy(r[i+1:], r[i:])
		r[i] = add
		return r
	}
	r[i] = add
	copy(r[i+1:], r[end:])
	return r[:len(r)-(end-i)+1]
}

// redoReplayBatch bounds unmerged replay ranges per key before a merge.
const redoReplayBatch = 1 << 16

// redoNormalize returns the same union redoMerge builds incrementally: sorted,
// disjoint ranges with overlapping and adjacent ranges joined. It reuses r.
func redoNormalize(r []syncRedoRange) []syncRedoRange {
	sort.Slice(r, func(i, j int) bool { return r[i].First < r[j].First })
	out := r[:0]
	for _, x := range r {
		if n := len(out); n > 0 && x.First <= out[n-1].Last+1 {
			out[n-1].Last = max(out[n-1].Last, x.Last)
			continue
		}
		out = append(out, x)
	}
	return out
}
func redoRuns(ids []int64) []syncRedoRange {
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	var out []syncRedoRange
	for _, id := range ids {
		out = redoMerge(out, syncRedoRange{id, id})
	}
	return out
}
func (g *syncRedoGuard) remember(w syncRedoWork) {
	for _, k := range w.Keys {
		for _, r := range w.IDs {
			g.seen[k] = redoMerge(g.seen[k], r)
		}
	}
}

// Called before an actual backend operation, and only committed after success.
func (g *syncRedoGuard) begin(ctx context.Context, phase, store string, names []string, from, until int64) (func(bool), error) {
	if g == nil {
		return func(bool) {}, nil
	}
	b, ok := ctx.Value(syncRedoContextKey{}).(*syncRedoBatch)
	if !ok {
		return func(bool) {}, nil
	}
	b.mu.Lock()
	groups := map[string][]int64{}
	for _, n := range names {
		if id, ok := b.ids[n]; ok {
			groups[b.reasons[n]] = append(groups[b.reasons[n]], id)
		}
	}
	b.mu.Unlock()
	g.mu.Lock()
	var repeated int
	k := redoKey(phase, store, from, until)
	for _, id := range groups[""] {
		if redoContains(g.seen[k], id) {
			repeated++
		}
	}
	g.mu.Unlock()
	if repeated > 0 {
		if err := g.report(phase, store, from, until, repeated); err != nil {
			return nil, err
		}
	}
	return func(success bool) {
		if !success {
			return
		}
		var work []syncRedoWork
		for reason, ids := range groups {
			work = append(work, syncRedoWork{Keys: []uint64{k}, IDs: redoRuns(ids), Reason: reason})
		}
		g.mu.Lock()
		for _, w := range work {
			g.remember(w)
		}
		g.mu.Unlock()
		b.mu.Lock()
		b.work = append(b.work, work...)
		b.mu.Unlock()
	}, nil
}
func (g *syncRedoGuard) report(phase, store string, from, until int64, n int) error {
	reason := "completed backend work repeated without recorded invalidation"
	event("sync_redo", map[string]any{"level": "error", "phase": phase, "store": store, "from": from, "until": until, "redo_names": n, "reason": reason})
	return g.j.append(syncRow{Kind: "redo", Phase: phase, Source: store, Names: n})
}
func (s *syncer) redoImport(ctx context.Context, source string, names []string, phase string) error {
	if s.redo == nil {
		return nil
	}
	// A fill is explicitly driven by newly observed holes. That observation is
	// already durable in verified rows; record the cause with the copy receipt.
	if phase == "fill" {
		event("sync_work_invalidated", map[string]any{"phase": "import", "reason": "verified_holes", "source": source, "names": len(names)})
		return nil
	}
	if s.j.checkpoint != nil {
		// This authenticated checkpoint is only created after every initial copy
		// completed successfully. Any later initial-copy call is repeated work;
		// fresh repairs must be driven by a new hole observation through fill.
		return s.redo.report("import", source, 0, 0, len(names))
	}
	s.redo.mu.Lock()
	rows := s.redo.imports[source]
	repeated := 0
	for _, n := range names {
		end := sort.Search(len(rows), func(i int) bool { return rows[i].First > n })
		start := sort.Search(len(s.redo.importMax[source]), func(i int) bool { return s.redo.importMax[source][i] >= n })
		for i := end - 1; i >= start; i-- {
			r := rows[i]
			if r.Last < n {
				continue
			}
			selected := r.Selected
			at := sort.SearchStrings(selected, n)
			if (at < len(selected) && selected[at] == n || r.Names == 1 && r.First == n) && !r.Forbidden {
				failed := false
				for _, f := range r.Failed {
					if f == n {
						failed = true
						break
					}
				}
				for _, x := range r.Resolved {
					if x.Name == n {
						failed = true
						break
					}
				}
				if !failed {
					repeated++
					break
				}
			}
		}
	}
	s.redo.mu.Unlock()
	if repeated > 0 {
		return s.redo.report("import", source, 0, 0, repeated)
	}
	return nil
}
func (s *syncer) rememberRedoImport(row syncRow) {
	if s.redo == nil {
		return
	}
	s.redo.mu.Lock()
	defer s.redo.mu.Unlock()
	rows := s.redo.imports[row.Source]
	at := sort.Search(len(rows), func(i int) bool { return rows[i].First >= row.First })
	rows = append(rows, syncRow{})
	copy(rows[at+1:], rows[at:])
	rows[at] = row
	s.redo.imports[row.Source] = rows
	s.redo.reindexImports(row.Source, at)
}
func cloneRedoCounts(in map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
func syncRedoCounts(state string) (map[string]int64, error) {
	counts := map[string]int64{}
	data, err := os.ReadFile(filepath.Join(state, "progress.json"))
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if err == nil {
		var p progressSnapshot
		if err = json.Unmarshal(data, &p); err != nil {
			return nil, err
		}
		counts = cloneRedoCounts(p.RedoNames)
	}
	entries, err := os.ReadDir(state)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "ready-recovery-") && !strings.HasPrefix(entry.Name(), "ready-fill-") && !strings.HasPrefix(entry.Name(), "ready-proof-") {
			continue
		}
		child, err := syncRedoCounts(filepath.Join(state, entry.Name()))
		if err != nil {
			return nil, err
		}
		for phase, n := range child {
			counts[phase] += n
		}
	}
	return counts, nil
}

// A missing primary journal alongside durable artifacts is evidence of lost
// state, never permission to silently rediscover and copy the same scope.
func (c *syncCommand) prepareStateIntent() error {
	entries, err := os.ReadDir(c.sync.state)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	existing := false
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".lock") {
			existing = true
			break
		}
	}
	if !existing {
		return nil
	}
	if c.freshReason != "" {
		if c.resuming || c.legacyJournal {
			return errors.New("--allow-fresh-start requires directory --state without --resume")
		}
		parent := c.sync.state
		next, err := os.MkdirTemp(parent, "fresh-")
		if err != nil {
			return err
		}
		c.sync.state, c.copy.state = next, next
		c.config.Flags["state"] = next
		event("sync_fresh_start", map[string]any{"level": "warn", "reason": c.freshReason, "existing_state": parent, "state": next, "prior_state_preserved": true})
		return nil
	}
	if selected, err := c.selectReadyScope(); err != nil {
		return err
	} else if selected {
		return nil
	}
	if !c.resuming {
		return errors.New("state already contains work; use --resume, or --allow-fresh-start=REASON to create a separate run preserving prior state")
	}
	st, err := os.Stat(filepath.Join(c.sync.state, "sync.jsonl"))
	if err == nil && st.Size() > 0 {
		return nil
	}
	// A config-only directory can be the first startup interrupted before work.
	// Any other artifact means the missing journal must fail closed.
	for _, e := range entries {
		if e.Name() != "config.json" && e.Name() != "scope-parent.json" && !strings.HasPrefix(e.Name(), "config-") && !strings.HasSuffix(e.Name(), ".lock") {
			return fmt.Errorf("lost sync journal in existing state %s; refusing silent fresh start (artifact %s)", c.sync.state, e.Name())
		}
	}
	return nil
}

func (g *syncRedoGuard) reindexImports(source string, from int) {
	rows := g.imports[source]
	last := g.importMax[source]
	if len(last) < len(rows) {
		last = append(last, make([]string, len(rows)-len(last))...)
	}
	for i := from; i < len(rows); i++ {
		last[i] = rows[i].Last
		if i > 0 {
			last[i] = max(last[i], last[i-1])
		}
	}
	g.importMax[source] = last
}
func redoWithout(r []syncRedoRange, ids []int64) []syncRedoRange {
	out := append([]syncRedoRange(nil), r...)
	for _, id := range ids {
		at := sort.Search(len(out), func(i int) bool { return out[i].Last >= id })
		if at == len(out) || out[at].First > id {
			continue
		}
		hit := out[at]
		switch {
		case hit.First == id && hit.Last == id:
			out = append(out[:at], out[at+1:]...)
		case hit.First == id:
			out[at].First++
		case hit.Last == id:
			out[at].Last--
		default:
			out[at].Last = id - 1
			out = append(out, syncRedoRange{})
			copy(out[at+2:], out[at+1:])
			out[at+1] = syncRedoRange{id + 1, hit.Last}
		}
	}
	return out
}

// Unresolved names are pending, so their reads cannot become clean receipts.
func (g *syncRedoGuard) cleanWork(ctx context.Context, row syncRow) []syncRedoWork {
	if g == nil {
		return nil
	}
	b, ok := ctx.Value(syncRedoContextKey{}).(*syncRedoBatch)
	if !ok {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	var dirty []int64
	add := func(n string) {
		if id, ok := b.ids[n]; ok {
			dirty = append(dirty, id)
		}
	}
	for _, h := range row.Holes {
		add(h.Name)
	}
	for _, n := range row.Unindexed {
		add(n)
	}
	for _, n := range row.Short {
		add(n)
	}
	for _, x := range row.Skipped {
		add(x.Name)
	}
	for _, x := range row.Resolved {
		add(x.Name)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for k, r := range g.seen {
		g.seen[k] = redoWithout(r, dirty)
	}
	var work []syncRedoWork
	for _, w := range b.work {
		w.IDs = redoWithout(w.IDs, dirty)
		if len(w.IDs) > 0 {
			merged := false
			for i := range work {
				if work[i].Reason == w.Reason && reflect.DeepEqual(work[i].IDs, w.IDs) {
					work[i].Keys = append(work[i].Keys, w.Keys...)
					merged = true
					break
				}
			}
			if !merged {
				work = append(work, w)
			}
		}
	}
	for i := range work {
		sort.Slice(work[i].Keys, func(a, b int) bool { return work[i].Keys[a] < work[i].Keys[b] })
	}
	sort.Slice(work, func(a, b int) bool {
		x, y := work[a], work[b]
		if x.Reason != y.Reason {
			return x.Reason < y.Reason
		}
		for i := 0; i < min(len(x.IDs), len(y.IDs)); i++ {
			if x.IDs[i].First != y.IDs[i].First {
				return x.IDs[i].First < y.IDs[i].First
			}
			if x.IDs[i].Last != y.IDs[i].Last {
				return x.IDs[i].Last < y.IDs[i].Last
			}
		}
		if len(x.IDs) != len(y.IDs) {
			return len(x.IDs) < len(y.IDs)
		}
		for i := 0; i < min(len(x.Keys), len(y.Keys)); i++ {
			if x.Keys[i] != y.Keys[i] {
				return x.Keys[i] < y.Keys[i]
			}
		}
		return len(x.Keys) < len(y.Keys)
	})
	return work
}

// Reload each controller's own counters, including completed READY children,
// so a client replacement does not reset the monitored unit total.
func (p *progressMetrics) restoreRedoMetrics(state string) {
	if p.redoByPath == nil {
		p.redoByPath = map[string]map[string]int64{}
	}
	if p.redoNames == nil {
		p.redoNames = map[string]int64{}
	}
	var visit func(string)
	visit = func(dir string) {
		path := filepath.Join(dir, "progress.json")
		if data, err := os.ReadFile(path); err == nil {
			var snap progressSnapshot
			if json.Unmarshal(data, &snap) == nil {
				p.redoByPath[metricStatePath(path)] = cloneRedoCounts(snap.RedoNames)
			}
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			return
		}
		for _, e := range entries {
			if e.IsDir() && (strings.HasPrefix(e.Name(), "ready-recovery-") || strings.HasPrefix(e.Name(), "ready-fill-") || strings.HasPrefix(e.Name(), "ready-proof-")) {
				visit(filepath.Join(dir, e.Name()))
			}
		}
	}
	visit(state)
	visit(state + ".sync")
	for _, counts := range p.redoByPath {
		for phase, n := range counts {
			p.redoNames[phase] += n
		}
	}
}
