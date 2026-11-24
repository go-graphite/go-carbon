package downsize

import (
	"container/heap"
	"os"
	"path/filepath"
	"sort"
)

// Only an entered, ordinary legacy union round receives cursor compatibility.
// New schedulers journal scope before the first observation. Upgrades may have
// appended scope/recovery rows, but cannot erase the original provenance.
func (s *syncer) adoptingLegacyRound() bool {
	if s.resume.Phase != "verify" || s.windowOnly() || s.o.readyFrontier || s.o.rescanAll {
		return false
	}
	s.j.mu.Lock()
	defer s.j.mu.Unlock()
	legacy, priorLegacy, observed, scopeSeen, union := false, false, false, false, false
	for _, r := range s.j.rows {
		if r.Kind == "legacy-round-repair" && r.Round == s.resume.Round {
			return true
		}
		if r.Kind == "verification-round-scope" {
			if r.Round == s.resume.Round && r.Phase != "union" {
				return false
			}
			scopeSeen = true
			if r.Round == s.resume.Round && r.Phase == "union" {
				union = true
			}
		}
		if r.Kind == "verified" {
			if r.Round == s.resume.Round {
				observed = true
			}
			if !scopeSeen && r.Round == s.resume.Round {
				legacy = true
			}
			if !scopeSeen && r.Round == s.resume.Round-1 {
				priorLegacy = true
			}
		}
	}
	// A kill immediately after entering the next legacy round can precede its
	// first receipt. The preceding original round still establishes provenance.
	legacy = legacy || !observed && priorLegacy && s.resume.Round > 1
	// Later legacy rounds without a frontier used sparse repair sets.
	if legacy && s.resume.Round > 1 && !union {
		_, err := os.Stat(filepath.Join(s.o.state, "verification-scope.json"))
		return err == nil
	}
	return legacy
}

type legacyRoundSpan struct {
	first, end string
	row        int
}
type legacyRoundProgress struct {
	rows     []syncRow
	spans    []legacyRoundSpan
	cursor   int
	owned    func(string) bool
	unscoped *legacyRoundProgress
}
type legacyRowHeap []int

func (h legacyRowHeap) Len() int           { return len(h) }
func (h legacyRowHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h legacyRowHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *legacyRowHeap) Push(v any)        { *h = append(*h, v.(int)) }
func (h *legacyRowHeap) Pop() any          { a := *h; v := a[len(a)-1]; *h = a[:len(a)-1]; return v }

// Receipt bounds are the old round's durable traversal cursors. Adopt their
// observations, including dirty ones, without applying a new batching/proof
// contract. Discovery's per-store cursors are NOT verification completion.
// Earliest observations own overlaps: today's recovery/verified rows cannot
// erase an old hole, skip or unindexed result. Uncovered names still run live.
// Memory is proportional to journal batches, never the catalog's name count.
func (s *syncer) legacyRoundProgress(round int) (*legacyRoundProgress, error) {
	if round != s.resume.Round || !s.adoptingLegacyRound() {
		return nil, nil
	}
	return s.legacyRoundReceipts(round, false, true)
}

// Reconstruct observations without reading stores or replaying completed fills.
func (s *syncer) legacyRoundReceipts(round int, newest, recordBoundary bool, includeScoped ...bool) (*legacyRoundProgress, error) {
	p := &legacyRoundProgress{owned: s.owner}
	scopeSeen, adopted := false, false
	original := map[string]int{}
	for _, r := range s.j.find(func(r syncRow) bool {
		return r.Kind == "verification-round-scope" || r.Kind == "legacy-round-adoption" && r.Round == round || r.Kind == "verified" && r.Round == round
	}) {
		if r.Kind == "verification-round-scope" {
			scopeSeen = true
		}
		if r.Kind == "legacy-round-adoption" && r.Round == round {
			adopted = true
		}
		// Intermediate binaries may have verified source-only batches. Their
		// bounds do not prove the interleaved target-only members were visited.
		if scopeSeen && !adopted && !(len(includeScoped) > 0 && includeScoped[0]) {
			continue
		}
		if r.Kind != "verified" || r.Round != round {
			continue
		}
		if r.Names <= 0 || r.First == "" || r.Last < r.First {
			return nil, s.receiptContradiction("invalid legacy round receipt")
		}
		if !scopeSeen && !adopted && !newest {
			// The old verifier indexed receipts by First, with the last saved
			// observation winning. Keep that rule within the original prefix.
			if i, ok := original[r.First]; ok {
				p.rows[i] = r
				continue
			}
			original[r.First] = len(p.rows)
		}
		p.rows = append(p.rows, r)
	}
	if newest {
		for i, k := 0, len(p.rows)-1; i < k; i, k = i+1, k-1 {
			p.rows[i], p.rows[k] = p.rows[k], p.rows[i]
		}
	}
	p.compileSpans(false)
	if s.owner != nil {
		p.unscoped = &legacyRoundProgress{rows: p.rows}
		p.unscoped.compileSpans(true)
	}
	if !adopted && recordBoundary {
		if err := s.j.append(syncRow{Kind: "legacy-round-adoption", Round: round}); err != nil {
			return nil, err
		}
	}
	return p, nil
}

// Ring filtering is deterministic. Two range indexes retain broad historical
// receipts for non-owned names without storing one edge per catalog name.
func (p *legacyRoundProgress) compileSpans(skipOwned bool) {
	type edge struct {
		name  string
		row   int
		start bool
	}
	var edges []edge
	for i, r := range p.rows {
		if skipOwned && r.OwnershipScoped {
			continue
		}
		// Sparse repair batches can straddle names whose newer observations
		// were already adopted. Their bounds cannot overwrite those names.
		if r.SelectionComplete {
			for _, n := range r.Selected {
				edges = append(edges, edge{n, i, true}, edge{n + "\x00", i, false})
			}
		} else {
			// Metric names contain no NUL, so Last+NUL is exclusive.
			edges = append(edges, edge{r.First, i, true}, edge{r.Last + "\x00", i, false})
		}
	}
	sort.Slice(edges, func(i, j int) bool { return edges[i].name < edges[j].name })
	active := make([]bool, len(p.rows))
	h := &legacyRowHeap{}
	prev := ""
	for i := 0; i < len(edges); {
		name := edges[i].name
		for h.Len() > 0 && !active[(*h)[0]] {
			heap.Pop(h)
		}
		if h.Len() > 0 && prev < name {
			p.spans = append(p.spans, legacyRoundSpan{prev, name, (*h)[0]})
		}
		for i < len(edges) && edges[i].name == name {
			e := edges[i]
			active[e.row] = e.start
			if e.start {
				heap.Push(h, e.row)
			}
			i++
		}
		prev = name
	}
}
func (p *legacyRoundProgress) owner(name string) int {
	if p.unscoped != nil && !p.owned(name) {
		return p.unscoped.owner(name)
	}
	i := sort.Search(len(p.spans), func(i int) bool { return p.spans[i].end > name })
	if i < len(p.spans) && p.spans[i].first <= name {
		return p.spans[i].row
	}
	return -1
}
func (p *legacyRoundProgress) contains(name string, mask ...uint64) bool {
	if p == nil {
		return false
	}
	if p.unscoped != nil && (!p.owned(name) || len(mask) > 0 && mask[0] == 0) {
		return p.unscoped.contains(name)
	}
	for p.cursor < len(p.spans) && p.spans[p.cursor].end <= name {
		p.cursor++
	}
	return p.cursor < len(p.spans) && p.spans[p.cursor].first <= name
}
func (p *legacyRoundProgress) observations(collect func(syncRow)) {
	if p == nil {
		return
	}
	for i, r := range p.rows {
		out := syncRow{}
		for _, h := range r.Holes {
			if p.owner(h.Name) == i {
				out.Holes = append(out.Holes, h)
			}
		}
		for _, n := range r.Unindexed {
			if p.owner(n) == i {
				out.Unindexed = append(out.Unindexed, n)
			}
		}
		for _, k := range r.Skipped {
			if p.owner(k.Name) == i {
				out.Skipped = append(out.Skipped, k)
			}
		}
		for _, v := range r.Resolved {
			if p.owner(v.Name) == i {
				out.Resolved = append(out.Resolved, v)
			}
		}
		collect(out)
	}
}
