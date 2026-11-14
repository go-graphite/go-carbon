package downsize

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

// Stores hold tens of millions of names, so discovery never keeps a catalog
// in memory: every store's byte-sorted catalog is paged in the background and
// k-way merged straight into union.tsv (name, source mask, on target) and
// missing.tsv (names only sources hold). Verification streams union.tsv.

// syncNameSource yields names with their source masks in byte order; next
// returns io.EOF at the end.
type syncNameSource struct {
	total int
	next  func() (string, uint64, error)
	close func()
}

func (s *syncer) unionPath() string   { return filepath.Join(s.o.state, "union.tsv") }
func (s *syncer) missingPath() string { return filepath.Join(s.o.state, "missing.tsv") }

type syncLister struct {
	pages    chan []string
	errc     chan error
	page     []string
	head     string
	done     bool
	finished chan struct{}
}

func (s *syncer) startList(ctx context.Context, store, after string) *syncLister {
	if s.o.namesFile != "" {
		return s.startScopedList(ctx, store, after)
	}
	l := &syncLister{pages: make(chan []string, 4), errc: make(chan error, 1), finished: make(chan struct{})}
	go func() {
		defer close(l.pages)
		defer close(l.finished)
		// Each store merges its catalog with the same sorted file. Keep only
		// one name in memory, even for multi-million-name copy inventories.
		var scope *bufio.Scanner
		var wanted string
		advanceScope := func() bool {
			for scope.Scan() {
				wanted = strings.TrimSpace(scope.Text())
				if wanted != "" {
					return true
				}
			}
			wanted = ""
			return false
		}
		if s.o.namesFile != "" {
			file, err := os.Open(s.o.namesFile)
			if err != nil {
				l.errc <- fmt.Errorf("--names: %w", err)
				return
			}
			defer file.Close()
			scope = bufio.NewScanner(file)
			scope.Buffer(make([]byte, 256<<10), 1<<20)
			advanceScope()
		}
		for {
			page, err := s.b.List(ctx, store, s.o.prefix, after, syncListPage)
			if err != nil {
				l.errc <- fmt.Errorf("list %s: %w", store, err)
				return
			}
			if len(page) == 0 {
				return
			}
			for i, n := range page {
				if n <= after || (i > 0 && n <= page[i-1]) {
					l.errc <- fmt.Errorf("list %s: catalog not byte-sorted at %q", store, n)
					return
				}
			}
			after = page[len(page)-1]
			if scope != nil {
				var selected []string
				for _, n := range page {
					for wanted != "" && wanted < n {
						advanceScope()
					}
					if wanted == n {
						selected = append(selected, n)
					}
				}
				if err := scope.Err(); err != nil {
					l.errc <- fmt.Errorf("--names: %w", err)
					return
				}
				page = selected
			}
			select {
			case l.pages <- page:
			case <-ctx.Done():
				return
			}
		}
	}()
	return l
}

func (l *syncLister) advance(ctx context.Context) error {
	for len(l.page) == 0 {
		var page []string
		var ok bool
		select {
		case page, ok = <-l.pages:
		case <-ctx.Done():
			return ctx.Err()
		}
		if !ok {
			select {
			case err := <-l.errc:
				return err
			default:
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			l.done = true
			return nil
		}
		l.page = page
	}
	l.head, l.page = l.page[0], l.page[1:]
	return nil
}

type syncTSV struct {
	file *os.File
	w    *bufio.Writer
}

// discover durably merges consumed catalog prefixes. A journal checkpoint binds
// per-source cursors to exact offsets in both output files; recovery drops only
// the tail written after the last committed checkpoint.
func (s *syncer) discover(ctx context.Context) error {
	if rows := s.j.find(func(r syncRow) bool { return r.Kind == "discovered" }); len(rows) > 0 {
		r := rows[0]
		s.sum.Discovered, s.sum.Missing, s.sum.NotOwned, s.sum.OnTarget, s.both = r.Names, r.Missing, r.NotOwned, r.OnTarget, r.Both
		event("sync_discover_resumed", map[string]any{"names": r.Names, "missing": r.Missing})
		return nil
	}
	s.phase("discover")
	s.prog.begin("discover", 0)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stores := append([]string{s.o.to}, s.o.from...)
	if s.o.namesFile != "" {
		stores = append(stores, "@names")
	}
	cp := syncDiscoveryCheckpoint{Cursors: map[string]string{}}
	if rows := s.j.find(func(r syncRow) bool { return r.Kind == "discover_checkpoint" }); len(rows) > 0 {
		cp = *rows[len(rows)-1].Discovery
		cp.Cursors = cloneCursors(cp.Cursors)
		event("sync_discovery_resumed", map[string]any{"names": cp.Names, "cursors": cp.Cursors})
	}
	union, err := openDurableTSV(s.unionPath(), cp.UnionBytes)
	if err != nil {
		return err
	}
	defer union.file.Close()
	missing, err := openDurableTSV(s.missingPath(), cp.MissingBytes)
	if err != nil {
		return err
	}
	defer missing.file.Close()
	names, nMissing, notOwned, onTarget, both := cp.Names, cp.Missing, cp.NotOwned, cp.OnTarget, cp.Both
	s.prog.done.Store(int64(names))
	checkpoint := func() error {
		u, err := union.durableOffset()
		if err != nil {
			return err
		}
		m, err := missing.durableOffset()
		if err != nil {
			return err
		}
		cp.UnionBytes, cp.MissingBytes = u, m
		cp.Names, cp.Missing, cp.NotOwned, cp.OnTarget, cp.Both = names, nMissing, notOwned, onTarget, both
		saved := cp
		saved.Cursors = cloneCursors(cp.Cursors)
		return s.j.append(syncRow{Kind: "discover_checkpoint", Discovery: &saved})
	}
	// Commit newly-created directory entries before any cursor can reference them.
	if err := syncDirectory(s.o.state); err != nil {
		return err
	}
	listers := make([]*syncLister, len(stores))
	defer func() {
		cancel()
		for _, l := range listers {
			if l != nil {
				<-l.finished
			}
		}
	}()
	for i, store := range stores {
		if store == "@names" {
			listers[i] = s.startExplicitNames(ctx, cp.Cursors[store])
		} else {
			listers[i] = s.startList(ctx, store, cp.Cursors[store])
		}
	}
	for _, l := range listers {
		if err := l.advance(ctx); err != nil {
			return err
		}
	}
	lastCheckpoint, processed := time.Now(), 0
	fail := func(err error) error { return errors.Join(err, checkpoint()) }
	for {
		head, found := "", false
		for _, l := range listers {
			if !l.done && (!found || l.head < head) {
				head, found = l.head, true
			}
		}
		if !found {
			break
		}
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		if processed >= syncListPage || time.Since(lastCheckpoint) >= 3*time.Second {
			if err := checkpoint(); err != nil {
				return err
			}
			lastCheckpoint, processed = time.Now(), 0
		}
		var mask uint64
		inTarget := false
		for i, l := range listers {
			if l.done || l.head != head {
				continue
			}
			if i == 0 {
				inTarget = true
			} else if i <= len(s.o.from) {
				mask |= 1 << uint(i-1)
			}
		}
		owned := inTarget || s.owner == nil || s.owner(head)
		if !owned {
			notOwned++
		} else {
			names++
			s.prog.done.Add(1)
			t := 0
			if inTarget {
				t = 1
				onTarget++
				if mask != 0 {
					both++
				}
			} else {
				nMissing++
				fmt.Fprintf(missing.w, "%s\t%x\n", head, mask)
			}
			fmt.Fprintf(union.w, "%s\t%x\t%d\n", head, mask, t)
		}
		processed++
		for i, l := range listers {
			if !l.done && l.head == head {
				cp.Cursors[stores[i]] = head
			}
		}
		// All cursors and both output rows now describe the same consumed prefix.
		for _, l := range listers {
			if !l.done && l.head == head {
				if err := l.advance(ctx); err != nil {
					return fail(err)
				}
			}
		}
	}
	if err := checkpoint(); err != nil {
		return err
	}
	s.sum.Discovered, s.sum.Missing, s.sum.NotOwned, s.sum.OnTarget, s.both = names, nMissing, notOwned, onTarget, both
	if err := s.j.append(syncRow{Kind: "discovered", Names: names, Missing: nMissing, NotOwned: notOwned, OnTarget: onTarget, Both: both}); err != nil {
		return err
	}
	event("sync_discovered", map[string]any{"names": names, "on_target": onTarget, "on_target_and_source": both, "missing": nMissing, "not_owned": notOwned})
	s.endPhase("discover", names)
	return nil
}

func parseUnionLine(line string) (name string, mask uint64, onTarget bool, err error) {
	parts := strings.Split(line, "\t")
	if len(parts) < 2 {
		return "", 0, false, fmt.Errorf("bad state line %q", line)
	}
	mask, err = strconv.ParseUint(parts[1], 16, 64)
	return parts[0], mask, len(parts) > 2 && parts[2] == "1", err
}

func openLines(path string) (*os.File, *bufio.Scanner, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 256<<10), 1<<20)
	return f, scanner, nil
}

// unionSource streams the names to verify in round 1: on the target (or
// copied by this sync) and held by at least one source.
func (s *syncer) unionSource() syncNameSource {
	var f *os.File
	var scanner *bufio.Scanner
	total := s.sum.Discovered
	if s.legacyCopyReceipts {
		total = s.both + len(s.copied)
	}
	return syncNameSource{
		total: total,
		next: func() (string, uint64, error) {
			if f == nil {
				var err error
				if f, scanner, err = openLines(s.unionPath()); err != nil {
					return "", 0, err
				}
			}
			for scanner.Scan() {
				name, mask, onTarget, err := parseUnionLine(scanner.Text())
				if err != nil {
					return "", 0, err
				}
				if s.legacyCopyReceipts && (mask == 0 || (!onTarget && !s.copied[name])) {
					continue
				}
				return name, mask, nil
			}
			if err := scanner.Err(); err != nil {
				return "", 0, err
			}
			return "", 0, io.EOF
		},
		close: func() {
			if f != nil {
				_ = f.Close()
			}
		},
	}
}

// setSource yields a small re-verify set with masks read back from union.tsv.
func (s *syncer) setSource(set []string) (syncNameSource, error) {
	want := make(map[string]bool, len(set))
	for _, n := range set {
		want[n] = true
	}
	f, scanner, err := openLines(s.unionPath())
	if err != nil {
		return syncNameSource{}, err
	}
	defer f.Close()
	type entry struct {
		name string
		mask uint64
	}
	var picked []entry
	for scanner.Scan() && len(picked) < len(want) {
		name, mask, _, err := parseUnionLine(scanner.Text())
		if err != nil {
			return syncNameSource{}, err
		}
		if want[name] {
			picked = append(picked, entry{name, mask})
		}
	}
	if err := scanner.Err(); err != nil {
		return syncNameSource{}, err
	}
	i := 0
	return syncNameSource{total: len(picked), next: func() (string, uint64, error) {
		if i >= len(picked) {
			return "", 0, io.EOF
		}
		i++
		return picked[i-1].name, picked[i-1].mask, nil
	}, close: func() {}}, nil
}

type syncMissing struct {
	name string
	mask uint64
}

func (s *syncer) readMissing() ([]syncMissing, error) {
	f, scanner, err := openLines(s.missingPath())
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []syncMissing
	for scanner.Scan() {
		name, mask, _, err := parseUnionLine(scanner.Text())
		if err != nil {
			return nil, err
		}
		out = append(out, syncMissing{name, mask})
	}
	return out, scanner.Err()
}

func syncShort(archives []syncArchive) bool {
	if len(archives) == 0 {
		return false
	}
	for _, a := range archives {
		if a.Step*a.Points > syncShortRetention {
			return false
		}
	}
	return true
}

// firstSource is the first holder in --from order that is not forbidden, or
// -1.
func firstSource(mask uint64, forbid map[string]bool, from []string) int {
	for i := range from {
		if mask&(1<<uint(i)) != 0 && !forbid[from[i]] {
			return i
		}
	}
	return -1
}

// dropShort checks bounded parallel batches and journals each completed batch.
// Missing source metadata is distinct from short retention, so another source
// can still supply a name that disappeared since discovery.
func (s *syncer) dropShort(ctx context.Context, src int, names []string) (keep, short, absent []string, err error) {
	type key struct {
		first, last string
		count       int
	}
	cached := map[key]syncRow{}
	for _, r := range s.j.find(func(r syncRow) bool { return r.Kind == "prechecked" && r.Source == s.o.from[src] }) {
		cached[key{r.First, r.Last, r.Names}] = r
	}
	count := (len(names) + s.o.batch - 1) / s.o.batch
	rows := make([]syncRow, count)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int)
	var wg sync.WaitGroup
	var once sync.Once
	var firstErr error
	fail := func(e error) { once.Do(func() { firstErr = e; cancel() }) }
	for worker := 0; worker < min(s.o.streams, count); worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				chunk := names[i*s.o.batch : min((i+1)*s.o.batch, len(names))]
				k := key{chunk[0], chunk[len(chunk)-1], len(chunk)}
				if r, ok := cached[k]; ok {
					rows[i] = r
					s.prog.done.Add(int64(len(chunk)))
					continue
				}
				// Over-limit legacy names cannot be imported. Leave them in keep
				// for the copier to journal as failures, without metadata I/O.
				var valid []string
				for _, n := range chunk {
					if chunkstore.ValidateMetricName(n) == nil {
						valid = append(valid, n)
					}
				}
				info := map[string][]syncArchive{}
				var e error
				if len(valid) > 0 {
					info, e = s.b.Info(ctx, s.o.from[src], valid)
				}
				if e != nil {
					fail(e)
					return
				}
				r := syncRow{Kind: "prechecked", Source: s.o.from[src], First: k.first, Last: k.last, Names: k.count}
				for _, n := range chunk {
					if chunkstore.ValidateMetricName(n) != nil {
						continue
					} else if _, ok := info[n]; !ok {
						r.Absent = append(r.Absent, n)
					} else if !s.o.includeShort && syncShort(info[n]) {
						r.Short = append(r.Short, n)
					}
				}
				if e := s.j.append(r); e != nil {
					fail(e)
					return
				}
				rows[i] = r
				s.prog.done.Add(int64(len(chunk)))
			}
		}()
	}
feed:
	for i := range rows {
		select {
		case jobs <- i:
		case <-ctx.Done():
			break feed
		}
	}
	close(jobs)
	wg.Wait()
	if firstErr != nil {
		return nil, nil, nil, firstErr
	}
	if ctx.Err() != nil {
		return nil, nil, nil, ctx.Err()
	}
	for i, r := range rows {
		skip := map[string]bool{}
		for _, n := range r.Short {
			skip[n] = true
			short = append(short, n)
		}
		for _, n := range r.Absent {
			skip[n] = true
			absent = append(absent, n)
		}
		for _, n := range names[i*s.o.batch : min((i+1)*s.o.batch, len(names))] {
			if !skip[n] {
				keep = append(keep, n)
			}
		}
	}
	return keep, short, absent, nil
}

func (s *syncer) copyMissing(ctx context.Context) error {
	// Older window campaigns recorded full-history imports with the same
	// per-source receipts as non-window campaigns. Replay that protocol for
	// their lifetime so switching transports cannot repeat completed imports.
	legacy := len(s.j.find(func(r syncRow) bool {
		return r.Kind == "copied" && (r.Phase == "missing" || r.Phase == "missing-batch")
	})) > 0
	s.legacyCopyReceipts = s.windowOnly() && legacy
	if s.windowOnly() && !legacy {
		return s.copyMissingWindow(ctx)
	}
	missing, err := s.readMissing()
	if err != nil {
		return err
	}
	done := map[string]syncRow{}
	for _, r := range s.j.find(func(r syncRow) bool { return r.Kind == "copied" && r.Phase == "missing" }) {
		done[r.Source] = r
	}
	s.phase("copy")
	s.prog.begin("copy", len(missing))
	short := map[string]bool{}
	work := 0
	for src, source := range s.o.from {
		var names []string
		for _, m := range missing {
			if !s.copied[m.name] && !short[m.name] && !s.failedCp[m.name] && firstSource(m.mask, s.forbid, s.o.from) == src {
				names = append(names, m.name)
			}
		}
		if r, ok := done[source]; ok {
			for _, n := range r.Short {
				short[n] = true
			}
			s.applyCopy(names, r.Short, r.Copied, r.Failed, r.Forbidden)
			continue
		}
		if len(names) == 0 {
			continue
		}
		// Batch widths depend on the stream budget, which may change on resume.
		// Recover old receipts by their sorted name bounds before making new waves.
		for _, r := range s.j.find(func(r syncRow) bool {
			return r.Kind == "copied" && r.Phase == "missing-batch" && r.Source == source
		}) {
			first := sort.SearchStrings(names, r.First)
			last := sort.Search(len(names), func(i int) bool { return names[i] > r.Last })
			if first >= last {
				continue
			}
			batch := names[first:last]
			shortNames := map[string]bool{}
			for _, n := range r.Short {
				shortNames[n] = true
			}
			count := 0
			for _, n := range batch {
				if !shortNames[n] {
					count++
				}
			}
			// A precheck may have omitted absent names inside the bounds. Do not
			// credit an ambiguous range without an exact accounting of its names.
			if count != r.Names {
				continue
			}
			for _, n := range r.Short {
				short[n] = true
			}
			s.applyCopy(batch, r.Short, r.Copied, r.Failed, r.Forbidden)
		}
		remaining := names[:0]
		for _, n := range names {
			if !s.copied[n] && !short[n] && !s.failedCp[n] {
				remaining = append(remaining, n)
			}
		}
		names = remaining
		absentSource := map[string]bool{}
		// Copy each bounded wave as soon as its precheck finishes. Preparation
		// for a huge catalog must not delay all imports until the last name.
		wave := s.o.batch * s.o.streams
		for start, group := 0, 1; start < len(names); start, group = start+wave, group+1 {
			chunk := names[start:min(start+wave, len(names))]
			s.prog.begin("precheck", len(chunk))
			checkedAt := time.Now()
			keep, shortNames, absent, err := s.dropShort(ctx, src, chunk)
			if err != nil {
				return err
			}
			for _, n := range shortNames {
				short[n] = true
			}
			for _, n := range absent {
				absentSource[n] = true
			}
			event("sync_prechecked", map[string]any{"source": source, "names": len(chunk), "keep": len(keep), "short": len(shortNames), "absent": len(absent), "group": group, "seconds": time.Since(checkedAt).Seconds(), "names_per_second": float64(len(chunk)) / time.Since(checkedAt).Seconds()})
			s.prog.begin("copy", len(keep))
			res := syncCopyResult{}
			completed := s.j.find(func(r syncRow) bool {
				return r.Kind == "copied" && r.Phase == "missing-batch" && r.Source == source && r.Round == group && len(keep) > 0 && r.Names == len(keep) && r.First == keep[0] && r.Last == keep[len(keep)-1]
			})
			if len(completed) > 0 {
				r := completed[len(completed)-1]
				res = syncCopyResult{Copied: r.Copied, Failed: r.Failed, Forbidden: r.Forbidden}
			} else if len(keep) > 0 {
				if res, err = s.copyFrom(ctx, "missing-batch", group, src, keep, shortNames...); err != nil {
					return err
				}
			}
			s.applyCopy(keep, nil, res.Copied, res.Failed, res.Forbidden)
			work += len(chunk)
			s.prog.done.Store(int64(len(keep)))
			if res.Forbidden {
				break
			}
		}
		for i := range missing {
			if absentSource[missing[i].name] {
				missing[i].mask &^= 1 << uint(src)
			}
		}
	}
	s.sum.Short = len(short)
	for _, m := range missing {
		if !s.copied[m.name] && !short[m.name] && !s.failedCp[m.name] && firstSource(m.mask, s.forbid, s.o.from) < 0 {
			reason := "missing_only_on_forbidden_source"
			if m.mask == 0 {
				continue // Fresh all-source verification resolves stale catalogs below.
			}
			s.addResidual(syncHole{Name: m.name, Source: s.sourcesOf(m.mask)}, reason)
		}
	}
	s.endPhase("copy", work)
	return nil
}

func (s *syncer) applyCopy(names, short []string, copied int, failed []string, forbidden bool) {
	if forbidden {
		return
	}
	skip := map[string]bool{}
	for _, n := range short {
		skip[n] = true
	}
	for _, n := range failed {
		skip[n] = true
		s.failedCp[n] = true
		s.addResidual(syncHole{Name: n}, "copy_failed")
	}
	for _, n := range names {
		if !skip[n] {
			s.copied[n] = true
		}
	}
	s.sum.Copied += copied
	s.sum.CopyFailed += len(failed)
}

// sortedKeys is a helper for deterministic output.
func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Explicit scope names are discovery candidates even when every catalog omits
// them. Their synthetic cursor carries no source bit and makes absence durable.
func (s *syncer) startExplicitNames(ctx context.Context, after string) *syncLister {
	l := &syncLister{pages: make(chan []string, 4), errc: make(chan error, 1), finished: make(chan struct{})}
	go func() {
		defer close(l.pages)
		defer close(l.finished)
		f, scan, err := openLines(s.o.namesFile)
		if err != nil {
			l.errc <- err
			return
		}
		defer f.Close()
		var page []string
		send := func() bool {
			select {
			case l.pages <- page:
				page = nil
				return true
			case <-ctx.Done():
				return false
			}
		}
		for scan.Scan() {
			n := strings.TrimSpace(scan.Text())
			if n <= after || !strings.HasPrefix(n, s.o.prefix) {
				continue
			}
			page = append(page, n)
			if len(page) == syncListPage && !send() {
				return
			}
		}
		if err := scan.Err(); err != nil {
			l.errc <- err
			return
		}
		if len(page) > 0 {
			send()
		}
	}()
	return l
}

// Explicit inventories use bounded metadata existence probes rather than walking
// an unrelated multi-million-name catalog. The synthetic lister retains names
// absent from every probe, so absence still requires the verifier's full proof.
func (s *syncer) startScopedList(ctx context.Context, store, after string) *syncLister {
	l := &syncLister{pages: make(chan []string, 4), errc: make(chan error, 1), finished: make(chan struct{})}
	go func() {
		defer close(l.pages)
		defer close(l.finished)
		f, scan, err := openLines(s.o.namesFile)
		if err != nil {
			l.errc <- err
			return
		}
		defer f.Close()
		// Each store gets a bounded share of the stream budget. A cohort keeps
		// memory bounded and is emitted in input order despite concurrent reads.
		parallel := max(1, min(4, s.o.streams/(len(s.o.from)+1)))
		var batch []string
		flush := func() bool {
			if len(batch) == 0 {
				return true
			}
			pages := make([][]string, (len(batch)+499)/500)
			errs := make([]error, len(pages))
			var wg sync.WaitGroup
			for i := range pages {
				names := batch[i*500 : min(len(batch), (i+1)*500)]
				wg.Go(func() {
					// Discovery needs authoritative membership, not point equality.
					present, err := s.b.Info(ctx, store, names)
					errs[i] = err
					for _, n := range names {
						if _, found := present[n]; found {
							pages[i] = append(pages[i], n)
						}
					}
				})
			}
			wg.Wait()
			if err := errors.Join(errs...); err != nil {
				l.errc <- err
				return false
			}
			batch = nil
			for _, page := range pages {
				select {
				case l.pages <- page:
				case <-ctx.Done():
					return false
				}
			}
			return true
		}
		for scan.Scan() {
			n := strings.TrimSpace(scan.Text())
			if n > after && strings.HasPrefix(n, s.o.prefix) {
				batch = append(batch, n)
				if len(batch) >= 500*parallel && !flush() {
					return
				}
			}
		}
		if err := scan.Err(); err != nil {
			l.errc <- err
			return
		}
		flush()
	}()
	return l
}
