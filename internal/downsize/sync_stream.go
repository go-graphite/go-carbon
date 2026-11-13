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
	pages chan []string
	errc  chan error
	page  []string
	head  string
	done  bool
}

func (s *syncer) startList(ctx context.Context, store string) *syncLister {
	l := &syncLister{pages: make(chan []string, 4), errc: make(chan error, 1)}
	go func() {
		defer close(l.pages)
		after := ""
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
		page, ok := <-l.pages
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
	path string
	file *os.File
	w    *bufio.Writer
}

func createTSV(path string) (*syncTSV, error) {
	f, err := os.Create(path + ".tmp")
	if err != nil {
		return nil, err
	}
	return &syncTSV{path: path, file: f, w: bufio.NewWriterSize(f, 1<<20)}, nil
}

func (t *syncTSV) commit() error {
	if err := errors.Join(t.w.Flush(), t.file.Sync(), t.file.Close()); err != nil {
		return err
	}
	return os.Rename(t.path+".tmp", t.path)
}

func (t *syncTSV) abort() { _ = t.file.Close(); _ = os.Remove(t.path + ".tmp") }

// discover merges the catalogs. The files are renamed into place before the
// journal row, so a resumed run reuses them instead of relisting.
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
	listers := make([]*syncLister, len(stores))
	for i, store := range stores {
		listers[i] = s.startList(ctx, store)
	}
	for _, l := range listers {
		if err := l.advance(ctx); err != nil {
			return err
		}
	}
	union, err := createTSV(s.unionPath())
	if err != nil {
		return err
	}
	missing, err := createTSV(s.missingPath())
	if err != nil {
		union.abort()
		return err
	}
	fail := func(err error) error { union.abort(); missing.abort(); return err }
	names, nMissing, notOwned, onTarget, both := 0, 0, 0, 0, 0
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
		var mask uint64
		inTarget := false
		for i, l := range listers {
			if l.done || l.head != head {
				continue
			}
			if i == 0 {
				inTarget = true
			} else {
				mask |= 1 << uint(i-1)
			}
			if err := l.advance(ctx); err != nil {
				return fail(err)
			}
		}
		if !inTarget && s.owner != nil && !s.owner(head) {
			notOwned++
			continue
		}
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
	if err := union.commit(); err != nil {
		missing.abort()
		return err
	}
	if err := missing.commit(); err != nil {
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
	return syncNameSource{
		total: s.both + len(s.copied),
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
				if mask != 0 && (onTarget || s.copied[name]) {
					return name, mask, nil
				}
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

// dropShort removes names whose every archive keeps at most 3 days, by the
// holder's retentions. Names the holder does not index yet are kept.
func (s *syncer) dropShort(ctx context.Context, src int, names []string) (keep, short []string, err error) {
	for start := 0; start < len(names); start += s.o.batch {
		chunk := names[start:min(start+s.o.batch, len(names))]
		info, err := s.b.Info(ctx, s.o.from[src], chunk)
		if err != nil {
			return nil, nil, fmt.Errorf("info %s: %w", s.o.from[src], err)
		}
		for _, n := range chunk {
			if syncShort(info[n]) {
				short = append(short, n)
			} else {
				keep = append(keep, n)
			}
		}
	}
	return keep, short, nil
}

func (s *syncer) copyMissing(ctx context.Context) error {
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
		keep, shortNames, err := s.dropShort(ctx, src, names)
		if err != nil {
			return err
		}
		for _, n := range shortNames {
			short[n] = true
		}
		res := syncCopyResult{}
		if len(keep) > 0 {
			if res, err = s.copyFrom(ctx, "missing", 0, src, keep, shortNames...); err != nil {
				return err
			}
		} else if err := s.j.append(syncRow{Kind: "copied", Phase: "missing", Source: source, Short: shortNames}); err != nil {
			return err
		}
		s.applyCopy(names, shortNames, res.Copied, res.Failed, res.Forbidden)
		work += len(names)
		s.prog.done.Add(int64(len(names)))
	}
	s.sum.Short = len(short)
	for _, m := range missing {
		if !s.copied[m.name] && !short[m.name] && !s.failedCp[m.name] && firstSource(m.mask, s.forbid, s.o.from) < 0 {
			s.addResidual(syncHole{Name: m.name, Source: s.sourcesOf(m.mask)}, "missing_only_on_forbidden_source")
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
