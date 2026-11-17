package v2index

import (
	"context"
	"path/filepath"
	"sort"
	"strings"
)

// MatchPaths uses the sorted name projection itself as the discovery index.
// Literal prefixes seek directly into the snapshot. Directory matches skip
// their entire descendant range, so a broad find visits namespaces, not every
// metric beneath each namespace. No secondary all-name image is rebuilt.
func (s *Snapshot) MatchPaths(ctx context.Context, glob string, yield func(string) error) error {
	return s.VisitPaths(ctx, glob, func(path string) error {
		matched, err := filepath.Match("/"+glob, path)
		if err == nil && matched {
			return yield(path)
		}
		return nil
	})
}

// VisitPaths enumerates possible virtual paths for a glob without imposing a
// matcher dialect. Carbonserver reuses its configured classic matcher.
func (s *Snapshot) VisitPaths(ctx context.Context, glob string, yield func(string) error) error {
	literal := glob
	if i := strings.IndexAny(literal, "*?[\\"); i >= 0 {
		literal = literal[:i]
	}
	literal = strings.TrimSuffix(literal, ".wsp")
	prefix := strings.ReplaceAll(literal, "/", ".")
	depth := strings.Count(glob, "/") + 1
	for i := sort.SearchStrings(s.names, prefix); i < len(s.names); {
		if err := ctx.Err(); err != nil {
			return err
		}
		name := s.names[i]
		if !strings.HasPrefix(name, prefix) {
			break
		}
		path := strings.ReplaceAll(name, ".", "/") + ".wsp"
		cut, seen := -1, 0
		for j := range path {
			if path[j] == '/' {
				seen++
				if seen == depth {
					cut = j
					break
				}
			}
		}
		i++
		if cut >= 0 {
			path = path[:cut]
			// Every descendant of this directory produces the same candidate path.
			directory := strings.ReplaceAll(path, "/", ".") + "."
			end := directory[:len(directory)-1] + "/" // '.' + 1, exclusive prefix end
			i += sort.Search(len(s.names)-i, func(j int) bool { return s.names[i+j] >= end })
		}
		if err := yield("/" + path); err != nil {
			return err
		}
	}
	return ctx.Err()
}
