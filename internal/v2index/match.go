package v2index

import (
	"context"
	"path/filepath"
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
	c := s.seek(prefix)
	var path strings.Builder
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		name, level := c.peek()
		if level < 0 || len(name) < len(prefix) || string(name[:len(prefix)]) != prefix {
			break
		}
		// Path separators are exactly the name's dots, so the directory at
		// this depth ends at the depth-th dot.
		cut, seen := -1, 0
		for j, b := range name {
			if b == '.' {
				seen++
				if seen == depth {
					cut = j
					break
				}
			}
		}
		end := name
		if cut >= 0 {
			end = name[:cut]
		}
		path.Reset()
		path.Grow(len(end) + 5)
		path.WriteByte('/')
		for _, b := range end {
			if b == '.' {
				b = '/'
			}
			path.WriteByte(b)
		}
		if cut < 0 {
			path.WriteString(".wsp")
			c.advance(level)
		} else {
			// Every descendant of this directory produces the same candidate path.
			directory := string(end) + "/" // '.' + 1, exclusive prefix end
			c.advance(level)
			c.skipTo(directory)
		}
		if err := yield(path.String()); err != nil {
			return err
		}
	}
	return ctx.Err()
}
