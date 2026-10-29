package chunkstore

import (
	"context"
	"errors"
	"strings"
	"sync"
)

const catalogChangeBytes = 8 << 20

type catalogChange struct {
	version uint64
	name    string
}

// Discovery needs changes to names and policies, not point revisions. Keep a
// bounded in-process journal; restart or overflow requires one catalog scan.
type catalogChanges struct {
	mu             sync.Mutex
	version, floor uint64
	bytes          int
	entries        []catalogChange
}

func (s *Store) catalogChanged(name string) {
	c := s.catalogChanges.Load()
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.version++
	if c.bytes+len(name)+32 > catalogChangeBytes {
		c.entries, c.bytes, c.floor = nil, 0, c.version
	}
	if len(name)+32 <= catalogChangeBytes {
		c.entries = append(c.entries, catalogChange{c.version, strings.Clone(name)})
		c.bytes += len(name) + 32
	}
}

func (s *Store) resetCatalogChanges() {
	if c := s.catalogChanges.Load(); c != nil {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.version++
		c.floor = c.version
		c.entries, c.bytes = nil, 0
	}
}

// CatalogChanges returns committed metadata changed since after. A reset asks
// the caller to scan the catalog and retain the returned cursor. Events follow
// successful commits under the metric stripe. Missing names conservatively
// require a scan; ordinary point updates do not enter this journal.
func (s *Store) CatalogChanges(ctx context.Context, after uint64) (entries []Metadata, cursor uint64, reset bool, err error) {
	c := s.catalogChanges.Load()
	if c == nil {
		s.catalogChanges.CompareAndSwap(nil, &catalogChanges{version: 1, floor: 1})
		c = s.catalogChanges.Load()
	}
	c.mu.Lock()
	cursor = c.version
	reset = after < c.floor || after > cursor
	var names []string
	if !reset {
		seen := make(map[string]struct{})
		for i := len(c.entries) - 1; i >= 0 && c.entries[i].version > after; i-- {
			name := c.entries[i].name
			if _, ok := seen[name]; !ok {
				names = append(names, name)
				seen[name] = struct{}{}
			}
		}
	}
	c.mu.Unlock()
	for _, name := range names {
		m, readErr := s.Metadata(ctx, name)
		if errors.Is(readErr, ErrNotFound) {
			return nil, cursor, true, nil
		}
		if readErr != nil {
			return nil, after, false, readErr
		}
		entries = append(entries, m)
	}
	return entries, cursor, reset, ctx.Err()
}
