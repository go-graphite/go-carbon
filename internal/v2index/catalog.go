// Package v2index publishes immutable live-name views for carbonserver discovery.
// It is a projection, not an ID allocator, durable catalog or expiry authority.
package v2index

import (
	"errors"
	"iter"
	"slices"
	"strings"
	"sync/atomic"
)

// Source returns a detached, immutable host-wide union of live catalog names.
// Revisions increase on every membership change, including expiry/recreation.
// Implementations must not expose mutable producer-local NameCatalog state.
type Source interface{ Snapshot() *Snapshot }

// Snapshot may be held by readers after the source publishes a replacement.
type Snapshot struct {
	revision uint64
	names    []string
}

func (s *Snapshot) Revision() uint64        { return s.revision }
func (s *Snapshot) Len() int                { return len(s.names) }
func (s *Snapshot) Names() iter.Seq[string] { return slices.Values(s.names) }

// EqualNames compares a sorted, unique candidate without materializing another
// copy of this immutable view. This is used by the periodic discovery refresh.
func (s *Snapshot) EqualNames(names []string) bool { return slices.Equal(s.names, names) }

// Catalog is a single-control-owner projection; Snapshot is safe concurrently.
// Publish/Apply belong at the catalog publication boundary, never per point.
// Persisted catalog recovery must finish before attaching discovery.
type Catalog struct{ current atomic.Pointer[Snapshot] }

func (c *Catalog) Snapshot() *Snapshot {
	if s := c.current.Load(); s != nil {
		return s
	}
	return &Snapshot{}
}

func namesCopy(names []string) ([]string, error) {
	out := slices.Clone(names)
	for _, name := range out {
		// Slash/NUL and empty components do not have an unambiguous classic path.
		if name == "" || strings.ContainsAny(name, "/\x00") || strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".") || strings.Contains(name, "..") {
			return nil, errors.New("v2 index: name has no classic metric path")
		}
	}
	slices.Sort(out)
	return slices.Compact(out), nil
}

// Publish replaces all names atomically. Stale publications fail closed.
// Names and the input slice may be reused after return.
func (c *Catalog) Publish(revision uint64, names []string) error {
	old := c.Snapshot()
	if revision <= old.revision {
		return errors.New("v2 index: stale catalog revision")
	}
	next, err := namesCopy(names)
	if err != nil {
		return err
	}
	c.current.Store(&Snapshot{revision: revision, names: next})
	return nil
}

// Apply adds admitted names and removes committed expiry names in one view.
// Removal wins within a batch. The serial owner must fence stale expiry against
// recreated names before calling Apply. This projection cannot decide liveness.
func (c *Catalog) Apply(revision uint64, add, remove []string) error {
	old := c.Snapshot()
	if revision <= old.revision {
		return errors.New("v2 index: stale catalog revision")
	}
	additions, err := namesCopy(add)
	if err != nil {
		return err
	}
	deletions := slices.Clone(remove)
	slices.Sort(deletions)
	next := make([]string, 0, len(old.names)+len(additions))
	i, j, k := 0, 0, 0
	for i < len(old.names) || j < len(additions) {
		var name string
		if j == len(additions) || i < len(old.names) && old.names[i] < additions[j] {
			name = old.names[i]
			i++
		} else {
			name = additions[j]
			j++
			if i < len(old.names) && old.names[i] == name {
				i++
			}
		}
		for k < len(deletions) && deletions[k] < name {
			k++
		}
		if k == len(deletions) || deletions[k] != name {
			next = append(next, name)
		}
	}
	c.current.Store(&Snapshot{revision: revision, names: next})
	return nil
}
