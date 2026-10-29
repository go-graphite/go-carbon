package carbonserver

import (
	"bytes"
	"strings"
)

// literalChildren recognizes a literal parent followed by a single star. Walk
// its child states directly: seeking from the FST root for every namespace
// otherwise allocates decoded states for the same ancestors over and over.
// The ordinary glob compiler still validates the complete expression first.
func (s *indexSnapshot) literalChildren(a *snapshotGlob) (fstState, string, bool) {
	if s.keys == nil || len(a.components) == 0 || a.components[len(a.components)-1].expr != "*" {
		return fstState{}, "", false
	}
	state := s.keys.accept(s.keys.start(), 0)
	var prefix strings.Builder
	for _, component := range a.components[:len(a.components)-1] {
		if strings.ContainsAny(component.expr, "*?[]{}\\\x00") {
			return fstState{}, "", false
		}
		state = s.keys.accept(s.keys.acceptString(state, component.expr), 0)
		prefix.WriteString(component.expr)
		prefix.WriteByte('.')
	}
	return state, prefix.String(), true
}

func (s *indexSnapshot) queryChildren(parent fstState, prefix string, limit int) (names []string, leaves []bool, nodes []*trieNode, lookups uint32, err error) {
	paired := make(map[string]bool)
	var scratch []byte
	s.keys.namesUntil(parent, &scratch, func(entry []byte, final bool, row uint64, child fstState) bool {
		// A terminal file key precedes any descendants of that same key.
		if final && bytes.HasSuffix(entry, []byte(".wsp")) {
			lookups++
			name := prefix + string(entry[:len(entry)-4])
			if !paired[name] {
				var node *trieNode
				node, err = s.fileNode(name, row)
				if err != nil {
					return false
				}
				names, leaves, nodes = append(names, name), append(leaves, true), append(nodes, node)
				if len(names) >= limit {
					return false
				}
			}
		}
		if !child.ok {
			return true
		}
		lookups++
		name := prefix + string(entry)
		// Preserve the metric/namespace pair and its order even at the limit.
		entryState := s.keys.acceptBytes(parent, entry)
		if found, metricRow := s.keys.final(s.keys.acceptString(entryState, ".wsp")); found {
			var node *trieNode
			node, err = s.fileNode(name, metricRow)
			if err != nil {
				return false
			}
			names, leaves, nodes = append(names, name), append(leaves, true), append(nodes, node)
			paired[name] = true
		}
		names, leaves, nodes = append(names, name), append(leaves, false), append(nodes, s.directoryNode(name))
		return len(names) < limit
	})
	if err != nil {
		return nil, nil, nil, lookups, err
	}
	return names, leaves, nodes, lookups, nil
}
