package carbonserver

import (
	"bytes"
	"encoding/binary"
	"errors"
	"runtime"
	"slices"
	"strings"
	"sync"

	"github.com/blevesearch/vellum"
)

// snapshotGlob uses the same Graphite compiler as the mutable trie. It matches
// both leaf .wsp paths and a namespace prefix. After a namespace match, query
// skips the whole descendant range rather than enumerating its leaf metrics.
// State 1 is reserved because vellum treats that value as a dead state.
type snapshotGlob struct {
	components  []*gmatcher
	leaf        *gmatcher
	states      []snapshotGlobState
	stateIDs    map[string]int
	ids         map[*gstate]uint32
	transitions map[uint64]int
}

type snapshotGlobState struct {
	component               int
	common, leaf, directory *gdstate
}

const (
	snapshotGlobDead        = 0
	snapshotGlobStart       = 2
	snapshotGlobDescendants = 3
)

func newSnapshotGlob(expr string, expand func([]string) ([]string, error)) (*snapshotGlob, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		expr = "*"
	}
	a := &snapshotGlob{states: make([]snapshotGlobState, 4), stateIDs: make(map[string]int), ids: make(map[*gstate]uint32), transitions: make(map[uint64]int)}
	for _, component := range strings.Split(expr, "/") {
		if component == "" {
			continue
		}
		m, err := newGlobState(component, expand)
		if err != nil {
			return nil, err
		}
		a.components = append(a.components, m)
	}
	if len(a.components) > 0 {
		leaf, err := newGlobState(a.components[len(a.components)-1].expr+".wsp", expand)
		if err != nil {
			return nil, err
		}
		a.leaf = leaf
	}
	return a, nil
}

func (a *snapshotGlob) Start() int {
	if len(a.components) == 0 {
		return snapshotGlobDead
	}
	return snapshotGlobStart
}

func (a *snapshotGlob) IsMatch(state int) bool {
	if state == snapshotGlobDescendants {
		return true
	}
	return state > snapshotGlobDescendants && a.states[state].leaf != nil && a.states[state].leaf.matched()
}

func (*snapshotGlob) CanMatch(state int) bool        { return state >= snapshotGlobStart }
func (*snapshotGlob) WillAlwaysMatch(state int) bool { return state == snapshotGlobDescendants }

func (a *snapshotGlob) startComponent(component int) int {
	s := snapshotGlobState{component: component}
	if component == len(a.components)-1 {
		s.leaf = a.leaf.dstate()
		s.directory = a.components[component].dstate()
	} else {
		s.common = a.components[component].dstate()
	}
	return a.intern(s)
}

func (a *snapshotGlob) Accept(state int, b byte) int {
	if state == snapshotGlobDescendants {
		return state
	}
	if !a.CanMatch(state) {
		return snapshotGlobDead
	}
	if state == snapshotGlobStart {
		if b != 0 || len(a.components) == 0 {
			return snapshotGlobDead
		}
		return a.startComponent(0)
	}
	key := uint64(state)<<8 | uint64(b)
	if next, ok := a.transitions[key]; ok {
		return next
	}
	s := a.states[state]
	next := snapshotGlobDead
	if b == 0 {
		if s.common != nil && s.common.matched() {
			next = a.startComponent(s.component + 1)
		} else if s.directory != nil && s.directory.matched() {
			next = snapshotGlobDescendants
		}
	} else {
		if s.common != nil {
			s.common = s.common.step(b)
		}
		if s.leaf != nil {
			s.leaf = s.leaf.step(b)
		}
		if s.directory != nil {
			s.directory = s.directory.step(b)
		}
		next = a.intern(s)
	}
	a.transitions[key] = next
	return next
}

func (a *snapshotGlob) intern(state snapshotGlobState) int {
	key := binary.LittleEndian.AppendUint32(nil, uint32(state.component))
	live := false
	for _, set := range []*gdstate{state.common, state.leaf, state.directory} {
		var ids []uint32
		if set != nil {
			for _, s := range set.gstates {
				id, ok := a.ids[s]
				if !ok {
					id = uint32(len(a.ids))
					a.ids[s] = id
				}
				ids = append(ids, id)
			}
		}
		slices.Sort(ids)
		ids = slices.Compact(ids)
		if len(ids) > 0 {
			live = true
		}
		key = binary.LittleEndian.AppendUint32(key, uint32(len(ids)))
		for _, id := range ids {
			key = binary.LittleEndian.AppendUint32(key, id)
		}
	}
	if !live {
		return snapshotGlobDead
	}
	if id, ok := a.stateIDs[string(key)]; ok {
		return id
	}
	id := len(a.states)
	a.states = append(a.states, state)
	a.stateIDs[string(key)] = id
	return id
}

type snapshotQueryNodes struct{ files, directories sync.Map }

func (s *indexSnapshot) fileNode(name string, row uint64) (*trieNode, error) {
	defer runtime.KeepAlive(s)
	if node, ok := s.nodes.files.Load(name); ok {
		return node.(*trieNode), nil
	}
	values, err := s.metadata.get(row)
	if err != nil {
		return nil, err
	}
	if values[3] == 0 {
		values[3] = s.openedAt
	}
	node := newFileNode(0, values[0], values[1], values[2], values[3])
	actual, _ := s.nodes.files.LoadOrStore(name, node)
	return actual.(*trieNode), nil
}

func (s *indexSnapshot) directoryNode(name string) *trieNode {
	if node, ok := s.nodes.directories.Load(name); ok {
		return node.(*trieNode)
	}
	node := &trieNode{c: trieDirectorySeparator, childrens: emptyTrieNodes}
	node.meta.Store(newDirMeta())
	actual, _ := s.nodes.directories.LoadOrStore(name, node)
	return actual.(*trieNode)
}

func (s *indexSnapshot) query(expr string, limit int, expand func([]string) ([]string, error)) (names []string, leaves []bool, nodes []*trieNode, lookups uint32, err error) {
	defer runtime.KeepAlive(s)
	a, err := newSnapshotGlob(expr, expand)
	if err != nil {
		return nil, nil, nil, 0, err
	}
	iterator, err := s.index.Search(a, nil, nil)
	pairedFiles := make(map[string]bool)
	if iterator != nil {
		defer iterator.Close()
	}
	for err == nil {
		lookups++
		key, row := iterator.Current()
		// Valid snapshot keys always start with the root separator.
		path := key[1:]
		boundary := -1
		for i, depth := 0, 0; i < len(path); i++ {
			if path[i] != 0 {
				continue
			}
			depth++
			if depth == len(a.components) {
				boundary = i
				break
			}
		}
		var name string
		var node *trieNode
		leaf := boundary < 0
		if leaf {
			name = strings.ReplaceAll(string(path[:len(path)-4]), "\x00", ".")
			if pairedFiles[name] {
				err = iterator.Next()
				continue
			}
			node, err = s.fileNode(name, row)
			if err != nil {
				return nil, nil, nil, lookups, err
			}
		} else {
			name = strings.ReplaceAll(string(path[:boundary]), "\x00", ".")
			node = s.directoryNode(name)
			// The trie emits a metric and same-named namespace together, with
			// the metric first, even when that pair crosses the result limit.
			counterpart := append([]byte(nil), key[:boundary+1]...)
			counterpart = append(counterpart, ".wsp"...)
			metricRow, found, getErr := s.index.Get(counterpart)
			if getErr != nil {
				return nil, nil, nil, lookups, getErr
			}
			if found {
				file, nodeErr := s.fileNode(name, metricRow)
				if nodeErr != nil {
					return nil, nil, nil, lookups, nodeErr
				}
				names = append(names, name)
				leaves = append(leaves, true)
				nodes = append(nodes, file)
				pairedFiles[name] = true
			}
		}
		names = append(names, name)
		leaves = append(leaves, leaf)
		nodes = append(nodes, node)
		if len(names) >= limit {
			break
		}
		if leaf {
			err = iterator.Next()
		} else {
			// NUL is the final byte of the matching namespace prefix. Its
			// successor skips descendants without skipping a sibling metric.
			next := append([]byte(nil), key[:boundary+2]...)
			next[len(next)-1] = 1
			err = iterator.Seek(next)
		}
	}
	if errors.Is(err, vellum.ErrIteratorDone) {
		err = nil
	}
	return names, leaves, nodes, lookups, err
}

// namespaceExists follows the prefix itself, without seeking a successor key.
// Quota accounting only needs membership here; two ordered range searches per
// mutable namespace can dominate shutdown on a large saved catalogue.
func (s *indexSnapshot) namespaceExists(name string) bool {
	defer runtime.KeepAlive(s)
	state := s.index.Start()
	for _, b := range snapshotNamespacePrefix(name) {
		state = s.index.Accept(state, b)
		if !s.index.CanMatch(state) {
			return false
		}
	}
	return true
}

// namespaceLookup shares ancestor states across one batch of namespace checks.
// A large mutable overlay often repeats a long common prefix in every name;
// restarting the FST walk at the root for each namespace amplifies that work.
// The returned function is private to one quota refresh and is not concurrent.
func (s *indexSnapshot) namespaceLookup() func(string) bool {
	root := s.index.Accept(s.index.Start(), 0)
	states := map[string]int{"/": root, "": root}
	var stateFor func(string) int
	stateFor = func(name string) int {
		if state, ok := states[name]; ok {
			return state
		}
		parent, component := "/", name
		if at := strings.LastIndexByte(name, '.'); at >= 0 {
			parent, component = name[:at], name[at+1:]
		}
		state := stateFor(parent)
		for i := 0; i < len(component) && s.index.CanMatch(state); i++ {
			state = s.index.Accept(state, component[i])
		}
		if s.index.CanMatch(state) {
			state = s.index.Accept(state, 0)
		}
		states[name] = state
		return state
	}
	return func(name string) bool {
		defer runtime.KeepAlive(s)
		return s.index.CanMatch(stateFor(name))
	}
}

func (s *indexSnapshot) lowerBound(key []byte) (uint64, error) {
	defer runtime.KeepAlive(s)
	i, err := s.index.Iterator(key, nil)
	if i != nil {
		defer i.Close()
	}
	if errors.Is(err, vellum.ErrIteratorDone) {
		return s.metadata.count, nil
	}
	if err != nil {
		return 0, err
	}
	_, row := i.Current()
	return row, nil
}

func snapshotNamespacePrefix(name string) []byte {
	if name == "/" || name == "" {
		return []byte{0}
	}
	prefix := append([]byte{0}, name...)
	for i, b := range prefix {
		if b == '.' {
			prefix[i] = 0
		}
	}
	return append(prefix, 0)
}

func (s *indexSnapshot) namespaceRange(name string) (uint64, uint64, error) {
	prefix := snapshotNamespacePrefix(name)
	start, err := s.lowerBound(prefix)
	if err != nil {
		return 0, 0, err
	}
	prefix[len(prefix)-1] = 1
	end, err := s.lowerBound(prefix)
	return start, end, err
}

func (s *indexSnapshot) namespaceUsage(name string) (QuotaUsage, error) {
	start, end, err := s.namespaceRange(name)
	if err != nil {
		return QuotaUsage{}, err
	}
	sums, err := s.metadata.usage(start, end)
	if err != nil {
		return QuotaUsage{}, err
	}
	dirs, err := s.namespaceChildren(name)
	if err != nil {
		return QuotaUsage{}, err
	}
	return QuotaUsage{Namespaces: dirs, Metrics: int64(end - start), LogicalSize: sums[0], PhysicalSize: sums[1], DataPoints: sums[2]}, nil
}

func (s *indexSnapshot) namespaceChildren(name string) (int64, error) {
	defer runtime.KeepAlive(s)
	prefix := snapshotNamespacePrefix(name)
	end := bytes.Clone(prefix)
	end[len(end)-1] = 1
	i, err := s.index.Iterator(prefix, end)
	if i != nil {
		defer i.Close()
	}
	var count int64
	for err == nil {
		key, _ := i.Current()
		if boundary := bytes.IndexByte(key[len(prefix):], 0); boundary >= 0 {
			count++
			next := bytes.Clone(key[:len(prefix)+boundary+1])
			next[len(next)-1] = 1
			err = i.Seek(next)
		} else {
			err = i.Next()
		}
	}
	if errors.Is(err, vellum.ErrIteratorDone) {
		err = nil
	}
	return count, err
}
