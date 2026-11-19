package v2index

import (
	"runtime"
	"sort"

	"github.com/go-graphite/go-carbon/internal/v2core"
)

// MappedNames returns the immutable base generation, excluding heap tails.
// The source retains the run, whose cleanup owns the mapping. Base positions
// remain resolvable after removal from discovery or replacement of the file.
func (s *Snapshot) MappedNames() v2core.PolicyNameSource {
	if len(s.levels) == 0 || s.levels[0].path == "" {
		return nil
	}
	return s.levels[0].run
}

func (r *run) FindName(name string) (uint32, bool) {
	b := sort.Search(len(r.blocks), func(i int) bool { return string(r.first(i)) > name }) - 1
	if b < 0 {
		return 0, false
	}
	it := levelIter{l: &level{run: r}, off: r.blocks[b], i: b * blockNames}
	for it.i < min(r.n, (b+1)*blockNames) && it.next() {
		if string(it.key) == name {
			return uint32(it.i - 1), true
		}
		if string(it.key) > name {
			break
		}
	}
	runtime.KeepAlive(r)
	return 0, false
}

func (r *run) NameAt(pos uint32) string {
	name := string(r.at(int(pos), nil))
	runtime.KeepAlive(r)
	return name
}
