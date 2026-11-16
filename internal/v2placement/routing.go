// Package v2placement defines the version-one portable placement contract.
// Names are exact post-rewrite routing bytes; this package never normalizes them.
package v2placement

import (
	"encoding/base64"
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"strconv"
	"strings"
)

const Version = 1
const NameVersion = "post-rewrite-bytes-v1"
const VShards = 4096

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Routing is immutable after bootstrap. Prefixes use longest byte-prefix match;
// an empty prefix is the required fallback. Namespace IDs and prefixes are unique.
type Routing struct {
	NameVersion string      `json:"name_version"`
	JumpSizes   []uint32    `json:"jump_sizes"`
	Span        uint32      `json:"span"`
	Namespaces  []Namespace `json:"namespaces"`
}
type Namespace struct {
	ID     string `json:"id"`
	Prefix string `json:"prefix"`
}
type Selector struct {
	Namespace string   `json:"namespace"`
	Cell      []uint32 `json:"cell"`
	Start     uint32   `json:"start"`
	End       uint32   `json:"end"`
}

// GroupID encodes the entire selector, with no shortened hashes or host identity.
func (s Selector) GroupID() string {
	cells := make([]string, len(s.Cell))
	for i, v := range s.Cell {
		cells[i] = strconv.FormatUint(uint64(v), 10)
	}
	return fmt.Sprintf("%s/cell-%s/v%d-%d", base64.RawURLEncoding.EncodeToString([]byte(s.Namespace)), strings.Join(cells, "-"), s.Start, s.End)
}

// LegacySlot freezes the legacy FNV-1a + jump function, including float64
// arithmetic and uint64 overflow. Ring size is bootstrap metadata, never discovery.
func LegacySlot(name string, size uint32) (uint32, error) {
	if size == 0 || size > 1<<31-1 {
		return 0, fmt.Errorf("invalid jump size")
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(name))
	key := h.Sum64()
	b, j := int64(-1), int64(0)
	for j < int64(size) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}
	return uint32(b), nil
}

func (r Routing) Validate() error {
	if r.NameVersion != NameVersion || r.Span == 0 || r.Span > VShards || VShards%r.Span != 0 || len(r.JumpSizes) == 0 || len(r.Namespaces) == 0 {
		return fmt.Errorf("invalid routing contract")
	}
	for i, n := range r.JumpSizes {
		if n == 0 || n > 1<<31-1 || (i > 0 && n <= r.JumpSizes[i-1]) {
			return fmt.Errorf("jump sizes must be positive, sorted and unique")
		}
	}
	fallback := false
	ids := map[string]bool{}
	prefixes := map[string]bool{}
	for _, n := range r.Namespaces {
		if n.ID == "" || ids[n.ID] || prefixes[n.Prefix] {
			return fmt.Errorf("duplicate or empty namespace")
		}
		ids[n.ID] = true
		prefixes[n.Prefix] = true
		fallback = fallback || n.Prefix == ""
	}
	if !fallback {
		return fmt.Errorf("namespace fallback required")
	}
	return nil
}
func (r Routing) Select(name string) (Selector, uint32, error) {
	if err := r.Validate(); err != nil {
		return Selector{}, 0, err
	}
	if name == "" {
		return Selector{}, 0, fmt.Errorf("empty routing name")
	}
	ns := ""
	length := -1
	for _, n := range r.Namespaces {
		if strings.HasPrefix(name, n.Prefix) && len(n.Prefix) > length {
			ns = n.ID
			length = len(n.Prefix)
		}
	}
	v := crc32.Checksum([]byte(name), castagnoli) & (VShards - 1)
	s := Selector{Namespace: ns, Start: v / r.Span * r.Span, End: (v/r.Span + 1) * r.Span, Cell: make([]uint32, len(r.JumpSizes))}
	for i, n := range r.JumpSizes {
		s.Cell[i], _ = LegacySlot(name, n)
	}
	return s, v, nil
}

// A larger jump ring either retains the prior slot or selects one of its new
// slots. Enumerating the full Cartesian product would require impossible PGs.
func (r Routing) validSelector(s Selector) bool {
	if len(s.Cell) != len(r.JumpSizes) || s.Start >= VShards || s.Start%r.Span != 0 || s.End != s.Start+r.Span {
		return false
	}
	found := false
	for _, n := range r.Namespaces {
		found = found || n.ID == s.Namespace
	}
	if !found {
		return false
	}
	for i, v := range s.Cell {
		if v >= r.JumpSizes[i] || (i > 0 && v < r.JumpSizes[i-1] && v != s.Cell[i-1]) {
			return false
		}
	}
	return true
}
func (r Routing) groupCount() (uint64, error) {
	n := uint64(len(r.Namespaces)) * uint64(VShards/r.Span)
	for i, size := range r.JumpSizes {
		factor := uint64(size)
		if i > 0 {
			factor = uint64(size-r.JumpSizes[i-1]) + 1
		}
		if n > 1000000/factor {
			return 0, fmt.Errorf("placement exceeds one million groups")
		}
		n *= factor
	}
	return n, nil
}

// Bootstrap creates every feasible cell/range, preserving each explicit slot.
// Callers fill producer inventories before validating or using the document.
func Bootstrap(r Routing, replicas []BootstrapReplica) ([]Group, error) {
	if e := r.Validate(); e != nil {
		return nil, e
	}
	count, e := r.groupCount()
	if e != nil {
		return nil, e
	}
	for _, b := range replicas {
		found := false
		for _, n := range r.JumpSizes {
			found = found || n == b.JumpSize
		}
		if !found || len(b.Slots) != int(b.JumpSize) {
			return nil, fmt.Errorf("invalid bootstrap slots")
		}
	}
	groups := make([]Group, 0, int(count))
	cell := make([]uint32, len(r.JumpSizes))
	var visit func(int)
	visit = func(i int) {
		if i < len(cell) {
			for v := uint32(0); v < r.JumpSizes[i]; v++ {
				if i > 0 && v < r.JumpSizes[i-1] && v != cell[i-1] {
					continue
				}
				cell[i] = v
				visit(i + 1)
			}
			return
		}
		for _, ns := range r.Namespaces {
			for start := uint32(0); start < VShards; start += r.Span {
				g := Group{Selector: Selector{Namespace: ns.ID, Cell: append([]uint32(nil), cell...), Start: start, End: start + r.Span}}
				for _, b := range replicas {
					idx := 0
					for r.JumpSizes[idx] != b.JumpSize {
						idx++
					}
					id := b.Slots[cell[idx]]
					g.Replicas = append(g.Replicas, Replica{ID: b.ID, Epoch: 1, Writer: id, Read: []string{id}, CopySources: []string{id}})
				}
				groups = append(groups, g)
			}
		}
	}
	visit(0)
	return groups, nil
}
