package hashing

import (
	"fmt"
	"strings"
)

// FNV1aHashRing represents carbon-c-relay's more efficient hash variant
// of CarbonHashRing
type FNV1aHashRing struct {
	ring     []RingEntry
	nodes    []Node
	replicas int
}

func NewFNV1aHashRing() *FNV1aHashRing {
	var chr = new(FNV1aHashRing)
	chr.ring = make([]RingEntry, 0, 10)
	chr.nodes = make([]Node, 0, 10)
	chr.replicas = 100

	return chr
}

func computeFNV1aRingPosition(key string) (result int) {
	// compute 32-bits FNV1a hash
	digest := Fnv1a32([]byte(key))

	// and trim it to the 16bit space
	result = int((digest >> 16) ^ (digest & uint32(0xFFFF)))
	return
}

func (t Node) FNV1aKeyValue() string {
	if t.Instance == "" {
		return fmt.Sprintf("%s:%d", t.Server, t.Port)
	}
	return fmt.Sprintf("%s", t.Instance)
}

func (t *FNV1aHashRing) String() string {
	servers := make([]string, 0)

	for i := 0; i < len(t.nodes); i++ {
		servers = append(servers, t.nodes[i].String())
	}
	return fmt.Sprintf("[fnv1a: %d nodes, %d replicas, %d ring members %s]",
		len(t.nodes), t.replicas, len(t.ring), strings.Join(servers, " "))
}

func (t *FNV1aHashRing) Replicas() int {
	return t.replicas
}

func (t *FNV1aHashRing) SetReplicas(r int) {
	t.replicas = r
}

func (t *FNV1aHashRing) AddNode(node Node) {
	t.nodes = append(t.nodes, node)
	for i := 0; i < t.replicas; i++ {
		var e RingEntry
		replica_key := fmt.Sprintf("%d-%s", i, node.FNV1aKeyValue())
		e.position = computeFNV1aRingPosition(replica_key)
		e.node = node
		t.ring = insertRing(t.ring, e)
	}
}

func (t *FNV1aHashRing) RemoveNode(node Node) {
	var i int

	// Find node in nodes
	for i = 0; i < len(t.nodes); {
		if node.String() == t.nodes[i].String() {
			t.nodes = append(t.nodes[:i], t.nodes[i+1:]...)
		} else {
			i++
		}
	}

	// Remove matching ring locations
	for i = 0; i < len(t.ring); {
		if node.String() == t.ring[i].node.String() {
			t.ring = append(t.ring[:i], t.ring[i+1:]...)
		} else {
			i++
		}
	}
}

func (t *FNV1aHashRing) GetNode(key string) Node {
	if len(t.ring) == 0 {
		panic("HashRing is empty")
	}

	e := RingEntry{computeFNV1aRingPosition(key), NewNode(key, 0, "")}
	i := mod(bisectLeft(t.ring, e), len(t.ring))
	return t.ring[i].node
}

func (t *FNV1aHashRing) GetNodes(key string) []Node {
	if len(t.ring) == 0 {
		panic("HashRing is empty")
	}

	result := make([]Node, 0)
	seen := make(map[string]bool)
	e := RingEntry{computeFNV1aRingPosition(key), NewNode(key, 0, "")}
	index := mod(bisectLeft(t.ring, e), len(t.ring))
	last := index - 1

	for len(seen) < len(t.nodes) && index != last {
		next := t.ring[index]
		if !seen[next.node.String()] {
			seen[next.node.String()] = true
			result = append(result, next.node)
		}
		index = mod((index + 1), len(t.ring))
	}

	return result
}

func (t *FNV1aHashRing) BucketsPerNode() map[string]int {
	if len(t.ring) == 0 {
		panic("HashRing is empty")
	}

	hash := make(map[string]int)
	max := 0xFFFF
	last := t.ring[len(t.ring)-1]
	for i, e := range t.ring {
		buckets := 0
		if i == 0 {
			buckets = (max - last.position) + e.position
		} else {
			buckets = e.position - last.position
		}

		hash[e.node.String()] = hash[e.node.String()] + buckets
		last = e
	}

	return hash
}

func (t *FNV1aHashRing) Len() int {
	return len(t.nodes)
}

func (t *FNV1aHashRing) Nodes() []Node {
	return t.nodes
}
