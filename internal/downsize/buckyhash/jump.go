package hashing

import (
	"fmt"
	"strings"
)

// XorShift generates a predictable random-ish hash from the given integer.
// This method is also used by carbon-c-relay for replication in a Jump
// hash ring.
// http://vigna.di.unimi.it/ftp/papers/xorshift.pdf
func XorShift(i uint64) uint64 {
	i ^= i >> 12
	i ^= i << 25
	i ^= i >> 27
	return i * 2685821657736338717
}

// Fnv1a32 returns a 32 bit hash of the given data using the FNV-1a hashing
// algorithm.  Golang's libraries natively support this hashing, but I need
// something simpler.
func Fnv1a32(data []byte) uint32 {
	var hash uint32 = 2166136261
	for _, d := range data {
		hash = (hash ^ uint32(d)) * 16777619
	}
	return hash
}

// Fnv1a64 returns a 64 bit hash of the given data using the FNV-1a hashing
// algorithm.  Golang's libraries natively support this hashing, but I need
// something simpler.
func Fnv1a64(data []byte) uint64 {
	var hash uint64 = 14695981039346656037
	for _, d := range data {
		hash = (hash ^ uint64(d)) * 1099511628211
	}
	return hash
}

// Jump returns a bucket index less that buckets using Google's Jump
// consistent hashing algorithm: http://arxiv.org/pdf/1406.2294.pdf
// Note that the return is int for convienance and will not be larger than
// an int32.
func Jump(key uint64, buckets int) int {
	var b int64 = -1
	var j int64 = 0
	for j < int64(buckets) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(1<<31) / float64(key>>33+1)))
	}
	return int(b)
}

// JumpHashRing stores the hashring information.
type JumpHashRing struct {
	ring     []Node
	replicas int
}

// NewJumpHashRing creates a new hashring configured with the given replicas
// such that the number of solutions matches the number of replicas.
func NewJumpHashRing(replicas int) *JumpHashRing {
	chr := new(JumpHashRing)
	chr.replicas = replicas
	return chr
}

// String displays the buckets in the hashring and their index numbers.
func (chr *JumpHashRing) String() string {
	s := make([]string, 0)
	s = append(s, "jump_fnv1a:")
	for i := range chr.ring {
		s = append(s, fmt.Sprintf("%3d:%s", i, chr.ring[i].Server))
	}

	return strings.Join(s, "\t")
}

// Replicas returns the number of replicas the hash ring is configured for.
func (chr *JumpHashRing) Replicas() int {
	return chr.replicas
}

// Len returns the number of buckets in the hash ring.
func (chr *JumpHashRing) Len() int {
	return len(chr.ring)
}

// Nodes returns the Nodes in the hashring
func (chr *JumpHashRing) Nodes() []Node {
	return chr.ring
}

// AddNode adds a Node to the Jump Hash Ring.  Jump only operates on the
// number of buckets so we assume that AddNode will not be used to attempt
// to insert a Node in the middle of the ring as that will affect the mapping
// of buckets to server addresses.  This uses the instance value to define
// an order of the slice of Nodes.  Empty ("") instance values will be
// appended to the end of the slice.
func (chr *JumpHashRing) AddNode(node Node) {
	if node.Instance == "" {
		chr.ring = append(chr.ring, node)
	} else {
		i := 0
		for i = 0; i < chr.Len() && node.Instance >= chr.ring[i].Instance; i++ {
		}
		chr.ring = append(chr.ring, node)  // Make room
		copy(chr.ring[i+1:], chr.ring[i:]) // Shuffle array
		chr.ring[i] = node                 // insert new node
	}
}

// RemoveNode removes the last node in the ring regardless of the value of
// the given node which is here to implement our interface.
func (chr *JumpHashRing) RemoveNode(node Node) {
	chr.ring = chr.ring[:len(chr.ring)-1]
}

// GetNode returns a bucket for the given key using Google's Jump Hash
// algorithm.
func (chr *JumpHashRing) GetNode(key string) Node {
	var key64 uint64 = Fnv1a64([]byte(key))
	idx := Jump(key64, len(chr.ring))
	// fmt.Printf("JUMP: %s => %x => %d\n", key, key64, idx)
	return chr.ring[idx]
}

// GetNodes returns a slice of Node objects one for each replica where the
// object is stored.
func (chr *JumpHashRing) GetNodes(key string) []Node {
	ring := make([]Node, 0)
	ret := make([]Node, 0)
	h := Fnv1a64([]byte(key))
	i := len(chr.ring)
	j := 0
	r := chr.replicas

	// We need to alter the ring as we go along, make a safe place
	copy(ring, chr.ring)
	for i > 0 {
		j = Jump(h, i)
		ret = append(ret, chr.ring[j])

		if r--; r <= 0 {
			break
		}

		// Generate a new unique hash
		h = XorShift(h)

		// Remove the previously selected bucket from our list
		i--
		ring[j] = ring[i]
	}
	return ret
}
