package hashing

import (
	"crypto/md5"
	"errors"
	"fmt"
)

// Compiled uses the original buckytools ring construction and hash primitives.
// Carbon/FNV have only 65536 metric positions: compile each to its exact owner.
type Compiled struct {
	algo   string
	nodes  []Node
	owners []uint16
	jump   *JumpHashRing
	copies int
}

func Compile(algo string, nodes []Node, copies, virtualNodes int) (*Compiled, error) {
	if len(nodes) == 0 || len(nodes) > 65535 || copies < 1 || copies > len(nodes) || copies > 64 {
		return nil, errors.New("ring needs 1..65535 nodes and 1..min(N,64) physical replicas")
	}
	c := &Compiled{algo: algo, copies: copies}
	var entries []RingEntry
	switch algo {
	case "jump_fnv1a":
		if copies != 1 {
			return nil, errors.New("jump replication >1 is not supported by the production relay or safe in buckytools")
		}
		c.jump = NewJumpHashRing(copies)
		for _, n := range nodes {
			c.jump.AddNode(n)
		}
		c.nodes = c.jump.Nodes()
		return c, nil
	case "carbon":
		r := NewCarbonHashRing()
		r.SetReplicas(virtualNodes)
		for _, n := range nodes {
			r.AddNode(n)
		}
		entries = r.ring
	case "fnv1a":
		r := NewFNV1aHashRing()
		r.SetReplicas(virtualNodes)
		for _, n := range nodes {
			r.AddNode(n)
		}
		entries = r.ring
	default:
		return nil, fmt.Errorf("unknown hash %q", algo)
	}
	if virtualNodes < 1 || virtualNodes > 10000 {
		return nil, errors.New("virtual_nodes must be 1..10000")
	}
	c.nodes = nodes
	indices := make(map[Node]uint16, len(nodes))
	for i, n := range nodes {
		indices[n] = uint16(i)
	}
	c.owners = make([]uint16, 65536*copies)
	i := 0
	for position := 0; position < 65536; position++ {
		for i < len(entries) && entries[i].position < position {
			i++
		}
		start := i % len(entries)
		owners := c.owners[position*copies : (position+1)*copies]
		found := 0
		for j := 0; j < len(entries) && found < copies; j++ {
			n := indices[entries[(start+j)%len(entries)].node]
			seen := false
			for k := 0; k < found; k++ {
				if owners[k] == n {
					seen = true
					break
				}
			}
			if !seen {
				owners[found] = n
				found++
			}
		}
		if found != copies {
			return nil, errors.New("not enough distinct owners")
		}
	}
	return c, nil
}

func (c *Compiled) Nodes() []Node { return c.nodes }

// Lookup appends indices into Nodes to caller-owned storage, without allocation.
func (c *Compiled) Lookup(key string, dst []int) []int {
	return c.LookupHash(c.KeyHash(key), dst)
}

// KeyHash can be shared by rings using the same algorithm.
func (c *Compiled) KeyHash(key string) uint64 {
	if c.jump != nil {
		return Fnv1a64([]byte(key))
	}
	if c.algo == "carbon" {
		// md5.Sum's dynamic []byte conversion allocates for typical long names.
		// Bound the common conversion on the stack; retain upstream for long keys.
		var buf [256]byte
		if len(key) <= len(buf) {
			copy(buf[:], key)
			digest := md5.Sum(buf[:len(key)])
			return uint64(digest[0])<<8 | uint64(digest[1])
		}
		return uint64(computeCarbonRingPosition(key))
	}
	return uint64(computeFNV1aRingPosition(key))
}

func (c *Compiled) LookupHash(key uint64, dst []int) []int {
	if c.jump != nil {
		return append(dst, Jump(key, len(c.nodes)))
	}
	p := int(key)
	for _, n := range c.owners[p*c.copies : (p+1)*c.copies] {
		dst = append(dst, int(n))
	}
	return dst
}
