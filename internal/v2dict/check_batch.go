package v2dict

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/zeebo/xxh3"
	"slices"
)

// NameID is an exact live-name assertion during catalog recovery.
type NameID struct {
	Group  string
	VShard uint16
	Name   string
	ID     uint32
}

// CheckNames validates catalog bindings through the reverse index. A matching
// 64-bit live-name hash avoids decompression; mismatches use exact block decoding.
// Fallback positions are sorted so each requested frame is decoded once.
// It retains no decoded blocks or name copies after the call.
func (d *Dict) CheckNames(names []NameID) error {
	type check struct {
		n    NameID
		pos  int
		done bool
	}
	checks := make([]check, len(names))
	for i := range d.shards {
		d.shards[i].mu.RLock()
	}
	defer func() {
		for i := range d.shards {
			d.shards[i].mu.RUnlock()
		}
	}()
	s := d.snap.Load()
	var key [keyBuf]byte
	for i, n := range names {
		if err := checkGroup(n.Group); err != nil {
			return err
		}
		checks[i].n = n
		k := append(scopeKey(key[:0], n.Group, n.VShard), n.Name...)
		v := d.shard(n.VShard).buf.get(k)
		if v == nil {
			v = s.frozenGet(k)
		}
		if v != nil {
			e := decode(v)
			if e.Deleted() || e.ID != n.ID {
				return fmt.Errorf("v2dict: name/ID mismatch")
			}
			checks[i].done = true
		}
	}
	for li := len(s.levels) - 1; li >= 0; li-- {
		l := s.levels[li]
		for i := range checks {
			c := &checks[i]
			c.pos = -1
			if !c.done {
				c.pos = l.position(scopeKey(key[:0], c.n.Group, c.n.VShard), c.n.ID)
				if c.pos >= 0 && l.compact {
					want := append(scopeKey(key[:0], c.n.Group, c.n.VShard), c.n.Name...)
					h := binary.LittleEndian.Uint64(l.m[l.hashOff+8*c.pos:])
					if h != 0 && h == xxh3.Hash(want) {
						if newer, ok := getLevels(s.levels[li+1:], want); ok && (newer.ID != c.n.ID || newer.Deleted()) {
							return fmt.Errorf("v2dict: name/ID mismatch")
						}
						c.done, c.pos = true, -1
					}
				}
			}
		}
		slices.SortFunc(checks, func(a, b check) int { return a.pos - b.pos })
		it := levelIter{l: l}
		block := -1
		for i := range checks {
			c := &checks[i]
			if c.done || c.pos < 0 {
				continue
			}
			b := c.pos / l.blockN
			if b != block {
				it.off, it.i, it.key = 0, b*l.blockN, it.key[:0]
				it.next()
				block = b
			}
			for it.valid && it.i <= c.pos {
				it.next()
			}
			n := c.n
			want := append(scopeKey(key[:0], n.Group, n.VShard), n.Name...)
			e := decode(it.val)
			// The reverse column is an index, not an authority. Validate its exact
			// scope/name/ID and check newer name entries for deletion or revival.
			if !it.valid || !bytes.Equal(it.key, want) || e.ID != n.ID || e.Deleted() {
				return fmt.Errorf("v2dict: name/ID mismatch")
			}
			if newer, ok := getLevels(s.levels[li+1:], want); ok && (newer.ID != n.ID || newer.Deleted()) {
				return fmt.Errorf("v2dict: name/ID mismatch")
			}
			c.done = true
		}
	}
	for _, c := range checks {
		if !c.done {
			return fmt.Errorf("v2dict: unknown name/ID")
		}
	}
	return nil
}
