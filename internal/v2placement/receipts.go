package v2placement

import "fmt"

// Position is a component of a durable owner/journal frontier, not a timestamp.
type Position struct {
	Owner  string `json:"owner"`
	Cursor uint64 `json:"cursor"`
}
type Frontier []Position

func (f Frontier) Validate() error {
	seen := map[string]bool{}
	if len(f) == 0 {
		return fmt.Errorf("empty frontier")
	}
	for _, p := range f {
		if p.Owner == "" || seen[p.Owner] {
			return fmt.Errorf("invalid frontier")
		}
		seen[p.Owner] = true
	}
	return nil
}
func (f Frontier) Covers(other Frontier) bool {
	if f.Validate() != nil || other.Validate() != nil {
		return false
	}
	for _, p := range other {
		found := false
		for _, q := range f {
			found = found || q.Owner == p.Owner && q.Cursor >= p.Cursor
		}
		if !found {
			return false
		}
	}
	return true
}

type Roots struct {
	Parts      string `json:"parts"`
	Catalog    string `json:"catalog"`
	Policy     string `json:"policy"`
	Tombstones string `json:"tombstones"`
}

func (r Roots) Validate() error {
	for _, v := range []string{r.Parts, r.Catalog, r.Policy, r.Tombstones} {
		if !validDigest(v) {
			return fmt.Errorf("invalid coverage root")
		}
	}
	return nil
}

type EndMarker struct {
	Lane    Lane   `json:"lane"`
	Final   uint64 `json:"final"`
	Durable uint64 `json:"durable"`
}

// Fence is evidence emitted only after intake closes and every registered lane
// has drained into durable core commits. Digests identify evidence, not signatures;
// transport authentication and durable-core attestation remain caller obligations.
type Fence struct {
	Group     string      `json:"group"`
	Replica   string      `json:"replica"`
	Store     string      `json:"store"`
	Epoch     uint64      `json:"epoch"`
	MapDigest string      `json:"map_digest"`
	Operation string      `json:"operation"`
	WriterSet string      `json:"writer_set"`
	Ends      []EndMarker `json:"ends"`
	Frontier  Frontier    `json:"frontier"`
	Roots     Roots       `json:"roots"`
}

func (f Fence) Digest() string { return digest(f) }
func (f Fence) Validate(d Document) error {
	r, e := d.Lookup(f.Group, f.Replica)
	if e != nil {
		return e
	}
	if f.Store != r.Writer || f.Epoch != r.Epoch || f.MapDigest != d.Digest() || f.Operation == "" || f.WriterSet != digest(r.Producers) || len(f.Ends) != len(r.Producers) {
		return fmt.Errorf("fence identity mismatch")
	}
	seen := map[Lane]bool{}
	for _, m := range f.Ends {
		found := false
		for _, l := range r.Producers {
			found = found || l == m.Lane
		}
		if !found || seen[m.Lane] || m.Final != m.Durable {
			return fmt.Errorf("missing or undrained producer lane")
		}
		seen[m.Lane] = true
	}
	if e = f.Frontier.Validate(); e != nil {
		return e
	}
	return f.Roots.Validate()
}

// Coverage binds the target's installed generation to all historical source
// roots and the exact old fence. Hashes are per manifest/part, never per name.
type Coverage struct {
	Group       string        `json:"group"`
	Replica     string        `json:"replica"`
	Store       string        `json:"store"`
	Epoch       uint64        `json:"epoch"`
	MapDigest   string        `json:"map_digest"`
	Operation   string        `json:"operation"`
	FenceDigest string        `json:"fence_digest"`
	Frontier    Frontier      `json:"frontier"`
	Roots       Roots         `json:"roots"`
	Required    []Requirement `json:"required"`
}

func (c Coverage) Digest() string { return digest(c) }

type Activation struct {
	Fence    Fence    `json:"fence"`
	Coverage Coverage `json:"coverage"`
}

func (a Activation) Validate(old, next Document) error {
	f, c := a.Fence, a.Coverage
	if e := f.Validate(old); e != nil {
		return e
	}
	before, e := old.Lookup(f.Group, f.Replica)
	if e != nil {
		return e
	}
	after, e := next.Lookup(f.Group, f.Replica)
	if e != nil {
		return e
	}
	if before.Epoch == ^uint64(0) || after.Epoch != before.Epoch+1 || f.Operation != next.Operation || c.Operation != next.Operation || c.Group != f.Group || c.Replica != f.Replica || c.Store != after.Writer || c.Epoch != after.Epoch || c.MapDigest != next.Digest() || c.FenceDigest != f.Digest() {
		return fmt.Errorf("activation identity mismatch")
	}
	if !c.Frontier.Covers(f.Frontier) {
		return fmt.Errorf("target frontier has gaps")
	}
	if e = c.Roots.Validate(); e != nil {
		return e
	}
	seen := map[Requirement]bool{}
	for _, q := range c.Required {
		if seen[q] {
			return fmt.Errorf("duplicate target coverage")
		}
		seen[q] = true
	}
	if len(seen) != len(after.Required) {
		return fmt.Errorf("historical coverage differs")
	}
	for _, q := range after.Required {
		if !seen[q] {
			return fmt.Errorf("missing historical coverage")
		}
	}
	return nil
}
func (d Document) ValidateTransition(next Document, receipts []Activation) error {
	if e := d.ValidateSuccessor(next); e != nil {
		return e
	}
	used := make([]bool, len(receipts))
	for _, g := range d.Groups {
		for _, r := range g.Replicas {
			n, _ := next.Lookup(g.Selector.GroupID(), r.ID)
			if n.Epoch == r.Epoch {
				continue
			}
			found := false
			for i, a := range receipts {
				if a.Fence.Group == g.Selector.GroupID() && a.Fence.Replica == r.ID {
					if found {
						return fmt.Errorf("duplicate activation")
					}
					if e := a.Validate(d, next); e != nil {
						return e
					}
					used[i] = true
					found = true
				}
			}
			if !found {
				return fmt.Errorf("epoch activation requires fence and coverage")
			}
		}
	}
	for _, v := range used {
		if !v {
			return fmt.Errorf("unrelated activation receipt")
		}
	}
	return nil
}
