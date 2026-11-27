package v2placement

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"unicode/utf8"
)

type Store struct {
	ID        string `json:"id"`
	Write     string `json:"write"`
	Read      string `json:"read"`
	Copy      string `json:"copy"`
	Blackhole bool   `json:"blackhole,omitempty"`
}

// Lane names an authenticated producer incarnation and one ordered stream.
type Lane struct {
	Producer string `json:"producer"`
	Stream   string `json:"stream"`
}

// Requirement names immutable historical coverage, including metadata and masks.
type Requirement struct {
	Source string `json:"source"`
	Root   string `json:"root"`
}
type Replica struct {
	ID          string        `json:"id"`
	Epoch       uint64        `json:"write_epoch"`
	Writer      string        `json:"writer"`
	Read        []string      `json:"read"`
	CopySources []string      `json:"copy_sources"`
	Producers   []Lane        `json:"producers"`
	Required    []Requirement `json:"required"`
}
type Group struct {
	Selector Selector  `json:"selector"`
	Replicas []Replica `json:"replicas"`
}

// BootstrapReplica freezes explicit legacy slots, including holes and repeated
// endpoints. It is never reconstructed from discovered hosts.
type BootstrapReplica struct {
	ID       string   `json:"id"`
	JumpSize uint32   `json:"jump_size"`
	Slots    []string `json:"slots"`
}

// Document is a complete immutable map. Store IDs name durable incarnations.
// Digests use Go's compact JSON of these structs in declared field order; array
// order is significant. Decoders must preserve it, rather than hash input JSON.
type Document struct {
	Schema      int                `json:"schema"`
	Domain      string             `json:"domain"`
	Revision    uint64             `json:"map_revision"`
	Predecessor string             `json:"predecessor_digest"`
	Operation   string             `json:"operation"`
	Routing     Routing            `json:"routing"`
	Bootstrap   []BootstrapReplica `json:"bootstrap"`
	Stores      []Store            `json:"stores"`
	Groups      []Group            `json:"groups"`
}

func digest(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func validDigest(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == 32 && s == hex.EncodeToString(b)
}
func (d Document) Digest() string { return digest(d) }
func Decode(r io.Reader) (Document, error) {
	var d Document
	b, e := io.ReadAll(io.LimitReader(r, (16<<20)+1))
	if e != nil {
		return d, e
	}
	if !utf8.Valid(b) {
		return d, fmt.Errorf("invalid UTF-8 placement JSON")
	}
	if len(b) > 16<<20 {
		return d, fmt.Errorf("placement exceeds 16 MiB")
	}
	// Duplicate object fields have ambiguous cross-language meaning: reject them.
	if e = uniqueFields(b); e != nil {
		return d, e
	}
	dec := json.NewDecoder(bytesReader(b))
	dec.DisallowUnknownFields()
	if e = dec.Decode(&d); e != nil {
		return d, e
	}
	var extra any
	if e = dec.Decode(&extra); !errors.Is(e, io.EOF) {
		return d, fmt.Errorf("trailing placement content")
	}
	return d, d.Validate()
}
func (d Document) Validate() error {
	if !validStrings(reflect.ValueOf(d)) {
		return fmt.Errorf("placement strings must be valid UTF-8")
	}
	if d.Schema != Version || d.Domain == "" || d.Revision == 0 || d.Operation == "" || (d.Revision == 1 && d.Predecessor != "") || (d.Revision > 1 && !validDigest(d.Predecessor)) {
		return fmt.Errorf("invalid placement version/identity")
	}
	if err := d.Routing.Validate(); err != nil {
		return err
	}
	stores := map[string]Store{}
	for _, s := range d.Stores {
		if s.ID == "" {
			return fmt.Errorf("empty store incarnation")
		}
		if _, ok := stores[s.ID]; ok {
			return fmt.Errorf("duplicate store incarnation")
		}
		if s.Blackhole {
			if s.Write != "" || s.Read != "" || s.Copy != "" {
				return fmt.Errorf("blackhole has endpoints")
			}
		} else if s.Write == "" || s.Read == "" || s.Copy == "" {
			return fmt.Errorf("missing store endpoints")
		}
		stores[s.ID] = s
	}
	boot := map[string]BootstrapReplica{}
	for _, b := range d.Bootstrap {
		if b.ID == "" || len(b.Slots) != int(b.JumpSize) {
			return fmt.Errorf("invalid bootstrap replica")
		}
		if _, ok := boot[b.ID]; ok {
			return fmt.Errorf("duplicate bootstrap replica")
		}
		found := false
		for _, n := range d.Routing.JumpSizes {
			found = found || n == b.JumpSize
		}
		if !found {
			return fmt.Errorf("unknown bootstrap size")
		}
		for _, id := range b.Slots {
			if _, ok := stores[id]; !ok {
				return fmt.Errorf("unknown bootstrap store")
			}
		}
		boot[b.ID] = b
	}
	if len(boot) == 0 {
		return fmt.Errorf("no bootstrap replicas")
	}
	groups := map[string]bool{}
	for _, g := range d.Groups {
		s := g.Selector
		id := s.GroupID()
		if groups[id] || !d.Routing.validSelector(s) {
			return fmt.Errorf("invalid or duplicate group selector %s", id)
		}
		groups[id] = true
		if len(g.Replicas) != len(boot) {
			return fmt.Errorf("missing replica")
		}
		replicas := map[string]bool{}
		for _, r := range g.Replicas {
			b, exists := boot[r.ID]
			_, writerExists := stores[r.Writer]
			if !exists || replicas[r.ID] || r.Epoch == 0 || !writerExists {
				return fmt.Errorf("invalid writer/epoch")
			}
			replicas[r.ID] = true
			if d.Revision == 1 {
				idx := 0
				for d.Routing.JumpSizes[idx] != b.JumpSize {
					idx++
				}
				if r.Epoch != 1 || r.Writer != b.Slots[s.Cell[idx]] {
					return fmt.Errorf("bootstrap reshuffles legacy ownership")
				}
			}
			if err := validateLanes(r.Producers); err != nil {
				return err
			}
			if !stores[r.Writer].Blackhole && len(r.Producers) == 0 {
				return fmt.Errorf("writer inventory required")
			}
			for _, set := range [][]string{r.Read, r.CopySources} {
				seen := map[string]bool{}
				for _, v := range set {
					st, ok := stores[v]
					if !ok || seen[v] || st.Blackhole {
						return fmt.Errorf("invalid location")
					}
					seen[v] = true
				}
			}
			if !stores[r.Writer].Blackhole && !contains(r.Read, r.Writer) {
				return fmt.Errorf("writer absent from readers")
			}
			seen := map[Requirement]bool{}
			for _, q := range r.Required {
				if _, ok := stores[q.Source]; !ok || !validDigest(q.Root) || seen[q] || !contains(r.Read, q.Source) || !contains(r.CopySources, q.Source) {
					return fmt.Errorf("invalid historical coverage requirement")
				}
				seen[q] = true
			}
		}
	}
	expected, err := d.Routing.groupCount()
	if err != nil {
		return err
	}
	if uint64(len(groups)) != expected {
		return fmt.Errorf("missing placement groups: got %d want %d", len(groups), expected)
	}
	return nil
}
func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
func validateLanes(lanes []Lane) error {
	seen := map[Lane]bool{}
	for _, l := range lanes {
		if l.Producer == "" || l.Stream == "" || seen[l] {
			return fmt.Errorf("invalid or duplicate producer lane")
		}
		seen[l] = true
	}
	return nil
}
func (d Document) Lookup(group, replica string) (Replica, error) {
	for _, g := range d.Groups {
		if g.Selector.GroupID() == group {
			for _, r := range g.Replicas {
				if r.ID == replica {
					return r, nil
				}
			}
		}
	}
	return Replica{}, fmt.Errorf("unmapped group/replica %s/%s", group, replica)
}

type Selection struct {
	Group     string  `json:"group"`
	VShard    uint32  `json:"vshard"`
	Replica   Replica `json:"replica"`
	MapDigest string  `json:"map_digest"`
	Revision  uint64  `json:"map_revision"`
}

func (d Document) Select(name, replica string) (Selection, error) {
	if err := d.Validate(); err != nil {
		return Selection{}, err
	}
	s, v, e := d.Routing.Select(name)
	if e != nil {
		return Selection{}, e
	}
	r, e := d.Lookup(s.GroupID(), replica)
	return Selection{s.GroupID(), v, r, d.Digest(), d.Revision}, e
}

// ValidateSuccessor checks structure and monotonicity. Use ValidateTransition for
// adoption: topology validation alone never authorizes an ownership change.
func (d Document) ValidateSuccessor(next Document) error {
	if err := d.Validate(); err != nil {
		return err
	}
	if err := next.Validate(); err != nil {
		return err
	}
	if d.Revision == ^uint64(0) || next.Revision != d.Revision+1 || next.Predecessor != d.Digest() || next.Operation == d.Operation || next.Domain != d.Domain || !reflect.DeepEqual(next.Routing, d.Routing) || !reflect.DeepEqual(next.Bootstrap, d.Bootstrap) || len(next.Groups) != len(d.Groups) {
		return fmt.Errorf("stale predecessor or changed frozen routing")
	}
	for _, s := range d.Stores {
		found := false
		for _, n := range next.Stores {
			if n.ID == s.ID {
				found = true
				if n.Blackhole != s.Blackhole {
					return fmt.Errorf("changed store incarnation kind")
				}
			}
		}
		if !found {
			return fmt.Errorf("removed historical store incarnation")
		}
	}
	for _, g := range d.Groups {
		for _, r := range g.Replicas {
			n, e := next.Lookup(g.Selector.GroupID(), r.ID)
			if e != nil {
				return e
			}
			if n.Epoch < r.Epoch || (n.Epoch > r.Epoch && (r.Epoch == ^uint64(0) || n.Epoch != r.Epoch+1)) || (n.Writer != r.Writer && n.Epoch == r.Epoch) {
				return fmt.Errorf("invalid epoch transition")
			}
			if n.Epoch == r.Epoch && !reflect.DeepEqual(n.Producers, r.Producers) {
				return fmt.Errorf("producer inventory change requires a fenced epoch")
			}
			// Source/read retirement requires its own coverage protocol, not an edit.
			for _, id := range r.Read {
				if !contains(n.Read, id) {
					return fmt.Errorf("read retirement requires coverage protocol")
				}
			}
			for _, id := range r.CopySources {
				if !contains(n.CopySources, id) {
					return fmt.Errorf("source retirement requires coverage protocol")
				}
			}
			for _, q := range r.Required {
				found := false
				for _, v := range n.Required {
					found = found || v == q
				}
				if !found {
					return fmt.Errorf("removed historical coverage requirement")
				}
			}
		}
	}
	return nil
}

func validStrings(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.String:
		return utf8.ValidString(v.String())
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if !validStrings(v.Field(i)) {
				return false
			}
		}
	case reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			if !validStrings(v.Index(i)) {
				return false
			}
		}
	}
	return true
}
