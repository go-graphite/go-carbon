package carbonserver

import (
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"unsafe"
)

// The previous boolean-state compiler is kept as an independent oracle.
// Compare the complete cyclic automaton, including every accepted byte and
// ordered edge, so matching and malformed-expression behavior stay exact.
var booleanEndGstate = &booleanGstate{}

type booleanGmatcher struct {
	exact   bool
	root    *booleanGstate
	expr    string
	dstates []*booleanGdstate
	dsindex int //nolint:unused

}

type booleanGstate struct {
	// TODO: make c compact
	c    [maxGstateCLen]bool
	next []*booleanGstate
}

type booleanGdstate struct {
	gstates []*booleanGstate
}

func (g *booleanGmatcher) dstate() *booleanGdstate { return g.dstates[len(g.dstates)-1] }
func (g *booleanGmatcher) push(s *booleanGdstate)  { g.dstates = append(g.dstates, s) }
func (g *booleanGmatcher) pop(i int) {
	if len(g.dstates) <= i {
		g.dstates = g.dstates[:1]
		return
	}
	g.dstates = g.dstates[:len(g.dstates)-i]
}

func (g *booleanGstate) String() string {
	var toStr func(g *booleanGstate) string
	var ref = map[*booleanGstate]string{}
	toStr = func(g *booleanGstate) string {
		if r, ok := ref[g]; ok {
			return r
		}

		if g == booleanEndGstate {
			ref[g] = "end"
			return "end"
		}
		var b []byte
		for c, t := range g.c {
			if t {
				if isAlphanumeric(byte(c)) {
					b = append(b, byte(c))
				} else {
					b = append(b, '.')
				}
			}
		}
		var r = string(b)
		ref[g] = r + "(...)"
		for _, n := range g.next {
			r += "(" + toStr(n) + ")"
		}

		ref[g] = r
		return r
	}

	return toStr(g)
}

func (g booleanGdstate) String() string {
	var r []string
	for _, s := range g.gstates {
		r = append(r, s.String())
	}
	return strings.Join(r, ",")
}

func (g *booleanGdstate) step(c byte) *booleanGdstate {
	// if g.next[c] != nil {
	// 	g.cacheHit++
	// 	return g.next[c]
	// }

	var ng booleanGdstate
	for _, s := range g.gstates {
		if s.c[c] || s.c['*'] {
			for _, ns := range s.next {
				ng.gstates = g.add(ng.gstates, ns)
			}
		}
	}
	// g.next[c] = &ng
	return &ng
}

func (g *booleanGdstate) add(list []*booleanGstate, s *booleanGstate) []*booleanGstate {
	if s.c[gstateSplit] {
		for _, ns := range s.next {
			list = g.add(list, ns)
		}

		return list
	}

	list = append(list, s)
	return list
}

func (g *booleanGdstate) matched() bool {
	for _, s := range g.gstates {
		if s == booleanEndGstate {
			return true
		}
	}
	return false
}

// TODO:
// add range value validation
// nolint:unparam // FIXME - nothing calls or uses the expand callback
func booleanNewGlobState(expr string, expand func(globs []string) ([]string, error)) (*booleanGmatcher, error) {
	var m = booleanGmatcher{root: &booleanGstate{}, exact: true, expr: expr}
	var cur = m.root
	var alters [][2]*booleanGstate

	for i := 0; i < len(expr); i++ {
		c := expr[i]
		switch c {
		case '[':
			m.exact = false
			s := &booleanGstate{}
			i++
			if i >= len(expr) {
				return nil, errors.New("glob: broken range syntax")
			}
			negative := expr[i] == '^'
			if negative {
				i++
			}
			for i < len(expr) && expr[i] != ']' {
				if expr[i] == '-' {
					if i+1 >= len(expr) {
						return nil, errors.New("glob: missing closing range")
					}
					if expr[i-1] > expr[i+1] {
						return nil, errors.New("glob: range start is bigger than range end")
					}
					// a simple check to make sure that range doesn't ends with 0xff,
					// which would causes endless loop bellow
					if expr[i-1] > 128 || expr[i+1] > 128 {
						return nil, errors.New("glob: range overflow")
					}

					for j := expr[i-1] + 1; j <= expr[i+1]; j++ {
						if j != '*' {
							s.c[j] = true
						}
					}

					i += 2
					continue
				}

				s.c[expr[i]] = true
				i++
			}
			if i >= len(expr) || expr[i] != ']' {
				return nil, errors.New("glob: missing ]")
			}

			if negative {
				// TODO: revisit
				for i := 32; i <= 126; i++ {
					if i != '*' {
						s.c[i] = !s.c[i]
					}
				}
			}

			cur.next = append(cur.next, s)
			cur = s

		case '?':
			m.exact = false
			var star booleanGstate
			star.c['*'] = true
			cur.next = append(cur.next, &star)
			cur = &star

		case '*':
			m.exact = false

			// de-dup multi stars: *** -> *
			for ; i+1 < len(expr) && expr[i+1] == '*'; i++ {

			}

			var split, star booleanGstate
			star.c['*'] = true
			split.c[gstateSplit] = true
			split.next = append(split.next, &star)
			cur.next = append(cur.next, &split)
			star.next = append(star.next, &split)

			cur = &split
		case '{':
			alterStart := &booleanGstate{c: [maxGstateCLen]bool{gstateSplit: true}}
			alterEnd := &booleanGstate{c: [maxGstateCLen]bool{gstateSplit: true}}
			cur.next = append(cur.next, alterStart)
			cur = alterStart
			alters = append(alters, [2]*booleanGstate{alterStart, alterEnd})

		case '}':
			if len(alters) == 0 {
				return nil, errors.New("glob: missing {")
			}
			cur.next = append(cur.next, alters[len(alters)-1][1])
			cur = alters[len(alters)-1][1]
			alters = alters[:len(alters)-1]

		case ',':
			if len(alters) > 0 {
				cur.next = append(cur.next, alters[len(alters)-1][1])
				cur = alters[len(alters)-1][0]
				continue
			}

			// TODO: should return error?
			fallthrough
		default:
			s := &booleanGstate{}
			s.c[c] = true
			cur.next = append(cur.next, s)
			cur = s
		}
	}
	cur.next = append(cur.next, booleanEndGstate)

	if len(alters) > 0 {
		return nil, errors.New("glob: missing }")
	}

	var droot booleanGdstate
	for _, s := range m.root.next {
		droot.gstates = droot.add(droot.gstates, s)
	}
	m.dstates = append(m.dstates, &droot)

	return &m, nil
}

func TestGlobBitsetMatchesBooleanStates(t *testing.T) {
	patterns := []string{"", "*", "***", "?", "[a-z]", "[^a-z]", "{a,b}", "{a,{b,c}}", "[", "[z-a]", "[a-", "{", "}", ",", "a,b", "a{b,}c", "a*b?c", "空间", "services/api/instances/worker42/requests/latency/seconds/maximum"}
	for c := range 256 {
		patterns = append(patterns, string([]byte{byte(c)}))
	}
	r := rand.New(rand.NewSource(8144))
	tokens := []string{"a", "0", "*", "?", "[ab]", "[^ab]", "{x,y}", "{a,{b,c}}", "/", ".", ",", "_", "[z-a]", "[", "}", "{"}
	for range 5000 {
		var b strings.Builder
		for range r.Intn(16) {
			b.WriteString(tokens[r.Intn(len(tokens))])
		}
		patterns = append(patterns, b.String())
	}
	for _, expr := range patterns {
		old, oe := booleanNewGlobState(expr, nil)
		next, ne := newGlobState(expr, nil)
		if fmt.Sprint(oe) != fmt.Sprint(ne) {
			t.Fatalf("%q errors old=%v new=%v", expr, oe, ne)
		}
		if oe != nil {
			continue
		}
		if old.exact != next.exact || old.expr != next.expr {
			t.Fatal(expr, "metadata differs")
		}
		type pair struct {
			o *booleanGstate
			n *gstate
		}
		q := []pair{{old.root, next.root}}
		seen := make(map[pair]bool)
		for len(q) > 0 {
			p := q[len(q)-1]
			q = q[:len(q)-1]
			if seen[p] {
				continue
			}
			seen[p] = true
			if (p.o == booleanEndGstate) != (p.n == endGstate) {
				t.Fatal(expr, "sentinel differs")
			}
			for c := range maxGstateCLen {
				if p.o.c[c] != p.n.hasChar(c) {
					t.Fatalf("%q char %d differs", expr, c)
				}
			}
			if len(p.o.next) != len(p.n.next) {
				t.Fatal(expr, "edges differ")
			}
			for i := range p.o.next {
				q = append(q, pair{p.o.next[i], p.n.next[i]})
			}
		}
	}
	t.Logf("state bytes %d -> %d; expressions %d", unsafe.Sizeof(booleanGstate{}), unsafe.Sizeof(gstate{}), len(patterns))
}
func BenchmarkGlobStateSets(b *testing.B) {
	for _, expr := range []string{"wallclock_sysctl", "per_persona.{api,worker}.summary.*", "{a,b,c}.{d,e,f}.[a-z]*"} {
		b.Run(expr, func(b *testing.B) {
			b.Run("old", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					_, err := booleanNewGlobState(expr, nil)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("bitset", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					_, err := newGlobState(expr, nil)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
