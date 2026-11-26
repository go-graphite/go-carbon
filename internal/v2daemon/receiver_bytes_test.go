package v2daemon

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"unsafe"

	"github.com/go-graphite/go-carbon/internal/namefp"
	"github.com/go-graphite/go-carbon/points"
)

func byteNames(names []string) ([]byte, []int) {
	var text []byte
	ends := make([]int, len(names))
	for i, name := range names {
		text = append(text, name...)
		ends[i] = len(text)
	}
	return text, ends
}

// Byte input and codec strings share the same fingerprint table. New names,
// retries after refusal, and quota callbacks must retain their named behavior.
func TestApplicationOwnerByteNames(t *testing.T) {
	for _, quota := range []bool{false, true} {
		t.Run(fmt.Sprint(quota), func(t *testing.T) {
			s, resolve := openProbeStore(t)
			p := s.Producers[0]
			queuedName := mirrorQueue(p)
			resolves := map[string]int{}
			var rejected []string
			o := &applicationOwner{p: p, resolve: func(name string) ([]byte, error) { resolves[name]++; return resolve(name) }, reject: func(name string, err error) {
				if !errors.Is(err, ErrNameTooLong) {
					t.Error(err)
				}
				rejected = append(rejected, name)
			}}
			var seen []string
			checkCanonical := true
			if quota {
				o.throttle = func(p *points.Points, _ bool) bool {
					seen = append(seen, p.Metric)
					if local, ok := o.names.Get(namefp.Name(p.Metric)); checkCanonical && ok && unsafe.StringData(p.Metric) != unsafe.StringData(o.p.applications[local].Name()) {
						t.Error("quota received a new string for an already registered name")
					}
					return p.Metric == "quota.blocked"
				}
			}
			bad := "bad." + strings.Repeat("x", points.MaxMetricNameBytes)
			names := []string{"ok.a", bad, bad, "ok.a", "ok.b", "quota.blocked", "ok.b"}
			text, ends := byteNames(names)
			hashes := make([]uint64, len(names))
			for i, name := range names {
				hashes[i] = namefp.Route(name)
			}
			if err := o.AppendBytes(text, ends, hashes, probePoints(len(names)), nil, 1010); err != nil {
				t.Fatal(err)
			}
			clear(text) // registration and retained reject callbacks cannot alias intake
			if len(rejected) != 2 || rejected[0] != bad || rejected[1] != bad {
				t.Fatal("refused names corrupted")
			}
			want := []string{"ok.a", "ok.a", "ok.b"}
			if !quota {
				want = append(want, "quota.blocked")
			}
			want = append(want, "ok.b")
			for i, name := range want {
				if got := queuedName(i); got != name {
					t.Fatalf("point %d name=%q want=%q", i, got, name)
				}
			}
			if quota && fmt.Sprint(seen) != fmt.Sprint(names) {
				t.Fatal("quota received wrong names")
			}
			// Reuse the now-zeroed buffer for known names, this time without route hashes.
			text, ends = byteNames([]string{"ok.b", "ok.a", "ok.b"})
			if err := o.AppendBytes(text, ends, nil, probePoints(3), nil, 1020); err != nil {
				t.Fatal(err)
			}
			checkCanonical = false // codec strings can borrow their own dictionary
			if err := o.Append([]string{"ok.a"}, nil, probePoints(1), nil, 1020); err != nil {
				t.Fatal(err)
			}
			if resolves["ok.a"] != 1 || resolves["ok.b"] != 1 {
				t.Fatal("byte/string fingerprints diverged", resolves)
			}
		})
	}
}
