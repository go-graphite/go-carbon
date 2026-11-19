package v2core

import (
	"bytes"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

type testPolicyNames []string

func (s testPolicyNames) FindName(name string) (uint32, bool) {
	for i, n := range s {
		if n == name {
			return uint32(i), true
		}
	}
	return 0, false
}
func (s testPolicyNames) NameAt(pos uint32) string { return strings.Clone(s[pos]) }

func TestMappedPolicyNamesTailAndGeneration(t *testing.T) {
	c := &PolicyCatalog{}
	_, raw := rollupTestPolicy(t, "service.old", "1s:1d", sealrollup.Last, .5)
	h, err := c.intern(7, "service.old", raw)
	if err != nil {
		t.Fatal(err)
	}
	if h.policy.Name != "" {
		t.Fatal("identity retained heap name")
	}
	if err := c.BindMappedNames(testPolicyNames{"service.old"}); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.nameTail.Load(uint64(7)); ok {
		t.Fatal("mapped name remains in heap tail")
	}
	old := func() *policyNamePage { p, _ := c.ids.Load(uint64(0)); return p.(*policyIDPage).names.Load() }()
	// Position zero deliberately means a different name in the successor.
	if err := c.BindMappedNames(testPolicyNames{"service.new"}); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	if h.Name() != "service.old" || old.sources[0].NameAt(0) != "service.old" || !bytes.Equal(h.Definition(), raw) {
		t.Fatal("resolved an identity through the wrong generation")
	}
	_, raw2 := rollupTestPolicy(t, "service.new", "1s:1d", sealrollup.Last, .5)
	fresh, err := c.intern(8, "service.new", raw2)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.Name() != "service.new" {
		t.Fatal("fresh tail name missing")
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 20 {
				if h.Name() != "service.old" || fresh.Name() != "service.new" {
					t.Error("concurrent generation mismatch")
				}
			}
		})
	}
	for range 4 {
		if err := c.BindMappedNames(testPolicyNames{"service.new", "service.old"}); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	if !bytes.Equal(fresh.Definition(), raw2) {
		t.Fatal("canonical bytes changed")
	}
}

func BenchmarkMappedPolicyNameDecode(b *testing.B) {
	for _, mapped := range []bool{false, true} {
		b.Run(fmt.Sprint(mapped), func(b *testing.B) {
			c := &PolicyCatalog{}
			names := make(testPolicyNames, 1024)
			handles := make([]*PolicyIdentity, len(names))
			for i := range names {
				names[i] = fmt.Sprintf("service.%08d.%s", i, strings.Repeat("x", 96))
				raw, err := EncodeRollupPolicy(RollupPolicy{Name: v2part.ID(names[i]), Group: "test", Config: sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 1, Count: 86400}}}})
				if err != nil {
					b.Fatal(err)
				}
				handles[i], err = c.intern(uint64(i+1), names[i], raw)
				if err != nil {
					b.Fatal(err)
				}
			}
			if mapped {
				if err := c.BindMappedNames(names); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if handles[(uint64(i)*2654435761)%1024].Name() == "" {
					b.Fatal("empty")
				}
			}
		})
	}
}
