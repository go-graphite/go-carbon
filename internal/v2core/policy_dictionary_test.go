package v2core

import (
	"bytes"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"unsafe"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2dict"
)

// A sealed identity must release its heap name, while canonical definitions,
// collision checks and escaped names remain exact across dictionary flushes.
func TestPolicyCatalogDictionaryOwnsNames(t *testing.T) {
	d, err := v2dict.Open(t.TempDir(), v2dict.Options{BufferBytes: 256})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	c := &PolicyCatalog{}
	c.dict.Store(d)
	if n := unsafe.Sizeof(PolicyIdentity{}); n != 64 {
		t.Fatalf("identity grew: %d bytes", n)
	}
	for i, name := range []string{"plain.metric", `escaped.\"metric`, "unicode.é.metric", strings.Repeat("long.", 200) + "tail"} {
		_, raw := rollupTestPolicy(t, name, "1s:1d", sealrollup.Last, .5)
		h, err := c.admit(name, raw)
		if err != nil {
			t.Fatal(err)
		}
		e, _, err := d.Append(string(h.policy.Group), h.vshard, []byte(name), 0)
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		for range 4 {
			wg.Go(func() {
				for range 100 {
					if h.Name() != name || !bytes.Equal(h.Definition(), raw) {
						t.Error("identity changed during dictionary handoff")
						return
					}
				}
			})
		}
		h.rememberDictID(e.ID)
		if h.name.Load() != nil {
			t.Fatal("catalog retained heap name after dictionary handoff")
		}
		runtime.GC()
		if err := d.Flush(); err != nil {
			t.Fatal(err)
		}
		wg.Wait()
		if got, err := c.intern(h.id, name, raw); err != nil || got != h {
			t.Fatal("dictionary identity changed", err)
		}
		if _, err := c.intern(h.id, name+"changed", raw); err == nil {
			t.Fatal("accepted name collision")
		}
		_, changed := rollupTestPolicy(t, name, "1s:1d", sealrollup.Sum, .5)
		if _, err := c.intern(h.id, name, changed); err == nil {
			t.Fatal("accepted policy change")
		}
		if h.Name() != name {
			t.Fatalf("lost flushed name %s", fmt.Sprint(i))
		}
	}
}
