package cache

import (
	"fmt"
	"runtime"
	"testing"
	"weak"

	"github.com/zeebo/xxh3"
)

func TestStreamDirectHintInvalidation(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	name := []byte("stream.direct.lifecycle")
	s := c.ResolveStreamName(name).(*StreamSeries)
	c.addStreamSeries(s, 1, 1)
	sh := c.data[s.hash&(shardCount-1)]
	if s.item == nil {
		t.Fatal("direct hint not published with cold admission")
	}
	original := s.item
	// Growth must preserve the old numeric hint's cold-refresh boundaries.
	oldCapacity := len(sh.items.slots)
	for i := 0; len(sh.items.slots) == oldCapacity; i++ {
		n := fmt.Appendf(nil, "stream.direct.growth.%d", i)
		if xxh3.Hash(n)&(shardCount-1) == s.hash&(shardCount-1) {
			c.AddPoint(n, 1, 1)
		}
	}
	if s.item != nil {
		t.Fatal("rehash kept direct hint")
	}
	c.addStreamSeries(s, 2, 2)
	if s.item != original {
		t.Fatal("rehash did not rebind original item")
	}
	// Replacement is a different lifetime even with the same name and hash.
	replacement := newItem(string(name), 0)
	sh.mu.Lock()
	sh.items.setHashed(s.hash, replacement)
	sh.mu.Unlock()
	if s.item != nil {
		t.Fatal("replacement retained old item")
	}
	c.addStreamSeries(s, 3, 3)
	if s.item != replacement {
		t.Fatal("replacement not rebound")
	}
	p, ok := c.PopNotConfirmed(string(name))
	if !ok || s.item != nil {
		t.Fatal("writeout retained direct item")
	}
	c.Requeue(p)
	c.addStreamSeries(s, 4, 4)
	got := c.Get(string(name))
	if len(got) != 2 || got[0].Value != 3 || got[1].Value != 4 {
		t.Fatalf("replacement/requeue order changed: %v", got)
	}
	c.Pop(string(name))
	if s.item != nil {
		t.Fatal("pop retained direct item")
	}
}

func TestStreamDirectHintDoesNotRetainIdentity(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	name := []byte("stream.direct.abandoned")
	dropped := func() weak.Pointer[StreamSeries] {
		s := c.ResolveStreamName(name).(*StreamSeries)
		c.addStreamSeries(s, 1, 1)
		return weak.Make(s)
	}()
	for range 10 {
		runtime.GC()
		if dropped.Value() == nil {
			break
		}
	}
	if dropped.Value() != nil {
		t.Fatal("resident item retained abandoned stream identity")
	}
	s := c.ResolveStreamName(name).(*StreamSeries)
	c.addStreamSeries(s, 2, 2)
	if s.item == nil || len(c.Get(string(name))) != 2 {
		t.Fatal("dead reverse hint prevented rebinding")
	}
	runtime.KeepAlive(c)
	runtime.KeepAlive(s)
}

func TestStreamDirectHintMultipleIdentities(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	name := []byte("stream.direct.alias")
	owner := c.ResolveStreamName(name).(*StreamSeries)
	c.addStreamSeries(owner, 1, 1)
	// A bounded weak-index replacement can create another live identity for
	// the same name. It must retain the existing numeric fallback safely.
	other := &StreamSeries{name: string(name), cache: c, hash: owner.hash}
	for i := range 20 {
		c.addStreamSeries(other, float64(i+2), int64(i+2))
	}
	if other.item != nil || owner.item == nil {
		t.Fatal("multiple direct owners")
	}
	if got := c.Get(string(name)); len(got) != 21 {
		t.Fatalf("alias lost points: %d", len(got))
	}
	c.Pop(string(name))
	if owner.item != nil {
		t.Fatal("removal did not clear owner")
	}
	c.addStreamSeries(other, 99, 99)
	if other.item == nil {
		t.Fatal("alias could not own replacement")
	}
	if got := c.Get(string(name)); len(got) != 1 || got[0].Value != 99 {
		t.Fatal("alias used removed item")
	}
}
