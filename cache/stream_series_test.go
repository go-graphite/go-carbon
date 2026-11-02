package cache

import (
	"runtime"
	"testing"
	"unsafe"
	"weak"
)

func TestStreamSeriesSharesCacheNameAndDoesNotPinBatch(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	name := []byte("example.series")
	c.AddPoint(name, 1, 1)
	a := c.ResolveStreamName(name).(*StreamSeries)
	b := c.ResolveStreamName(append([]byte(nil), name...)).(*StreamSeries)
	if a != b {
		t.Fatal("definitions did not share a series identity")
	}
	sh := c.data[a.hash&(shardCount-1)]
	sh.mu.Lock()
	it := sh.items.getBytesHashed(a.hash, name)
	if unsafe.StringData(a.name) != unsafe.StringData(it.Metric) {
		t.Fatal("series copied a resident cache name")
	}
	old := weak.Make(it)
	it = nil
	sh.mu.Unlock()
	var slot any = a
	c.AddStreamPoint(name, 2, 2, &slot)
	if len(c.Get(string(name))) != 2 {
		t.Fatal("shared hint lost a point")
	}
	c.Pop(string(name))
	for range 10 {
		runtime.GC()
		if old.Value() == nil {
			break
		}
	}
	if old.Value() != nil {
		t.Fatal("wire series retained a popped cache batch")
	}
	c.AddStreamPoint(name, 3, 3, &slot)
	if got := c.Get(string(name)); len(got) != 1 || got[0].Value != 3 {
		t.Fatal("series did not re-resolve a removed cache entry")
	}
	runtime.KeepAlive(a)
	runtime.KeepAlive(slot)
}

func TestStreamGenerationExhaustionUsesOrdinaryLookup(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	name := []byte("example.exhausted")
	series := c.ResolveStreamName(name).(*StreamSeries)
	sh := c.data[series.hash&(shardCount-1)]
	sh.mu.Lock()
	sh.streamGeneration = ^uint64(0)
	sh.mu.Unlock()
	var slot any = series
	c.AddStreamPoint(name, 1, 1, &slot)
	if series.hint.Load() != 0 {
		t.Fatal("zero generation was published as a live hint")
	}
	c.Pop(string(name))
	c.AddStreamPoint(name, 2, 2, &slot)
	if got := c.Get(string(name)); len(got) != 1 || got[0].Value != 2 {
		t.Fatal("exhausted generations did not fall back safely")
	}
}
