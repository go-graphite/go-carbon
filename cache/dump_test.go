package cache

import (
	"bufio"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

type NopWriter struct {
}

func (w *NopWriter) Write(b []byte) (int, error) {
	return len(b), nil
}

func fillCacheForDump() *Cache {
	metrics := 1000000
	pointsCount := 5

	c := New()
	c.SetMaxSize(uint64(pointsCount*metrics + 1))

	baseTimestamp := time.Now().Unix()

	for i := 0; i < metrics; i++ {
		for j := 0; j < pointsCount; j++ {
			c.Add(points.OnePoint(
				fmt.Sprintf("carbon.localhost.cache.size%d", i),
				42.15*float64(j),
				baseTimestamp+int64(j),
			))
		}
	}

	return c
}

func BenchmarkDump(b *testing.B) {
	c := fillCacheForDump()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w := bufio.NewWriterSize(&NopWriter{}, 1048576) // 1Mb
		c.Dump(w)
		w.Flush()
	}
}

func BenchmarkDumpBinary(b *testing.B) {
	c := fillCacheForDump()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w := bufio.NewWriterSize(&NopWriter{}, 1048576) // 1Mb
		c.DumpBinary(w)
		w.Flush()
	}

}

func TestVisitMetricNamesIncludesInFlightAndCached(t *testing.T) {
	c := New()
	c.Add(points.OnePoint("one", 1, 1))
	c.Add(points.OnePoint("two", 2, 2))
	if _, ok := c.PopNotConfirmed("one"); !ok {
		t.Fatal("missing in-flight batch")
	}
	c.Add(points.OnePoint("one", 3, 3))
	before := c.Size()
	names := map[string]int{}
	if err := c.VisitMetricNames(func(name string) error { names[name]++; return nil }); err != nil {
		t.Fatal(err)
	}
	if names["one"] != 2 || names["two"] != 1 || len(names) != 2 || c.Size() != before {
		t.Fatal("name visit changed or omitted cache state", names, c.Size(), before)
	}
	if got := c.Get("one"); len(got) != 2 || got[0].Value != 1 || got[1].Value != 3 {
		t.Fatal("in-flight/current history changed", got)
	}
}

func TestVisitMetricNamesParallelMatchesSerial(t *testing.T) {
	c := New()
	for i := range 5000 {
		c.Add(points.OnePoint(fmt.Sprintf("m.%d", i), 1, 1))
	}
	if p, ok := c.PopNotConfirmed("m.7"); !ok || p == nil {
		t.Fatal("pop")
	}
	var serial []string
	_ = c.VisitMetricNames(func(name string) error { serial = append(serial, name); return nil })
	var mu sync.Mutex
	var parallel []string
	if err := c.VisitMetricNamesParallel(8, func(name string) error {
		mu.Lock()
		parallel = append(parallel, name)
		mu.Unlock()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	slices.Sort(serial)
	slices.Sort(parallel)
	if !slices.Equal(serial, parallel) || len(serial) != 5000 {
		t.Fatal("parallel visit differs", len(serial), len(parallel))
	}
	stop := errors.New("stop")
	if err := c.VisitMetricNamesParallel(4, func(string) error { return stop }); !errors.Is(err, stop) {
		t.Fatal(err)
	}
}
