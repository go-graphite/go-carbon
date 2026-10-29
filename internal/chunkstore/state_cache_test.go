package chunkstore

import (
	"fmt"
	"reflect"
	"testing"
)

func testState(name string, id uint64) *metricState {
	return newMetricState(Metadata{MetricConfig: MetricConfig{Name: name, Retentions: []Retention{{Step: 60, Count: 10}}}, ID: id}).seal()
}

func TestStateCacheReaderAndWriterRules(t *testing.T) {
	c := newStateCache(1<<30, nil)
	const stripe = 7
	first := testState("a", 1)

	// A fill applies only while the epoch the reader captured holds and the
	// name has no entry.
	before := c.epoch(stripe)
	c.fill(stripe, "a", before, first)
	if got := c.get(stripe, "a", before); got != first {
		t.Fatalf("filled state not served: %v", got)
	}

	// A mutation hides the entry from readers but not from its writer.
	c.beginMutation(stripe, "a")
	if got := c.get(stripe, "a", c.epoch(stripe)); got != nil {
		t.Fatal("pending entry served to a reader")
	}
	if got := c.locked(stripe, "a"); got != first {
		t.Fatal("writer did not see the committed state")
	}
	// Nor can a reader fill it meanwhile.
	c.fill(stripe, "a", c.epoch(stripe), testState("a", 99))
	duringMutation := c.epoch(stripe)

	second := testState("a", 2)
	c.installMutation(stripe, "a", second)
	if got := c.get(stripe, "a", c.epoch(stripe)); got != nil {
		t.Fatal("state installed before the mutation ended")
	}
	c.endMutation(stripe, "a")
	// A reader that captured its epoch before the end may hold a view from
	// before the commit: it must not use the new state.
	if got := c.get(stripe, "a", duringMutation); got != nil {
		t.Fatal("new state served to a reader whose view may predate its commit")
	}
	if got := c.get(stripe, "a", c.epoch(stripe)); got != second {
		t.Fatalf("installed state not served: %v", got)
	}

	// A mutation that installs nothing forgets the name.
	c.beginMutation(stripe, "a")
	c.endMutation(stripe, "a")
	if c.get(stripe, "a", c.epoch(stripe)) != nil || c.locked(stripe, "a") != nil {
		t.Fatal("state survived a mutation that did not install it")
	}

	// A fill whose epoch moved is dropped: some mutation began or ended since.
	stale := c.epoch(stripe)
	c.beginMutation(stripe, "other")
	c.endMutation(stripe, "other")
	c.fill(stripe, "a", stale, first)
	if c.get(stripe, "a", c.epoch(stripe)) != nil {
		t.Fatal("fill applied after the shard's epoch moved")
	}
	if s := c.stats(); s.Entries != 0 || s.Bytes != 0 {
		t.Fatalf("accounting after forgetting every name: %+v", s)
	}
}

func TestStateCacheLockedFill(t *testing.T) {
	c := newStateCache(1<<30, nil)
	const stripe = 11
	// A reader whose view opened before the latest commit ended...
	c.beginMutation(stripe, "a")
	beforeEnd := c.epoch(stripe)
	c.endMutation(stripe, "a")
	// ...must not use a state recorded under the stripe after it.
	locked := testState("a", 1)
	c.fillLocked(stripe, "a", locked)
	if c.get(stripe, "a", beforeEnd) != nil {
		t.Fatal("locked fill served to a reader whose view may predate the last commit")
	}
	if c.get(stripe, "a", c.epoch(stripe)) != locked || c.locked(stripe, "a") != locked {
		t.Fatal("locked fill not served")
	}
	// It never replaces an existing entry.
	c.fillLocked(stripe, "a", testState("a", 2))
	if c.locked(stripe, "a") != locked {
		t.Fatal("locked fill replaced an entry")
	}
}

func TestStateCacheEvictionKeepsPendingMarkers(t *testing.T) {
	st := testState("x", 1)
	c := newStateCache(3*st.size*metricStripes, nil)
	const stripe = 3
	for i := range 10 {
		c.fill(stripe, fmt.Sprint("m", i), c.epoch(stripe), testState(fmt.Sprint("m", i), uint64(i)))
	}
	if got := c.shards[stripe].bytes; got > c.shardBudget {
		t.Fatalf("shard holds %d bytes over its %d budget", got, c.shardBudget)
	}
	if c.get(stripe, "m9", c.epoch(stripe)) == nil || c.get(stripe, "m0", c.epoch(stripe)) != nil {
		t.Fatal("eviction is not least recently used first")
	}
	// An entry pending a mutation may lose its state, never its marker.
	c.beginMutation(stripe, "m9")
	for i := 10; i < 20; i++ {
		c.fill(stripe, fmt.Sprint("m", i), c.epoch(stripe), testState(fmt.Sprint("m", i), uint64(i)))
	}
	c.fill(stripe, "m9", c.epoch(stripe), st)
	if e := c.shards[stripe].entries["m9"]; e == nil || e.pending != 1 {
		t.Fatal("eviction dropped a pending marker")
	}
	c.endMutation(stripe, "m9")
	if s := c.stats(); s.Evictions == 0 || s.Bytes != c.shards[stripe].bytes {
		t.Fatalf("eviction accounting: %+v, shard %d", s, c.shards[stripe].bytes)
	}
}

func TestMetricStateKeys(t *testing.T) {
	st := newMetricState(Metadata{})
	st.replacePrefix([]byte("n/"), map[string][]byte{"n/b": {2}, "n/a": {1}})
	st.set([]byte("f1h"), []byte{9})
	if v, known := st.lookup([]byte("n/a")); !known || !reflect.DeepEqual(v, []byte{1}) {
		t.Fatal("present key under a complete prefix")
	}
	if v, known := st.lookup([]byte("n/zz")); !known || v != nil {
		t.Fatal("a key missing under a complete prefix must be known absent")
	}
	if _, known := st.lookup([]byte("f2h")); known {
		t.Fatal("an unread key outside complete prefixes must be unknown")
	}
	if got := st.visit([]byte("n/")); !reflect.DeepEqual(got, []string{"n/a", "n/b"}) {
		t.Fatalf("visit order %v", got)
	}
	st.deletePrefix([]byte("n/"))
	if v, known := st.lookup([]byte("n/a")); !known || v != nil || len(st.visit([]byte("n/"))) != 0 {
		t.Fatal("prefix delete left a key present")
	}
	st.deletePrefix([]byte("f"))
	if v, known := st.lookup([]byte("f1h")); !known || v != nil {
		t.Fatal("a deleted known key must be known absent")
	}
	clone := st.clone(Metadata{ID: 5})
	clone.set([]byte("n/c"), []byte{3})
	if _, known := st.lookup([]byte("n/c")); known && st.keys["n/c"] != nil {
		t.Fatal("clone shares its keys with the original")
	}
}
