package chunkstore

import (
	"context"
	"errors"
	"testing"
	"time"
)

// readPathCodec reports whether each update's probe went through a shared
// iterator. With hold set, an update reports, waits for hold, and probes
// again, failing if its read path changed meanwhile.
type readPathCodec struct {
	bucketCodec
	shared chan bool
	hold   chan struct{}
}

func (c *readPathCodec) Update(_ context.Context, io ContinuationIO, _ Metadata, _ []Point, _ int, _ func() time.Time, _ *ContinuationMaintenance) error {
	cio := io.(*continuationIO)
	if _, err := cio.Probe([]byte("plain")); err != nil {
		return err
	}
	shared := cio.iter != nil
	c.shared <- shared
	if c.hold == nil {
		return nil
	}
	<-c.hold
	iter := cio.iter
	if _, err := cio.Probe([]byte("plain")); err != nil {
		return err
	}
	if (cio.iter != nil) != shared || cio.iter != iter {
		return errors.New("an update in flight changed its read path")
	}
	return nil
}

// SetUpdatePointLookups switches the updates that start after it, and an
// update in flight keeps its own read path and iterator.
func TestSetUpdatePointLookupsSwitchesLaterUpdates(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	codec := &readPathCodec{shared: make(chan bool, 1)}
	s, now := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, Continuation: codec})
	defer closeTestStore(t, s)
	createTestMetric(t, s, "read.path")
	update := func() error {
		return s.UpdateMany(ctx, "read.path", []Point{{Timestamp: int64(now) - 60, Value: 1}})
	}
	expect := func(want bool) {
		t.Helper()
		if err := update(); err != nil {
			t.Fatal(err)
		}
		if got := <-codec.shared; got != want {
			t.Fatalf("update read through a shared iterator: %v, want %v", got, want)
		}
		if s.Stats().UpdatePointLookups == want {
			t.Fatalf("Stats.UpdatePointLookups = %v with shared reads %v", !want, want)
		}
	}
	expect(true)
	s.SetUpdatePointLookups(true)
	expect(false)
	s.SetUpdatePointLookups(false)
	expect(true)
	for _, lookups := range []bool{false, true} {
		s.SetUpdatePointLookups(lookups)
		codec.hold = make(chan struct{})
		done := make(chan error, 1)
		go func() { done <- update() }()
		if got := <-codec.shared; got == lookups {
			t.Fatalf("held update shared %v with point lookups %v", got, lookups)
		}
		s.SetUpdatePointLookups(!lookups)
		close(codec.hold)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		codec.hold = nil
		expect(lookups)
	}
}
