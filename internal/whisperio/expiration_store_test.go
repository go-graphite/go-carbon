package whisperio

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

type pausedExpirationDrop struct {
	*ContinuationCodec
	once             sync.Once
	entered, release chan struct{}
}

func (c *pausedExpirationDrop) Drop(ctx context.Context, io store.ContinuationIO) error {
	c.once.Do(func() { close(c.entered) })
	select {
	case <-c.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	return c.ContinuationCodec.Drop(ctx, io)
}

// Write a new time bucket through the real codec while expiration's old view
// is pinned. A stale candidate must conflict and leave the new point readable.
func TestExpirationPreservesConcurrentWhisperWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const base = int64(1790000000)
	var now atomic.Int64
	now.Store(base)
	codec := &pausedExpirationDrop{ContinuationCodec: &ContinuationCodec{ReuseBlockKeys: true}, entered: make(chan struct{}), release: make(chan struct{})}
	db, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: codec, Now: func() time.Time { return time.Unix(now.Load(), 0) }, CacheSize: 1 << 20, MemTableSize: 1 << 20, SyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	var candidates []store.Metadata
	for _, name := range []string{"changed", "unchanged"} {
		_, err := db.Create(ctx, store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 172800}, {Step: 60, Count: 40320}, {Step: 3600, Count: 17520}}, AggregationMethod: store.Average, XFilesFactor: .5})
		if err != nil {
			t.Fatal(err)
		}
		points := make([]store.Point, 400)
		for i := range points {
			points[i] = store.Point{Timestamp: base - int64(len(points)-i), Value: float64(i % 17)}
		}
		if err := db.UpdateMany(ctx, name, points); err != nil {
			t.Fatal(err)
		}
		m, err := db.Metadata(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		candidates = append(candidates, m)
	}
	now.Store(base + 48*3600)
	done := make(chan error, 1)
	go func() {
		outcomes, err := db.DeleteBatchIfUnchanged(ctx, candidates)
		if err == nil && (len(outcomes) != 2 || !errors.Is(outcomes[0], store.ErrConflict) || outcomes[1] != nil) {
			err = fmt.Errorf("outcomes: %v", outcomes)
		}
		done <- err
	}()
	select {
	case <-codec.entered:
	case <-ctx.Done():
		t.Fatal("expiration preparation did not start")
	}
	written := make(chan error, 1)
	go func() {
		written <- db.UpdateMany(ctx, "changed", []store.Point{{Timestamp: now.Load() - 1, Value: 42}})
	}()
	var writeErr error
	joined := false
	select {
	case writeErr = <-written:
		joined = true
	case <-ctx.Done():
		writeErr = errors.New("expiration key collection blocked the writer")
	}
	close(codec.release)
	if !joined {
		<-written
	}
	if deleteErr := <-done; writeErr != nil || deleteErr != nil {
		t.Fatalf("write=%v delete=%v", writeErr, deleteErr)
	}
	if _, err := db.Metadata(ctx, "unchanged"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unchanged candidate survived: %v", err)
	}
	series, err := db.Fetch(ctx, "changed", int(now.Load()-2), int(now.Load()-1))
	if err != nil || series == nil || len(series.Values) != 1 || series.Values[0] != 42 {
		t.Fatalf("lost concurrent point: %+v %v", series, err)
	}
}
