package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type pausedUpdateCodec struct {
	batchTestCodec
	name             string
	entered, release chan struct{}
	once             sync.Once
}

func (c *pausedUpdateCodec) Update(ctx context.Context, io ContinuationIO, m Metadata, input []Point, retention int, clock func() time.Time, maintenance *ContinuationMaintenance) error {
	if m.Name == c.name {
		c.once.Do(func() { close(c.entered); <-c.release })
	}
	return c.batchTestCodec.Update(ctx, io, m, input, retention, clock, maintenance)
}

func TestUpdateReservationReleasesCollidingStripe(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprint(batch), func(t *testing.T) {
			codec := &pausedUpdateCodec{name: "held", entered: make(chan struct{}), release: make(chan struct{})}
			db, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, Continuation: codec, MetadataCacheSize: 8 << 20})
			defer closeTestStore(t, db)
			collision := ""
			for i := 0; ; i++ {
				collision = fmt.Sprintf("collision.%d", i)
				if db.metricStripe(collision) == db.metricStripe(codec.name) {
					break
				}
			}
			createTestMetric(t, db, codec.name)
			createTestMetric(t, db, collision)
			done := make(chan error, 1)
			go func() {
				if batch {
					done <- db.UpdateBatch(t.Context(), []MetricUpdate{{Name: codec.name, Points: []Point{{Value: 1}}}}, nil)
				} else {
					done <- db.UpdateMany(t.Context(), codec.name, []Point{{Value: 1}})
				}
			}()
			released := false
			defer func() {
				if !released {
					close(codec.release)
					<-done
				}
			}()
			select {
			case <-codec.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("codec not reached")
			}
			independent := make(chan error, 1)
			go func() {
				independent <- db.UpdateBatch(t.Context(), []MetricUpdate{{Name: collision, Points: []Point{{Value: 2}}}}, nil)
			}()
			select {
			case err := <-independent:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("codec retained unrelated metric stripe")
			}
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
			defer cancel()
			if err := db.TryUpdateMany(ctx, codec.name, []Point{{Value: 1}}); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("same-name reservation failed: %v", err)
			}
			close(codec.release)
			released = true
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if err := db.UpdateMany(t.Context(), codec.name, []Point{{Value: 1}}); err != nil {
				t.Fatal(err)
			}
			m, err := db.Metadata(t.Context(), codec.name)
			if err != nil || m.Revision != 3 {
				t.Fatalf("lost concurrent write: %+v %v", m, err)
			}
		})
	}
}
