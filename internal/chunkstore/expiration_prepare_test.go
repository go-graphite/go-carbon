package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

type pausedExpirationCodec struct {
	bucketCodec
	entered, release chan struct{}
	paused           atomic.Bool
}

func (c *pausedExpirationCodec) Drop(ctx context.Context, io ContinuationIO) error {
	if c.paused.CompareAndSwap(false, true) {
		close(c.entered)
		select {
		case <-c.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return c.bucketCodec.Drop(ctx, io)
}

func (*pausedExpirationCodec) Update(_ context.Context, io ContinuationIO, _ Metadata, _ []Point, _ int, _ func() time.Time, _ *ContinuationMaintenance) error {
	return io.Set(ContinuationBucketKey(7, []byte("new")), []byte("new write"))
}

// Finish a mutation while key collection is paused, pinning the optimistic
// window instead of depending on the scheduler to race the two operations.
func TestExpirationPreparationRevalidatesConcurrentMutation(t *testing.T) {
	for _, action := range []string{"update", "recreate"} {
		t.Run(action, func(t *testing.T) {
			codec := &pausedExpirationCodec{entered: make(chan struct{}), release: make(chan struct{})}
			s, now := openTestStore(t, "/store", Options{fs: vfs.NewMem(), ExactBlocks: true, TimeBuckets: true, Continuation: codec, MetadataCacheSize: 4 << 20})
			defer closeTestStore(t, s)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			old, peer := createTestMetric(t, s, "changed"), createTestMetric(t, s, "unchanged")
			done := make(chan error, 1)
			go func() {
				outcomes, err := s.DeleteBatchIfUnchanged(ctx, []Metadata{old, peer})
				if err == nil && (len(outcomes) != 2 || !errors.Is(outcomes[0], ErrConflict) || outcomes[1] != nil) {
					err = fmt.Errorf("outcomes: %v", outcomes)
				}
				done <- err
			}()
			select {
			case <-codec.entered:
			case <-ctx.Done():
				t.Fatal("preparation did not start")
			}
			mutated := make(chan error, 1)
			go func() {
				var err error
				if action == "update" {
					err = s.UpdateMany(ctx, old.Name, []Point{{Timestamp: int64(now), Value: 7}})
				} else if err = s.Delete(ctx, old.Name); err == nil {
					_, err = s.Create(ctx, old.MetricConfig)
				}
				mutated <- err
			}()
			joined := false
			var mutationErr error
			select {
			case mutationErr = <-mutated:
				joined = true
			case <-ctx.Done():
				mutationErr = errors.New("key collection blocked a writer")
			}
			close(codec.release)
			if !joined {
				<-mutated
			}
			deleteErr := <-done
			if mutationErr != nil || deleteErr != nil {
				t.Fatalf("mutation=%v delete=%v", mutationErr, deleteErr)
			}
			fresh, err := s.Metadata(ctx, old.Name)
			if err != nil || fresh.ID == old.ID && fresh.Generation == old.Generation && fresh.Revision == old.Revision {
				t.Fatalf("lost mutation: %+v %v", fresh, err)
			}
			if action == "update" {
				io := s.newContinuationIO(ctx, s.db, nil, fresh)
				value, err := io.Get(ContinuationBucketKey(7, []byte("new")))
				io.Close()
				if err != nil || string(value) != "new write" {
					t.Fatalf("lost new key: %q %v", value, err)
				}
			}
			if _, err := s.Metadata(ctx, peer.Name); !errors.Is(err, ErrNotFound) {
				t.Fatalf("unchanged peer survived: %v", err)
			}
		})
	}
}

type pausedRelayoutExpirationCodec struct{ pausedExpirationCodec }

func (*pausedRelayoutExpirationCodec) Create(ctx context.Context, io ContinuationIO, m Metadata, now func() time.Time) error {
	return relayoutCodec{}.Create(ctx, io, m, now)
}
func (*pausedRelayoutExpirationCodec) RelayoutPlan(ctx context.Context, io ContinuationIO) (RelayoutPlan, error) {
	return relayoutCodec{}.RelayoutPlan(ctx, io)
}
func (c *pausedRelayoutExpirationCodec) Drop(ctx context.Context, io ContinuationIO) error {
	if err := c.pausedExpirationCodec.Drop(ctx, io); err != nil {
		return err
	}
	value, err := io.Get([]byte("b"))
	if err != nil {
		return err
	}
	if string(value) == "v7" {
		return io.DeletePrefix(ContinuationBucketKey(0, nil))
	}
	return nil
}

func TestExpirationPreparationRevalidatesRelayout(t *testing.T) {
	codec := &pausedRelayoutExpirationCodec{pausedExpirationCodec: pausedExpirationCodec{entered: make(chan struct{}), release: make(chan struct{})}}
	s, _ := openTestStore(t, "/store", Options{fs: vfs.NewMem(), ExactBlocks: true, TimeBuckets: true, Continuation: codec})
	defer closeTestStore(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	old := createTestMetric(t, s, "expired")
	done := make(chan error, 1)
	go func() {
		outcomes, err := s.DeleteBatchIfUnchanged(ctx, []Metadata{old})
		if err == nil && (len(outcomes) != 1 || !errors.Is(outcomes[0], ErrConflict)) {
			err = fmt.Errorf("outcomes: %v", outcomes)
		}
		done <- err
	}()
	select {
	case <-codec.entered:
	case <-ctx.Done():
		t.Fatal("preparation did not start")
	}
	plan, err := codec.RelayoutPlan(ctx, s.newContinuationIO(ctx, s.db, nil, old))
	if err == nil {
		err = s.relayoutGroup(ctx, []relayoutItem{{m: old, plan: plan}})
	}
	close(codec.release)
	deleteErr := <-done
	if err != nil || deleteErr != nil {
		t.Fatalf("relayout=%v delete=%v", err, deleteErr)
	}
	fresh, err := s.Metadata(ctx, old.Name)
	if err != nil || fresh.Revision != old.Revision+1 || !fresh.LastUpdate.Equal(old.LastUpdate) {
		t.Fatalf("relayout fence/activity: %+v %v", fresh, err)
	}
	if countStoreKeys(t, s) != 1 {
		t.Fatal("lost relocated key")
	}
	outcomes, err := s.DeleteBatchIfUnchanged(ctx, []Metadata{fresh})
	if err != nil || len(outcomes) != 1 || outcomes[0] != nil || countStoreKeys(t, s) != 0 {
		t.Fatalf("fresh expiration: %v %v", outcomes, err)
	}
}
