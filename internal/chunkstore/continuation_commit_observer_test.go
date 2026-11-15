package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

type observedContinuationCodec struct {
	batchTestCodec
	observe func(Metadata)
	fail    string
}

func (c *observedContinuationCodec) Update(ctx context.Context, kv ContinuationIO, m Metadata, p []Point, r int, clock func() time.Time, maintenance *ContinuationMaintenance) error {
	if err := c.batchTestCodec.Update(ctx, kv, m, p, r, clock, maintenance); err != nil {
		return err
	}
	kv.(ContinuationCommitObserver).AfterContinuationCommit(c.observe)
	if m.Name == c.fail {
		return errors.New("injected after codec preparation")
	}
	return nil
}

func TestContinuationCommitObserverFollowsTransaction(t *testing.T) {
	for _, cached := range []bool{false, true} {
		for _, batch := range []bool{false, true} {
			t.Run(fmt.Sprintf("cached=%t/batch=%t", cached, batch), func(t *testing.T) {
				ctx := context.Background()
				var published []Metadata
				codec := &observedContinuationCodec{}
				opts := Options{ExactBlocks: true, Continuation: codec}
				if cached {
					opts.MetadataCacheSize = 1 << 20
				}
				db, _ := openTestStore(t, t.TempDir(), opts)
				defer closeTestStore(t, db)
				for _, name := range []string{"a", "b"} {
					createTestMetric(t, db, name)
				}
				codec.observe = func(m Metadata) {
					// Bypass application reservations to inspect the database transaction
					// itself: its new revision must already be committed before publication.
					committed, err := metadataFrom(db.db, m.Name)
					if err != nil || committed.Revision != m.Revision {
						t.Errorf("callback preceded commit: %+v %+v %v", m, committed, err)
					}
					published = append(published, m)
				}
				apply := func() error {
					if batch {
						return db.UpdateBatch(ctx, []MetricUpdate{{Name: "a", Points: []Point{{Value: 1}}}, {Name: "b", Points: []Point{{Value: 2}}}, {Name: "a", Points: []Point{{Value: 3}}}}, nil)
					}
					return db.UpdateMany(ctx, "a", []Point{{Value: 1}})
				}
				codec.fail = "a"
				if err := apply(); err == nil {
					t.Fatal("fault was ignored")
				}
				if len(published) != 0 {
					t.Fatal("failed transaction published retained state")
				}
				if batch {
					codec.fail = "b"
					if err := apply(); err == nil {
						t.Fatal("second-metric fault was ignored")
					}
					if len(published) != 0 {
						t.Fatal("partially prepared batch published retained state")
					}
				}
				codec.fail = ""
				if err := apply(); err != nil {
					t.Fatal(err)
				}
				want := 1
				if batch {
					want = 2
				}
				if len(published) != want {
					t.Fatalf("published %d callbacks want %d", len(published), want)
				}
				for _, m := range published {
					revision := uint64(2)
					if batch && m.Name == "a" {
						revision = 3
					}
					if m.Revision != revision {
						t.Fatalf("published an intermediate revision: %+v", m)
					}
				}
			})
		}
	}
}
