package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

func TestRelayoutCopiesDuringSealedReclaim(t *testing.T) {
	for _, cancelReclaim := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelReclaim), func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			fs := &relayoutCancelFS{FS: vfs.NewMem(), onOutput: func() { close(entered); <-release }}
			codec := &controlledRelayoutCodec{bytes: func(string) int { return 20 }}
			s, _ := openTestStore(t, "/store", Options{fs: fs, ExactBlocks: true, TimeBuckets: true, Continuation: codec, RelayoutReclaimBytes: 20})
			fs.store.Store(s)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			items := make([]relayoutItem, 3)
			for i := range items {
				m := createTestMetric(t, s, fmt.Sprintf("overlap.%d", i))
				p, err := codec.RelayoutPlan(ctx, s.newContinuationIO(ctx, s.db, nil, m))
				if err != nil {
					t.Fatal(err)
				}
				items[i] = relayoutItem{m: m, plan: p}
			}
			fs.armed.Store(true)
			done := make(chan error, 1)
			go func() { done <- s.relayoutGroup(ctx, items[:1]) }()
			joined := false
			defer func() {
				cancel()
				unblock()
				if !joined {
					<-done
				}
				closeTestStore(t, s)
			}()
			select {
			case <-entered:
			case <-time.After(time.Minute):
				t.Fatal("reclaim did not start")
			}
			// No timing assertion: the first compaction cannot finish until we
			// explicitly release it. A second publication must finish meanwhile.
			second := make(chan error, 1)
			go func() { second <- s.relayoutGroup(ctx, items[1:2]) }()
			select {
			case err := <-second:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Minute):
				t.Fatal("copy stopped behind compaction")
			}
			if st := s.RelayoutStats(); st.UnreclaimedBytes != 40 || st.Reclaims != 0 {
				t.Fatalf("two batches: %+v", st)
			}
			keys, _, n, err := s.readRelayoutReclaims(ctx)
			if err != nil || len(keys) != 2 || n != 40 {
				t.Fatalf("sealed and filling journals: %d/%d %v", len(keys), n, err)
			}
			if cancelReclaim {
				cancel()
			}
			unblock()
			err = <-done
			joined = true
			if cancelReclaim && !errors.Is(err, context.Canceled) || !cancelReclaim && err != nil {
				t.Fatal(err)
			}
			keys, _, n, err = s.readRelayoutReclaims(context.Background())
			want := uint64(20)
			if cancelReclaim {
				want = 40
			}
			if err != nil || n != want {
				t.Fatalf("removed another batch's obligation: %d/%d %v", len(keys), n, err)
			}
			if err := s.recoverRelayoutReclaim(context.Background()); err != nil {
				t.Fatal(err)
			}
			for _, item := range items[:2] {
				io := s.newContinuationIO(context.Background(), s.db, nil, item.m)
				if value, err := io.Get(ContinuationBucketKey(0, []byte("c"))); err != nil || string(value) != "group" {
					t.Fatalf("copy: %q %v", value, err)
				}
			}
		})
	}
}
