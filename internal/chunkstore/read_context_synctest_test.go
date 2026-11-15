package chunkstore

import (
	"context"
	"testing"
	"testing/synctest"
)

func TestReadContextParentsSynctest(t *testing.T) {
	var s Store
	s.initReadContexts()
	defer s.readCancel()

	synctest.Test(t, func(t *testing.T) {
		// Exercise every partition for a store created outside this bubble.
		for range s.readParents {
			ctx, release, err := s.readContext(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			release()
			release()
			if ctx.Err() != context.Canceled {
				t.Fatalf("released read context: %v", ctx.Err())
			}
		}
	})
	// Shutdown must be able to close the parent channels outside the bubble
	// where reads first registered, even though all reads have been released.
	s.readCancel()
}
