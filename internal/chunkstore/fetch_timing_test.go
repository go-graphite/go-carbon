package chunkstore

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

type timedFetchCodec struct {
	bucketCodec
	err error
}

func (c timedFetchCodec) Fetch(_ context.Context, io ContinuationIO, m Metadata, _, _ int, _ func() time.Time) (*Series, error) {
	if _, err := io.(ContinuationProber).Probe([]byte("plain")); err != nil {
		return nil, err
	}
	time.Sleep(25 * time.Millisecond)
	return &Series{Metadata: m}, c.err
}

func TestFetchTimingsSeparateBodyAndFinalCloses(t *testing.T) {
	failed := errors.New("fetch failed")
	for _, fetchErr := range []error{nil, failed} {
		synctest.Test(t, func(t *testing.T) {
			s, now := openTestStore(t, "/store", Options{fs: vfs.NewMem(), ExactBlocks: true,
				Continuation: timedFetchCodec{err: fetchErr}, FetchSharedIterator: true})
			defer closeTestStore(t, s)
			m := createTestMetric(t, s, "timed.fetch")
			_, err := s.Fetch(context.Background(), m.Name, now-60, now)
			if !errors.Is(err, fetchErr) {
				t.Fatalf("fetch error = %v, want %v", err, fetchErr)
			}
			got := s.FetchTimings()
			if got.Body.Calls != 1 || got.Body.Nanoseconds != uint64(25*time.Millisecond) ||
				got.ReadViewClose.Calls != 1 || got.ReadViewClose.Nanoseconds != 0 || got.IteratorClose.Calls != 1 {
				t.Fatalf("fetch timings: %+v", got)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err := s.Fetch(ctx, m.Name, now-60, now); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if after := s.FetchTimings(); after != got {
				t.Fatalf("fetch rejected before opening a view changed timings: %+v", after)
			}
		})
	}
}
