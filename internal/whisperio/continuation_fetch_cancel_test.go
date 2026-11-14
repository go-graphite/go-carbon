package whisperio

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

type failingClassicReadIO struct {
	store.ContinuationIO
	fail  func() error
	reads int
}

func (kv *failingClassicReadIO) Get(key []byte) ([]byte, error) {
	// Classic point groups are read after the header has opened successfully.
	if len(key) > 9 && key[9] == 'p' {
		kv.reads++
		if kv.reads > 1 {
			return nil, kv.fail()
		}
	}
	return kv.ContinuationIO.Get(key)
}

func TestContinuationFetchCanceledClassicRead(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprint("deadline=", deadline), func(t *testing.T) {
			kv, snapshot := absoluteAnchorFixture(t, []store.Point{{Timestamp: 999990, Value: 1}})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			want := context.Canceled
			if deadline {
				var expire context.CancelFunc
				ctx, expire = context.WithTimeout(context.Background(), 10*time.Millisecond)
				defer expire()
				want = context.DeadlineExceeded
			}
			io := &failingClassicReadIO{ContinuationIO: kv, fail: func() error {
				if deadline {
					<-ctx.Done()
				} else {
					cancel()
				}
				return ctx.Err()
			}}
			series, err := (&ContinuationCodec{}).Fetch(ctx, io, snapshot.Metadata, 999900, 1000000, classicTestClock)
			if series != nil || !errors.Is(err, want) {
				t.Fatalf("Fetch = %v, %v; want nil, %v", series, err, want)
			}
		})
	}
}

func TestContinuationFetchPreservesOtherPanics(t *testing.T) {
	kv, snapshot := absoluteAnchorFixture(t, []store.Point{{Timestamp: 999990, Value: 1}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	io := &failingClassicReadIO{ContinuationIO: kv, fail: func() error { cancel(); return errors.New("disk read failed") }}
	defer func() {
		got := recover()
		if got != "Failed to read baseInterval: disk read failed" {
			t.Fatalf("panic = %v", got)
		}
	}()
	_, _ = (&ContinuationCodec{}).Fetch(ctx, io, snapshot.Metadata, 999900, 1000000, classicTestClock)
}
