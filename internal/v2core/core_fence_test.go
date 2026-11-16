package v2core

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

func TestCoreFenceWaitsForEveryProducerAndPublication(t *testing.T) {
	cfg := coreTestConfig()
	cfg.MaxRows = 1024
	entered, release := make(chan struct{}), make(chan struct{})
	var blocking atomic.Bool
	e, err := openCoreWithHooks(t.TempDir(), cfg, coreHooks{Publish: func(id int, s *pebble.Snapshot, _ []row) error {
		defer s.Close()
		if id == 0 && blocking.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer e.close()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err = e.Append(0, []row{{1, 1, 0x8000000000000000, 1}}, 10, false); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- e.Fence(ctx, []uint64{10, 20}) }()
	select {
	case err := <-done:
		t.Fatalf("fence skipped producer 1: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if err = e.Append(1, []row{{2, 2, 0x7ff8000000000042, 1}}, 20, false); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case err := <-done:
		t.Fatalf("fence skipped publication: %v", err)
	default:
	}
	close(release)
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	for _, o := range e.owners {
		if o.sealCalls.Load() != 1 {
			t.Fatalf("forced seals=%d", o.sealCalls.Load())
		}
		if got := o.frontier.Load().Cursors; got[0] != 10 || got[1] != 20 {
			t.Fatal(got)
		}
	}
	if err = e.Fence(ctx, []uint64{10, 20}); err != nil {
		t.Fatal(err)
	}
	for _, o := range e.owners {
		if o.sealCalls.Load() != 1 {
			t.Fatal("covered fence forced another seal")
		}
	}
}

func TestCoreFenceFailureAndCancellation(t *testing.T) {
	cfg := coreTestConfig()
	sentinel := errors.New("group seal failed")
	e, err := openCoreWithHooks(t.TempDir(), cfg, coreHooks{PlanSeal: func(int, coreSealInput) (coreSealPlan, error) { return coreSealPlan{}, sentinel }})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	if err = e.Fence(ctx, []uint64{99, 99}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	cancel()
	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err = e.AppendBatches(0, []coreBatch{{ID: 1, Points: []row{{1, 1, 1, 1}}}}, 1, false); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err = e.Fence(ctx, []uint64{1, 0}); !errors.Is(err, sentinel) {
			t.Fatalf("lost seal failure: %v", err)
		}
	}
	for _, f := range e.DurableFrontiers() {
		if f[0] != 0 {
			t.Fatal("failed fence advanced cursor")
		}
	}
	if err = e.Close(); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
}
