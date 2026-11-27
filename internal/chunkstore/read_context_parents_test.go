package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"runtime/metrics"
	"sync"
	"testing"
	"time"
)

// This is the original single-parent implementation, retained as a cancellation
// oracle and a contention baseline.
func singleParentReadContext(s *Store, ctx context.Context) (context.Context, func(), error) {
	if err := s.ReadError(); err != nil {
		return ctx, func() {}, err
	}
	if err := ctx.Err(); err != nil {
		return ctx, func() {}, err
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.readCtx, cancel)
	return ctx, func() { stop(); cancel() }, nil
}

func TestReadContextParentsDifferential(t *testing.T) {
	for _, action := range []string{"parent", "store", "release", "already-parent", "already-store"} {
		t.Run(action, func(t *testing.T) {
			var stores [2]Store
			for i := range stores {
				stores[i].initReadContexts()
				defer stores[i].readCancel()
			}
			type key struct{}
			parent, cancel := context.WithCancel(context.WithValue(context.Background(), key{}, "preserved"))
			defer cancel()
			parent, deadlineCancel := context.WithDeadline(parent, time.Now().Add(time.Hour))
			defer deadlineCancel()
			if action == "already-parent" {
				cancel()
			}
			if action == "already-store" {
				for i := range stores {
					stores[i].readCancel()
				}
			}
			want, releaseWant, errWant := singleParentReadContext(&stores[0], parent)
			defer releaseWant()
			got, releaseGot, errGot := stores[1].readContext(parent)
			defer releaseGot()
			if !errors.Is(errGot, errWant) || !errors.Is(errWant, errGot) {
				t.Fatalf("errors: got %v, want %v", errGot, errWant)
			}
			if got.Value(key{}) != want.Value(key{}) {
				t.Fatal("context value changed")
			}
			gotDeadline, gotOK := got.Deadline()
			wantDeadline, wantOK := want.Deadline()
			if gotOK != wantOK || !gotDeadline.Equal(wantDeadline) {
				t.Fatal("deadline changed")
			}
			if errGot != nil {
				return
			}
			switch action {
			case "parent":
				cancel()
			case "store":
				for i := range stores {
					stores[i].readCancel()
				}
			case "release":
				releaseGot()
				releaseWant()
			}
			select {
			case <-got.Done():
			case <-time.After(2 * time.Second):
				t.Fatal("new context did not cancel")
			}
			select {
			case <-want.Done():
			case <-time.After(2 * time.Second):
				t.Fatal("oracle context did not cancel")
			}
			if !errors.Is(got.Err(), want.Err()) {
				t.Fatalf("cancellation: got %v, want %v", got.Err(), want.Err())
			}
		})
	}
}

func TestReadContextParentsCancelEveryPartition(t *testing.T) {
	var s Store
	s.initReadContexts()
	defer s.readCancel()
	contexts := make([]context.Context, 4*len(s.readParents))
	for i := range contexts {
		ctx, release, err := s.readContext(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		contexts[i] = ctx
	}
	s.readCancel()
	for _, ctx := range contexts {
		select {
		case <-ctx.Done():
		case <-time.After(2 * time.Second):
			t.Fatal("partition missed cancellation")
		}
	}
	if _, release, err := s.readContext(context.Background()); !errors.Is(err, ErrClosed) {
		release()
		t.Fatalf("read after cancellation: %v", err)
	}
}

func TestReadContextParentsConcurrentShutdown(t *testing.T) {
	for iteration := 0; iteration < 20; iteration++ {
		var s Store
		s.initReadContexts()
		start := make(chan struct{})
		var wg sync.WaitGroup
		for range 128 {
			wg.Go(func() {
				<-start
				ctx, release, err := s.readContext(context.Background())
				defer release()
				if err != nil {
					if !errors.Is(err, ErrClosed) {
						t.Errorf("admission error: %v", err)
					}
					return
				}
				select {
				case <-ctx.Done():
				case <-time.After(2 * time.Second):
					t.Error("registration racing shutdown missed cancellation")
				}
			})
		}
		close(start)
		s.readCancel()
		wg.Wait()
	}
}

func BenchmarkReadContextParents(b *testing.B) {
	for _, old := range []bool{true, false} {
		b.Run(fmt.Sprintf("single=%v", old), func(b *testing.B) {
			var s Store
			s.initReadContexts()
			defer s.readCancel()
			fn := (*Store).readContext
			if old {
				fn = singleParentReadContext
			}
			sample := []metrics.Sample{{Name: "/sync/mutex/wait/total:seconds"}}
			metrics.Read(sample)
			before := sample[0].Value.Float64()
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_, release, err := fn(&s, context.Background())
					if err != nil {
						b.Error(err)
						return
					}
					release()
				}
			})
			b.StopTimer()
			metrics.Read(sample)
			b.ReportMetric((sample[0].Value.Float64()-before)/float64(b.N), "mutex-wait-s/op")
		})
	}
}
