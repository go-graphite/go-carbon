package carbon

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestMigrationPriorityPausedWorkersCannotHideHints(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := newMigrationPriority()
	cold := make(chan string, 3)
	for i := range 3 {
		cold <- fmt.Sprintf("cold.%d", i)
	}
	entered := make(chan int, 4)
	converted := make(chan string, 4)
	released := make(chan int, 4)
	finished := make(chan error, 4)
	grants := make([]chan struct{}, 4)
	startWorker := func(i int) {
		grants[i] = make(chan struct{})
		go func() {
			finished <- p.runWorker(ctx, cold, func() bool {
				entered <- i
				select {
				case <-grants[i]:
					return true
				case <-ctx.Done():
					return false
				}
			}, func() { released <- i }, func(name string) error {
				converted <- name
				<-ctx.Done()
				return ctx.Err()
			})
		}()
	}
	waitEntered := func(want int) {
		t.Helper()
		select {
		case got := <-entered:
			if got != want {
				t.Fatalf("unexpected admission waiter %d; want %d", got, want)
			}
		case <-time.After(time.Second):
			t.Fatal("worker did not reach its admission barrier")
		}
	}
	for i := range 3 {
		startWorker(i)
		waitEntered(i)
	}
	startWorker(3)
	p.offer("hot")
	waitEntered(3)   // This worker observes a hint, then waits without admission.
	close(grants[0]) // Resume an earlier cold waiter first.
	select {
	case name := <-converted:
		if name != "hot" {
			t.Fatalf("paused waiter hid the priority metric; converted %s", name)
		}
	case <-time.After(time.Second):
		t.Fatal("resumed worker did not make progress")
	}
	close(grants[3]) // Its earlier notification has already been consumed.
	select {
	case i := <-released:
		if i != 3 {
			t.Fatalf("unexpected permit release from worker %d", i)
		}
	case <-time.After(time.Second):
		t.Fatal("stale priority notification leaked its admission permit")
	}
	cancel()
	for range 4 {
		select {
		case err := <-finished:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("worker lost cancellation: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("paused worker could not stop")
		}
	}
}

func TestMigrationPriorityEmptyScanFinishesWithoutAdmission(t *testing.T) {
	cold := make(chan string)
	close(cold)
	p := newMigrationPriority()
	err := p.runWorker(context.Background(), cold, func() bool {
		t.Fatal("an empty completed scan waited for a conversion permit")
		return false
	}, func() { t.Fatal("released a permit never acquired") }, func(string) error {
		t.Fatal("an empty scan converted a metric")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMigrationPriorityBoundsQueuedAndInFlightNames(t *testing.T) {
	p := newMigrationPriority()
	for i := range migrationPriorityCapacity * 2 {
		p.offer(fmt.Sprintf("metric.%d", i))
	}
	if len(p.pending) != migrationPriorityCapacity || len(p.ready) != migrationPriorityCapacity {
		t.Fatal("priority hints exceeded their memory bound")
	}
	first := <-p.ready
	p.offer("overflow")
	p.offer(first)
	if len(p.ready) != migrationPriorityCapacity-1 {
		t.Fatal("in-flight names did not count toward capacity")
	}
	p.complete(first)
	p.offer(first)
	if len(p.ready) != migrationPriorityCapacity {
		t.Fatal("completed hint could not be offered again")
	}
}

func TestMigrationPriorityDoesNotStarveColdScan(t *testing.T) {
	p := newMigrationPriority()
	cold := make(chan string, 2)
	cold <- "cold.1"
	cold <- "cold.2"
	streak := 0
	for i := range 16 {
		p.offer(fmt.Sprintf("hot.%d", i))
		name, hot, ok := p.next(context.Background(), cold, streak)
		if !ok {
			t.Fatal("live work was dropped")
		}
		if i == 3 || i == 7 {
			if hot || name != fmt.Sprintf("cold.%d", i/4+1) {
				t.Fatalf("cold scan starved after three hints: %s", name)
			}
		}
		if hot {
			p.complete(name)
			streak++
		} else {
			streak = 0
		}
	}
	close(cold)
	if _, _, ok := p.next(context.Background(), cold, migrationPriorityBurst); ok {
		t.Fatal("residual hints prevented completion after the source walk")
	}
}

func TestMigrationPriorityBurstOrdersColdAndHints(t *testing.T) {
	for _, tc := range []struct {
		burst int
		want  []string
	}{
		{0, []string{"cold.0", "cold.1", "cold.2", "hot.0"}},
		{1 << 30, []string{"hot.0", "hot.1", "hot.2", "cold.0"}},
	} {
		p := newMigrationPriorityBurst(tc.burst)
		cold := make(chan string, 3)
		for i := range 3 {
			cold <- fmt.Sprintf("cold.%d", i)
			p.offer(fmt.Sprintf("hot.%d", i))
		}
		streak := 0
		var got []string
		for range tc.want {
			name, hot, ok := p.next(context.Background(), cold, streak)
			if !ok {
				t.Fatal("work was dropped")
			}
			got = append(got, name)
			if hot {
				p.complete(name)
				streak++
			} else {
				streak = 0
			}
		}
		if fmt.Sprint(got) != fmt.Sprint(tc.want) {
			t.Fatalf("burst %d order %v, want %v", tc.burst, got, tc.want)
		}
	}
}

func TestMigrationPriorityCancellationAndContendedOffersNeverBlockWrites(t *testing.T) {
	p := newMigrationPriority()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, _, ok := p.next(ctx, make(chan string), 0); ok {
			t.Error("cancelled waiter returned a job")
		}
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("empty priority queue ignored cancellation")
	}
	p.mu.Lock()
	var writers sync.WaitGroup
	for range 96 {
		writers.Go(func() { p.offer("contended") })
	}
	writesDone := make(chan struct{})
	go func() { writers.Wait(); close(writesDone) }()
	select {
	case <-writesDone:
	case <-time.After(time.Second):
		p.mu.Unlock()
		t.Fatal("conversion queue blocked original-file writers")
	}
	p.mu.Unlock()
	if len(p.ready) != 0 {
		t.Fatal("contended best-effort hint was retained")
	}
	p.offer("after.stop") // Channels stay open for callbacks loaded before removal.
	if len(p.ready) != 1 {
		t.Fatal("late callback could not finish safely")
	}
}
