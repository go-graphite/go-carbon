package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestWorkerGateDrainsBeforeShrinkingAndCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		g := newWorkerGate(8)
		for range 8 {
			if err := g.acquire(ctx); err != nil {
				t.Fatal(err)
			}
		}
		g.setLimit(1)
		acquired := make(chan error, 1)
		go func() { acquired <- g.acquire(ctx) }()
		for range 7 {
			g.release()
		}
		select {
		case err := <-acquired:
			t.Fatalf("new work admitted before old budget drained: %v", err)
		case <-time.After(20 * time.Millisecond):
		}
		g.release()
		select {
		case err := <-acquired:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("worker admission did not resume")
		}
		g.release()
		g.setLimit(0)
		go func() { acquired <- g.acquire(ctx) }()
		cancel()
		select {
		case err := <-acquired:
			if err == nil {
				t.Fatal("paused gate admitted a canceled job")
			}
		case <-time.After(time.Second):
			t.Fatal("cancellation left a paused worker blocked")
		}
	})
}

func TestWorkerGateLiveResizingProcessesEveryJobOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		g := newWorkerGate(1)
		jobs := make(chan int, 32)
		seen := make([]atomic.Int32, 1000)
		var workers sync.WaitGroup
		for range 32 {
			workers.Go(func() {
				for id := range jobs {
					if err := g.acquire(ctx); err != nil {
						return
					}
					seen[id].Add(1)
					time.Sleep(50 * time.Microsecond)
					g.release()
				}
			})
		}
		resized := make(chan struct{})
		go func() {
			defer close(resized)
			for _, n := range []int{16, 2, 0, 32, 4, 24, 1, 8} {
				g.setLimit(n)
				time.Sleep(3 * time.Millisecond)
			}
		}()
		for id := range seen {
			select {
			case jobs <- id:
			case <-ctx.Done():
				t.Fatal("live resize stalled the job producer")
			}
		}
		close(jobs)
		done := make(chan struct{})
		go func() { workers.Wait(); close(done) }()
		select {
		case <-done:
		case <-ctx.Done():
			t.Fatal("workers did not finish after live resizing")
		}
		<-resized
		for id := range seen {
			if got := seen[id].Load(); got != 1 {
				t.Fatalf("job %d processed %d times", id, got)
			}
		}
	})
}

func TestResourceLeaseValidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.json")
	now := time.Now()
	base := resourceLease{Version: 1, SourceID: "test", Generation: 1, ExpiresAt: now.Unix() + 60, CPU: 4, Workers: 8, MemoryLimitBytes: 1 << 30}
	write := func(lease resourceLease) {
		t.Helper()
		data, err := json.Marshal(lease)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write(base)
	if got, err := readResourceLease(path, "test", now, 16, 32); err != nil || got != base {
		t.Fatalf("valid lease: %+v %v", got, err)
	}
	for _, edit := range []func(*resourceLease){
		func(l *resourceLease) { l.SourceID = "wrong-corpus" },
		func(l *resourceLease) { l.ExpiresAt = now.Unix() },
		func(l *resourceLease) { l.ExpiresAt = now.Unix() + 301 },
		func(l *resourceLease) { l.CPU = 17 },
		func(l *resourceLease) { l.Workers = 33 },
		func(l *resourceLease) { l.Workers = -1 },
		func(l *resourceLease) { l.Generation = 0 },
		func(l *resourceLease) { l.MemoryLimitBytes = 0 },
	} {
		bad := base
		edit(&bad)
		write(bad)
		if _, err := readResourceLease(path, "test", now, 16, 32); err == nil {
			t.Fatalf("unsafe lease accepted: %+v", bad)
		}
	}
	for _, bad := range []string{`{"unknown":1}`, `{} {}`, strings.Repeat(" ", 4097) + `{}`} {
		if err := os.WriteFile(path, []byte(bad), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := readResourceLease(path, "test", now, 16, 32); err == nil {
			t.Fatal("malformed resource lease accepted")
		}
	}
}

func TestResourceControlLiveIncreasePauseAndExpiredLease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		oldCPU := runtime.GOMAXPROCS(0)
		oldMemory := debug.SetMemoryLimit(-1)
		defer runtime.GOMAXPROCS(oldCPU)
		defer debug.SetMemoryLimit(oldMemory)
		path := filepath.Join(t.TempDir(), "control.json")
		r := newResourceControl("test", 1, 1, 2<<30, 8, 16)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		defer func() { cancel(); <-done }()
		go func() { defer close(done); r.watch(ctx, path) }()
		write := func(l resourceLease) {
			t.Helper()
			data, err := json.Marshal(l)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path+".tmp", data, 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(path+".tmp", path); err != nil {
				t.Fatal(err)
			}
		}
		wait := func(cpu, workers int) {
			t.Helper()
			time.Sleep(time.Second)
			synctest.Wait()
			got := r.snapshot()
			if got.CPU != cpu || got.Workers != workers || runtime.GOMAXPROCS(0) != cpu {
				t.Fatalf("live budget not applied: %+v", got)
			}
		}
		l := resourceLease{Version: 1, SourceID: "test", Generation: 1, ExpiresAt: time.Now().Unix() + 60, CPU: 4, Workers: 8, MemoryLimitBytes: 2 << 30}
		write(l)
		wait(4, 8)
		l.Generation++
		l.CPU, l.Workers = 1, 0
		write(l)
		wait(1, 0)
		l.ExpiresAt = time.Now().Unix() - 1
		write(l)
		wait(1, 1)
		l.Generation = 1
		l.ExpiresAt = time.Now().Unix() + 60
		l.CPU, l.Workers = 4, 8
		write(l)
		time.Sleep(time.Second)
		synctest.Wait()
		if got := r.snapshot(); got.CPU != 1 || got.Workers != 1 {
			t.Fatalf("stale generation reversed a throttle: %+v", got)
		}
	})
}
