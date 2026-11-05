package persister

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestDeferredReclaimStopWaitsForPruningWorker(t *testing.T) {
	r := reclaimTestQueue()
	db := &reclaimCompletionStore{}
	p := &Whisper{rootPath: t.TempDir(), incrementalStore: db, reclaim: r}
	dir := filepath.Join(p.rootPath, "leaf")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "metric.wsp")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	db.complete = func(store.Metadata) error {
		if p.reclaimer() != r {
			t.Error("draining worker lost access to its reclaimer")
		}
		return nil
	}
	r.queued.Store(1)
	// Model an already-dispatched batch whose worker is scheduled only after
	// Stop closes stop. Its real unlink/sync/prune path must finish before done.
	go func() {
		<-r.stop
		p.reclaimDirectory(r, dir, []reclaimJob{{name: "metric", path: path}})
		close(r.done)
	}()
	stopped := make(chan struct{})
	go func() { p.StopDeferredReclaim(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("StopDeferredReclaim blocked while a worker pruned a dispatched batch")
	}
	if db.completed.Load() != 1 || r.queued.Load() != 0 || p.reclaimer() != nil {
		t.Fatal("stop returned before the active cleanup completed")
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("worker did not prune its directory: %v", err)
	}
}

func TestDeferredReclaimLifecycleWaitsForDrainingWorkers(t *testing.T) {
	for _, action := range []string{"enable", "disable", "stop"} {
		t.Run(action, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			db := &reclaimCompletionStore{complete: func(store.Metadata) error {
				close(entered)
				<-release
				return nil
			}}
			p := &Whisper{rootPath: t.TempDir(), incrementalStore: db}
			p.SetDeferredReclaim(time.Nanosecond)
			defer p.StopDeferredReclaim()
			defer close(release)
			r := p.reclaimer()
			if !r.push(reclaimJob{name: "active", path: filepath.Join(p.rootPath, "active.wsp")}) {
				t.Fatal("queue rejected job")
			}
			r.wake <- struct{}{}
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("worker never started")
			}
			stopped := make(chan struct{})
			go func() { p.StopDeferredReclaim(); close(stopped) }()
			select {
			case <-r.stop:
			case <-time.After(5 * time.Second):
				t.Fatal("stop never signalled worker")
			}
			read := make(chan *deferredReclaim, 1)
			go func() { read <- p.reclaimer() }()
			select {
			case got := <-read:
				if got != r {
					t.Fatal("stopping reclaimer disappeared before its workers drained")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("stop blocked a reclaimer reader")
			}
			called, changed := make(chan struct{}), make(chan struct{})
			go func() {
				close(called)
				switch action {
				case "enable":
					p.SetDeferredReclaim(2 * time.Hour)
				case "disable":
					p.SetDeferredReclaim(0)
				case "stop":
					p.StopDeferredReclaim()
				}
				close(changed)
			}()
			<-called
			select {
			case <-changed:
				t.Fatal("lifecycle call returned before the old workers drained")
			case <-time.After(20 * time.Millisecond):
			}
			release <- struct{}{}
			for _, done := range []chan struct{}{stopped, changed} {
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("lifecycle call blocked after the worker drained")
				}
			}
			if db.completed.Load() != 1 || r.root != nil {
				t.Fatal("stop did not join workers and close the old root descriptor")
			}
			if next := p.reclaimer(); action == "enable" {
				if next == nil || next == r || next.delay != 2*time.Hour {
					t.Fatal("enable did not start a replacement reclaimer after draining")
				}
			} else if next != nil {
				t.Fatal("disable or stop left a reclaimer running")
			}
		})
	}
}
