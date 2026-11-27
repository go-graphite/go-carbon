package v2core

import (
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

func TestParallelOwnerOpen(t *testing.T) {
	prior := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(prior)
	root := t.TempDir()
	cfg := coreTestConfig()
	cfg.Shards = 4
	var opened atomic.Int32
	ready := make(chan struct{})
	e, err := openCoreWithHooks(root, cfg, coreHooks{PublishBatches: func(_ int, s *pebble.Snapshot, _ []coreBatch, _ uint64) error {
		if err := s.Close(); err != nil {
			return err
		}
		if opened.Add(1) == 4 {
			close(ready)
		}
		select {
		case <-ready:
			return nil
		case <-time.After(5 * time.Second):
			return errors.New("owner opens did not overlap")
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err = e.close(); err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("initial publication failed")
	if _, err = openCoreWithHooks(root, cfg, coreHooks{PublishBatches: func(owner int, s *pebble.Snapshot, _ []coreBatch, _ uint64) error {
		if err := s.Close(); err != nil {
			return err
		}
		if owner == 2 {
			return sentinel
		}
		return nil
	}}); !errors.Is(err, sentinel) {
		t.Fatal("lost initial owner failure", err)
	}
	// Also catches leaked opening snapshots and database locks on error.
	e, err = openCore(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.close(); err != nil {
		t.Fatal(err)
	}
}
