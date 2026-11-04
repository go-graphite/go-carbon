package chunkstore

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/restartcpu"
)

// Restart with a real checkpoint and CPU-bound maintenance outside admission.
// A one-minute deadline detects a stuck loader without asserting wall-clock speed.
func TestMetadataCheckpointRestartMaintenanceCPU(t *testing.T) {
	dir := t.TempDir()
	seedCheckpoint(t, dir)
	db, err := Open(dir, checkpointOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	budget := restartcpu.New(.2)
	var stop atomic.Bool
	burned := make(chan uint64, 1)
	go func() {
		x := uint64(1)
		for !stop.Load() {
			for range 10000 {
				x = x*6364136223846793005 + 1
			}
		}
		burned <- x
	}()
	defer func() { stop.Store(true); <-burned }()
	// Ensure maintenance has consumed the initial five-millisecond credit.
	deadline := time.Now().Add(time.Minute)
	for {
		budget.ObserveWriteout(0, 0)
		if s := budget.State(); s.Process > s.Allowance {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("CPU meter did not observe maintenance work")
		}
		time.Sleep(10 * time.Millisecond)
	}
	var grantedCores float64
	options := PrewarmOptions{Workers: 1, CheckpointWorker: func() CheckpointPacer {
		p := budget.NewCheckpoint(nil)
		grantedCores = budget.State().Checkpoint
		return p
	}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	stats, err := db.LoadMetadataCheckpointAfter(ctx, options)
	s := budget.State()
	t.Logf("checkpoint valid=%v runtime=%s entries=%d process_cores=%.3f allowance=%.3f grants=%d granted_cores=%.3f error=%v", stats.Valid, stats.Duration, stats.Entries, s.Process, s.Allowance, s.CheckpointGrants, grantedCores, err)
	if err != nil || !stats.Valid || stats.Entries != 3 || s.CheckpointGrants == 0 || grantedCores <= 0 {
		t.Fatalf("maintenance starved checkpoint: %+v %v grants=%d", stats, err, s.CheckpointGrants)
	}
}
