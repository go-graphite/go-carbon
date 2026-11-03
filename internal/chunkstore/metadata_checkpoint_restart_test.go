package chunkstore

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/restartcpu"
)

// Restart with a real checkpoint and CPU-bound maintenance outside admission.
// A short parent deadline scales the production 600s failure to a local test.
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
	time.Sleep(1100 * time.Millisecond)
	budget.ObserveWriteout(0, 0)
	if s := budget.State(); s.Process <= s.Allowance {
		t.Fatalf("CPU burner did not exceed allowance: %+v", s)
	}
	pacer := budget.NewCheckpoint(nil)
	defer pacer.Finish()
	grantedCores := budget.State().Checkpoint
	options := PrewarmOptions{Workers: 1, CheckpointWait: pacer.Wait}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	stats, err := db.LoadMetadataCheckpointAfter(ctx, options)
	s := budget.State()
	t.Logf("checkpoint valid=%v runtime=%s entries=%d process_cores=%.3f allowance=%.3f grants=%d granted_cores=%.3f error=%v", stats.Valid, stats.Duration, stats.Entries, s.Process, s.Allowance, s.CheckpointGrants, grantedCores, err)
	if err != nil || !stats.Valid || stats.Entries != 3 || s.CheckpointGrants == 0 || grantedCores <= 0 {
		t.Fatalf("maintenance starved checkpoint: %+v %v grants=%d", stats, err, s.CheckpointGrants)
	}
}
