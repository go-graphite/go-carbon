package restartcpu

import (
	"context"
	"math"
	"runtime"
	"time"
)

// Checkpoint paces one worker's bounded checkpoint batches. Elapsed
// batch time is a conservative CPU estimate: I/O and lock waits are included.
// Validation workers still use WaitCheckpoint for required dependent reads.
// Each stream owns its pacer; call Finish after loading, including on failure.
type Checkpoint struct {
	budget   *Budget
	last     time.Time
	owed     time.Duration
	progress func() (uint64, uint64)
}

// CheckpointWorkers leaves at least half the allowance for mandatory input,
// reads and writeout. Fractional shares use one duty-paced worker. An explicit
// worker setting can reduce concurrency; zero uses the available allowance.
func (b *Budget) CheckpointWorkers(requested int) int {
	workers := max(1, min(256, runtime.GOMAXPROCS(0), int(math.Ceil(b.cores/2))))
	if requested > 0 {
		workers = min(workers, requested)
	}
	return workers
}

func (b *Budget) NewCheckpoint(progress func() (uint64, uint64)) *Checkpoint {
	b.mu.Lock()
	if b.checkpoints.Add(1) == 1 {
		b.checkpointWork.Store(0)
		b.checkpointTotal.Store(0)
	}
	b.mu.Unlock()
	return &Checkpoint{budget: b, progress: progress}
}

func (p *Checkpoint) report() {
	if p.progress != nil {
		work, total := p.progress()
		if total > 0 {
			// Repeated loads stream the same file. Preserve the furthest known
			// position when restoration offers another set of pending names.
			for old := p.budget.checkpointWork.Load(); work > old; old = p.budget.checkpointWork.Load() {
				if p.budget.checkpointWork.CompareAndSwap(old, work) {
					break
				}
			}
			p.budget.checkpointTotal.Store(total)
		}
	}
}

func (p *Checkpoint) Finish() {
	p.report()
	p.budget.checkpoints.Add(-1)
}

// checkpointShareLocked lends up to half the allowance to the worker pool. When
// writeout is below its floor and CPU has less room than the full stream share,
// it gets a quarter share, retaining progress while yielding to writeout.
// Bulk workers duty-pace this bounded mandatory share even under process debt.
func (b *Budget) checkpointShareLocked() float64 {
	if b.checkpoints.Load() == 0 {
		return 0
	}
	share := min(float64(b.checkpoints.Load()), b.cores/2)
	// One-second confirmation samples vary with batching. A material shortfall
	// yields CPU; a few completions crossing a sample boundary do not stretch
	// the accelerator's completion time. Warning accounting stays exact.
	if b.floor > 0 && b.rate < .95*b.floor && b.process > b.cores-share {
		share /= 4
	}
	return share
}

func (p *Checkpoint) accountWindow(ctx context.Context) (float64, error) {
	b := p.budget
	p.report()
	work := 0.
	if !p.last.IsZero() {
		work = time.Since(p.last).Seconds()
	}
	p.last = time.Time{}
	b.mu.Lock()
	share := b.checkpointShareLocked() / float64(max(1, b.checkpoints.Load()))
	b.mu.Unlock()
	if b.Active() && share > 0 && work > 0 {
		// Coalesce tiny delays: a timer per 32-record window would round up
		// on coarse clocks and make the stream much slower than its share.
		// Duty debt is bounded by 5ms plus one batch; idle time banks nothing.
		p.owed += time.Duration(work * max(0, 1/share-1) * float64(time.Second))
		if p.owed >= 5*time.Millisecond {
			delay := p.owed
			p.owed = 0
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return work, ctx.Err()
			case <-timer.C:
			}
		}
	}
	return work, ctx.Err()
}

// Idle settles the last active window without admitting another one. Queue
// waits after this call are excluded from the worker's CPU estimate.
func (p *Checkpoint) Idle(ctx context.Context) error {
	_, err := p.accountWindow(ctx)
	return err
}

func (p *Checkpoint) Wait(ctx context.Context) error {
	work, err := p.accountWindow(ctx)
	if err != nil {
		return err
	}
	b := p.budget
	for b.Active() {
		if err := ctx.Err(); err != nil {
			return err
		}
		delay := b.reserveWork(time.Now(), true, max(work, b.writeCost()/16), true)
		if delay == 0 {
			break
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	b.checkpointGrants.Add(1)
	p.last = time.Now()
	return nil
}
