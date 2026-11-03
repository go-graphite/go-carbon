package chunkstore

import (
	"context"
	"errors"
	"log"
	"maps"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

const (
	DefaultRelayoutMinFreeBytes      = 200 << 30
	DefaultRelayoutMaxCompactionDebt = 32 << 30
)

// One pressure state is shared by all workers. Only transitions into a pause
// count/log, rather than every worker or every poll. Limits change together.
type relayoutGuard struct {
	mu               sync.Mutex
	minFree, maxDebt uint64
	changed          chan struct{}
	paused           bool
	reason           string
	pauses           map[string]uint64
}

// SetRelayoutLimits applies live pressure limits, including during a pause.
func (s *Store) SetRelayoutLimits(minFreeBytes, maxCompactionDebt uint64) error {
	if minFreeBytes == 0 || maxCompactionDebt == 0 {
		return errors.New("relayout free-space and compaction-debt limits must be positive")
	}
	g := &s.relayout.guard
	g.mu.Lock()
	defer g.mu.Unlock()
	g.minFree, g.maxDebt = minFreeBytes, maxCompactionDebt
	if g.changed != nil {
		close(g.changed)
	}
	g.changed = make(chan struct{})
	return nil
}

func (g *relayoutGuard) stats(stats *RelayoutStats) {
	g.mu.Lock()
	defer g.mu.Unlock()
	stats.Paused, stats.PauseReason = g.paused, g.reason
	stats.Pauses = maps.Clone(g.pauses)
	stats.MinFreeBytes, stats.MaxCompactionDebt = g.minFree, g.maxDebt
}

func (g *relayoutGuard) clear() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.paused, g.reason = false, ""
}

func (s *Store) waitRelayout(ctx context.Context) error {
	return s.relayout.guard.wait(ctx, func() (uint64, uint64, error) {
		usage, err := s.fs.GetDiskUsage(s.dir)
		// In-memory filesystems have no capacity. Real usage failures pause
		// closed and retry; a missing sample must never admit more copies.
		if errors.Is(err, vfs.ErrUnsupported) {
			err = nil
			usage.AvailBytes = ^uint64(0)
		}
		return usage.AvailBytes, s.db.Metrics().Compact.EstimatedDebt, err
	})
}

func (g *relayoutGuard) wait(ctx context.Context, sample func() (free, debt uint64, err error)) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		reason, changed := g.observe(sample)
		if reason == "" {
			return nil
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-changed:
			timer.Stop()
		case <-timer.C:
		}
	}
}

func (g *relayoutGuard) observe(sample func() (uint64, uint64, error)) (string, <-chan struct{}) {
	// Serialize observation and publication so workers cannot publish an
	// older observation after another worker has entered a pause.
	g.mu.Lock()
	defer g.mu.Unlock()
	free, debt, diskErr := sample()
	reason := ""
	switch {
	case diskErr != nil:
		reason = "disk_usage_error"
	case free < g.minFree:
		reason = "low_free_space"
	case debt > g.maxDebt:
		reason = "compaction_debt"
	}
	if reason == "" {
		if g.paused {
			log.Printf("relayout resumed: free_bytes=%d compaction_debt=%d", free, debt)
		}
		g.paused, g.reason = false, ""
		return "", nil
	}
	if !g.paused {
		if g.pauses == nil {
			g.pauses = make(map[string]uint64)
		}
		g.pauses[reason]++
		log.Printf("[WARN] relayout paused: reason=%s free_bytes=%d min_free_bytes=%d compaction_debt=%d max_compaction_debt=%d disk_error=%v", reason, free, g.minFree, debt, g.maxDebt, diskErr)
	}
	g.paused, g.reason = true, reason
	return reason, g.changed
}
