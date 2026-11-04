package chunkstore

import (
	"context"
	"errors"
	"maps"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
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

func (g *relayoutGuard) wake() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.changed != nil {
		close(g.changed)
	}
	g.changed = make(chan struct{})
}

func (s *Store) relayoutPressure() (uint64, uint64, error) {
	usage, err := s.fs.GetDiskUsage(s.dir)
	if errors.Is(err, vfs.ErrUnsupported) {
		err = nil
		usage.AvailBytes = ^uint64(0)
	}
	return usage.AvailBytes, s.db.Metrics().Compact.EstimatedDebt, err
}
func (s *Store) waitRelayout(ctx context.Context) error { return s.waitRelayoutSize(ctx, 0) }
func (s *Store) waitRelayoutSize(ctx context.Context, needed uint64) error {
	return s.relayout.guard.waitPolicy(ctx, s.relayoutPressure, func() error { return s.reclaimRelayout(ctx, false, needed) }, needed)
}

func (g *relayoutGuard) wait(ctx context.Context, sample func() (free, debt uint64, err error)) error {
	return g.waitPolicy(ctx, sample, nil, 0)
}

func (g *relayoutGuard) waitPolicy(ctx context.Context, sample func() (uint64, uint64, error), before func() error, needed uint64) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if before != nil {
			if err := before(); err != nil {
				return err
			}
		}
		reason, changed := g.observePolicy(sample, needed)
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

func (g *relayoutGuard) observePolicy(sample func() (uint64, uint64, error), needed uint64) (string, <-chan struct{}) {
	// Serialize observation and publication so workers cannot publish an
	// older observation after another worker has entered a pause.
	g.mu.Lock()
	defer g.mu.Unlock()
	free, debt, diskErr := sample()
	reason := ""
	switch {
	case diskErr != nil:
		reason = "disk_usage_error"
	case free < g.minFree || needed > (free-g.minFree)/3:
		reason = "low_free_space"
	case debt > g.maxDebt:
		reason = "compaction_debt"
	}
	if reason == "" {
		if g.paused {
			zapwriter.Logger("storage").Info("relayout resumed", zap.Uint64("free_bytes", free), zap.Uint64("compaction_debt", debt))
		}
		g.paused, g.reason = false, ""
		return "", nil
	}
	if !g.paused {
		if g.pauses == nil {
			g.pauses = make(map[string]uint64)
		}
		g.pauses[reason]++
		zapwriter.Logger("storage").Warn("relayout paused", zap.String("reason", reason), zap.Uint64("free_bytes", free), zap.Uint64("min_free_bytes", g.minFree), zap.Uint64("compaction_debt", debt), zap.Uint64("max_compaction_debt", g.maxDebt), zap.Error(diskErr))
	}
	g.paused, g.reason = true, reason
	return reason, g.changed
}
