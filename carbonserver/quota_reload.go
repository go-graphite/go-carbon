package carbonserver

import (
	"fmt"
	"strings"

	"go.uber.org/zap"
)

func (listener *CarbonserverListener) getQuotas() []*Quota {
	quotas, _ := listener.quotas.Load().([]*Quota)
	return quotas
}

// ReloadQuotas schedules new rules on the index updater, which owns trie
// mutation. Queries and receivers keep using the live index throughout reload.
// Quota support must already be enabled at startup, including its size estimator.
func (listener *CarbonserverListener) ReloadQuotas(quotas []*Quota) error {
	if !listener.isQuotaEnabled() {
		if len(quotas) == 0 {
			return nil
		}
		return fmt.Errorf("enabling quotas requires a restart")
	}
	// Check all patterns without traversing the index before publishing anything.
	for _, quota := range quotas {
		if quota == nil {
			return fmt.Errorf("nil quota")
		}
		for _, part := range strings.Split(strings.TrimSpace(strings.ReplaceAll(quota.Pattern, ".", "/")), "/") {
			if part == "" {
				continue
			}
			if _, err := newGlobState(part, nil); err != nil {
				return fmt.Errorf("invalid quota pattern %q: %w", quota.Pattern, err)
			}
		}
	}
	// Keep an enabled, empty snapshot when every rule is removed. A nil snapshot
	// means quota support was never initialized and skips refresh work entirely.
	snapshot := make([]*Quota, len(quotas))
	for i, q := range quotas {
		rule := *q
		snapshot[i] = &rule
	}
	listener.quotas.Store(snapshot)
	select {
	case listener.quotaReload <- struct{}{}:
	default:
	}
	return nil
}

func (listener *CarbonserverListener) refreshQuotaRules() {
	if listener.getMetricStore() != nil {
		listener.metricStoreIndexMu.Lock()
		defer listener.metricStoreIndexMu.Unlock()
	}
	index := listener.CurrentFileIndex()
	if index == nil || index.trieIdx == nil {
		// Initial publication uses the new snapshot.
		return
	}
	quotas := listener.getQuotas()
	if _, err := index.trieIdx.applyQuotas(listener.quotaUsageReportFrequency, quotas...); err != nil {
		listener.logger.Error("failed to reload quota rules", zap.Error(err))
		return
	}
	listener.logger.Info("quota rules reloaded", zap.Int("rules", len(quotas)))
}
