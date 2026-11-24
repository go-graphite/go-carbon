package carbonserver

import (
	"fmt"
	"sort"
)

// QuotaTrimRule is a materialized namespace, not the glob that selected it.
// The last rule matching the same node wins, just as for ingestion quotas.
type QuotaTrimRule struct {
	Namespace  string
	StatMetric string
	Quota      Quota
}

// QuotaTrimRules returns an immutable policy snapshot from the live index.
// Refresh first so newly created namespaces and polled rule changes participate.
func (listener *CarbonserverListener) QuotaTrimRules() ([]QuotaTrimRule, error) {
	if !listener.UsesSharedCatalogIndex() {
		return nil, fmt.Errorf("quota trim requires catalog-owned discovery; finish file migration first")
	}
	if err := listener.RefreshMetricStoreIndex(); err != nil {
		return nil, err
	}
	listener.metricStoreIndexMu.Lock()
	defer listener.metricStoreIndexMu.Unlock()
	index := listener.CurrentFileIndex()
	if index == nil || index.trieIdx == nil {
		return nil, fmt.Errorf("quota trim requires a ready trie index")
	}
	ti := index.trieIdx
	// A poll may have published rules ahead of the updater's reload event.
	if _, err := ti.applyQuotas(listener.quotaUsageReportFrequency, listener.getQuotas()...); err != nil {
		return nil, err
	}
	var rules []QuotaTrimRule
	for path, meta := range ti.quotaNodes {
		q, _ := meta.quota.Load().(*Quota)
		if q == nil || q.OverQuota != "delete-newest" {
			continue
		}
		name := path
		if path == "/" {
			name = "root"
		}
		rules = append(rules, QuotaTrimRule{Namespace: path, StatMetric: quotaMetricName(q.StatMetricPrefix, name), Quota: *q})
	}
	sort.Slice(rules, func(i, j int) bool { return rules[i].Namespace < rules[j].Namespace })
	return rules, nil
}
