package carbonserver

import (
	"strings"
	"sync/atomic"

	"github.com/go-graphite/go-carbon/helper"

	"github.com/go-graphite/go-carbon/points"
)

// A generation contains only configured namespaces. Metrics sharing their
// deepest quota share its route; no per-metric slice or closure is allocated.
type throughputRoutes struct {
	generation uint64
	depth      int
	root       *throughputRoute
	byPath     map[string]*throughputRoute
}

type throughputRoute struct {
	owner      *throughputRoutes
	namespaces []*throughputUsagePerNamespace
}

func (tu *throughputQuotaManager) publishRoutes(depth int) {
	routes := &throughputRoutes{generation: tu.generation.Add(1), depth: depth, byPath: make(map[string]*throughputRoute)}
	routes.root = &throughputRoute{owner: routes}
	if root := tu.load("/"); root != nil {
		routes.root.namespaces = append(routes.root.namespaces, root)
	}
	tu.entries.Range(func(key, value any) bool {
		path := key.(string)
		if path == "/" {
			return true
		}
		ns := append([]*throughputUsagePerNamespace(nil), routes.root.namespaces...)
		for i := 0; i < len(path); i++ {
			if path[i] == '.' {
				if parent := tu.load(path[:i]); parent != nil {
					ns = append(ns, parent)
				}
			}
		}
		ns = append(ns, value.(*throughputUsagePerNamespace))
		routes.byPath[path] = &throughputRoute{owner: routes, namespaces: ns}
		return true
	})
	tu.routes.Store(routes)
}

func (r *throughputRoutes) resolve(metric string) *throughputRoute {
	route := r.root
	for i, depth := 0, 0; depth <= r.depth; depth++ {
		next := strings.IndexByte(metric[i:], '.')
		if next < 0 {
			break
		}
		i += next
		if match := r.byPath[metric[:i]]; match != nil {
			route = match
		}
		i++
	}
	return route
}

// ShouldThrottleMetricWithState caches namespace resolution on a cache item.
// The cache owns state under its shard lock. A quota reload or an index with a
// different manager invalidates it; counter resets use the live recorders.
func (listener *CarbonserverListener) ShouldThrottleMetricWithState(ps *points.Points, inCache bool, state *any) bool {
	idx := listener.CurrentFileIndex()
	if idx == nil || idx.trieIdx == nil {
		return false
	}
	ti := idx.trieIdx
	var routes *throughputRoutes
	if ti.throughputs != nil {
		routes = ti.throughputs.routes.Load()
	}
	if routes == nil {
		return ti.throttle(ps, inCache)
	}
	route, _ := (*state).(*throughputRoute)
	if route == nil || route.owner != routes {
		route = routes.resolve(ps.Metric)
		*state = route
	}
	if ti.checkThroughput(ps, route.namespaces) {
		return true
	}
	return !inCache && ti.throttleMetric(ps)
}

func (ti *trieIndex) checkThroughput(ps *points.Points, namespaces []*throughputUsagePerNamespace) bool {
	count := ti.metricDataLen(ps)
	reset := ti.getResetFrequency()
	for _, ns := range namespaces {
		if !ns.withinQuota(count, reset) {
			atomic.AddInt64(&ns.quotaUsage().Throttled, count)
			if ns.quota().DroppingPolicy != QDPNone {
				return true
			}
		}
	}
	stripe := helper.HashString(ps.Metric)
	for _, ns := range namespaces {
		ns.increase(count, stripe)
	}
	return false
}
