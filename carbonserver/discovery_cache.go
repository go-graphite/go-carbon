package carbonserver

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/go-graphite/go-carbon/helper"
)

const defaultDiscoveryCacheTTL int32 = 300

type discoveryCacheTTLRule struct {
	prefix  string
	seconds int32
}

// SetDiscoveryCacheTTLs configures literal prefix overrides for both discovery
// caches. Call it before serving requests. The longest matching prefix wins.
func (listener *CarbonserverListener) SetDiscoveryCacheTTLs(overrides map[string]time.Duration) error {
	rules := make([]discoveryCacheTTLRule, 0, len(overrides))
	seen := make(map[string]struct{}, len(overrides))
	for prefix, ttl := range overrides {
		prefix = strings.TrimSuffix(prefix, ".")
		if prefix == "" || strings.HasPrefix(prefix, ".") || strings.Contains(prefix, "..") || strings.ContainsAny(prefix, "*?[]{}\\/") {
			return fmt.Errorf("discovery-cache-ttl prefix %q must be a literal metric prefix", prefix)
		}
		if ttl < time.Second || ttl%time.Second != 0 || ttl/time.Second > math.MaxInt32 {
			return fmt.Errorf("discovery-cache-ttl for %q must be a positive whole number of seconds, at most %d", prefix, math.MaxInt32)
		}
		if _, ok := seen[prefix]; ok {
			return fmt.Errorf("duplicate discovery-cache-ttl prefix %q", prefix)
		}
		seen[prefix] = struct{}{}
		rules = append(rules, discoveryCacheTTLRule{prefix, int32(ttl / time.Second)})
	}
	sort.Slice(rules, func(i, j int) bool { return len(rules[i].prefix) > len(rules[j].prefix) })
	listener.discoveryCacheTTLs = rules
	return nil
}

func (listener *CarbonserverListener) discoveryCacheTTL(queries []string) int32 {
	if len(listener.discoveryCacheTTLs) == 0 || len(queries) == 0 {
		return defaultDiscoveryCacheTTL
	}
	var longestTTL int32
	for _, query := range queries {
		ttl := defaultDiscoveryCacheTTL
		for _, rule := range listener.discoveryCacheTTLs {
			if query == rule.prefix || (len(query) > len(rule.prefix) && query[len(rule.prefix)] == '.' && strings.HasPrefix(query, rule.prefix)) {
				ttl = rule.seconds
				break
			}
		}
		// A batch containing an unrelated query keeps that query's normal TTL.
		if ttl > longestTTL {
			longestTTL = ttl
		}
	}
	return longestTTL
}

// Fixed hash buckets avoid an unbounded namespace invalidation map. Collisions
// only refresh an unrelated cache entry; they never hide a newly indexed name.
func discoveryBucket(name string) int {
	if end := strings.IndexByte(name, '.'); end >= 0 {
		name = name[:end]
	}
	if strings.ContainsAny(name, "*?[]{}\\/") {
		return 0
	}
	return int(helper.HashString(name)%256) + 1
}

func (listener *CarbonserverListener) markDiscoveryChange(metric string) {
	now := time.Now().Unix()
	listener.discoveryChanges[0].Store(now)
	listener.discoveryChanges[discoveryBucket(metric)].Store(now)
}

// Coalesce invalidations within a second. The extra epoch at the next second
// expires results computed earlier in the last arrival's second even if no
// more names arrive. Stable namespaces retain their normal cache TTL/hit rate.
func (listener *CarbonserverListener) discoveryVersion(queries []string) int64 {
	var changed int64
	for _, query := range queries {
		changed = max(changed, listener.discoveryChanges[discoveryBucket(query)].Load())
	}
	if changed != 0 && time.Now().Unix() > changed {
		changed++
	}
	return changed
}
