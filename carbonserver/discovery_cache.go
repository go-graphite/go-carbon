package carbonserver

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
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
