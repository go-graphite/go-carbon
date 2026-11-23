package downsize

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	// syncCachePause is the cache fill ratio that pauses the copy outright.
	syncCachePause = 0.95
	// syncCacheMisses is how many failed polls release a paused copy.
	syncCacheMisses = 3
)

// cacheFill reads a store's write-cache fill ratio from carbonserver
// /admin/info: the fuller of points/limit and estimated bytes/byte limit.
// bounded is false when the store reports no cache limit at all.
func cacheFill(ctx context.Context, client *http.Client, url string) (ratio float64, bounded bool, err error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, false, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, false, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	var info struct {
		Cache *struct {
			Size           float64 `json:"size"`
			Limit          float64 `json:"limit"`
			EstimatedBytes float64 `json:"estimated_bytes"`
			ByteLimit      float64 `json:"byte_limit"`
		} `json:"cache"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&info); err != nil {
		return 0, false, err
	}
	if info.Cache == nil {
		return 0, false, fmt.Errorf("no cache info")
	}
	if info.Cache.Limit > 0 {
		ratio, bounded = info.Cache.Size/info.Cache.Limit, true
	}
	if info.Cache.ByteLimit > 0 {
		ratio, bounded = max(ratio, info.Cache.EstimatedBytes/info.Cache.ByteLimit), true
	}
	return ratio, bounded, nil
}

// govern slows the copy while any store's write cache is filling, as bucky's
// go-carbon health check does. Imports bypass the cache, so its growth means
// the persister is losing to copy load. Each poll with the fullest cache at or
// above threshold halves the rate; at syncCachePause the copy pauses until the
// cache is back under threshold. Healthy polls raise the rate by a quarter.
//
// urls maps each store to its /admin/info URL. An unreachable store holds the
// current rate; after syncCacheMisses failed polls a paused copy resumes, so a
// lost telemetry path cannot park the copy forever.
func (r *syncRateLimit) govern(ctx context.Context, client *http.Client, urls map[string]string, threshold float64, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	state, misses := "free", 0
	unbounded := map[string]bool{}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		worst, worstHost, failed := 0.0, "", error(nil)
		for host, url := range urls {
			ratio, bounded, err := cacheFill(ctx, client, url)
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				failed = fmt.Errorf("%s: %w", host, err)
				continue
			}
			if !bounded && !unbounded[host] {
				unbounded[host] = true
				event("sync_cache_guard", map[string]any{"level": "warn", "store": host, "state": "cache has no limit; this store cannot throttle the copy"})
			}
			if ratio > worst {
				worst, worstHost = ratio, host
			}
		}
		next, rate := state, int64(0)
		switch {
		case worst >= threshold || worst >= syncCachePause:
			misses = 0
			pause := worst >= syncCachePause || state == "paused"
			next = "throttled"
			if pause {
				next = "paused"
			}
			rate = r.throttle(pause, fmt.Sprintf("cache pressure on %s (%.0f%% full, threshold %.0f%%), %s", worstHost, worst*100, threshold*100, next))
		case failed != nil:
			// Unknown is not healthy: hold the rate, never raise it.
			if misses++; misses == syncCacheMisses {
				r.unpause()
				event("sync_cache_guard", map[string]any{"level": "warn", "state": "cache telemetry unavailable; holding the current rate", "error": failed.Error()})
				if state == "paused" {
					next = "throttled"
				}
			}
		default:
			misses = 0
			next = "free"
			if rate = r.relax(); rate > 0 {
				next = "throttled"
			}
		}
		if next != state {
			event("sync_cache_throttle", map[string]any{"state": next, "previous": state, "store": worstHost, "cache_fill": worst, "threshold": threshold, "bytes_per_second": rate})
			state = next
		}
	}
}
