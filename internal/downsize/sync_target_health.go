package downsize

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Target health is admission control, not a transfer failure. A paused batch
// keeps its names and waits outside the batch's read/retry/idle time budgets.
// One gate belongs to one sync target; concurrent batches share its pause clock.
type targetHealthOptions struct {
	enabled   bool
	high, low float64
}

type targetHealthSample struct{ points, limit, dropped float64 }

type targetHealthGate struct {
	mu           sync.Mutex
	target       string
	high, low    float64
	pausedAt     time.Time
	seconds      float64
	changed      chan struct{}
	previous     float64
	havePrevious bool
}

func newTargetHealthGate(target string, high, low float64) *targetHealthGate {
	return &targetHealthGate{target: target, high: high, low: low, changed: make(chan struct{})}
}

func (g *targetHealthGate) update(sample targetHealthSample, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	paused := !g.pausedAt.IsZero()
	next := paused
	occupancy := float64(0)
	reason := "cache_occupancy"
	if err != nil {
		// Stale overload evidence must never leave a pass waiting forever.
		event("sync_target_health_unavailable", map[string]any{"level": "warn", "target": g.target, "error": err.Error(), "action": "continue"})
		next, reason = false, "health_unavailable"
	} else {
		occupancy = sample.points / sample.limit
		overflow := g.havePrevious && sample.dropped > g.previous
		g.previous, g.havePrevious = sample.dropped, true
		if overflow || occupancy >= g.high {
			next = true
			if overflow {
				reason = "overflow_dropped_increased"
			}
		} else if occupancy < g.low {
			next = false
		}
	}
	if next == paused {
		return
	}
	kind := "sync_target_health_resumed"
	if next {
		g.pausedAt = time.Now()
		kind = "sync_target_health_paused"
	} else {
		g.seconds += time.Since(g.pausedAt).Seconds()
		g.pausedAt = time.Time{}
	}
	close(g.changed)
	g.changed = make(chan struct{})
	fields := map[string]any{"target": g.target, "reason": reason, "high": g.high, "low": g.low}
	if err == nil {
		fields["occupancy"], fields["cache_points"], fields["cache_limit_points"], fields["overflow_dropped_points_total"] = occupancy, sample.points, sample.limit, sample.dropped
	}
	event(kind, fields)
}

func (g *targetHealthGate) wait(ctx context.Context) error {
	if g == nil {
		return ctx.Err()
	}
	for {
		g.mu.Lock()
		paused, changed := !g.pausedAt.IsZero(), g.changed
		g.mu.Unlock()
		if !paused {
			return ctx.Err()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (g *targetHealthGate) snapshot() (bool, float64) {
	if g == nil {
		return false, 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	seconds := g.seconds
	paused := !g.pausedAt.IsZero()
	if paused {
		seconds += time.Since(g.pausedAt).Seconds()
	}
	return paused, seconds
}

// /admin/info's cache scope has size/limit but no overflow counter. Use the
// filtered Prometheus endpoint for all three values from one scrape instead.
func readTargetHealth(ctx context.Context, client *http.Client, endpoint string) (targetHealthSample, error) {
	var sample targetHealthSample
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return sample, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return sample, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return sample, fmt.Errorf("target health HTTP %d", resp.StatusCode)
	}
	values := map[string]*float64{"cache_points": &sample.points, "cache_limit_points": &sample.limit, "cache_overflow_dropped_points_total": &sample.dropped}
	seen := map[string]bool{}
	scan := bufio.NewScanner(io.LimitReader(resp.Body, 1<<20))
	for scan.Scan() {
		fields := strings.Fields(scan.Text())
		if len(fields) < 2 {
			continue
		}
		dest, ok := values[fields[0]]
		if !ok {
			continue
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || seen[fields[0]] {
			return sample, fmt.Errorf("invalid target health metric %s", fields[0])
		}
		*dest, seen[fields[0]] = value, true
	}
	if err := scan.Err(); err != nil {
		return sample, err
	}
	if len(seen) != len(values) || sample.limit <= 0 {
		return sample, fmt.Errorf("target health requires cache_points, positive cache_limit_points and cache_overflow_dropped_points_total")
	}
	return sample, nil
}

// The initial poll finishes before work starts. A failed or hung scrape opens
// the gate within five seconds; polling continues so monitoring can recover.
func (g *targetHealthGate) monitor(ctx context.Context, endpoint string, interval time.Duration) func() {
	ctx, cancel := context.WithCancel(ctx)
	client := &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	poll := func() { sample, err := readTargetHealth(ctx, client, endpoint); g.update(sample, err) }
	poll()
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				poll()
			}
		}
	}()
	return func() { cancel(); <-done; client.CloseIdleConnections() }
}

func targetHealthURL(target string, port int) string {
	u, _ := url.Parse(target) // SyncMain has already normalized the target.
	u.Host = net.JoinHostPort(u.Hostname(), strconv.Itoa(port))
	u.Path, u.RawQuery = "/metrics", ""
	q := u.Query()
	for _, name := range []string{"cache_points", "cache_limit_points", "cache_overflow_dropped_points_total"} {
		q.Add("name[]", name)
	}
	u.RawQuery = q.Encode()
	return u.String()
}
