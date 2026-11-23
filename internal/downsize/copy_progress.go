package downsize

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

// copyProgress tracks what a long copy is doing for the periodic progress
// line and the stall watchdog. All methods are nil-safe.
type copyProgress struct {
	now              func() time.Time // immutable after construction
	restartWaiting   atomic.Int64
	mu               sync.Mutex
	waiting          map[string]time.Time // host -> not answering since
	lastSuccess      time.Time
	retries          atomic.Int64
	inFlight         atomic.Int64
	total            atomic.Int64 // cumulative names, including completed receipts; -1 until counted
	completedAtStart int64        // immutable after opening the journal, before workers start
}

func newCopyProgress() *copyProgress {
	p := &copyProgress{waiting: map[string]time.Time{}, now: time.Now, lastSuccess: time.Now()}
	p.total.Store(-1)
	return p
}

// Retain a previously counted denominator while the catalog is counted again.
// The journal cursor is authoritative even for older remaining-only snapshots.
func (p *copyProgress) resume(state string, completed int64) {
	p.completedAtStart = completed
	var previous progressSnapshot
	if data, err := os.ReadFile(state + ".progress.json"); err == nil && json.Unmarshal(data, &previous) == nil {
		p.total.Store(max(completed, previous.Total))
	}
}

func (p *copyProgress) down(host string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.waiting[host]; !ok {
		p.waiting[host] = p.now()
	}
}

func (p *copyProgress) up(hosts ...string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, h := range hosts {
		delete(p.waiting, h)
	}
}

func (p *copyProgress) retried() {
	if p != nil {
		p.retries.Add(1)
	}
}

func (p *copyProgress) success() {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.lastSuccess = p.now()
	p.mu.Unlock()
}

// waits describes hosts currently waited for, longest first.
func (p *copyProgress) waits(roles map[string]string) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	type wait struct {
		host  string
		since time.Time
	}
	var all []wait
	for h, since := range p.waiting {
		all = append(all, wait{h, since})
	}
	sort.Slice(all, func(a, b int) bool { return all[a].since.Before(all[b].since) })
	out := make([]string, 0, len(all))
	for _, w := range all {
		out = append(out, fmt.Sprintf("%s %s down, waiting %s", roles[w.host], w.host, time.Since(w.since).Round(time.Second)))
	}
	return out
}

// stuck reports how long no batch succeeded while every host answered and
// work was in flight; host-down time and idle time do not count.
func (p *copyProgress) stuck() time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.restartWaiting.Load() > 0 || len(p.waiting) > 0 || p.inFlight.Load() == 0 {
		p.lastSuccess = p.now()
	}
	return p.now().Sub(p.lastSuccess)
}

type copySkip struct {
	Source, Name string
	Status       int
	Reason       string
}

func (c *planFreeCopier) skip(s copySkip) int {
	c.skipMu.Lock()
	defer c.skipMu.Unlock()
	c.skipped = append(c.skipped, s)
	return len(c.skipped)
}

func (c *planFreeCopier) skippedCount() int {
	c.skipMu.Lock()
	defer c.skipMu.Unlock()
	return len(c.skipped)
}

// retrySkipped gives every skipped name one more single-name attempt after the
// main pass. Names that still fail stay in the skipped list.
func (c *planFreeCopier) retrySkipped(ctx context.Context) error {
	c.skipMu.Lock()
	list := c.skipped
	c.skipped = nil
	c.skipMu.Unlock()
	if len(list) == 0 {
		return nil
	}
	event("retry_skipped", map[string]any{"names": len(list)})
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	work := make(chan copySkip)
	var wg sync.WaitGroup
	for range max(1, min(c.o.streams, len(list))) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for s := range work {
				c.progress.inFlight.Add(1)
				err := c.send(ctx, s.Source, []string{s.Name}, true)
				c.progress.inFlight.Add(-1)
				if err != nil {
					cancel(err)
				}
			}
		}()
	}
	for _, s := range list {
		if chunkstore.ValidateMetricName(s.Name) != nil {
			// The immutable name limit cannot heal after a transfer retry.
			// Its failure is already durable; keep it in the final report.
			c.skip(s)
			continue
		}
		select {
		case work <- s:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
	}
	close(work)
	wg.Wait()
	if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, context.Canceled) {
		return cause
	}
	return ctx.Err()
}

// writeSkipped records the still-skipped names next to the journal (source,
// name, HTTP status, reason; tab separated) and removes a stale list.
func (c *planFreeCopier) writeSkipped() (string, error) {
	path := c.o.state + ".skipped"
	c.skipMu.Lock()
	list := append([]copySkip(nil), c.skipped...)
	c.skipMu.Unlock()
	if len(list) == 0 {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		return "", nil
	}
	sort.Slice(list, func(a, b int) bool {
		if list[a].Source != list[b].Source {
			return list[a].Source < list[b].Source
		}
		return list[a].Name < list[b].Name
	})
	var b strings.Builder
	for _, s := range list {
		fmt.Fprintf(&b, "%s\t%s\t%d\t%s\n", s.Source, s.Name, s.Status, strings.ReplaceAll(s.Reason, "\n", " "))
	}
	return path, os.WriteFile(path, []byte(b.String()), 0o644)
}

// countTotal adds the remaining owned names to the durable starting cursor.
// Newly appended receipts are already included in that remaining-name pass.
func (c *planFreeCopier) countTotal(ctx context.Context, owner func(string) bool) {
	total := c.progress.completedAtStart
	for _, from := range c.o.froms {
		next, closeNames, err := c.nameSource(ctx, from)
		if err != nil {
			return
		}
		cover := c.journal.cover(from)
		for {
			name, err := next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				closeNames()
				if ctx.Err() == nil {
					event("progress_count_failed", map[string]any{"source": from, "error": err.Error()})
				}
				return
			}
			if cover(name) || owner != nil && !owner(name) {
				continue
			}
			total++
		}
		closeNames()
	}
	c.progress.total.Store(total)
}

// report prints one progress line per interval until ctx ends, and fails the
// copy loudly (errCopyStall) when no batch has succeeded for --stall-timeout
// while every host answered and work was in flight.
func (c *planFreeCopier) report(ctx context.Context, stall func(error)) {
	interval := c.o.progressInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	roles := map[string]string{c.o.to: "target"}
	for _, from := range c.o.froms {
		roles[from] = "source"
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	lastAt, lastCopied, lastBytes, lastRetries, last429 := c.started, int64(0), int64(0), int64(0), int64(0)
	var lastTarget429 copyBackpressure
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		now := time.Now()
		copied, bytes := c.stats.copied.Load(), c.stats.bytes.Load()
		retries := c.progress.retries.Load()
		backpressure, target429 := c.m.backpressureSnapshot(c.o.to)
		targetDelta := target429.since(lastTarget429)
		seconds := now.Sub(lastAt).Seconds()
		rate := float64(copied-lastCopied) / seconds
		mbps := float64(bytes-lastBytes) / seconds / 1e6
		skipped := c.skippedCount()
		snapshot := c.progressSnapshot()
		done, total := snapshot.Done, c.progress.total.Load()
		if total >= 0 {
			total = snapshot.Total
		}
		busy := c.progress.inFlight.Load()
		waits := c.progress.waits(roles)
		fields := map[string]any{"copied": copied, "skipped": skipped, "done": done, "names_per_second": rate, "mb_per_second": mbps,
			"avg_names_per_second": float64(copied) / now.Sub(c.started).Seconds(), "retries": retries - lastRetries, "http_429s": backpressure - last429, "target_429s": targetDelta.total(), "source_429s": backpressure - last429 - targetDelta.total(), "target_429_reasons": targetDelta.reasons(),
			"streams_busy": busy, "streams": c.o.streams, "waiting": waits, "elapsed": now.Sub(c.started).Round(time.Second).String()}
		eta := "unknown"
		totalText := "?"
		if total >= 0 {
			fields["total"] = total
			totalText = fmt.Sprint(total)
			if left := total - done; left <= 0 {
				eta = "0s"
			} else if avg := float64(copied) / now.Sub(c.started).Seconds(); rate > 0 || avg > 0 {
				eta = (time.Duration(float64(left)/max(rate, avg)) * time.Second).Round(time.Second).String()
			}
		}
		fields["eta"] = eta
		var target storageSample
		probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, statusErr := c.m.requestOnce(probeCtx, c.o.to, "/v1/transfer/status", nil, &target)
		cancel()
		if statusErr == nil {
			fields["target_l0_sublevels"], fields["target_compaction_debt_bytes"], fields["target_write_stall"] = target.L0Sublevels, target.CompactionDebt, target.WriteStallActive
		}
		limiter := "headroom"
		cacheGuard, paused := c.o.rate.limiter()
		if paused {
			// A deliberate cache pause is not a stall.
			c.progress.success()
		}
		switch {
		case cacheGuard != "":
			limiter = cacheGuard
		case len(waits) > 0:
			limiter = waits[0]
		case targetDelta.total() > 0:
			limiter = targetDelta.limiter()
		case backpressure > last429:
			limiter = "source HTTP 429 backpressure"
		case retries > lastRetries:
			limiter = "transient errors, retrying"
		case busy >= int64(c.o.streams):
			limiter = fmt.Sprintf("transfer streams (%d/%d busy: target import)", busy, c.o.streams)
		case busy == 0:
			limiter = "name discovery"
		}
		fields["limiter"] = limiter
		fields["line"] = fmt.Sprintf("%d/%s names, %.0f names/s, %.1f MB/s, ETA %s, limiter: %s, retries %d, skipped %d", done, totalText, rate, mbps, eta, limiter, retries-lastRetries, skipped)
		event("progress", fields)
		lastAt, lastCopied, lastBytes, lastRetries, last429 = now, copied, bytes, retries, backpressure
		lastTarget429 = target429
		if stuck := c.progress.stuck(); stuck >= c.o.stallTimeout {
			event("copy_stalled", map[string]any{"level": "error", "stuck_seconds": stuck.Seconds(), "stall_timeout": c.o.stallTimeout.String(), "streams_busy": busy, "note": "no batch succeeded while every source and the target answered"})
			stall(fmt.Errorf("%w: no batch succeeded for %s with every host up (--stall-timeout %s)", errCopyStall, stuck.Round(time.Second), c.o.stallTimeout))
			return
		}
	}
}
