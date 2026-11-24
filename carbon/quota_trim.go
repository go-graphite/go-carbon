package carbon

import (
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/go-graphite/go-carbon/carbonserver"
	"github.com/go-graphite/go-carbon/helper"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

func validateQuotaTrimConfig(cfg *Config) error {
	enabled := cfg.Whisper.QuotaTrimEnabled
	for _, q := range cfg.Whisper.Quotas {
		if q.OverQuota == "" {
			continue
		}
		if q.OverQuota != "delete-newest" || q.Metrics <= 0 || q.DroppingPolicy != "new" {
			return fmt.Errorf("invalid quota trim policy for %q: requires positive metrics and dropping-policy new", q.Pattern)
		}
		enabled = true
	}
	if enabled && (cfg.Whisper.StorageBackend != "pebble-chunk" || cfg.Whisper.StoreMirrorFiles ||
		cfg.Whisper.StoreIncrementalMigration && !cfg.incrementalMigrationComplete ||
		!cfg.Whisper.Enabled || !cfg.Carbonserver.Enabled || !cfg.Carbonserver.TrieIndex ||
		!cfg.Carbonserver.ConcurrentIndex || cfg.Carbonserver.RealtimeIndex <= 0 || cfg.Whisper.QuotasFilename == "") {
		return errors.New("quota trim requires pebble-chunk, enabled persister, quotas-file, concurrent realtime trie carbonserver, completed file migration and no rollback mirror")
	}
	return nil
}

type quotaTrimCounts struct{ candidates, deleted, skipped uint64 }
type quotaTrimStats struct {
	mu     sync.Mutex
	total  quotaTrimCounts
	errors uint64
	rules  map[string]quotaTrimCounts
}

func (s *quotaTrimStats) record(name string, c quotaTrimCounts) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rules == nil {
		s.rules = make(map[string]quotaTrimCounts)
	}
	v := s.rules[name]
	v.candidates += c.candidates
	v.deleted += c.deleted
	v.skipped += c.skipped
	s.rules[name] = v
}
func (s *quotaTrimStats) Stat(send helper.StatCallback) {
	s.mu.Lock()
	total, errs, rules := s.total, s.errors, s.rules
	s.total, s.errors, s.rules = quotaTrimCounts{}, 0, nil
	s.mu.Unlock()
	emit := func(suffix string, c quotaTrimCounts) {
		send("candidates"+suffix, float64(c.candidates))
		send("deleted"+suffix, float64(c.deleted))
		send("skipped"+suffix, float64(c.skipped))
	}
	emit("", total)
	send("errors", float64(errs))
	for name, c := range rules {
		emit("."+name, c)
	}
}

func trimContains(namespace, name string) bool {
	return namespace == "/" || strings.HasPrefix(name, namespace+".")
}

// Keep only the newest max-deletes metadata records, independent of catalog
// traversal order. ID allocation order approximates local first appearance.
type trimHeap []store.Metadata

func (h trimHeap) Len() int { return len(h) }
func (h trimHeap) Less(i, j int) bool {
	return h[i].ID < h[j].ID || h[i].ID == h[j].ID && h[i].Name < h[j].Name
}
func (h trimHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *trimHeap) Push(x any)   { *h = append(*h, x.(store.Metadata)) }
func (h *trimHeap) Pop() any     { a := *h; x := a[len(a)-1]; *h = a[:len(a)-1]; return x }

// Use committed catalog counts rather than periodically refreshed usage. The
// single maintenance worker serializes trimming and TTL expiration. Reserve all
// overlapping namespace budgets before dispatch, including children at cap.
// A skipped/conflicting candidate is retried next pass, never replaced with an
// older name in the same pass. Memory is O(matched nodes + max-deletes).
func (r *metricExpirer) planQuotaTrim(ctx context.Context, rules []carbonserver.QuotaTrimRule) ([]store.Metadata, []int64, error) {
	counts := make([]int64, len(rules))
	budget := expirationBudget{rate: r.rate}
	scan := func(visit func(store.Metadata)) error {
		return store.EachPage(ctx, r.db.ListPage, "", expirationPageSize, func(page []store.Metadata) error {
			if err := budget.wait(ctx, len(page)); err != nil {
				return err
			}
			for _, m := range page {
				visit(m)
			}
			return ctx.Err()
		})
	}
	if err := scan(func(m store.Metadata) {
		for i, q := range rules {
			if trimContains(q.Namespace, m.Name) {
				counts[i]++
			}
		}
	}); err != nil {
		return nil, nil, err
	}
	excess := make([]int64, len(rules))
	anyExcess := false
	for i, q := range rules {
		excess[i] = max(counts[i]-q.Quota.Metrics, 0)
		anyExcess = anyExcess || excess[i] > 0
	}
	if !anyExcess {
		return nil, excess, nil
	}
	var newest trimHeap
	if err := scan(func(m store.Metadata) {
		matched := false
		for i, q := range rules {
			if !trimContains(q.Namespace, m.Name) {
				continue
			}
			if excess[i] == 0 {
				return
			}
			matched = true
		}
		if !matched {
			return
		}
		heap.Push(&newest, m)
		if newest.Len() > r.maxDeletes {
			heap.Pop(&newest)
		}
	}); err != nil {
		return nil, nil, err
	}
	sort.Slice(newest, func(i, j int) bool {
		return newest[i].ID > newest[j].ID || newest[i].ID == newest[j].ID && newest[i].Name > newest[j].Name
	})
	remaining := append([]int64(nil), excess...)
	selected := make([]store.Metadata, 0, len(newest))
	for _, m := range newest {
		allowed := true
		for i, q := range rules {
			if trimContains(q.Namespace, m.Name) && remaining[i] == 0 {
				allowed = false
				break
			}
		}
		if !allowed {
			continue
		}
		selected = append(selected, m)
		for i, q := range rules {
			if trimContains(q.Namespace, m.Name) {
				remaining[i]--
			}
		}
	}
	return selected, excess, nil
}

func (r *metricExpirer) sweep(ctx context.Context) error {
	if r.trimRules == nil {
		return r.sweepExpiration(ctx)
	}
	deleted, err := r.trimQuota(ctx)
	if err != nil {
		r.stats.trim.mu.Lock()
		r.stats.trim.errors++
		r.stats.trim.mu.Unlock()
		return fmt.Errorf("trim namespace quotas: %w", err)
	}
	if r.expiration == 0 && !r.rules.Enabled() {
		return nil
	}
	// TTL deletion remains independent of namespace caps. Share the total pass
	// limit; a quota pass may consume it all. Both phases use the same workers.
	left := r.maxDeletes - deleted
	if left <= 0 {
		return nil
	}
	old := r.maxDeletes
	r.maxDeletes = left
	defer func() { r.maxDeletes = old }()
	return r.sweepExpiration(ctx)
}

func (r *metricExpirer) trimQuota(ctx context.Context) (int, error) {
	rules, err := r.trimRules()
	if err != nil {
		return 0, err
	}
	if len(rules) == 0 {
		return 0, nil
	}
	if r.maxDeletes <= 0 {
		return 0, errors.New("quota trim requires a positive max-deletes")
	}
	selected, excess, err := r.planQuotaTrim(ctx, rules)
	if err != nil {
		return 0, err
	}
	policies := make(map[string]string, len(selected))
	planned := make([]uint64, len(rules))
	samples := make([][]string, len(rules))
	deleted := make([]atomic.Uint64, len(rules))
	var totalDeleted atomic.Uint64
	// Cache one exact rule identity per node. Receipts name the first matching
	// opted-in rule; all overlapping budgets are still reserved and counted.
	// Sharing strings keeps receipt planning O(nodes + candidates), even with
	// deeply nested rules, rather than serializing every ancestor per name.
	rulePolicies := make([]string, len(rules))
	for i, q := range rules {
		data, err := json.Marshal(struct{ QuotaTrim carbonserver.QuotaTrimRule }{q})
		if err != nil {
			return 0, err
		}
		rulePolicies[i] = string(data)
	}
	for _, m := range selected {
		for i, q := range rules {
			if trimContains(q.Namespace, m.Name) {
				planned[i]++
				if len(samples[i]) < 10 {
					samples[i] = append(samples[i], m.Name)
				}
				if _, exists := policies[m.Name]; !exists {
					policies[m.Name] = rulePolicies[i]
				}
			}
		}
	}
	defer func() {
		total := quotaTrimCounts{uint64(len(selected)), totalDeleted.Load(), uint64(len(selected)) - totalDeleted.Load()}
		s := &r.stats.trim
		s.mu.Lock()
		s.total.candidates += total.candidates
		s.total.deleted += total.deleted
		s.total.skipped += total.skipped
		s.mu.Unlock()
		for i, q := range rules {
			c := quotaTrimCounts{planned[i], deleted[i].Load(), planned[i] - deleted[i].Load()}
			s.record(q.StatMetric, c)
			zapwriter.Logger("quota").Info("quota trim namespace", zap.String("namespace", q.Namespace), zap.String("rule", q.Quota.Pattern), zap.Int64("cap", q.Quota.Metrics), zap.Int64("excess", excess[i]), zap.Bool("dryRun", r.trimDryRun), zap.Uint64("candidates", c.candidates), zap.Uint64("deleted", c.deleted), zap.Uint64("skipped", c.skipped), zap.Strings("samples", samples[i]))
		}
	}()
	if len(selected) == 0 {
		return 0, nil
	}
	// Reuse the expiration executor verbatim: partitioned workers, bounded batch
	// queues, delete pacing, pressure guard, cache check, revision fence, receipts,
	// tombstone flush and index notification. No separate deletion implementation.
	executor := &metricExpirer{
		db: r.db, now: r.now, pending: r.pending, notify: r.notify, stats: r.stats,
		rate: r.rate, deleteRate: r.deleteRate, deleteWorkers: r.deleteWorkers,
		maxDeletes: r.maxDeletes, maxL0: r.maxL0, maxDebt: r.maxDebt, dryRun: r.trimDryRun,
		candidates: selected, quotaPolicies: policies,
		onDeleted: func(m store.Metadata) {
			totalDeleted.Add(1)
			for i, q := range rules {
				if trimContains(q.Namespace, m.Name) {
					deleted[i].Add(1)
				}
			}
		},
	}
	err = executor.sweepExpiration(ctx)
	return int(totalDeleted.Load()), err
}

func (r *metricExpirer) eachCandidatePage(ctx context.Context, visit func([]store.Metadata) error) error {
	if r.candidates == nil {
		return store.EachPage(ctx, r.db.ListPage, "", expirationPageSize, visit)
	}
	for start := 0; start < len(r.candidates); start += expirationPageSize {
		if err := visit(r.candidates[start:min(start+expirationPageSize, len(r.candidates))]); err != nil {
			return err
		}
	}
	return ctx.Err()
}
