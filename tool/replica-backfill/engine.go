package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type engineConfig struct {
	from, until int64 // gap window, unix seconds, inclusive
	lookback    int64 // seconds before from that prove a metric active; 0 disables the check
	batch       int
	workers     int
	dryRun      bool
	printGaps   int
	checkpoint  string
	progress    time.Duration
}

type engineStats struct {
	names, resumed, scanned, invalid, inactive, complete, withGaps atomic.Int64
	missingSlots, fillable, unfilledMetrics, stepMismatch          atomic.Int64
	fallbackMetrics, batches, failedBatches                        atomic.Int64
}

type engine struct {
	cfg      engineConfig
	local    *client
	replicas []*client
	out      *writer // nil in dry-run mode
	stats    engineStats
	printed  atomic.Int64
	ckpt     *checkpointTracker
}

type batch struct {
	seq   int
	names []string
}

// gap is one metric's missing local slots in the window.
type gap struct {
	step  int64 // local step; 0 when the local server returned no series
	slots []int64
}

func (e *engine) run(ctx context.Context, names []string) error {
	sort.Strings(names)
	names = compactStrings(names)
	e.stats.names.Store(int64(len(names)))
	done := ""
	if e.ckpt != nil {
		done = e.ckpt.state.DoneThrough
		if e.ckpt.state.Complete {
			log.Printf("checkpoint %s: run already complete", e.cfg.checkpoint)
			e.stats.resumed.Store(int64(len(names)))
			return nil
		}
	}
	if done != "" {
		skip := sort.SearchStrings(names, done)
		if skip < len(names) && names[skip] == done {
			skip++
		}
		e.stats.resumed.Store(int64(skip))
		names = names[skip:]
		log.Printf("checkpoint %s: resuming after %q, %d metrics skipped", e.cfg.checkpoint, done, skip)
	}

	jobs := make(chan batch)
	var writerStopped <-chan struct{}
	if e.out != nil {
		writerStopped = e.out.stopped
	}
	var batches []batch
	for i := 0; i < len(names); i += e.cfg.batch {
		batches = append(batches, batch{len(batches), names[i:min(i+e.cfg.batch, len(names))]})
	}
	if e.ckpt != nil {
		e.ckpt.begin(batches)
	}
	stopProgress := e.reportProgress()
	defer stopProgress()

	var wg sync.WaitGroup
	for range max(e.cfg.workers, 1) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for b := range jobs {
				select {
				case <-ctx.Done():
					return
				case <-writerStopped:
					return
				default:
				}
				err := e.process(ctx, b)
				e.stats.batches.Add(1)
				if err != nil {
					e.stats.failedBatches.Add(1)
					if ctx.Err() == nil {
						log.Printf("batch %d (%s .. %s) failed: %v", b.seq, b.names[0], b.names[len(b.names)-1], err)
					}
					continue
				}
				if e.ckpt != nil {
					e.ckpt.done(b.seq)
				}
			}
		}()
	}
feed:
	for _, b := range batches {
		select {
		case jobs <- b:
		case <-ctx.Done():
			break feed
		case <-writerStopped:
			break feed
		}
	}
	close(jobs)
	wg.Wait()
	if e.ckpt != nil {
		if err := e.ckpt.save(); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if n := e.stats.failedBatches.Load(); n > 0 {
		return fmt.Errorf("%d batches failed; rerun with the same checkpoint to retry them", n)
	}
	return nil
}

func compactStrings(s []string) []string {
	out := s[:0]
	for i, v := range s {
		if i == 0 || v != s[i-1] {
			out = append(out, v)
		}
	}
	return out
}

// validName rejects names the plaintext protocol cannot carry and names
// carbonserver would expand as globs.
func validName(n string) bool {
	return n != "" && !strings.ContainsAny(n, " \t\r\n*?[]{}")
}

// localGaps finds the window slots the local host lacks. A metric without any
// local point in [from-lookback, until] is inactive and skipped.
func (e *engine) localGaps(names []string, local map[string]series) map[string]gap {
	gaps := make(map[string]gap)
	for _, n := range names {
		s, ok := local[n]
		if !ok || s.Step <= 0 {
			if e.cfg.lookback > 0 {
				e.stats.inactive.Add(1)
				continue
			}
			gaps[n] = gap{}
			continue
		}
		active := e.cfg.lookback == 0
		var slots []int64
		for i, v := range s.Values {
			ts := s.Start + int64(i)*s.Step
			present := !math.IsNaN(v)
			active = active || present
			if !present && ts >= e.cfg.from && ts <= e.cfg.until {
				slots = append(slots, ts)
			}
		}
		switch {
		case !active:
			e.stats.inactive.Add(1)
		case len(slots) == 0:
			e.stats.complete.Add(1)
		default:
			gaps[n] = gap{step: s.Step, slots: slots}
		}
	}
	return gaps
}

// fill returns the replica points for g, or ok=false when the replica has
// nothing usable for this metric (absent, empty, or at another step).
func (e *engine) fill(name string, g gap, s series) (pts []point, ok, mismatch bool) {
	if s.Step <= 0 {
		return nil, false, false
	}
	if g.step == 0 {
		// No local series: every replica point in the window is missing.
		for i, v := range s.Values {
			ts := s.Start + int64(i)*s.Step
			if !math.IsNaN(v) && ts >= e.cfg.from && ts <= e.cfg.until {
				pts = append(pts, point{name, v, ts})
			}
		}
		return pts, len(pts) > 0, false
	}
	if s.Step != g.step {
		return nil, false, true
	}
	hasData := false
	for _, v := range s.Values {
		if !math.IsNaN(v) {
			hasData = true
			break
		}
	}
	for _, ts := range g.slots {
		if v, in := s.at(ts); in && !math.IsNaN(v) {
			pts = append(pts, point{name, v, ts})
		}
	}
	return pts, hasData, false
}

// process compares one batch with the local host and fills its gaps from the
// replicas in order, asking a later replica only for metrics an earlier one
// failed on or had no data for.
func (e *engine) process(ctx context.Context, b batch) error {
	names := make([]string, 0, len(b.names))
	for _, n := range b.names {
		if validName(n) {
			names = append(names, n)
		} else {
			e.stats.invalid.Add(1)
		}
	}
	e.stats.scanned.Add(int64(len(b.names)))
	if len(names) == 0 {
		return nil
	}
	// carbonserver returns slots after the requested start, so ask from one
	// second before the window to include a slot at exactly from.
	local, err := e.local.fetch(ctx, names, e.cfg.from-e.cfg.lookback-1, e.cfg.until)
	if err != nil {
		return fmt.Errorf("local fetch: %w", err)
	}
	gaps := e.localGaps(names, local)
	if len(gaps) == 0 {
		return nil
	}
	e.stats.withGaps.Add(int64(len(gaps)))
	pending := make([]string, 0, len(gaps))
	for _, n := range names {
		if g, ok := gaps[n]; ok {
			pending = append(pending, n)
			e.stats.missingSlots.Add(int64(len(g.slots)))
		}
	}

	var pts []point
	mismatched := make(map[string]bool)
	answered := false
	var lastErr error
	for i, r := range e.replicas {
		if len(pending) == 0 {
			break
		}
		if i > 0 {
			e.stats.fallbackMetrics.Add(int64(len(pending)))
		}
		got, err := r.fetch(ctx, pending, e.cfg.from-1, e.cfg.until)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			lastErr = err
			log.Printf("replica %s: %v; trying the next replica for %d metrics", r.base, err, len(pending))
			continue
		}
		answered = true
		next := pending[:0:0]
		for _, n := range pending {
			p, ok, mismatch := e.fill(n, gaps[n], got[n])
			if mismatch {
				mismatched[n] = true
			}
			if !ok {
				next = append(next, n)
				continue
			}
			delete(mismatched, n)
			pts = append(pts, p...)
			e.printGap(n, gaps[n], len(p))
		}
		pending = next
	}
	// Without any replica answer nothing is known: fail so a rerun repeats the
	// batch. Metrics only a failed fallback could have filled count as unfilled.
	if !answered {
		return fmt.Errorf("every replica failed: %w", lastErr)
	}
	e.stats.unfilledMetrics.Add(int64(len(pending)))
	e.stats.stepMismatch.Add(int64(len(mismatched)))
	e.stats.fillable.Add(int64(len(pts)))
	if e.out == nil || len(pts) == 0 {
		return nil
	}
	return e.out.write(ctx, pts)
}

func (e *engine) printGap(name string, g gap, fill int) {
	if e.cfg.printGaps <= 0 || e.printed.Add(1) > int64(e.cfg.printGaps) {
		return
	}
	first, last := int64(0), int64(0)
	if len(g.slots) > 0 {
		first, last = g.slots[0], g.slots[len(g.slots)-1]
	}
	log.Printf("gap %s: step=%d missing=%d (%s .. %s) fillable=%d", name, g.step, len(g.slots),
		time.Unix(first, 0).UTC().Format(time.RFC3339), time.Unix(last, 0).UTC().Format(time.RFC3339), fill)
}

func (e *engine) reportProgress() func() {
	if e.cfg.progress <= 0 {
		return func() {}
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(e.cfg.progress)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				s := &e.stats
				msg := fmt.Sprintf("progress: scanned=%d/%d with_gaps=%d fillable=%d unfilled_metrics=%d failed_batches=%d replica_requests=%d",
					s.scanned.Load()+s.resumed.Load(), s.names.Load(), s.withGaps.Load(), s.fillable.Load(),
					s.unfilledMetrics.Load(), s.failedBatches.Load(), e.replicaRequests())
				if e.out != nil {
					msg += fmt.Sprintf(" sent=%d paused=%s", e.out.stats.points.Load(), time.Duration(e.out.stats.pausedNanos.Load()).Round(time.Second))
				}
				log.Print(msg)
			}
		}
	}()
	return func() { close(stop); wg.Wait() }
}

func (e *engine) replicaRequests() int64 {
	var n int64
	for _, r := range e.replicas {
		n += r.stats.requests.Load()
	}
	return n
}

// checkpointState is persisted so a restarted run skips finished metrics.
// Names are processed in sorted order; every name <= DoneThrough is done.
type checkpointState struct {
	Version     int       `json:"version"`
	From        int64     `json:"from"`
	Until       int64     `json:"until"`
	DoneThrough string    `json:"done_through"`
	Complete    bool      `json:"complete"`
	Updated     time.Time `json:"updated"`
}

type checkpointTracker struct {
	path     string
	mu       sync.Mutex
	state    checkpointState
	batches  []batch
	begun    bool
	finished map[int]bool
	next     int // lowest unfinished batch
	saved    time.Time
	every    time.Duration
}

func loadCheckpoint(path string, from, until int64) (*checkpointTracker, error) {
	t := &checkpointTracker{path: path, state: checkpointState{Version: 1, From: from, Until: until}, every: 5 * time.Second}
	b, err := os.ReadFile(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return t, nil
	case err != nil:
		return nil, err
	}
	var s checkpointState
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("checkpoint %s: %w", path, err)
	}
	if s.Version != 1 || s.From != from || s.Until != until {
		return nil, fmt.Errorf("checkpoint %s is for window %d..%d, not %d..%d; remove it or pick another --checkpoint", path, s.From, s.Until, from, until)
	}
	t.state = s
	return t, nil
}

func (t *checkpointTracker) begin(batches []batch) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.batches, t.finished, t.next, t.begun = batches, make(map[int]bool), 0, true
}

// done records a finished batch and advances DoneThrough over the contiguous
// finished prefix. A failed batch holds the watermark, so a rerun repeats it.
func (t *checkpointTracker) done(seq int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.finished[seq] = true
	for t.next < len(t.batches) && t.finished[t.next] {
		names := t.batches[t.next].names
		t.state.DoneThrough = names[len(names)-1]
		delete(t.finished, t.next)
		t.next++
	}
	if t.next == len(t.batches) {
		t.state.Complete = true
	}
	if time.Since(t.saved) >= t.every {
		if err := t.saveLocked(); err != nil {
			log.Printf("checkpoint save failed: %v", err)
		}
	}
}

func (t *checkpointTracker) save() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.begun && t.next == len(t.batches) {
		t.state.Complete = true
	}
	return t.saveLocked()
}

func (t *checkpointTracker) saveLocked() error {
	t.state.Updated = time.Now().UTC()
	t.saved = time.Now()
	b, err := json.MarshalIndent(t.state, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(t.path), "."+filepath.Base(t.path)+".*")
	if err != nil {
		return err
	}
	_, err = tmp.Write(append(b, '\n'))
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp.Name(), t.path)
	}
	if err != nil {
		_ = os.Remove(tmp.Name())
	}
	return err
}
