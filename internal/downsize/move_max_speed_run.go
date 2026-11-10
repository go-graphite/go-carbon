package downsize

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Maximum speed is an explicit copy-only override. The original plan and journal
// identity remain unchanged; earlier approvals and frozen observations survive.
func (m *mover) configureMaximumSpeed(ctx context.Context, origins []string) (bool, error) {
	path := os.Getenv("RESIZE_MAX_SPEED_DECISION")
	cachePausedStart := os.Getenv("RESIZE_CACHE_PAUSED_ON_START")
	if cachePausedStart != "" && (cachePausedStart != "1" || path == "") {
		return false, errors.New("RESIZE_CACHE_PAUSED_ON_START requires explicit MAX mode and value1")
	}
	if path == "" {
		if m.journal.maxSpeedDecisionSHA != "" {
			return false, errors.New("saved maximum-speed decision requires RESIZE_MAX_SPEED_DECISION on resume")
		}
		return false, nil
	}
	if m.delete || m.blockReads || len(m.planData.Whole) > 0 || m.options.localSources != "" || m.localTarget != "" {
		return false, errors.New("maximum-speed decision is copy-only without read isolation")
	}
	sum := os.Getenv("RESIZE_MAX_SPEED_DECISION_SHA256")
	if err := verifyMaxSpeedDecision(path, sum); err != nil {
		return false, err
	}
	// Controller changes need no new approval, but the existing WRITE-OK must
	// still bind the exact plan manifest being resumed.
	approval, err := os.ReadFile(os.Getenv("RESIZE_MAX_SPEED_WRITE_OK"))
	if err != nil {
		return false, err
	}
	approvalSum := sha256.Sum256(approval)
	if hex.EncodeToString(approvalSum[:]) != os.Getenv("RESIZE_MAX_SPEED_WRITE_OK_SHA256") {
		return false, errors.New("existing WRITE-OK SHA mismatch")
	}
	var bound struct {
		PlanID  string `json:"approved_plan_id"`
		PlanSHA string `json:"approved_plan_sha256"`
		Phase   string `json:"phase"`
		Delete  *bool  `json:"delete"`
	}
	if err := json.Unmarshal(approval, &bound); err != nil {
		return false, err
	}
	raw, err := os.ReadFile(filepath.Join(m.planData.root, "manifest.json"))
	if err != nil {
		return false, err
	}
	manifestSum := sha256.Sum256(raw)
	if bound.PlanID != m.planData.PlanID || bound.PlanSHA != hex.EncodeToString(manifestSum[:]) || bound.Phase != "copy-only" || bound.Delete == nil || *bound.Delete {
		return false, errors.New("maximum-speed requires existing WRITE-OK for the exact copy-only plan")
	}
	readBaseline, err := maximumSpeedClusterReadBaseline(m.planData.Spec.Cluster, origins)
	if err != nil {
		return false, err
	}
	m.journal.maxSpeed = true
	m.journal.preserveRows = true
	if saved := m.journal.admission; saved != nil {
		// Preserve every original envelope; no fresh warm-up or re-baseline.
		m.tuner.backlog = saved.Policy
		for host, h := range m.tuner.hosts {
			if old, ok := saved.Hosts[host]; ok {
				h.baseline, h.previous, h.storePrevious = old.Baseline, &old.Previous, &old.StorePrevious
				h.debtBaseline, h.latencyBaseline = old.DebtBaseline, old.LatencyBaseline
				h.growth, h.foregroundRate, h.readBaseline, h.backgroundCPU = old.Growth, old.ForegroundRate, old.ReadBaseline, old.BackgroundCPU
				h.debtHigh, h.debtGrowth, h.latencyHigh = old.DebtHigh, old.DebtGrowth, old.LatencyHigh
				if old.MaxSpeed != nil {
					copy := *old.MaxSpeed
					h.maxSpeed = &copy
				}
			}
		}
	}
	var destinations []string
	for _, r := range m.planData.Streams {
		destinations = append(destinations, r.targetURL)
	}
	for _, r := range m.planData.Whole {
		destinations = append(destinations, r.TargetURL)
	}
	cacheSeeded := false
	if cachePausedStart == "1" {
		for _, destination := range destinations {
			h := m.tuner.hosts[destination]
			if h.maxSpeed == nil {
				h.maxSpeed = &maxSpeedHostState{}
			}
			if !h.maxSpeed.CacheGuardInitialized {
				h.maxSpeed.CachePaused, h.maxSpeed.CacheLimited, h.maxSpeed.CacheGuardInitialized = true, true, true
				cacheSeeded = true
				event("max_speed_cache_pause_adopted", map[string]any{"destination": destination, "operation": m.journal.operation, "reason": "explicit operator handoff of existing cache/age pause"})
			}
		}
	}
	m.tuner.enableMaxSpeed(destinations, sum)
	m.tuner.maxSpeed.readBaselineMS = readBaseline
	// A pull sends response headers only after export/import/verification work.
	// The former90s header deadline repeatedly discarded healthy heavy batches.
	// Configure the fresh transport before its first request; the client's10m
	// overall deadline and caller contexts still bound stalled transfers.
	if m.client != nil {
		if transport, ok := m.client.Transport.(*http.Transport); ok {
			transport.ResponseHeaderTimeout = 0
		}
	}

	if m.journal.maxSpeedDecisionSHA != sum {
		if err := m.journal.appendRow(journalRow{Operation: m.journal.operation, Plan: m.journal.plan, MaxSpeedDecisionSHA256: sum}); err != nil {
			return false, err
		}
	}
	if cacheSeeded {
		// Persist the inherited real pause before the first network poll can
		// block or be interrupted; no task, operation or baseline is reset.
		if err := m.saveAdmission(); err != nil {
			return false, err
		}
	}
	event("maximum_speed_enabled", map[string]any{"decision_sha256": sum, "plan_id": m.planData.PlanID, "original_write_ok_sha256": hex.EncodeToString(approvalSum[:]), "operation": m.journal.operation, "copy_only": true, "read_baseline_ms": readBaseline, "read_baseline_source": "owner 15:20 pre-resize 30-minute cluster evidence"})
	return true, m.poll(ctx, origins)
}

func maximumSpeedClusterReadBaseline(cluster string, origins []string) (float64, error) {
	switch strings.ToLower(cluster) {
	case "pool-slow":
		return 22, nil
	case "pool-f", "pool-a", "pool-b", "pool-c", "pool-d", "pool-e":
		return 4, nil // Explicit upper bound of the owner's pre-resize1–4ms evidence.
	}
	// Synthetic local fixtures have no production hosts or traffic.
	local := true
	for _, host := range origins {
		local = local && loopbackOrigin(host)
	}
	if local {
		return 4, nil
	}
	return 0, errors.New("maximum-speed cluster lacks an explicit pre-resize read baseline")
}

// Independent bounded pools keep every source moving even when another host's
// safety pause parks its own workers. Destination admission remains shared.
func (m *mover) runCopyWorkers(ctx context.Context, report func(error)) error {
	sources := []string{""}
	if m.tuner.maxSpeed != nil {
		unique := map[string]bool{}
		for _, r := range m.planData.Streams {
			unique[r.sourceURL] = true
		}
		sources = nil
		for source := range unique {
			sources = append(sources, source)
		}
		sort.Strings(sources)
	}
	total := min(64, m.streams*len(m.tuner.hosts))
	if len(sources) == 0 {
		return nil
	}
	// A very large topology retains the shared pool; this campaign has four
	// source hosts and therefore sixteen independent workers per source.
	if len(sources) > total {
		sources = []string{""}
	}
	var producers, workers sync.WaitGroup
	var first error
	var once sync.Once
	for i, source := range sources {
		count := total / len(sources)
		if i < total%len(sources) {
			count++
		}
		tasks := make(chan task, 2*count)
		for range count {
			workers.Add(1)
			go func() {
				defer workers.Done()
				for {
					select {
					case <-ctx.Done():
						return
					case task, ok := <-tasks:
						if !ok {
							return
						}
						var err error
						if m.tuner.maxSpeed != nil {
							err = m.executeMaximumSpeed(ctx, task)
						} else {
							err = m.execute(ctx, task)
						}
						if err != nil {
							report(err)
							return
						}
					}
				}
			}()
		}
		producers.Add(1)
		go func(source string) {
			defer producers.Done()
			defer close(tasks)
			if err := m.produceFiltered(ctx, tasks, source, false); err != nil {
				once.Do(func() { first = err })
				report(err)
			}
		}(source)
	}
	producers.Wait()
	workers.Wait()
	return first
}

// Temporary transport failures retry the same task and journal. Integrity
// failures bisect the task to isolate and durably record the offending name;
// siblings continue without accepting a bad proof as a verified copy.
func (m *mover) executeMaximumSpeed(ctx context.Context, t task) error {
	for {
		if t.NativeParent != "" {
			m.stateMu.Lock()
			parent, ok := m.journal.tasks[t.NativeParent]
			if ok && parent.Native != nil && parent.Native.Pending != nil {
				pending := parent.Native.Pending
				if taskKey(*pending) == taskKey(t) {
					t = *pending
				}
			}
			m.stateMu.Unlock()
		}
		err := m.execute(ctx, t)
		if err == nil || ctx.Err() != nil {
			return err
		}
		retry, integrity := maximumSpeedErrorKind(err)
		if retry {
			event("maximum_speed_retry", map[string]any{"source": t.Source, "target": t.Target, "stream": t.Stream, "start": t.Start, "end": t.End, "error": err.Error()})
			if err := sleepContext(ctx, min(m.tick, 5*time.Second)); err != nil {
				return err
			}
			continue
		}
		if !integrity {
			// Unexpected local failures (including naked journal I/O errors)
			// cannot be turned into data-loss evidence or retried indefinitely.
			return err
		}
		if len(t.Names) > 1 {
			middle := len(t.Names) / 2
			left, right := t, t
			left.End = t.Start + int64(middle)
			left.Names = t.Names[:middle]
			right.Start = left.End
			right.Names = t.Names[middle:]
			if len(t.NativeOrdinals) > 0 {
				left.NativeOrdinals = t.NativeOrdinals[:middle]
				right.NativeOrdinals = t.NativeOrdinals[middle:]
			}

			left.Phase, right.Phase = "", ""
			left.Proofs, right.Proofs = nil, nil
			if err := m.executeMaximumSpeed(ctx, left); err != nil {
				return err
			}
			return m.executeMaximumSpeed(ctx, right)
		}
		if len(t.Names) != 1 {
			return fmt.Errorf("cannot isolate integrity failure in unnamed task: %w", err)
		}
		record := integritySkip{Source: t.Source, Target: t.Target, Stream: t.Stream, Start: t.Start, End: t.End, Name: t.Names[0], Reason: err.Error(), At: time.Now().UTC().Format(time.RFC3339Nano)}
		m.stateMu.Lock()
		saveErr := m.journal.appendRow(journalRow{Operation: m.journal.operation, Plan: m.journal.plan, SkippedIntegrity: &record})
		if saveErr == nil {
			t.Phase = "done"
			t.Names = nil
			t.Proofs = nil
			t.Bytes = 0
			saveErr = m.journal.save(t)
		}
		m.stateMu.Unlock()
		if saveErr != nil {
			return saveErr
		}
		event("skipped_integrity", map[string]any{"record": record, "copy_verified": false})
		return nil
	}
}

func maximumSpeedErrorKind(err error) (retry, integrity bool) {
	var badData *integrityError
	if errors.As(err, &badData) {
		return false, true
	}
	var filesystem *os.PathError
	if errors.As(err, &filesystem) {
		return false, false
	}
	// syscall.Errno implements net.Error too. A naked journal EIO/ENOSPC must
	// remain fatal; only an actual network operation may reinterpret it as I/O.
	var errno syscall.Errno
	var operation *net.OpError
	if errors.As(err, &errno) && !errors.As(err, &operation) {
		return false, false
	}
	var api *apiError
	if errors.As(err, &api) {
		return api.Status == 401 || api.Status == 403 || api.Status == 408 || api.Status == 429 || api.Status >= 500, false
	}
	var transport net.Error
	return errors.As(err, &transport) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF), false
}
