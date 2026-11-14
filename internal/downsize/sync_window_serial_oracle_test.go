package downsize

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

// Frozen pre-parallel CopyWindow from 1c5cf3ebf. Keep this serial oracle
// independent of the worker scheduler when comparing stored point bits.
func (b *syncLiveBackend) serialCopyWindowOracle(ctx context.Context, source, target string, names []string, journalPath string, windows syncRanges) (syncCopyResult, error) {
	if b.o.noCopy {
		return syncCopyResult{Failed: append([]string(nil), names...)}, nil
	}
	for _, host := range []string{source, target} {
		var cap struct{ NativeTimeWindow, NativeExactCombine, NativeDeferredCombine bool }
		if _, err := b.m.requestOnce(ctx, host, "/v1/native/capabilities", nil, &cap); err != nil {
			var api *apiError
			if errors.As(err, &api) && api.Status == http.StatusForbidden {
				return syncCopyResult{Forbidden: true}, nil
			}
			return syncCopyResult{}, err
		}
		if !cap.NativeTimeWindow || host == target && (!cap.NativeExactCombine || !cap.NativeDeferredCombine) {
			return syncCopyResult{}, fmt.Errorf("window-only sync requires native window combine on %s", host)
		}
	}
	failed := map[string]bool{}
	for wi, w := range windows {
		beforeStarted := time.Now()
		before, beforeErr := b.Digest(ctx, target, names, w.From, w.Until)
		event("sync_window_digest", map[string]any{"stage": "before", "source": source, "names": len(names), "seconds": time.Since(beforeStarted).Seconds()})
		data, _ := json.Marshal([]any{source, target, names, w.From, w.Until})
		hash := sha256.Sum256(data)
		id := hex.EncodeToString(hash[:])
		pointPath := fmt.Sprintf("%s.window-%d-%s.window-points.json", journalPath, wi, id[:16])
		measurement := windowPointCount{Source: source, Window: w}
		if beforeErr == nil {
			n := countWindowPoints(before)
			measurement.Before = &n
		}
		measurement, err := readWindowPointCount(pointPath, measurement)
		if err != nil {
			return syncCopyResult{}, err
		}
		j, err := openJournal(fmt.Sprintf("%s.window-%d-%s", journalPath, wi, id[:16]), id)
		if err != nil {
			return syncCopyResult{}, err
		}
		m := &mover{options: options{tokenEnv: b.o.tokenEnv, transferIdle: b.o.copyIdle, bulkCombine: true, nativeWindow: &chunkstore.NativeWindow{From: w.From, Until: w.Until}}, client: b.client, journal: j, planData: &manifest{PlanID: id}}
		if m.transferIdle <= 0 {
			m.transferIdle = defaultTransferIdle
		}
		c, release, err := b.windowController(ctx, m, source)
		if err != nil {
			j.close()
			return syncCopyResult{}, err
		}
		err = func() error {
			defer func() { release() }()
			defer j.close()
			for start := 0; start < len(names); start += int(nativeRangeNames) {
				end := min(start+int(nativeRangeNames), len(names))
				t := task{Stream: id, Source: source, Target: target, Start: int64(start), End: int64(end)}
				if saved, ok := j.tasks[taskKey(t)]; ok {
					t = saved
				}
				if t.Native == nil || !t.Native.complete() {
					if err := c.executeRange(ctx, t, names[start:end]); err != nil {
						// Split only this bounded rejected range, not the entire
						// (potentially multi-million-name) fill or other windows.
						path := fmt.Sprintf("%s.window-%d-%s.range-%d-%d", journalPath, wi, id[:16], start, end)
						// Recursive copies must not borrow another shared pin while
						// this parent holds its read lock: pin rotation takes
						// the write lock and would deadlock both borrowers.
						release()
						release = func() {}
						res, splitErr := b.splitRejectedWindow(ctx, source, target, names[start:end], path, syncRanges{w}, err)
						if splitErr != nil {
							return splitErr
						}
						if res.Forbidden {
							return errors.New("window split source or target became forbidden")
						}
						for _, n := range res.Failed {
							failed[n] = true
						}
						c, release, err = b.windowController(ctx, m, source)
						if err != nil {
							release = func() {}
							return err
						}
						continue
					}
					t = j.tasks[taskKey(t)]
					if err := b.o.rate.wait(ctx, t.Bytes); err != nil {
						return err
					}
				}
				// Native exports deliberately omit incomplete trailing coarse
				// buckets. Never count such an in-window omission as success.
				for _, skip := range t.Native.Receipt.WindowSkipped {
					if skip.Timestamp >= w.From && skip.Timestamp < w.Until && skip.Ordinal >= 0 && skip.Ordinal < end-start {
						failed[names[start+skip.Ordinal]] = true
						event("sync_window_boundary_skip", map[string]any{"level": "warn", "metric": names[start+skip.Ordinal], "timestamp": skip.Timestamp, "reason": skip.Reason})
					}
				}
				for i, n := range names[start:end] {
					if t.Native == nil || !t.Native.done(i) {
						failed[n] = true
					}
				}
			}
			return nil
		}()
		if err != nil {
			return syncCopyResult{}, err
		}
		afterStarted := time.Now()
		after, afterErr := b.Digest(ctx, target, names, w.From, w.Until)
		if measurement.After == nil && afterErr == nil {
			if len(failed) > 0 {
				measurement.Before = nil
			}
			n := countWindowPoints(after)
			measurement.After = &n
			if err := atomicStateJSON(pointPath, measurement); err != nil {
				return syncCopyResult{}, err
			}
		}
		event("sync_window_digest", map[string]any{"stage": "after", "source": source, "names": len(names), "seconds": time.Since(afterStarted).Seconds()})
		count := func(d map[string][]syncDigest) int64 {
			var n int64
			for _, archives := range d {
				for _, a := range archives {
					n += a.Count
				}
			}
			return n
		}
		if beforeErr == nil && afterErr == nil {
			event("sync_window_points", map[string]any{"scope": "serving_archive", "source": source, "names": len(names), "window": w.Name, "before": count(before), "after": count(after), "added": count(after) - count(before)})
		}
	}
	return syncCopyResult{Copied: len(names) - len(failed), Failed: sortedKeys(failed)}, nil
}
