package carbon

import (
	"io"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/recovery"
)

// Existing tiny frames usually contain one point per metric. A bounded window
// makes historical replay benefit from repeated metrics without crossing a
// segment/epoch. The decode pipeline keeps its two-buffer ownership contract.
// Only completed windows publish a physical cursor; partial application retains
// its source and is replayed safely after interruption.
func (s *startupInput) replayCaptureBatches(reader io.Reader, source *cache.RecoverySource, sink *startupSink, advance func(int64, int)) (int, error) {
	var raw []byte
	var groups captureBatch
	count, frames := 0, 0
	var end int64
	applied := 0
	limit := min(8<<20, max(64<<10, s.policy.readBuffer*2))
	maxFrames := 32
	if policy, ok := sink.admission.(interface{ RecoveryBatchingEnabled() bool }); ok && !policy.RecoveryBatchingEnabled() {
		maxFrames = 1
	}

	flush := func() error {
		if count == 0 {
			return nil
		}
		if err := groups.prepare(raw, sink); err != nil {
			return err
		}
		if err := groups.checkCount(count); err != nil {
			return err
		}
		reserved := sink.admission.HasRecoveryCapacityBatch(count, groups.cost)
		for i := range groups.used {
			g := &groups.groups[i]
			if reserved {
				if err := s.waitCapture(); err != nil {
					return err
				}
				groups.applyGroup(i, source, sink, false)
				continue
			}
			// A grouped record can exceed a hard count/byte ceiling even though each
			// original frame fit. Split only when admission refuses, retaining FIFO.
			cost := g.metric.OverheadBytes()
			if sink.groupedAdd == nil {
				cost = sink.admission.RecoveryMetricBytes([]byte(g.name))
			}
			for start := 0; start < len(g.data); {
				count := len(g.data) - start
				for !sink.admission.HasRecoveryCapacityBatch(count, cost) {
					if err := s.waitCapture(); err != nil {
						return err
					}
					if count > 1 {
						count = max(1, count/2)
						continue
					}
					time.Sleep(time.Millisecond)
				}
				if err := s.waitCapture(); err != nil {
					return err
				}
				groups.applyRange(i, start, start+count, source, sink, false)
				start += count
			}
		}
		advance(end, count)
		applied += count
		raw = raw[:0]
		count, frames = 0, 0
		return nil
	}
	_, err := recovery.ReadCaptureFramesPipeline(reader, s.policy.readBuffer,
		func(_ []byte, _ int, stop <-chan struct{}) (int64, error) { return 0, s.waitCaptureUntil(stop) },
		func(frame []byte, n int, offset, _ int64) error {
			if frames > 0 && (len(raw)+len(frame) > limit || frames >= maxFrames) {
				if err := flush(); err != nil {
					return err
				}
			}
			if raw == nil {
				capacity := max(limit, len(frame))
				if maxFrames == 1 {
					capacity = len(frame)
				}
				raw = make([]byte, 0, capacity)
			}
			raw = append(raw, frame...)
			count += n
			frames++
			end = offset
			if len(raw) >= limit || frames >= maxFrames {
				return flush()
			}
			return nil
		})
	// On torn/corrupt tails, retain the same complete-prefix behavior as the
	// frame reader. Cancellation must not apply anything further.
	if stopErr := s.waitCapture(); stopErr != nil {
		return applied, stopErr
	}
	if flushErr := flush(); flushErr != nil {
		return applied, flushErr
	}
	return applied, err
}
