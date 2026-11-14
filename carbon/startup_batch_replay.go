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
	raw = make([]byte, 0, limit)
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
			// Pricing and lookup were fused once per metric. Admission is per group
			// so a large window never requires more headroom than a metric update.
			cost := g.metric.OverheadBytes()
			if sink.groupedAdd == nil {
				cost = sink.admission.RecoveryMetricBytes([]byte(g.name))
			}
			for !reserved && !sink.admission.HasRecoveryCapacityBatch(len(g.data), cost) {
				if err := s.waitCapture(); err != nil {
					return err
				}
				time.Sleep(time.Millisecond)
			}
			if err := s.waitCapture(); err != nil {
				return err
			}
			groups.applyGroup(i, source, sink, false)
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
			if frames > 0 && (len(raw)+len(frame) > limit || frames >= 32) {
				if err := flush(); err != nil {
					return err
				}
			}
			raw = append(raw, frame...)
			count += n
			frames++
			end = offset
			if len(raw) >= limit || frames >= 32 {
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
