package carbon

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/go-graphite/go-carbon/internal/inputlog"
)

// A durable byte cursor skips application only: decoding the prefix reconstructs
// dictionary and delta/XOR state before the unprotected suffix is replayed.
func (s *startupInput) replayInputSegment(seg *startupSegment, sink *startupSink, from int64, advance func(int64, int)) (int, error) {
	replay, err := inputlog.NewSegmentReplay(seg.file)
	if err != nil {
		return 0, err
	}
	defer replay.Close()
	var batch captureBatch
	total := 0
	previous := int64(64)
	for {
		if err := s.waitCapture(); err != nil {
			return total, err
		}
		records, end, n, err := replay.Next(seg.size.Load())
		if errors.Is(err, io.EOF) {
			return total, nil
		}
		if err != nil {
			return total, err
		}
		if from != 0 && from > previous && from < end {
			return total, fmt.Errorf("startup cursor %d is not a complete input log frame", from)
		}
		previous = end
		if end <= from {
			continue
		}
		batch.prepareRecords(records, sink)
		// Journal replay already groups points per metric. The same resolver/pricer
		// used by capture batching supplies one identity to the ordinary cache insert.
		for i := range batch.used {
			g := &batch.groups[i]
			cost := g.metric.OverheadBytes()
			if sink.groupedAdd == nil {
				cost = sink.admission.RecoveryMetricBytes([]byte(g.name))
			}
			for start := 0; start < len(g.data); {
				count := len(g.data) - start
				for !sink.admission.HasRecoveryCapacityBatch(count, cost) {
					if err := s.waitCapture(); err != nil {
						return total, err
					}
					if count > 1 {
						count = max(1, count/2)
					} else {
						time.Sleep(time.Millisecond)
					}
				}
				if err := s.waitCapture(); err != nil {
					return total, err
				}
				batch.applyRange(i, start, start+count, seg.source, sink, false)
				start += count
			}
		}
		advance(end, n)
		total += n
	}
}
