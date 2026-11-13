package helper

import (
	"sync"
	"sync/atomic"
)

const metricQueueShards = 64

// MetricQueue retains each distinct name until its insertion is acknowledged.
// It has no capacity limit: memory follows the number and length of pending names.
// A notification channel wakes the consumer; it never carries metric names.
// Take and Ack have one serialized consumer, while Add supports many producers.
type MetricQueue struct {
	shards [metricQueueShards]metricQueueShard
	size   atomic.Int64
	wake   chan struct{}
	next   int
}

type metricQueueShard struct {
	sync.Mutex
	names   map[string]struct{}
	waiting []string
}

func NewMetricQueue() *MetricQueue { return &MetricQueue{wake: make(chan struct{}, 1)} }

func (q *MetricQueue) Add(name string) {
	sh := &q.shards[HashString(name)%metricQueueShards]
	sh.Lock()
	if _, exists := sh.names[name]; exists {
		sh.Unlock()
		return
	}
	if sh.names == nil {
		sh.names = make(map[string]struct{})
	}
	sh.names[name] = struct{}{}
	sh.waiting = append(sh.waiting, name)
	q.size.Add(1)
	notify := len(sh.waiting) == 1
	sh.Unlock()
	if notify {
		select {
		case q.wake <- struct{}{}:
		default:
		}
	}
}

func (q *MetricQueue) Len() int {
	if q == nil {
		return 0
	}
	return int(q.size.Load())
}
func (q *MetricQueue) Ready() <-chan struct{} {
	if q == nil {
		return nil
	}
	return q.wake
}

// Take removes at most limit names from the waiting lists. Names remain deduplicated
// until Ack, including while they are being inserted into the index.
func (q *MetricQueue) Take(limit int) []string {
	if q == nil || limit <= 0 {
		return nil
	}
	batch := make([]string, 0, min(limit, q.Len()))
	for range metricQueueShards {
		sh := &q.shards[q.next]
		q.next = (q.next + 1) % metricQueueShards
		sh.Lock()
		n := min(limit-len(batch), len(sh.waiting))
		batch = append(batch, sh.waiting[:n]...)
		clear(sh.waiting[:n])
		sh.waiting = sh.waiting[n:]
		if len(sh.waiting) == 0 {
			sh.waiting = nil
		}
		sh.Unlock()
		if len(batch) == limit {
			break
		}
	}
	return batch
}

func (q *MetricQueue) Ack(batch []string) {
	var locked *metricQueueShard
	for _, name := range batch {
		sh := &q.shards[HashString(name)%metricQueueShards]
		if sh != locked {
			if locked != nil {
				if len(locked.names) == 0 {
					locked.names = nil
				}
				locked.Unlock()
			}
			sh.Lock()
			locked = sh
		}
		delete(sh.names, name)
	}
	if locked != nil {
		if len(locked.names) == 0 {
			locked.names = nil
		}
		locked.Unlock()
	}
	q.size.Add(-int64(len(batch)))
}
