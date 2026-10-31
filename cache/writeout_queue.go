package cache

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/internal/phasecpu"
)

type WriteoutQueue struct {
	sync.RWMutex
	cache *Cache

	// Writeout queue. Usage:
	// q := <- queue
	// p := cache.Pop(q.Metric)
	queue     chan string
	refreshAt time.Time
	rebuild   func(abort chan bool) chan bool // return chan waiting for complete
	wake      chan struct{}
	// Nonnegative nanoseconds relative to epoch, plus one (zero means unset).
	wakeAt atomic.Int64
	epoch  time.Time
}

func NewWriteoutQueue(cache *Cache) *WriteoutQueue {
	q := &WriteoutQueue{
		cache: cache,
		queue: nil,
		wake:  make(chan struct{}, 1),
		epoch: time.Now(),
	}
	q.rebuild = q.makeRebuildCallback(time.Now(), time.Time{})
	return q
}

// notifyAt coalesces arrivals into one shared wake-up. Sparse arrivals only
// advance the timer; they do not cause repeated full-cache scans.
func (q *WriteoutQueue) notifyAt(deadline time.Time) {
	// Paused startup deadlines saturate Sub at MaxInt64. Reserve the extra
	// nanosecond before adding one; a wrapped hint suppresses future wake-ups.
	next := max(0, min(deadline.Sub(q.epoch).Nanoseconds(), int64(1<<63-2))) + 1
	for {
		old := q.wakeAt.Load()
		if old != 0 && old <= next {
			return
		}
		if q.wakeAt.CompareAndSwap(old, next) {
			select {
			case q.wake <- struct{}{}:
			default:
			}
			return
		}
	}
}

func (q *WriteoutQueue) makeRebuildCallback(nextRebuildTime, notBefore time.Time) func(chan bool) chan bool {
	var nextRebuildOnce sync.Once
	nextRebuildComplete := make(chan bool)

	nextRebuild := func(abort chan bool) chan bool {
		// next rebuild
		nextRebuildOnce.Do(func() {
			go func() {
				q.waitForRebuild(nextRebuildTime, notBefore, abort)
				if !q.updateAdmitted(abort) {
					// A stopped consumer must not leave a completed callback
					// installed: the same cache can serve a new persister.
					now := time.Now()
					q.Lock()
					q.refreshAt = now
					q.rebuild = q.makeRebuildCallback(now, now)
					q.Unlock()
				}
				close(nextRebuildComplete)
			}()
		})

		return nextRebuildComplete
	}

	return nextRebuild
}

func (q *WriteoutQueue) waitForRebuild(next, notBefore time.Time, abort chan bool) {
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		deadline := next
		if hint := q.wakeAt.Load(); hint != 0 {
			wake := q.epoch.Add(time.Duration(hint - 1))
			if deadline.IsZero() || wake.Before(deadline) {
				deadline = wake
			}
		}
		var tick <-chan time.Time
		if !deadline.IsZero() {
			if deadline.Before(notBefore) {
				deadline = notBefore
			}
			delay := time.Until(deadline)
			if delay <= 0 {
				return
			}
			if timer == nil {
				timer = time.NewTimer(delay)
			} else {
				timer.Reset(delay)
			}
			tick = timer.C
		}
		select {
		case <-tick:
			return
		case <-abort:
			return
		case <-q.wake:
			if timer != nil {
				timer.Stop()
			}
		}
	}
}

func (q *WriteoutQueue) update() {
	phasecpu.Set(phasecpu.WriteoutQueue)
	defer phasecpu.Set(phasecpu.Other)
	// Clear hints before scanning so concurrent arrivals cannot lose a wake-up.
	q.wakeAt.Store(0)
	queue, next, hasCold := q.cache.makeQueuePlan(time.Now())
	notBefore := time.Now().Add(100 * time.Millisecond)
	if queue != nil || !q.cache.settings.Load().(*cacheSettings).batching() {
		next = notBefore
	}

	q.Lock()
	q.queue = queue
	q.refreshAt = time.Time{}
	if hasCold && queue != nil {
		q.refreshAt = time.Now().Add(time.Second)
	}
	q.cache.queueChannelBytes.Store(int64(cap(queue)) * 16)
	q.rebuild = q.makeRebuildCallback(next, notBefore)
	q.Unlock()
}

func (q *WriteoutQueue) get(abort chan bool) string {
QueueLoop:
	for {
		select {
		case <-abort:
			return ""
		default:
		}
		q.RLock()
		queue := q.queue
		rebuild := q.rebuild
		refreshAt := q.refreshAt
		q.RUnlock()
		if !refreshAt.IsZero() && !time.Now().Before(refreshAt) {
			select {
			case <-rebuild(abort):
				continue QueueLoop
			case <-abort:
				return ""
			}
		}

		select {
		case metric := <-queue:
			if !q.cache.WriteoutReady(metric) {
				continue QueueLoop
			}
			// pop from cache
			return metric
		case <-abort:
			return ""
		default:
			// queue is empty, create new
			select {
			case <-rebuild(abort):
				// wait for rebuild
				continue QueueLoop
			case <-abort:
				return ""
			}
		}
	}
}

func (q *WriteoutQueue) Get(abort chan bool) string { // skipcq: RVV-B0001
	return q.get(abort)
}

// TryGet takes an already queued, ready metric without waiting for a rebuild.
// Persister workers use it to fill a commit from work that is available now.
func (q *WriteoutQueue) TryGet() string {
	q.RLock()
	queue := q.queue
	refreshAt := q.refreshAt
	q.RUnlock()
	if !refreshAt.IsZero() && !time.Now().Before(refreshAt) {
		return ""
	}
	for {
		select {
		case metric := <-queue:
			if q.cache.WriteoutReady(metric) {
				return metric
			}
		default:
			return ""
		}
	}
}

// A long cold snapshot must not hide newly warm work until it empties. Refresh
// it once a second during warm-first startup. Unconsumed names still belong to
// the cache and are selected again; the consumer reserves names across rebuilds.
func (q *WriteoutQueue) updateAdmitted(abort chan bool) bool {
	select {
	case <-abort:
		return false
	default:
	}
	active, begin, end := q.cache.restartScheduling()
	if active && begin != nil {
		if !begin(abort) {
			return false
		}
		if end != nil {
			defer end()
		}
	}
	select {
	case <-abort:
		return false
	default:
	}
	q.update()
	return true
}
