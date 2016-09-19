package cache

import (
	"github.com/lomik/go-carbon/cache/cmap"
	"github.com/lomik/go-carbon/points"
	"sort"
	"sync/atomic"
	"time"
	//"github.com/Sirupsen/logrus"
)

type WriteoutQueue struct {
	queuedPoints         []*points.Points
	cacheStats           *cacheStats
	cachedPoints         *cmap.ConcurrentMap
	writeoutCompleteChan chan struct{}
	activeWorkers        int32
	writeStrategy        WriteStrategy
	writeoutStart        time.Time
}

type byLength []*points.Points
type byTimestamp []*points.Points

func (q byLength) Len() int           { return len(q) }
func (q byLength) Swap(i, j int)      { q[i], q[j] = q[j], q[i] }
func (q byLength) Less(i, j int) bool { return len(q[i].Data) < len(q[j].Data) }

func (q byTimestamp) Len() int           { return len(q) }
func (q byTimestamp) Swap(i, j int)      { q[i], q[j] = q[j], q[i] }
func (q byTimestamp) Less(i, j int) bool { return q[i].Data[0].Timestamp > q[j].Data[0].Timestamp }

type WriteStrategy int

const (
	MaximumLength WriteStrategy = iota
	TimestampOrder
	Noop
)

func NewQueue(cachedPoints *cmap.ConcurrentMap, cacheStats *cacheStats) WriteoutQueue {
	return WriteoutQueue{
		queuedPoints:         make([]*points.Points, 0),
		cacheStats:           cacheStats,
		activeWorkers:        0,
		cachedPoints:         cachedPoints,
		writeStrategy:        MaximumLength,
		writeoutCompleteChan: make(chan struct{}),
		writeoutStart:        time.Time{},
	}
}

// builds queue according to configured writeStrategy
func (q *WriteoutQueue) Update() bool {

	// can occasionally be called when we actually can't update
	if atomic.LoadInt32(&q.activeWorkers) != 0 {
		return false
	}

	start := time.Now()

	newQueue := q.queuedPoints[:0]
	q.cachedPoints.IterCb(func(_ string, v *points.Points) { newQueue = append(newQueue, v) })
	q.queuedPoints = newQueue

	atomic.AddUint32(&q.cacheStats.queueBuildTimeMs, uint32(time.Now().Sub(start)/time.Millisecond))
	atomic.AddUint32(&q.cacheStats.queueBuildCnt, 1)

	q.writeoutStart = time.Now()

	return true
}

const MIN_POINTS_PER_BATCH = 512 // at least 512 points per batch

func (q *WriteoutQueue) Chop(n int) (res [][]*points.Points) {
	numPoints := len(q.queuedPoints)
	if numPoints == 0 {
		return nil
	}

	step := numPoints / n
	if step < MIN_POINTS_PER_BATCH {
		step = MIN_POINTS_PER_BATCH
	}
	res = make([][]*points.Points, (numPoints+step-1)/step)

	var i, j = 0, 0
	for ; j < numPoints-step; i, j = i+1, j+step {
		res[i] = q.queuedPoints[j : j+step]
	}
	res[i] = q.queuedPoints[j:]
	return
}

func (q *WriteoutQueue) process_complete(metricsCount, pointsCount int) {
	activeWorkersLeft := atomic.AddInt32(&q.activeWorkers, -1)

	q.update_cache_stats(metricsCount, pointsCount)

	if activeWorkersLeft == 0 { // we were last one, kick off queue update
		atomic.AddUint32(&q.cacheStats.queueWriteoutTimeMs, uint32(time.Now().Sub(q.writeoutStart)/time.Millisecond))
		q.writeoutCompleteChan <- struct{}{}
	}
}

func (q *WriteoutQueue) update_cache_stats(_, pointsCount int) {
	atomic.AddInt32(&q.cacheStats.sizeShared, -int32(pointsCount))
}

// deletes elements from map if it is empty
func deleteIfEmptyCb(exists bool, valueInMap *points.Points, _ *points.Points) (*points.Points, bool) {
	// Shard lock always held by Upsert() which calls this function as a callback
	// valueInMap.Lock can never be held by anybody at this point of time,
	// because potential placed it could be held are:
	//
	//   - upsert callbacks in Cache.Add/Cache.AddSingle - for upsert to be called Shard lock needs to be
	//       aquired and due to us being here, it means that we already have it and Cache.Add* funcs are
	//       waiting for it. So in worst case point will be added back to map once they get it
	//
	//   - p.Shift() call in WriteoutQueue.Process - because there is a single writeout queue only
	//       and each point in a queue is referenced at most once, it means that no concurrent
	//       persister processes can see same point, thefore this callback is called by
	//       only persister which this point belongs to, therefore no need to aquire p.Lock() to
	//       read p.Data

	if !exists {
		//somebody already removed element (can't happen really, see invariants above)
		return nil, true
	}
	if len(valueInMap.Data) == 0 {
		// everything goes as planned, nobody managed to add datapoints,removing from map
		return nil, true
	} else {
		// Somebody raced and added datapoints, locking value in map and returning it locked,as if
		// nothing happened and we didn't try to delete it...

		// It is a least likely outcome, sequence of locks must be:

		// Persister.Process(): p.Lock()
		// Cache.Add(): shard.Lock()
		// Cache.Add(): p.Lock() -> wait...[1]
		// Persister.Process(): if len(p.Data) == 0 { p.Unlock(); cachedPoints.Upsert(..., deleteIfEmptyCb) // <- us }
		// Persister.Process(): shard.Lock() -> wait...[2]
		// Cache.Add(): p.Lock() -> [1]...success // <- datapoints added here
		// Cache.Add(): p.Unlock(); shard.Unlock()
		// Persister.Process(): shard.Lock() -> [2]...sucess // deleteIfEmptyCb (us!) is called here
		// deleteIfEmptyCb(): if len(p.Data) != 0 { p.Lock() -> success }

		valueInMap.Lock()
		return valueInMap, false
	}
}

// Processes `count` elements from queue to start processing
// it ensured all the correct locking, so that PersistPointFunc doesn't
// need any of it
//
// returns how many metrcs were processed
func (q *WriteoutQueue) Process(batch []*points.Points, fn points.PersistPointFunc) (metricsCount int) {
	metricsCount = len(batch)
	pointsCount := 0

	// sort points in batches according to a configured strategy
	// safe to take all locks as each point can be only in 1 batch
	// TODO: measure times to do sorting as it directly affects CarbonLink response times

	switch q.writeStrategy {
	case MaximumLength:
		for _, p := range batch {
			p.Lock()
		}
		sort.Sort(byLength(batch))
		for _, p := range batch {
			p.Unlock()
		}
	case TimestampOrder:
		for _, p := range batch {
			p.Lock()
		}
		sort.Sort(byTimestamp(batch))
		for _, p := range batch {
			p.Unlock()
		}
	case Noop:
	}

	//h := make(map[int]int,16)

BATCH_LOOP:
	for j := 0; j < metricsCount; j++ {
		p := batch[j]
		p.Lock()
		numPoints := len(p.Data)
		//h[numPoints] = h[numPoints]+1

		// Test whether metric is empty, which means it wasn't updated since last updateQueue
		// so we delete it from cache. More sophisticated retention strategy might be developed
		// in the future.

		if numPoints == 0 {
			// OK, we are going to try delete datapoint, but it's not that simple:
			// we are holding p.Lock(), but to delete a datapoint we'd need to
			// get shard.Lock() (inside ConcurrentMap), which might already
			// be held by one of receiver and if we try to aquire it naively
			// we'd get a deadlock, if that receiver is processing same metric
			// name as the one we are trying to delete
			//
			// Multiple locks MUST be taken in same order (single locks can be
			// taken in any order, because there is no order :) ). Normal order
			// which receivers do is 'shard.Lock()' then 'p.Lock()' therefore we
			// must do the same (or to be precise MUST NOT do the opposite), therefore
			// we need to release p.Unlock() and aquire shared.Lock() to be able to
			// delete metric.
			//
			// Tricky part is that the moment we release p.Unlock(), we can't trust our
			// numPoints value anymore, because some receiver can race in front of us
			// and add datapoints to p BEFORE we manage to get shard.Lock() and p.Lock(),
			// therefore if we try to delete `p` with freshly adde datapoints we loose them
			// forever.
			//
			// We solve it in following way: we use cache.Upsert() which takes shard.Lock()
			// and then calls `deletIfEmptyCb` while shard.Lock() is held. Callback then have
			// 2 invariants:
			//   - len(p.Data) is still 0, that means nobody raced ahead of us, therefore we
			//     can proceed to deleting a metric
			//   - len(p.Data) != 0, that means some receiver managed to sneak in few dapoints.
			//     callback then leaves it in the map and returns `p` locked.
			//
			//  If we see that value was deleted from map, we move to next BATCH_LOOP iteration
			//  if value wasn't deleted, we set `numPoints` to number of datapoints metric has
			//  and  continue processing this datapoint as if `numPoints` above was not 0 and
			//  we never attempted to delete it.

			p.Unlock() // unlocking first

			// aquiring shard.Lock() and possibly deleting
			p, del := q.cachedPoints.Upsert(p.Metric, nil, deleteIfEmptyCb)

			if del { // delete was sucessfully, we are done, move on to next iteration
				continue BATCH_LOOP
			} else {
				// somebody raced between p.Unlock() and cachedPoints.Upsert() above
				// and added datapoints, update numPoints and continue as if we
				// didn't try to delete it at all.
				// It is safe to call len(p.Data) here because p was returned in a locked
				// state from Upsert() call above.
				numPoints = len(p.Data)
			}
		}

		// pass `p` in locked state
		if err := fn(p); err == nil {
			// fn() unlocks `p` upon return
			// no errors when commiting points, removing written
			// datapoints and shifting tail to beginning
			// NOTE: this is the only place where `p` is modified without shard.Lock held
			p.Shift(numPoints)
			pointsCount += numPoints
		}
	}

	//logrus.Infof("histogram: %v", h)

	q.process_complete(metricsCount, pointsCount)

	return metricsCount
}
