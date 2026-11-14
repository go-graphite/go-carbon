package cache

/*
Based on https://github.com/orcaman/concurrent-map
*/

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/helper"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/tags"
	"github.com/greatroar/blobloom"
	"github.com/zeebo/xxh3"
)

type WriteStrategy int

const (
	MaximumLength WriteStrategy = iota
	TimestampOrder
	Noop
	MetricNameOrder
)

const shardBits = 10
const shardCount = 1 << shardBits // 1024 - an arbitrary sized power of 2

type cacheSettings struct {
	maxSize           int64
	maxBytes          int64
	byteSlack         int64
	sizeSlack         int64 // see sizeSlackFor
	xlog              func(*points.Points) error
	tagsEnabled       bool
	writeoutScanYield bool
	writeoutMinPoints int
	writeoutMaxDelay  time.Duration
	writeoutSpread    float64
	// writeoutRestoreSpread: see SetWriteoutRestoreSpread.
	writeoutRestoreSpread bool
	// writeoutPhase anchors automatic eligibility independently of prior writes.
	writeoutPhase time.Time
	// writeoutSparseSpread: see SetWriteoutSparseSpread.
	writeoutSparseSpread bool
	// writeoutStartupSpread: see SetWriteoutStartupSpread.
	writeoutStartupSpread bool
	startupWriteout       *startupWriteout
}

// A "thread" safe map of type string:Anything.
// To avoid lock bottlenecks this map is dived to several (shardCount) map shards.
type Cache struct {
	mu                   sync.Mutex
	pending              atomic.Pointer[pendingRecovery]
	startupAt            atomic.Pointer[time.Time]
	recoveryAdmission    atomic.Pointer[recoveryAdmission]
	recoveryBlockedUntil atomic.Int64
	recoveryRejections   atomic.Uint64
	recoveryDrain        atomic.Bool

	writeoutEpoch   time.Time
	writeoutStateMu sync.Mutex
	queueLastBuild  time.Time
	// queueScratch keeps the last rebuild's sort buffer; see takeQueue.
	queueScratch      atomic.Pointer[queue]
	queueScratchBytes atomic.Int64
	queueChannelBytes atomic.Int64

	data []*Shard

	writeStrategy WriteStrategy
	writeoutQueue *WriteoutQueue
	logPressure   chan struct{} // bounded wake for intake waiting on journal space

	// Keep read-mostly settings off the cache lines written by counters and
	// queue bookkeeping, including machines with 128-byte cache lines.
	_        [128]byte
	settings atomic.Value // cacheSettings
	_        [128]byte

	stat struct {
		size                int64 // published part of the size, changing via atomic; see Shard.sizeDelta
		bytes               atomic.Int64
		queueBuildCnt       uint32 // number of times writeout queue was built
		queueBuildTimeMs    uint32 // time spent building writeout queue in milliseconds
		queueWriteoutTime   uint32 // in milliseconds
		overflowCnt         uint32 // drop packages if cache full
		queryCnt            uint32 // number of queries
		tagsNormalizeErrors uint32 // tags normalize errors count
		receivedPoints      atomic.Uint64
		recoveredPoints     atomic.Uint64
		xlogDivertedPoints  atomic.Uint64
		invalidTagPoints    atomic.Uint64
		xlogFailedPoints    atomic.Uint64
		nameTooLong         atomic.Uint64 // points refused because the name exceeds points.MaxMetricNameBytes

		droppedRealtimeIndex uint32 // new metrics failed to be indexed in realtime
	}

	newMetricsChan   chan string
	newMetricsQueue  *helper.MetricQueue
	newMetricCf      atomic.Pointer[indexFilter]
	filterGeneration atomic.Uint64
	metricExists     func(string) bool
	restartOrder     func() bool
	restartWarm      func(string) bool
	restartBegin     func(<-chan bool) bool
	restartEnd       func()

	throttleWithHash func(ps *points.Points, inCache bool, hash uint64, state *any) bool
}

type indexFilter struct {
	*blobloom.SyncFilter
	generation uint64
	capacity   uint64
}

// A "thread" safe string to anything map.
// Accounting writers hold mu, so load/store publication needs no atomic RMW.
// Readers use atomic loads and never wait for ingestion or dump locks.
type Shard struct {
	recoveredPoints     atomic.Uint64 // writers hold mu
	receivedPoints      atomic.Uint64 // live inputs refused/diverted after shard resolution
	inFlightPoints      atomic.Int64  // writers hold mu
	throttledPoints     atomic.Uint64 // writers hold mu
	overflowPoints      atomic.Uint64 // writers hold mu
	replayPoints        uint64        // accepted independently durable input, protected by mu
	livePoints          uint64        // accepted live points, protected by mu; excludes restoration
	livePointsPublished atomic.Uint64 // receipt publication, at most 63 accepted points behind

	streamGeneration uint64
	streamSeries     []seriesSlot
	streamSeriesUsed int
	seriesMu         sync.Mutex   // Guards the weak identity index, never acquired under mu.
	mu               sync.RWMutex // Read Write mutex, guards access to internal map.
	items            itemMap
	sizePublished    int64
	writeout         writeoutSummary
	notConfirmed     []*points.Points // linear search for value/slot
	flightPeak       int              // maximum simultaneous in-flight batches since a capacity reset
	recoverySources  map[*points.Points]recoverySourceRefs
	notConfirmedUsed int                 // search value in notConfirmed[:notConfirmedUsed]
	adds             map[string]struct{} // map to maintain all the new metric names
	// Only in-flight batches need a map; queued items hold their arrival.
	firstArrival map[*points.Points]time.Time
	// Points added (or removed, if negative) but not yet published to
	// Cache.stat.size. Publishing in batches keeps ingestion from serializing
	// on one shared counter.
	sizeDelta int64
	// size is the complete shard total, published under mu with one store per
	// update. Keep it beside sizeDelta on the cache line ingestion already owns.
	size                atomic.Int64
	metrics             atomic.Int32
	notConfirmedCount   atomic.Int32
	memory              atomic.Int64
	pointBytes          atomic.Int64
	streamSeriesBytes   atomic.Int64
	byteSize, byteDelta int64
	inFlightBytes       int64
	// scratch passes AddPoint's point to the throttle under mu.
	scratch     points.Points
	scratchData [1]points.Point
	// log records accepted points for the input log, when it is enabled.
	log shardLog
	// claimData decodes saved points a claim appends, under mu.
	claimData []points.Point
	// claimEncoded is reusable encoding scratch, also protected by mu.
	claimEncoded []byte
	// bytesQuarters tracks the lower quartile of popped items' compact
	// lengths, in quarter bytes. A new item starts with that capacity:
	// popping deletes the item, so every writeout would otherwise regrow its
	// successor 8, 16, 32, ... bytes. At most a quarter of items reserve
	// unused bytes. Size counts points, not bytes.
	bytesQuarters int
	// sparseLast holds, by item slot, the latest timestamp of points written
	// from this shard, when sparse spreading is enabled (see
	// SetWriteoutSparseSpread). Metrics sharing a slot keep the newest.
	sparseLast []uint32
	// throttleScratch passes state to the hook without a per-point escaping local.
	// The owning shard lock guards it; it is cleared after every callback.
	throttleScratch any
}

// maxBytesHint bounds the capacity reserved for a new item.
const maxBytesHint = 256

// minBytesHint holds a first point with a full timestamp and value.
const minBytesHint = 16

// Called with the shard locked. A longer item raises the estimate a quarter
// byte and a shorter one lowers it three, which balances at the quartile.
func (shard *Shard) notePopped(n int) {
	switch hint := shard.bytesQuarters >> 2; {
	case n > hint && hint < maxBytesHint:
		shard.bytesQuarters++
	case n < hint:
		shard.bytesQuarters = max(shard.bytesQuarters-3, 0)
	}
}

// Called with the shard locked.
func (shard *Shard) bytesHint() int {
	return max(shard.bytesQuarters>>2, minBytesHint)
}

// Creates a new cache instance
func New() *Cache {
	c := &Cache{
		data:          make([]*Shard, shardCount),
		writeoutEpoch: time.Now(),
		writeStrategy: Noop,
		logPressure:   make(chan struct{}, 1),
	}

	for i := 0; i < shardCount; i++ {
		c.data[i] = &Shard{
			notConfirmed: make([]*points.Points, 4),
		}
	}

	settings := cacheSettings{
		maxSize:           1000000,
		writeoutScanYield: true,
		byteSlack:         byteSlackFor(0),
		sizeSlack:         sizeSlackFor(1000000),
		tagsEnabled:       false,
		xlog:              nil,
	}

	c.settings.Store(&settings)
	for _, shard := range c.data {
		shard.writeout.epoch = c.writeoutEpoch
		shard.writeout.settings = &settings
		shard.writeout.stableSettings = &settings
	}

	c.writeoutQueue = NewWriteoutQueue(c)
	var memory int64
	for _, shard := range c.data {
		shard.byteSize = shard.memoryBytes()
		shard.memory.Store(shard.byteSize)
		memory += shard.byteSize
	}
	c.stat.bytes.Store(memory)
	return c
}

func (c *Cache) InitCacheScanAdds() {
	for _, shard := range c.data {
		shard.adds = make(map[string]struct{})
	}
}

// SetWriteStrategy ...
func (c *Cache) SetWriteStrategy(s string) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch s {
	case "max":
		c.writeStrategy = MaximumLength
	case "sorted":
		c.writeStrategy = TimestampOrder
	case "noop":
		c.writeStrategy = Noop
	case "lexical":
		c.writeStrategy = MetricNameOrder
	default:
		return fmt.Errorf("Unknown write strategy '%s', should be one of: max, sorted, noop, lexical", s)
	}
	return nil
}

// SetMaxSize of cache
func (c *Cache) SetMaxSize(maxSize uint64) {
	s := c.settings.Load().(*cacheSettings)
	newSettings := *s
	newSettings.maxSize = int64(maxSize)
	newSettings.sizeSlack = sizeSlackFor(newSettings.maxSize)
	c.settings.Store(&newSettings)
	c.writeoutQueue.notifyAt(time.Now())
}

// SetBloomSize of bloom filter
func (c *Cache) SetBloomSize(bloomSize uint64) {
	if c.newMetricsQueue != nil {
		c.newMetricCf.Store(nil)
		return
	}
	if bloomSize > 0 {
		filter := blobloom.NewSyncOptimized(blobloom.Config{
			Capacity: bloomSize, // Expected number of keys.
			FPRate:   1e-4,      // Accept one false positive per 10,000 lookups.
		})
		c.newMetricCf.Store(&indexFilter{SyncFilter: filter, generation: c.filterGeneration.Add(1), capacity: bloomSize})
	}
}

func (c *Cache) SetTagsEnabled(value bool) {
	s := c.settings.Load().(*cacheSettings)
	newSettings := *s
	newSettings.tagsEnabled = value
	c.settings.Store(&newSettings)
}

func (c *Cache) SetNewMetricsChan(ch chan string) { c.newMetricsChan = ch }

// SetNewMetricsQueue enables lossless, coalesced realtime notifications. Configure before input.
func (c *Cache) SetNewMetricsQueue(q *helper.MetricQueue) {
	c.newMetricsQueue = q
	if q != nil {
		c.newMetricCf.Store(nil)
	}
}

func (c *Cache) RealtimeIndexPending() int { return c.newMetricsQueue.Len() }

// SetMetricExists avoids notifying the realtime index about metrics it already
// contains. Configure this callback before starting ingestion.
func (c *Cache) SetMetricExists(exists func(string) bool) { c.metricExists = exists }

func (*Cache) Stop() {}

// Collect cache metrics
func (c *Cache) Stat(send helper.StatCallback) {
	if c.newMetricsQueue != nil {
		send("realtimeIndexPending", float64(c.newMetricsQueue.Len()))
	}
	send("streamSeriesIndexBytes", float64(c.StreamSeriesIndexBytes()))
	s := c.settings.Load().(*cacheSettings)

	send("size", float64(c.Size()))
	c.admissionStat(send)
	send("metrics", float64(c.Len()))
	send("maxSize", float64(s.maxSize))
	send("notConfirmed", float64(c.NotConfirmedLength()))
	// report elements in bloom filter
	if filter := c.newMetricCf.Load(); filter != nil {
		cfCount := filter.Cardinality()
		if uint64(cfCount) > filter.capacity {
			// full filter report +Inf cardinality
			cfCount = float64(filter.capacity)
		}
		send("cfCount", cfCount)
	}

	helper.SendAndSubstractUint32("queries", &c.stat.queryCnt, send)
	helper.SendAndSubstractUint32("tagsNormalizeErrors", &c.stat.tagsNormalizeErrors, send)
	send("nameTooLongRejectedPoints", float64(c.stat.nameTooLong.Load()))
	helper.SendAndSubstractUint32("overflow", &c.stat.overflowCnt, send)

	helper.SendAndSubstractUint32("queueBuildCount", &c.stat.queueBuildCnt, send)
	helper.SendAndSubstractUint32("queueBuildTimeMs", &c.stat.queueBuildTimeMs, send)
	helper.SendUint32("queueWriteoutTime", &c.stat.queueWriteoutTime, send)

	helper.SendAndSubstractUint32("droppedRealtimeIndex", &c.stat.droppedRealtimeIndex, send)
}

// GetShard returns shard under given key
func (c *Cache) GetShard(key string) *Shard {
	return c.data[helper.HashString(key)&(shardCount-1)]
}

func (c *Cache) Get(key string) []points.Point {
	atomic.AddUint32(&c.stat.queryCnt, 1)

	shard := c.GetShard(key)

	var data []points.Point
	shard.mu.Lock()
	// Immutable recovery bytes remain visible until this same lock protects
	// their complete handoff into items/notConfirmed.
	var pendingErr error
	data, pendingErr = c.pendingPoints(key)
	if pendingErr != nil {
		shard.mu.Unlock()
		// A validated immutable record must decode. Failing the read is safer
		// than returning a successful response with missing saved history.
		panic(fmt.Errorf("read validated recovery source: %w", pendingErr))
	}
	for _, p := range shard.notConfirmed[:shard.notConfirmedUsed] {
		if p != nil && p.Metric == key {
			if data == nil {
				// Limit capacity: appending below must not write into an
				// in-flight batch that an earlier reader may still alias.
				data = p.Data[:len(p.Data):len(p.Data)]
			} else {
				data = append(data, p.Data...)
			}
		}
	}

	// Published words never change, and this copy owns the partial tail,
	// so queued points decode after releasing the shard lock.
	var queued compactPoints
	if values := shard.items.get(key); values != nil {
		queued = values.compact
	}
	shard.mu.Unlock()
	if queued.n == 0 {
		return data
	}
	return queued.appendDecoded(data)
}

// Has reports whether key has unwritten points. Unlike Get it takes a read
// lock, copies nothing, and does not count as a cache query.
func (c *Cache) Has(key string) bool {
	shard := c.GetShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	if shard.items.get(key) != nil {
		return true
	}
	if r := c.pending.Load(); r != nil {
		if slot, found, err := r.bundle.Find(key); err != nil || (found && !r.isClaimed(slot)) {
			return true
		}
	}
	for _, p := range shard.notConfirmed[:shard.notConfirmedUsed] {
		if p != nil && p.Metric == key {
			return true
		}
	}
	return false
}

func (c *Cache) Confirm(p *points.Points) {
	shard := c.GetShard(p.Metric)

	shard.mu.Lock()
	removeNotConfirmed(shard, p)
	delete(shard.firstArrival, p)
	shard.log.untrack(p)
	shard.releaseSource(p)
	shard.releaseDrainedTracking()
	c.syncMemoryLocked(shard)
	shard.mu.Unlock()
	if r := c.pending.Load(); r != nil {
		r.persisted(p)
	}
}

func removeNotConfirmed(shard *Shard, p *points.Points) bool {
	for i := 0; i < shard.notConfirmedUsed; i++ {
		if shard.notConfirmed[i] != p {
			continue
		}

		copy(shard.notConfirmed[i:], shard.notConfirmed[i+1:shard.notConfirmedUsed])
		shard.notConfirmedUsed--
		shard.inFlightPoints.Store(shard.inFlightPoints.Load() - int64(len(p.Data)))
		shard.inFlightBytes -= flightMemory(p)
		shard.notConfirmed[shard.notConfirmedUsed] = nil
		return true
	}
	return false
}

// Requeue moves a failed write from the in-flight list back to the live cache.
func (c *Cache) Requeue(p *points.Points) {
	shard := c.GetShard(p.Metric)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if !removeNotConfirmed(shard, p) {
		return
	}

	r := c.pending.Load()
	if r != nil {
		r.failed(p)
	}
	count := len(p.Data)
	// The failed batch precedes any points queued since, and replaces them as
	// one new item that inherits both batches' tracking.
	values := newItem(p.Metric, 0)
	values.compact.appendPoints(p.Data)
	first := shard.firstArrival[p]
	delete(shard.firstArrival, p)
	if current := shard.items.get(p.Metric); current != nil {
		shard.writeout.add(current, -1)
		if r != nil {
			r.moved(shard.itemIdentity(current), values.identity())
		}
		shard.moveSource(shard.itemIdentity(current), values.identity())
		values.compact.appendPoints(current.compact.appendDecoded(nil))
		// A missing arrival marks restored or previously unbatched data ready.
		other := current.firstArrival
		if other.IsZero() || (!first.IsZero() && other.Before(first)) {
			first = other
		}
		shard.log.mergeItem(values, current)
	}
	if r != nil {
		r.moved(p, values.identity())
	}
	shard.moveSource(p, values.identity())
	values.firstArrival = first
	values.Arrival.Store(arrivalNanos(first))
	shard.log.mergeFlight(values, p)
	refs, independent := shard.recoverySources[values.pending]
	if !independent || refs.needsJournal() {
		shard.log.trackItem(values)
	}
	shard.items.set(p.Metric, values)
	shard.writeout.add(values, 1)
	shard.releaseDrainedTracking()
	c.syncMemoryLocked(shard)
	c.addSizeLocked(shard, int64(count), c.settings.Load().(*cacheSettings).sizeSlack)
	c.writeoutQueue.notifyAt(time.Now())
}

func (c *Cache) Len() int32 {
	var l int32
	for _, shard := range c.data {
		l += shard.metrics.Load()
	}
	return l
}

func (c *Cache) NotConfirmedLength() int32 {
	var l int32
	for _, shard := range c.data {
		l += shard.notConfirmedCount.Load()
	}
	return l
}

// IsEmpty reports whether the cache has neither queued nor in-flight points.
func (c *Cache) IsEmpty() bool {
	if r := c.pending.Load(); r != nil && r.remaining.Load() != 0 {
		return false
	}
	for _, shard := range c.data {
		shard.mu.RLock()
		empty := shard.items.len() == 0 && shard.notConfirmedUsed == 0
		shard.mu.RUnlock()
		if !empty {
			return false
		}
	}
	return true
}

// Size returns the number of cached points without taking shard locks. Each
// shard publishes its complete total, so there is no publication slack error
// (zero <= shardCount*sizeSlack), even while a delta moves to stat.size. The
// sum samples shards at different instants, rather than one global snapshot;
// concurrent updates can be observed before or after their publication.
// Hot paths use approxSize instead of visiting every shard.
func (c *Cache) Size() int64 {
	var size int64
	for _, shard := range c.data {
		size += shard.size.Load()
	}
	return size
}

// approxSize differs from Size by at most shardCount*sizeSlack points.
func (c *Cache) approxSize() int64 {
	return atomic.LoadInt64(&c.stat.size)
}

// sizeSlackFor bounds the unpublished part of the size to 1/64 of maxSize,
// and to 4096 points per shard. Small caches publish every change.
func sizeSlackFor(maxSize int64) int64 {
	const maxSlack = 4096
	if maxSize <= 0 {
		return maxSlack
	}
	return min(maxSize/(shardCount*64), maxSlack)
}

// addSizeLocked records n points added to (or removed from) the locked shard.
// It returns the approximate cache size before and after this change, which
// differ only when the shard publishes its accumulated delta.
func (c *Cache) addSizeLocked(shard *Shard, n, slack int64) (before, after int64) {
	shard.sizeDelta += n
	shard.size.Store(shard.sizePublished + shard.sizeDelta)
	if shard.sizeDelta >= -slack && shard.sizeDelta <= slack {
		size := atomic.LoadInt64(&c.stat.size)
		return size, size
	}
	delta := shard.sizeDelta
	shard.sizePublished += delta
	shard.sizeDelta = 0
	after = atomic.AddInt64(&c.stat.size, delta)
	return after - delta, after
}

func (c *Cache) DivertToXlog(w io.Writer) {
	s := c.settings.Load().(*cacheSettings)
	newSettings := *s
	newSettings.xlog = nil
	if w != nil {
		newSettings.xlog = func(p *points.Points) error { _, err := p.WriteTo(w); return err }
	}
	c.settings.Store(&newSettings)
	go c.wakeLogAdmission()
}

// DivertToBinaryXlog writes complete binary records under one lock. Binary dump
// readers in older releases already support this format when the file ends in .bin.
func (c *Cache) DivertToBinaryXlog(w io.Writer) {
	var mu sync.Mutex
	var buf []byte
	s := *c.settings.Load().(*cacheSettings)
	s.xlog = func(p *points.Points) error {
		mu.Lock()
		defer mu.Unlock()
		buf = p.AppendBinary(buf[:0])
		_, err := w.Write(buf)
		return err
	}
	c.settings.Store(&s)
	go c.wakeLogAdmission()
}

// Diversion may be installed by a callback already holding a shard lock.
// Wake asynchronously; waiters recheck diversion under their shard lock.
func (c *Cache) wakeLogAdmission() {
	for _, shard := range c.data {
		shard.mu.Lock()
		shard.log.wakeRestore()
		shard.mu.Unlock()
	}
}

// send metric to the new metrics channel
func sendMetricToNewMetricChan(c *Cache, metric string) bool {
	select {
	case c.newMetricsChan <- metric:
		return true
	default:
		atomic.AddUint32(&c.stat.droppedRealtimeIndex, 1)
		return false
	}
}

// Sets the given value under the specified key.
func (c *Cache) Add(p *points.Points) {
	c.add(p, false, false)
}

// AddPoint adds one point. name may alias a buffer the caller reuses: unlike
// Add, appending to a cached metric copies and allocates nothing.
func (c *Cache) AddPoint(name []byte, value float64, timestamp int64) {
	s := c.settings.Load().(*cacheSettings)
	// A miss goes through add, which claims saved history before insertion.
	// Existing items are already reconciled, including during pending recovery.
	if s.xlog != nil || s.tagsEnabled {
		c.add(points.OnePoint(string(name), value, timestamp), false, false)
		return
	}

	hash := xxh3.Hash(name)
	shard := c.data[hash&(shardCount-1)] // as GetShard
	shard.mu.Lock()
	values := shard.items.getBytesHashed(hash, name)
	exists := values != nil
	// Like add, recheck diversion under the lock.
	if c.settings.Load().(*cacheSettings).xlog != nil {
		shard.mu.Unlock()
		c.add(points.OnePoint(string(name), value, timestamp), false, false)
		return
	}
	if !exists {
		shard.mu.Unlock()
		c.add(points.OnePoint(string(name), value, timestamp), false, false)
		return
	}
	handled := func() bool {
		defer shard.mu.Unlock()
		return c.addPointLocked(shard, s, values, hash, value, timestamp)
	}()
	if !handled {
		c.add(points.OnePoint(string(name), value, timestamp), false, false)
	}
}

// addPointLocked returns false before admission when journal space is exhausted.
// The caller must release its lock and retry through the waiting intake path.
func (c *Cache) addPointLocked(shard *Shard, s *cacheSettings, values *item, hash uint64, value float64, timestamp int64) bool {
	if shard.log.first != nil && shard.log.entries.Len() >= maxShardLogEntries {
		return false
	}
	if c.throttleWithHash != nil {
		// The throttle does not retain the points of a cached metric.
		shard.scratchData[0] = points.Point{Value: value, Timestamp: timestamp}
		shard.scratch.Metric, shard.scratch.Data = values.Metric, shard.scratchData[:]
		throttled := c.throttleWithHash(&shard.scratch, true, hash, &values.throttle)
		shard.scratch.Metric = ""
		if throttled {
			shard.throttledPoints.Store(shard.throttledPoints.Load() + 1)
			shard.receivedPoints.Store(shard.receivedPoints.Load() + 1)
			return true
		}
	}
	if c.overflows(shard, s, 1) {
		shard.receivedPoints.Store(shard.receivedPoints.Load() + 1)
		return true
	}
	capacity := cap(values.compact.buf)
	logGrew := false
	values.compact.append(value, timestamp)
	shard.items.bytes += int64(cap(values.compact.buf) - capacity)
	if shard.log.first != nil {
		logGrew = shard.log.record(&values.StreamRef, value, timestamp)
	}
	if cap(values.compact.buf) != capacity || logGrew {
		shard.items.pointBytes += int64(cap(values.compact.buf) - capacity)
		c.syncMemoryLocked(shard)
	}
	shard.livePoints++
	if shard.livePoints&63 == 0 {
		shard.livePointsPublished.Store(shard.livePoints)
	}
	c.addedLocked(shard, s, values, 1, true, false)
	return true
}

// AddRestored adds dump data without starting a new buffering deadline. It
// waits for input-log space: the dump may be removed after Sync,
// so its points must never overflow the log's bounded admission buffer.
func (c *Cache) AddRestored(p *points.Points) {
	c.addRestored(p, false)
}

// AddRestoredBeforeInput keeps dump points durably logged but immediately
// eligible when startup must drain old history before opening receivers. It
// waits for cache capacity and bypasses live drop rules for accepted history.
func (c *Cache) AddRestoredBeforeInput(p *points.Points) {
	c.addRestored(p, true)
}

func (c *Cache) addRestored(p *points.Points, immediate bool) {
	if len(p.Data) > maxShardRestoredLogEntries {
		batch := *p
		for start := 0; start < len(p.Data); start += maxShardRestoredLogEntries {
			batch.Data = p.Data[start:min(start+maxShardRestoredLogEntries, len(p.Data))]
			c.addRestored(&batch, immediate)
		}
		return
	}
	for immediate && !c.HasRecoveryCapacity(len(p.Data)) {
		// Graceful stop may divert this accepted record to the dump WAL
		// after persistence stops. Do not hold it behind cache capacity then.
		if c.settings.Load().(*cacheSettings).xlog != nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	for wait := c.addWithRestore(p, true, false, immediate, nil, nil); wait != nil; wait = c.addWithRestore(p, true, false, immediate, nil, nil) {
		// Adjacent dump records can fill one shard, even for short metrics. Ask
		// the journal owner to collect now, rather than one buffer per
		// periodic sync tick. No I/O runs under a cache shard lock.
		select {
		case c.logPressure <- struct{}{}:
		default:
		}
		<-wait
	}
}

// AddRecovered admits previously accepted handoff input without live quotas.
// The caller provides backpressure. Pending recovery tracks the cache item
// until persistence confirms it, so its source remains available after a crash.
func (c *Cache) AddRecovered(p *points.Points) {
	c.add(p, true, true)
}

func (c *Cache) add(p *points.Points, restored, recovered bool) {
	c.addWaiting(p, restored, recovered, nil)
}

// addWaiting reserves bounded journal space before accepting live input. A
// completed Sync covers every accepted point; pressure requests an early cycle.
func (c *Cache) addWaiting(p *points.Points, restored, recovered bool, series *streamPointHint) {
	for {
		wait := c.addWithRestore(p, restored, recovered, false, series, nil)
		if wait == nil {
			return
		}
		if !recovered && len(p.Data) > maxShardLogEntries {
			batch := *p
			for start := 0; start < len(p.Data); start += maxShardLogEntries {
				batch.Data = p.Data[start:min(start+maxShardLogEntries, len(p.Data))]
				c.addWaiting(&batch, restored, recovered, series)
			}
			return
		}
		select {
		case c.logPressure <- struct{}{}:
		default:
		}
		<-wait
	}
}

func (c *Cache) addWithRestore(p *points.Points, restored, recovered, immediate bool, series *streamPointHint, source *RecoverySource) (wait <-chan struct{}) {
	s := c.settings.Load().(*cacheSettings)

	// Every new name reaches the cache here: the fast paths only append to
	// names already cached, so a refused name never takes cache memory.
	if len(p.Metric) > points.MaxMetricNameBytes {
		c.recordUnshardedInput(p, restored || recovered)
		c.rejectNameTooLong(p)
		return nil
	}

	if s.xlog != nil {
		c.recordUnshardedInput(p, restored || recovered)
		c.divert(s.xlog, p)
		return nil
	}

	if s.tagsEnabled {
		var err error
		p.Metric, err = tags.Normalize(p.Metric)
		if err != nil {
			atomic.AddUint32(&c.stat.tagsNormalizeErrors, 1)
			c.recordUnshardedInput(p, restored || recovered)
			c.stat.invalidTagPoints.Add(uint64(len(p.Data)))
			return nil
		}
	}

	// Get map shard.
	hash := xxh3.HashString(p.Metric)
	shard := c.data[hash&(shardCount-1)]
	shard.mu.Lock()
	accepted := false
	defer func() {
		if wait == nil {
			if restored || recovered {
				shard.recoveredPoints.Store(shard.recoveredPoints.Load() + uint64(len(p.Data)))
			} else if !accepted {
				shard.receivedPoints.Store(shard.receivedPoints.Load() + uint64(len(p.Data)))
			}
		}
		shard.mu.Unlock()
	}()

	// An Add may have loaded its settings before graceful diversion, then waited
	// behind the shard dump. Recheck under the same lock so it cannot miss both
	// the dump and the input log.
	if current := c.settings.Load().(*cacheSettings); current.xlog != nil {
		c.divert(current.xlog, p)
		return nil
	}
	if !recovered {
		limit := maxShardLogEntries
		if restored {
			limit = maxShardRestoredLogEntries
		}
		if wait := shard.log.space(len(p.Data), limit); wait != nil {
			return wait
		}
	}
	// Dump points the input log records stay durable until persisted, so
	// they may wait for a spread deadline (see SetWriteoutRestoreSpread).
	// Without the log the dump file goes after restore, leaving them only
	// here: they are written at once, as are recovered points, whose source
	// readers may wait for the cache to drain.
	spreadRestore := restored && !recovered && !immediate && s.writeoutRestoreSpread && s.batching() && shard.log.first != nil

	values, slot := shard.items.getHashedSlot(hash, p.Metric)
	exists := values != nil
	if !exists {
		if r := c.pending.Load(); r != nil {
			if err := c.claimForWrite(r, shard, p.Metric, len(p.Data), !restored || spreadRestore); err != nil {
				// A validated immutable record must decode; dropping its history
				// silently would lose already accepted points.
				panic(fmt.Errorf("claim validated recovery source: %w", err))
			}
			values, slot = shard.items.getHashedSlot(hash, p.Metric)
			exists = values != nil
		}
	}
	// Recovery attaches only to an empty cache with receivers closed. Every
	// insertion claims saved history first, so an existing item is reconciled
	// already; repeated live points need no mapped-index lookup under this lock.
	// Even a rejected point can refresh a stale hint for an existing item,
	// just as the old post-insertion lookup did.
	if series != nil && exists && hash == series.hash && p.Metric == series.name {
		shard.bindStreamHint(series.hint, values, slot)
	}
	var throttleState any
	liveAdmission := !recovered && (!restored || !immediate)
	if liveAdmission && c.throttleWithHash != nil {
		if exists {
			shard.throttleScratch = values.throttleState()
		}
		rejected := c.throttleWithHash(p, exists, hash, &shard.throttleScratch)
		throttleState = shard.throttleScratch
		shard.throttleScratch = nil
		if rejected {
			shard.throttledPoints.Store(shard.throttledPoints.Load() + uint64(len(p.Data)))
			return nil
		}
	}

	count := len(p.Data)
	if liveAdmission && c.overflows(shard, s, count) {
		return nil
	}

	if !exists {
		values = newItem(p.Metric, shard.bytesHint())
		slot = shard.items.setHashed(hash, values)
		if s.batching() && (!restored || spreadRestore) {
			now, held := s.writeoutTime(time.Now())
			if spreadRestore {
				values.firstArrival = s.restoreArrival(values, now)
			} else {
				values.firstArrival = now
				// Reloaded under the lock, so a table disabling just released
				// is not allocated again.
				if cur := c.settings.Load().(*cacheSettings); cur.writeoutSparseSpread && count > 0 {
					values.setFlag(itemSparse, shard.sparseSince(values, p.Data[0].Timestamp, cur.writeoutMaxDelay, shard.items.len()))
				}
			}
			c.spreadStartup(values, s, now, held)
		}

		if shard.adds != nil {
			shard.adds[p.Metric] = struct{}{}
		}
		// if no bloom filter - just add metric to new channel
		// if missed in cache, as it was before
		if c.newMetricsQueue == nil && c.newMetricsChan != nil && c.newMetricCf.Load() == nil {
			sendMetricToNewMetricChan(c, p.Metric)
		}
	}
	if restored && p.FirstArrival != 0 && !immediate && (!exists || !values.firstArrival.IsZero() && p.FirstArrival < values.firstArrival.UnixNano()) {
		if exists {
			shard.items.invalidateItemDeadline(values)
			shard.writeout.add(values, -1)
		}
		values.firstArrival = time.Unix(0, p.FirstArrival)
		values.setFlag(itemStartup, false)
		if exists {
			shard.writeout.add(values, 1)
		}
		c.writeoutQueue.notifyAt(writeoutDeadline(values, s))
	}
	if source != nil {
		shard.replayPoints += uint64(len(p.Data))
	}
	metadataChanged := shard.setItemThrottle(values, throttleState)
	if source != nil && (values.source == nil || values.source.stream != source.stream || values.source.sequence > source.sequence) {
		shard.trackSource(shard.itemIdentity(values), source)
		values.source = source
		metadataChanged = true
	}
	capacity, logCapacity := cap(values.compact.buf), shard.log.stagingBytes()
	values.compact.appendPoints(p.Data)
	shard.items.bytes += int64(cap(values.compact.buf) - capacity)

	// Independently owned sources carry their own confirmation references.
	// Continuously adding them to the finite checkpoint would prevent its
	// retirement forever. Saved history is already tracked by claimForWrite.
	if recovered && source == nil {
		if r := c.pending.Load(); r != nil {
			before := values.extraMemory()
			r.track(shard.itemIdentity(values))
			metadataChanged = metadataChanged || values.extraMemory() != before
		}
	}
	if shard.log.first != nil {
		if !recovered || source == nil || source.journal {
			metadataChanged = metadataChanged || !values.has(itemLogTracked)
			shard.log.trackItem(values)
		}
		// Recovered points keep their own durable source until persisted.
		if !recovered {
			shard.log.recordPoints(&values.StreamRef, p.Data)
		}
	}

	if !exists || metadataChanged || cap(values.compact.buf) != capacity || shard.log.stagingBytes() != logCapacity {
		shard.items.pointBytes += int64(cap(values.compact.buf) - capacity)
		c.syncMemoryLocked(shard)
	}
	if !restored && !recovered {
		accepted = true
		shard.livePoints += uint64(count)
		if shard.livePoints&63 < uint64(count) {
			shard.livePointsPublished.Store(shard.livePoints)
		}
	}
	c.addedLocked(shard, s, values, count, exists, restored && !spreadRestore && (!recovered || s.writeoutPhase.IsZero() || p.FirstArrival == 0 || immediate))
	values.Arrival.Store(arrivalNanos(values.firstArrival))
	if series != nil && !exists && hash == series.hash && p.Metric == series.name {
		shard.bindStreamHint(series.hint, values, slot)
	}
	return nil
}

func (c *Cache) overflows(shard *Shard, s *cacheSettings, count int) bool {
	if s.maxSize > 0 && c.approxSize() > s.maxSize || s.maxBytes > 0 && c.stat.bytes.Load()+c.queueMemory() > s.maxBytes {
		atomic.AddUint32(&c.stat.overflowCnt, uint32(count))
		shard.overflowPoints.Store(shard.overflowPoints.Load() + uint64(count))
		return true
	}
	return false
}

// addedLocked finishes adding count points to values, which the locked shard
// holds under values.Metric. immediate makes the item eligible at once, as for
// restored points that must not wait.
func (c *Cache) addedLocked(shard *Shard, s *cacheSettings, values *item, count int, exists, immediate bool) {
	if c.newMetricsQueue != nil {
		if values.indexGeneration == 0 {
			if c.metricExists == nil || !c.metricExists(values.Metric) {
				c.newMetricsQueue.Add(values.Metric)
			}
			values.indexGeneration = 1
		}
	} else if filter := c.newMetricCf.Load(); c.newMetricsChan != nil && filter != nil && values.indexGeneration != filter.generation {
		hash := helper.HashString(values.Metric)
		if filter.Has(hash) {
			values.indexGeneration = filter.generation
		} else if (c.metricExists != nil && c.metricExists(values.Metric)) || sendMetricToNewMetricChan(c, values.Metric) {
			// A full notification queue leaves the item retryable.
			filter.Add(hash)
			values.indexGeneration = filter.generation
		}
	}
	before, size := c.addSizeLocked(shard, int64(count), s.sizeSlack)
	updateSummary := !exists || immediate
	// An arrival may have loaded settings before a reload and waited behind
	// a scan using the new thresholds. Its count must invalidate that summary.
	if d := shard.items.deadlines; d != nil && d.settings != s {
		d.settings = nil
	}
	if s.batching() {
		n := values.count()
		// Spread is at most one half. Most batches are far below even that
		// lower bound; avoid computing their floating-point threshold per point.
		crossedThreshold := false
		if n >= s.writeoutMinPoints/2 {
			minPoints := s.minPointsFor(values)
			crossedThreshold = n >= minPoints && n-count < minPoints
		}
		if exists && (immediate || crossedThreshold || values.has(itemSparse) && n > sparseMaxPoints && n-count <= sparseMaxPoints) {
			shard.items.invalidateItemDeadline(values)
			updateSummary = true
		}
		if immediate || crossedThreshold || (s.underPressure(size) && !s.underPressure(before)) {
			c.writeoutQueue.notifyAt(time.Now())
		} else if !exists {
			c.writeoutQueue.notifyAt(writeoutDeadline(values, s))
		}
	}
	if updateSummary || shard.writeout.stableSettings != s {
		c.appendWriteout(shard, values, count, exists, immediate)
	}
}

// Pop removes an element from the map and returns it
func (c *Cache) Pop(key string) (p *points.Points, exists bool) {
	// Try to get shard.
	hash := xxh3.HashString(key)
	shard := c.data[hash&(shardCount-1)]
	shard.mu.Lock()
	values := shard.items.getHashed(hash, key)
	exists = values != nil
	if exists {
		shard.writeout.add(values, -1)
		shard.items.removeHashed(hash, key)
		values.firstArrival = time.Time{}
		shard.log.untrackItem(values)
		if values.pending != nil {
			shard.releaseSource(values.pending)
		}
		shard.notePopped(len(values.compact.buf))
		s := c.settings.Load().(*cacheSettings)
		shard.noteWritten(values, s)
		c.addSizeLocked(shard, -int64(values.count()), s.sizeSlack)
		c.syncMemoryLocked(shard)
	}
	shard.mu.Unlock()
	if exists {
		// Unreachable from the shard, the item decodes unlocked.
		p = values.decode()
		// The persister pops a metric it will never write, such as one whose
		// path is too long. Saved points it held are settled as well, or
		// pending recovery, and the first scan behind it, would wait forever.
		if r := c.pending.Load(); r != nil {
			r.persisted(p)
		}
	}

	return p, exists
}

func (c *Cache) PopNotConfirmed(key string) (p *points.Points, exists bool) {
	// Try to get shard.
	hash := xxh3.HashString(key)
	shard := c.data[hash&(shardCount-1)]
	shard.mu.Lock()
	values := shard.items.getHashed(hash, key)
	exists = values != nil
	if exists {
		shard.writeout.add(values, -1)
		shard.items.removeHashed(hash, key)
		shard.notePopped(len(values.compact.buf))
		s := c.settings.Load().(*cacheSettings)
		shard.noteWritten(values, s)
		c.addSizeLocked(shard, -int64(values.count()), s.sizeSlack)
		// Tracking follows the batch while a replacement item may be queued.
		// Get reads in-flight Data under this lock.
		p = values.decode()
		shard.log.pop(values, p)
		if !values.firstArrival.IsZero() {
			if shard.firstArrival == nil {
				shard.firstArrival = make(map[*points.Points]time.Time)
			}
			shard.firstArrival[p] = values.firstArrival
		}
		if shard.notConfirmedUsed < len(shard.notConfirmed) {
			shard.notConfirmed[shard.notConfirmedUsed] = p
		} else {
			shard.notConfirmed = append(shard.notConfirmed, p)
		}
		shard.notConfirmedUsed++
		shard.inFlightPoints.Store(shard.inFlightPoints.Load() + int64(len(p.Data)))
		shard.flightPeak = max(shard.flightPeak, shard.notConfirmedUsed)
		shard.inFlightBytes += flightMemory(p)
		c.syncMemoryLocked(shard)
	}
	shard.mu.Unlock()

	return p, exists
}

func (c *Cache) WriteoutQueue() *WriteoutQueue {
	return c.writeoutQueue
}

// called at every scan-frequency by fileListUpdater in carbonserver.
// Iterates over every shard to:
// - collect the new metric names (append shard.adds map to slice)
// - replace shard.adds with new empty map
func (c *Cache) GetRecentNewMetrics() []map[string]struct{} {
	metricNames := make([]map[string]struct{}, shardCount)
	for i := 0; i < shardCount; i++ {
		shard, newAdds := c.data[i], make(map[string]struct{})
		shard.mu.Lock()
		currNames := shard.adds
		shard.adds = newAdds
		c.syncMemoryLocked(shard)
		shard.mu.Unlock()
		metricNames[i] = currNames
	}
	return metricNames
}

func (c *Cache) SetThrottle(throttle func(ps *points.Points, inCache bool) bool) {
	c.throttleWithHash = nil
	if throttle != nil {
		c.throttleWithHash = func(ps *points.Points, inCache bool, _ uint64, _ *any) bool {
			return throttle(ps, inCache)
		}
	}
}

// SetThrottleWithState lets a throttle retain metric-specific lookup state.
// The shard lock guards state; the hook must invalidate it when rules change.
// Configure before starting ingestion, as with SetThrottle.
func (c *Cache) SetThrottleWithState(throttle func(ps *points.Points, inCache bool, state *any) bool) {
	c.throttleWithHash = nil
	if throttle != nil {
		c.throttleWithHash = func(ps *points.Points, inCache bool, _ uint64, state *any) bool {
			return throttle(ps, inCache, state)
		}
	}
}

// SetThrottleWithHash passes the cache's precomputed xxh3 name hash along with
// metric-specific state. A callback can reuse it for accounting stripes without
// hashing the name again. State is owned under the shard lock, as with
// SetThrottleWithState. Configure before starting ingestion.
func (c *Cache) SetThrottleWithHash(throttle func(ps *points.Points, inCache bool, hash uint64, state *any) bool) {
	c.throttleWithHash = throttle
}

func (c *Cache) GetInfo() map[string]interface{} {
	s, ok := c.settings.Load().(*cacheSettings)
	if !ok {
		return map[string]interface{}{}
	}

	return map[string]interface{}{
		"size":                c.Size(),
		"limit":               s.maxSize,
		"estimated_bytes":     c.EstimatedBytes(),
		"byte_limit":          s.maxBytes,
		"writeout_scan_yield": s.writeoutScanYield,
	}
}

// DivertToPointWriter requires a concurrency-safe writer. Once installed, a
// shard dump observes every earlier Add; every later Add goes to this writer.
func (c *Cache) DivertToPointWriter(write func(*points.Points) error) {
	s := *c.settings.Load().(*cacheSettings)
	s.xlog = write
	c.settings.Store(&s)
	go c.wakeLogAdmission()
}

// SetRestartOrder serves the oldest eligible queued points during restart.
// The configured strategy resumes when active returns false.
func (c *Cache) SetRestartOrder(active func() bool) {
	c.mu.Lock()
	c.restartOrder = active
	c.mu.Unlock()
}

// Keep small, repeatedly reused tracking capacity. A large completed batch must
// not pin the peak in-flight population for the lifetime of the process.
func (s *Shard) releaseDrainedTracking() {
	if s.notConfirmedUsed != 0 || s.flightPeak <= 64 {
		return
	}
	if cap(s.notConfirmed) > 64 {
		s.notConfirmed = make([]*points.Points, 4)
	}
	if len(s.firstArrival) == 0 {
		s.firstArrival = nil
	}
	if s.log.first != nil && len(s.log.first) == 0 {
		s.log.first = make(map[*points.Points]uint64)
	}
	s.flightPeak = 0
}
