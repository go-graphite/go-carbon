package cache

import (
	"math"
	"sync/atomic"
	"time"

	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

const recoveryMemoryInterval = 5 * time.Second

type recoveryAdmission struct {
	read      func() (uint64, bool)
	sampling  atomic.Bool
	limits    atomic.Pointer[recoveryLimits]
	remaining atomic.Int64
}

type recoveryLimits struct {
	bytes, bytesPerPoint, ceilingBytes int64
	at                                 time.Time
}

// SetRecoveryMemory enables byte-reserved recovery using current allocatable
// memory (after host/Go/cgroup safety reserves). Set before intake starts. Nil
// retains legacy half-cache policy. Explicit max-size/max-bytes become FULL
// hard ceilings in HasRecoveryCapacityFor; recovered points never pass through
// live drop admission. The count-only legacy predicate retains its old policy.
// There is no goroutine/lifecycle ownership: one caller refreshes every 5s.
func (c *Cache) SetRecoveryMemory(read func() (uint64, bool)) {
	if read == nil {
		c.recoveryAdmission.Store(nil)
		return
	}
	c.recoveryAdmission.Store(&recoveryAdmission{read: read})
}

// HasRecoveryCapacityFor reserves a named batch before AddRecovered. Supplying
// the name is essential: a one-point new metric can allocate much more than a
// packed point. Call once for each accepted batch; abandoned reservations expire
// at the next sample. This is the capture-scale integration hook. Before the
// host sampler is installed it retains the legacy policy and pressure signal.
func (c *Cache) HasRecoveryCapacityFor(name string, count int) bool {
	if a := c.recoveryAdmission.Load(); a != nil {
		return c.admitRecovery(a, name, count)
	}
	return c.HasRecoveryCapacity(count)
}

// RecoveryBlocked stays true through the next controller observation after a
// denial, including the gap between a persister confirmation and a retry. A
// successful/small record cannot clear another worker's pressure; it expires
// 10s after the LAST denial. No capture-specific callback is required.
func (c *Cache) RecoveryBlocked() bool {
	until := c.recoveryBlockedUntil.Load()
	if until == 0 {
		return false
	}
	if until > time.Now().UnixNano() {
		return true
	}
	c.recoveryBlockedUntil.CompareAndSwap(until, 0)
	return false
}

func (c *Cache) recordRecoveryAdmission(allowed bool) {
	if allowed {
		return
	}
	c.recoveryRejections.Add(1)
	now := time.Now()
	until := now.Add(2 * recoveryMemoryInterval).UnixNano()
	previous := c.recoveryBlockedUntil.Load()
	for previous < until && !c.recoveryBlockedUntil.CompareAndSwap(previous, until) {
		previous = c.recoveryBlockedUntil.Load()
	}
	if previous <= now.UnixNano() {
		c.writeoutQueue.notifyAt(now)
	}
}

func (c *Cache) refreshRecoveryMemory(a *recoveryAdmission, now time.Time) {
	if current := a.limits.Load(); current != nil && now.Sub(current.at) < recoveryMemoryInterval {
		return
	}
	if !a.sampling.CompareAndSwap(false, true) {
		return
	}
	defer a.sampling.Store(false)
	// Another sampler can finish between our first observation and CAS.
	// Recheck so it cannot refill a second burst inside the same interval.
	if current := a.limits.Load(); current != nil && now.Sub(current.at) < recoveryMemoryInterval {
		return
	}
	available, valid := a.read()
	// Spend at most a quarter of measured headroom before sampling again.
	// The rest covers live input, persister/native buffers and sample latency.
	window := int64(min(available/4, uint64(math.MaxInt64)))
	if !valid {
		window = 0
	}
	// A name/header can cost more than packed points. Use observed owned
	// bytes/point with slack, with a conservative 64 B/point minimum. The
	// shared byte reservation bounds concurrent check-then-add callers.
	perPoint := int64(64)
	if points := c.approxSize(); points > 0 {
		perPoint = max(perPoint, int64(math.Ceil(float64(c.EstimatedBytes())/float64(points)*1.25)))
	}
	old := a.limits.Load()
	ceiling := c.EstimatedBytes()
	ceiling += min(window, math.MaxInt64-ceiling)
	a.remaining.Store(window)
	a.limits.Store(&recoveryLimits{bytes: window, bytesPerPoint: perPoint, ceilingBytes: ceiling, at: now})
	if old == nil || old.bytes != window || old.bytesPerPoint != perPoint || old.ceilingBytes != ceiling {
		zapwriter.Logger("cache").Info("recovery memory admission adjusted", zap.Int64("window_bytes", window), zap.Int64("ceiling_bytes", ceiling), zap.Int64("bytes_per_point", perPoint), zap.Bool("valid", valid))
	}
}

func (c *Cache) admitRecovery(a *recoveryAdmission, name string, count int) bool {
	return c.admitRecoveryBytes(a, count, c.RecoveryMetricBytes([]byte(name)))
}

// RecoveryMetricBytes prices the name/header allocation for recovery. Capture
// batches use the same active-map-only lookup as single-metric admission.
func (c *Cache) RecoveryMetricBytes(name []byte) int64 {
	// Existing series need point capacity, not another item/name allocation on
	// every one-point batch. Only check the active map: treating an in-flight or
	// pending-only item as new is conservative and avoids scanning recovery state.
	// A concurrent eviction may still create one extra header per caller; the
	// unspent three quarters of headroom cover this bounded check/add race.
	shard := c.GetShard(string(name))
	shard.mu.RLock()
	exists := shard.items.get(string(name)) != nil
	shard.mu.RUnlock()
	if exists {
		return 0
	}
	return int64(len(name)) + 1024
}

// HasRecoveryCapacityBatch reserves a complete capture frame atomically. The
// caller supplies a conservative total of distinct name/header allocations;
// points, physical memory, hard settings and blocked pressure use the same
// policy as named admission. No capture-specific budget or sampler is created.
func (c *Cache) HasRecoveryCapacityBatch(count int, metricBytes int64) bool {
	if a := c.recoveryAdmission.Load(); a != nil {
		return c.admitRecoveryBytes(a, count, metricBytes)
	}
	return c.HasRecoveryCapacity(count)
}
func (c *Cache) admitRecoveryBytes(a *recoveryAdmission, count int, overhead int64) bool {
	now := time.Now()
	c.refreshRecoveryMemory(a, now)
	now = time.Now()
	l := a.limits.Load()
	allowed := false
	defer func() { c.recordRecoveryAdmission(allowed) }()
	if count < 0 || overhead < 0 || l == nil || now.Sub(l.at) > 2*recoveryMemoryInterval {
		return false
	}
	s := c.settings.Load().(*cacheSettings)
	upper := c.approxSize() + shardCount*sizeSlackFor(0)
	if s.maxSize > 0 && upper+int64(count) > s.maxSize && c.Size()+int64(count) > s.maxSize {
		return false
	}

	if int64(count) > (math.MaxInt64-overhead)/l.bytesPerPoint {
		return false
	}
	cost := int64(count)*l.bytesPerPoint + overhead
	ceiling := l.ceilingBytes
	if s.maxBytes > 0 {
		ceiling = min(ceiling, s.maxBytes)
	}
	if c.stat.bytes.Load()+shardCount*byteSlackFor(0)+c.queueMemory() > ceiling-cost && c.EstimatedBytes() > ceiling-cost {
		return false
	}
	for {
		credit := a.remaining.Load()
		if cost > credit {
			return false
		}
		if a.remaining.CompareAndSwap(credit, credit-cost) {
			allowed = true
			return true
		}
	}
}

// RecoveryCapacity returns the chosen reservation window, remaining bytes and
// denied checks. It never scans/locks cache shards and is safe during replay.
func (c *Cache) RecoveryCapacity() (window, remaining int64, rejections uint64) {
	if a := c.recoveryAdmission.Load(); a != nil {
		if l := a.limits.Load(); l != nil {
			window = l.bytes
		}
		remaining = a.remaining.Load()
	}
	return window, remaining, c.recoveryRejections.Load()
}

func (c *Cache) RecoveryByteCeiling() int64 {
	if a := c.recoveryAdmission.Load(); a != nil {
		if l := a.limits.Load(); l != nil {
			return l.ceilingBytes
		}
	}
	return 0
}
