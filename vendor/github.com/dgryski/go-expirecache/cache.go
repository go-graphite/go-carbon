package expirecache

import (
	"math/rand"
	"sync"
	"time"
)

type element struct {
	validUntil time.Time
	data       interface{}
	size       uint64
}

// Cache is an expiring cache.  It is safe for
type Cache struct {
	sync.RWMutex
	cache     map[string]element
	keys      []string
	totalSize uint64
	maxSize   uint64
}

// New creates a new cache with a maximum memory size
func New(maxSize uint64) *Cache {
	return &Cache{
		cache:   make(map[string]element),
		maxSize: maxSize,
	}
}

// Size returns the current memory size of the cache
func (ec *Cache) Size() uint64 {
	ec.RLock()
	s := ec.totalSize
	ec.RUnlock()
	return s
}

// Items returns the number of items in the cache
func (ec *Cache) Items() int {
	ec.RLock()
	k := len(ec.keys)
	ec.RUnlock()
	return k
}

// Get returns the item from the cache
func (ec *Cache) Get(k string) (item interface{}, ok bool) {
	ec.RLock()
	v, ok := ec.cache[k]
	ec.RUnlock()
	if !ok || v.validUntil.Before(timeNow()) {
		// Can't actually delete this element from the cache here since
		// we can't remove the key from ec.keys without a linear search.
		// It'll get removed during the next cleanup
		return nil, false
	}
	return v.data, ok
}

// GetOrSet returns the item from the cache or sets a new variable if it doesn't exist
func (ec *Cache) GetOrSet(k string, newValue interface{}, size uint64, expire int32) (item interface{}) {
	ec.Lock()
	v, ok := ec.cache[k]
	if !ok || v.validUntil.Before(timeNow()) {
		ec.actualSet(k, newValue, size, expire)
		ec.Unlock()
		return newValue
	}
	ec.Unlock()
	return v.data
}

// Set adds an item to the cache, with an estimated size and expiration time in seconds.
func (ec *Cache) Set(k string, v interface{}, size uint64, expire int32) {
	ec.Lock()
	ec.actualSet(k, v, size, expire)
	ec.Unlock()
}

func (ec *Cache) actualSet(k string, v interface{}, size uint64, expire int32) {
	oldv, ok := ec.cache[k]
	if !ok {
		ec.keys = append(ec.keys, k)
	} else {
		ec.totalSize -= oldv.size
	}

	ec.totalSize += size
	ec.cache[k] = element{validUntil: timeNow().Add(time.Duration(expire) * time.Second), data: v, size: size}

	for ec.maxSize > 0 && ec.totalSize > ec.maxSize {
		ec.randomEvict()
	}
}

func (ec *Cache) randomEvict() {
	slot := rand.Intn(len(ec.keys))
	k := ec.keys[slot]

	ec.keys[slot] = ec.keys[len(ec.keys)-1]
	ec.keys = ec.keys[:len(ec.keys)-1]

	v := ec.cache[k]
	ec.totalSize -= v.size

	delete(ec.cache, k)
}

// Cleaner starts a goroutine which wakes up periodically and removes all expired items from the cache.
func (ec *Cache) Cleaner(d time.Duration) {

	for {
		cleanerSleep(d)

		now := timeNow()
		ec.Lock()

		// We could potentially be holding this lock for a long time,
		// but since we keep the cache expiration times small, we
		// expect only a small number of elements here to loop over

		for i := 0; i < len(ec.keys); i++ {
			k := ec.keys[i]
			v := ec.cache[k]
			if v.validUntil.Before(now) {
				ec.totalSize -= v.size
				delete(ec.cache, k)

				ec.keys[i] = ec.keys[len(ec.keys)-1]
				ec.keys = ec.keys[:len(ec.keys)-1]
				i-- // so we reprocess this index
			}
		}

		ec.Unlock()
		cleanerDone()
	}
}

func (ec *Cache) StoppableApproximateCleaner(d time.Duration, exit <-chan struct{}) {
	for {
		select {
		case <-exit:
			return
		default:
		}

		cleanerSleep(d)

		ec.clean(timeNow())

		cleanerDone()
	}

}

// ApproximateCleaner starts a goroutine which wakes up periodically and removes a sample of expired items from the cache.
func (ec *Cache) ApproximateCleaner(d time.Duration) {
	for {
		cleanerSleep(d)

		ec.clean(timeNow())

		cleanerDone()
	}
}

func (ec *Cache) clean(now time.Time) {
	// every iteration, sample and clean this many items
	const sampleSize = 20
	// if we cleaned at least this many, run the loop again
	const rerunCount = 5


	for {
		var cleaned int
		// by doing short iterations and releasing the lock in between, we don't block other requests from progressing.
		ec.Lock()
		for i := 0; len(ec.keys) > 0 && i < sampleSize; i++ {
			idx := rand.Intn(len(ec.keys))
			k := ec.keys[idx]
			v := ec.cache[k]
			if v.validUntil.Before(now) {
				ec.totalSize -= v.size
				delete(ec.cache, k)

				ec.keys[idx] = ec.keys[len(ec.keys)-1]
				ec.keys = ec.keys[:len(ec.keys)-1]
				cleaned++
			}
		}
		ec.Unlock()
		if cleaned < rerunCount {
			// "clean enough"
			return
		}
	}
	return
}

var (
	timeNow      = time.Now
	cleanerSleep = time.Sleep
	cleanerDone  = func() {}
)
