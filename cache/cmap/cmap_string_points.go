// based on https://github.com/streamrail/concurrent-map , MIT License
package cmap

import (
	"github.com/lomik/go-carbon/points"
	"sync"
)

const SHARD_COUNT = 128

// A "thread" safe map of type string:Anything.
// To avoid lock bottlenecks this map is dived to several (SHARD_COUNT) map shards.
type ConcurrentMap []shard

// A "thread" safe string to anything map.
type shard struct {
	sync.RWMutex // Read Write mutex, guards access to internal map.
	items        map[string]*points.Points
	pad          [128]byte //  avoid false sharing
}

// Creates a new concurrent map.
func New() ConcurrentMap {
	m := make(ConcurrentMap, SHARD_COUNT)
	for i := 0; i < SHARD_COUNT; i++ {
		m[i].items = make(map[string]*points.Points)
	}
	return m
}

// Returns shard under given key
func (m *ConcurrentMap) GetShard(key string) *shard {
	sum := fnv32([]byte(key))
	return &(*m)[uint(sum)%uint(len(*m))]
}

// Sets the given value under the specified key.
func (m *ConcurrentMap) Set(key string, value *points.Points) {
	// Get map shard.
	shard := m.GetShard(key)
	shard.Lock()
	shard.items[key] = value
	shard.Unlock()
}

// Callback to return new element to be inserted into the map
// also reterns 'delete' flag which if set, element will be deleted from map
//
// It is called while lock is held, therefore it MUST NOT
// try to access other keys in same map, as it can lead to deadlock since
// Go sync.RWLock is not reentrant
//

type UpsertCb func(exist bool, valueInMap *points.Points, newValue *points.Points) (*points.Points, bool)

// Insert or Update - updates existing element or inserts a new one using UpsertCb
// returns element left in a map and 'del' flag value (if true, element was deleted from the map)
func (m *ConcurrentMap) Upsert(key string, value *points.Points, cb UpsertCb) (res *points.Points, del bool) {
	shard := m.GetShard(key)
	shard.Lock()
	v, exists := shard.items[key]
	res, del = cb(exists, v, value)
	if del {
		if exists { // micro optimisation: try to delete only if it was there
			delete(shard.items, key)
		}
	} else {
		if res != v { // micro optimisation: don't re-insert if same pointer
			shard.items[key] = res
		}
	}
	shard.Unlock()
	return res, del
}

type UpsertSingleCb func(exist bool, valueInMap *points.Points, newValue points.SinglePoint) *points.Points

func (m *ConcurrentMap) UpsertSingle(key string, value points.SinglePoint, cb UpsertSingleCb) (res *points.Points) {
	shard := m.GetShard(key)
	shard.Lock()
	v, ok := shard.items[key]
	res = cb(ok, v, value)
	if res != v {  // micro optimisation: don't re-insert if returned obj is same pointer as one already in map
		shard.items[key] = res
	}
	shard.Unlock()
	return res
}

// Retrieves an element from map under given key.
func (m *ConcurrentMap) Get(key string) (*points.Points, bool) {
	// Get shard
	shard := m.GetShard(key)
	shard.RLock()
	// Get item from shard.
	val, ok := shard.items[key]
	shard.RUnlock()
	return val, ok
}

// Returns the number of elements within the map.
func (m *ConcurrentMap) Count() int {
	count := 0
	for i := 0; i < len(*m); i++ {
		shard := &(*m)[i]
		shard.RLock()
		count += len(shard.items)
		shard.RUnlock()
	}
	return count
}

// Removes an element from the map.
func (m *ConcurrentMap) Remove(key string) {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	delete(shard.items, key)
	shard.Unlock()
}

// Removes an element from the map.
func (m *ConcurrentMap) Pop(key string) (v *points.Points, exists bool) {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	v, exists = shard.items[key]
	delete(shard.items, key)
	shard.Unlock()
	return v, exists
}

type IterCb func(key string, v *points.Points)

// Callback based iterator
func (m *ConcurrentMap) IterCb(fn IterCb) {
	for idx := range *m {
		shard := &(*m)[idx]
		shard.RLock()
		for key, value := range shard.items {
			fn(key, value)
		}
		shard.RUnlock()
	}
}

func fnv32(key []byte) uint32 {
	hash := uint32(2166136261)
	prime32 := uint32(16777619)
	for _, c := range key {
		hash *= prime32
		hash ^= uint32(c)
	}
	return hash
}
