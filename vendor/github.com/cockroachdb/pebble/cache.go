// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import "github.com/cockroachdb/pebble/internal/cache"

// Cache exports the cache.Cache type.
type Cache = cache.Cache

// NewCache creates a new cache of the specified size. Memory for the cache is
// allocated on demand, not during initialization. The cache is created with a
// reference count of 1. Each DB it is associated with adds a reference, so the
// creator of the cache should usually release their reference after the DB is
// created.
//
//	c := pebble.NewCache(...)
//	defer c.Unref()
//	d, err := pebble.Open(pebble.Options{Cache: c})
func NewCache(size int64) *cache.Cache {
	return cache.New(size)
}

// NewCacheWithShards creates a cache with an explicit positive shard count.
// Capacity and reference ownership are the same as NewCache. Larger shards can
// retain blocks that exceed the default per-shard capacity on high-core hosts,
// but reduce the concurrency available before cache-lock contention.
func NewCacheWithShards(size int64, shards int) *cache.Cache {
	return cache.NewWithShards(size, shards)
}
