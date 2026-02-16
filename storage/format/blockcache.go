// Copyright 2024 Vego Authors
// Licensed under the Apache License, Version 2.0

package format

import (
	"container/list"
	"hash/fnv"
	"sync"
	"sync/atomic"
)

// DefaultBlockCacheSize is the default cache size (64 MB)
const DefaultBlockCacheSize = 64 * 1024 * 1024

// DefaultBlockSize is the default block size for hints (64 KB)
const DefaultBlockSize = 64 * 1024

// DefaultNumShards is the default number of shards for the cache
const DefaultNumShards = 64

// cacheEntry represents a cached block
type cacheEntry struct {
	key  string
	data []byte
	size int64
}

// cacheShard represents a single shard of the cache
type cacheShard struct {
	mu     sync.RWMutex
	items  map[string]*list.Element
	lru    *list.List
	size   int64
}

// BlockCache provides a sharded LRU cache for data blocks
// This is used to reduce I/O for frequently accessed pages
// The cache is divided into multiple shards to reduce lock contention
type BlockCache struct {
	shards    []*cacheShard
	numShards int
	capacity  int64 // Max bytes to cache per shard

	// Global statistics (using atomic operations)
	hits      int64
	misses    int64
	evictions int64
}

// NewBlockCache creates a new sharded LRU block cache
// capacityBytes: total cache capacity in bytes
// numShards: number of shards (optional, defaults to DefaultNumShards)
func NewBlockCache(capacityBytes int64, numShards ...int) *BlockCache {
	n := DefaultNumShards
	if len(numShards) > 0 && numShards[0] > 0 {
		n = numShards[0]
	}

	// Calculate per-shard capacity
	perShardCapacity := capacityBytes / int64(n)
	if perShardCapacity < 1 {
		perShardCapacity = 1
	}

	shards := make([]*cacheShard, n)
	for i := 0; i < n; i++ {
		shards[i] = &cacheShard{
			items: make(map[string]*list.Element),
			lru:   list.New(),
		}
	}

	return &BlockCache{
		shards:    shards,
		numShards: n,
		capacity:  perShardCapacity,
	}
}

// getShard returns the shard for a given key
func (c *BlockCache) getShard(key string) *cacheShard {
	h := fnv.New64a()
	h.Write([]byte(key))
	return c.shards[h.Sum64()%uint64(c.numShards)]
}

// Get retrieves a cached block
// Returns (data, true) if found, (nil, false) otherwise
// The returned data is a copy to prevent external modifications
func (c *BlockCache) Get(key string) ([]byte, bool) {
	shard := c.getShard(key)

	// Step 1: Read lock query (no promotion)
	shard.mu.RLock()
	elem, ok := shard.items[key]
	shard.mu.RUnlock()

	if !ok {
		atomic.AddInt64(&c.misses, 1)
		return nil, false
	}

	// Step 2: Write lock promotion (double-check)
	shard.mu.Lock()
	if elem2, ok := shard.items[key]; ok && elem2 == elem {
		shard.lru.MoveToFront(elem)
		entry := elem.Value.(*cacheEntry)
		// Copy data to prevent external modification
		data := make([]byte, len(entry.data))
		copy(data, entry.data)
		shard.mu.Unlock()

		atomic.AddInt64(&c.hits, 1)
		return data, true
	}
	shard.mu.Unlock()

	// Entry was removed between RUnlock and Lock
	atomic.AddInt64(&c.misses, 1)
	return nil, false
}

// Put adds a block to the cache
// If the block is larger than shard capacity, it won't be cached
// If the cache is full, evicts least recently used items
// The data is copied to prevent external modifications
func (c *BlockCache) Put(key string, data []byte) {
	shard := c.getShard(key)

	// Copy data to prevent external modification
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	entrySize := int64(len(dataCopy))

	// Don't cache items larger than total cache capacity
	totalCapacity := c.capacity * int64(c.numShards)
	if entrySize > totalCapacity {
		return
	}

	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Check if already exists
	if elem, ok := shard.items[key]; ok {
		// Update existing entry
		entry := elem.Value.(*cacheEntry)
		shard.size -= entry.size
		entry.data = dataCopy
		entry.size = entrySize
		shard.size += entrySize
		shard.lru.MoveToFront(elem)
		return
	}

	// Evict items if necessary
	for shard.size+entrySize > c.capacity && shard.lru.Len() > 0 {
		c.evictOldestLocked(shard)
	}

	// Add new entry
	entry := &cacheEntry{
		key:  key,
		data: dataCopy,
		size: entrySize,
	}
	elem := shard.lru.PushFront(entry)
	shard.items[key] = elem
	shard.size += entrySize
}

// evictOldestLocked removes the least recently used item from a shard
// Must be called with shard lock held
func (c *BlockCache) evictOldestLocked(shard *cacheShard) {
	elem := shard.lru.Back()
	if elem == nil {
		return
	}

	entry := elem.Value.(*cacheEntry)
	delete(shard.items, entry.key)
	shard.lru.Remove(elem)
	shard.size -= entry.size

	atomic.AddInt64(&c.evictions, 1)
}

// Remove removes a specific key from the cache
func (c *BlockCache) Remove(key string) {
	shard := c.getShard(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	if elem, ok := shard.items[key]; ok {
		entry := elem.Value.(*cacheEntry)
		delete(shard.items, key)
		shard.lru.Remove(elem)
		shard.size -= entry.size
	}
}

// Invalidate removes a specific key from the cache (alias for Remove)
func (c *BlockCache) Invalidate(key string) {
	c.Remove(key)
}

// Clear removes all items from the cache
func (c *BlockCache) Clear() {
	for _, shard := range c.shards {
		shard.mu.Lock()
		shard.items = make(map[string]*list.Element)
		shard.lru = list.New()
		shard.size = 0
		shard.mu.Unlock()
	}
}

// Size returns the current cache size in bytes (sum of all shards)
func (c *BlockCache) Size() int64 {
	var total int64
	for _, shard := range c.shards {
		shard.mu.RLock()
		total += shard.size
		shard.mu.RUnlock()
	}
	return total
}

// Capacity returns the maximum cache capacity in bytes (total across all shards)
func (c *BlockCache) Capacity() int64 {
	return c.capacity * int64(c.numShards)
}

// Len returns the number of items in the cache (sum of all shards)
func (c *BlockCache) Len() int {
	var total int
	for _, shard := range c.shards {
		shard.mu.RLock()
		total += len(shard.items)
		shard.mu.RUnlock()
	}
	return total
}

// ShardCount returns the number of shards
func (c *BlockCache) ShardCount() int {
	return c.numShards
}

// Stats returns cache statistics
func (c *BlockCache) Stats() BlockCacheStats {
	hits := atomic.LoadInt64(&c.hits)
	misses := atomic.LoadInt64(&c.misses)
	evictions := atomic.LoadInt64(&c.evictions)

	return BlockCacheStats{
		ItemCount: c.Len(),
		Size:      c.Size(),
		Capacity:  c.Capacity(),
		Hits:      hits,
		Misses:    misses,
		Evictions: evictions,
		HitRate:   c.calculateHitRate(hits, misses),
	}
}

// calculateHitRate calculates the cache hit rate
func (c *BlockCache) calculateHitRate(hits, misses int64) float64 {
	total := hits + misses
	if total == 0 {
		return 0.0
	}
	return float64(hits) / float64(total)
}

// ResetStats resets the statistics counters
func (c *BlockCache) ResetStats() {
	atomic.StoreInt64(&c.hits, 0)
	atomic.StoreInt64(&c.misses, 0)
	atomic.StoreInt64(&c.evictions, 0)
}

// BlockCacheStats contains cache statistics
type BlockCacheStats struct {
	ItemCount int     // Number of items in cache
	Size      int64   // Current cache size in bytes
	Capacity  int64   // Maximum cache capacity in bytes
	Hits      int64   // Number of cache hits
	Misses    int64   // Number of cache misses
	Evictions int64   // Number of evictions
	HitRate   float64 // Cache hit rate (0.0 - 1.0)
}
