// Copyright 2024 Vego Authors
// Licensed under the Apache License, Version 2.0

package format

import (
	"container/list"
	"sync"
)

// BlockCache provides an LRU cache for data blocks
// This is used to reduce I/O for frequently accessed pages
type BlockCache struct {
	mu       sync.RWMutex
	capacity int64 // Max bytes to cache
	size     int64 // Current bytes cached
	items    map[string]*list.Element
	lru      *list.List
}

// cacheEntry represents a cached block
type cacheEntry struct {
	key  string
	data []byte
	size int64
}

// NewBlockCache creates a new LRU block cache
func NewBlockCache(capacityBytes int64) *BlockCache {
	return &BlockCache{
		capacity: capacityBytes,
		items:    make(map[string]*list.Element),
		lru:      list.New(),
	}
}

// Get retrieves a cached block
// Returns (data, true) if found, (nil, false) otherwise
func (c *BlockCache) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		// Move to front (most recently used)
		c.lru.MoveToFront(elem)
		entry := elem.Value.(*cacheEntry)
		return entry.data, true
	}

	return nil, false
}

// Put adds a block to the cache
// If the block is larger than cache capacity, it won't be cached
// If the cache is full, evicts least recently used items
func (c *BlockCache) Put(key string, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Don't cache items larger than capacity
	entrySize := int64(len(data))
	if entrySize > c.capacity {
		return
	}

	// Check if already exists
	if elem, ok := c.items[key]; ok {
		// Update existing entry
		entry := elem.Value.(*cacheEntry)
		c.size -= entry.size
		entry.data = data
		entry.size = entrySize
		c.size += entrySize
		c.lru.MoveToFront(elem)
		return
	}

	// Evict items if necessary
	for c.size+entrySize > c.capacity && c.lru.Len() > 0 {
		c.evictOldest()
	}

	// Add new entry
	entry := &cacheEntry{
		key:  key,
		data: data,
		size: entrySize,
	}
	elem := c.lru.PushFront(entry)
	c.items[key] = elem
	c.size += entrySize
}

// evictOldest removes the least recently used item
// Must be called with lock held
func (c *BlockCache) evictOldest() {
	elem := c.lru.Back()
	if elem == nil {
		return
	}

	entry := elem.Value.(*cacheEntry)
	delete(c.items, entry.key)
	c.lru.Remove(elem)
	c.size -= entry.size
}

// Remove removes a specific key from the cache
func (c *BlockCache) Remove(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		entry := elem.Value.(*cacheEntry)
		delete(c.items, key)
		c.lru.Remove(elem)
		c.size -= entry.size
	}
}

// Clear removes all items from the cache
func (c *BlockCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]*list.Element)
	c.lru = list.New()
	c.size = 0
}

// Size returns the current cache size in bytes
func (c *BlockCache) Size() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.size
}

// Capacity returns the maximum cache capacity in bytes
func (c *BlockCache) Capacity() int64 {
	return c.capacity
}

// Len returns the number of items in the cache
func (c *BlockCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}

// Stats returns cache statistics
func (c *BlockCache) Stats() BlockCacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return BlockCacheStats{
		ItemCount: len(c.items),
		Size:      c.size,
		Capacity:  c.capacity,
	}
}

// BlockCacheStats contains cache statistics
type BlockCacheStats struct {
	ItemCount int
	Size      int64
	Capacity  int64
}

// DefaultBlockCacheSize is the default cache size (64 MB)
const DefaultBlockCacheSize = 64 * 1024 * 1024

// DefaultBlockSize is the default block size for hints (64 KB)
const DefaultBlockSize = 64 * 1024
