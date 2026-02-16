// Copyright 2024 Vego Authors
// Licensed under the Apache License, Version 2.0

package format

import (
	"fmt"
	"sync"
	"testing"
)

// TestBlockCache_Basic tests basic put/get operations
func TestBlockCache_Basic(t *testing.T) {
	cache := NewBlockCache(1024) // 1 KB cache

	// Test empty cache
	if data, ok := cache.Get("key1"); ok {
		t.Errorf("Get on empty cache should return false, got %v", data)
	}

	// Put and get
	data := []byte("test data")
	cache.Put("key1", data)

	got, ok := cache.Get("key1")
	if !ok {
		t.Error("Get should find the item")
	}
	if string(got) != string(data) {
		t.Errorf("Get = %q, want %q", got, data)
	}

	// Get non-existent key
	if _, ok := cache.Get("key2"); ok {
		t.Error("Get should not find non-existent key")
	}
}

// TestBlockCache_Update tests updating existing entries
func TestBlockCache_Update(t *testing.T) {
	cache := NewBlockCache(1024)

	// Put initial value
	cache.Put("key1", []byte("value1"))

	// Update value
	cache.Put("key1", []byte("value2"))

	got, ok := cache.Get("key1")
	if !ok {
		t.Fatal("Get should find the item")
	}
	if string(got) != "value2" {
		t.Errorf("Get = %q, want value2", got)
	}

	// Cache should have only 1 item
	if cache.Len() != 1 {
		t.Errorf("Len = %d, want 1", cache.Len())
	}
}

// TestBlockCache_Eviction tests LRU eviction
func TestBlockCache_Eviction(t *testing.T) {
	// Create cache with 1 shard to test eviction deterministically
	cache := NewBlockCache(250, 1)

	data1 := make([]byte, 100)
	data2 := make([]byte, 100)
	data3 := make([]byte, 100)

	// Add two items
	cache.Put("key1", data1)
	cache.Put("key2", data2)

	// Both should be present
	if _, ok := cache.Get("key1"); !ok {
		t.Error("key1 should be present")
	}
	if _, ok := cache.Get("key2"); !ok {
		t.Error("key2 should be present")
	}

	// Add third item, should evict key1 (oldest)
	cache.Put("key3", data3)

	// key1 should be evicted
	if _, ok := cache.Get("key1"); ok {
		t.Error("key1 should have been evicted")
	}

	// key2 and key3 should be present
	if _, ok := cache.Get("key2"); !ok {
		t.Error("key2 should be present")
	}
	if _, ok := cache.Get("key3"); !ok {
		t.Error("key3 should be present")
	}
}

// TestBlockCache_LRU tests that recently accessed items are not evicted
func TestBlockCache_LRU(t *testing.T) {
	// Use single shard for deterministic testing
	cache := NewBlockCache(250, 1)

	data1 := make([]byte, 100)
	data2 := make([]byte, 100)

	cache.Put("key1", data1)
	cache.Put("key2", data2)

	// Access key1 to make it recently used
	cache.Get("key1")

	// Add third item, should evict key2 (least recently used)
	data3 := make([]byte, 100)
	cache.Put("key3", data3)

	// key1 should still be present (was accessed)
	if _, ok := cache.Get("key1"); !ok {
		t.Error("key1 should be present (was accessed)")
	}

	// key2 should be evicted
	if _, ok := cache.Get("key2"); ok {
		t.Error("key2 should have been evicted")
	}

	// key3 should be present
	if _, ok := cache.Get("key3"); !ok {
		t.Error("key3 should be present")
	}
}

// TestBlockCache_Remove tests removing specific keys
func TestBlockCache_Remove(t *testing.T) {
	cache := NewBlockCache(1024)

	cache.Put("key1", []byte("value1"))
	cache.Put("key2", []byte("value2"))

	// Remove key1
	cache.Remove("key1")

	if _, ok := cache.Get("key1"); ok {
		t.Error("key1 should be removed")
	}

	// key2 should still be present
	if _, ok := cache.Get("key2"); !ok {
		t.Error("key2 should be present")
	}

	// Cache size should be updated
	if cache.Size() != 6 { // "value2" = 6 bytes
		t.Errorf("Size = %d, want 6", cache.Size())
	}
}

// TestBlockCache_Invalidate tests invalidating specific keys
func TestBlockCache_Invalidate(t *testing.T) {
	cache := NewBlockCache(1024)

	cache.Put("key1", []byte("value1"))
	cache.Invalidate("key1")

	if _, ok := cache.Get("key1"); ok {
		t.Error("key1 should be invalidated")
	}
}

// TestBlockCache_Clear tests clearing the cache
func TestBlockCache_Clear(t *testing.T) {
	cache := NewBlockCache(1024)

	cache.Put("key1", []byte("value1"))
	cache.Put("key2", []byte("value2"))

	cache.Clear()

	if cache.Len() != 0 {
		t.Errorf("Len = %d, want 0", cache.Len())
	}
	if cache.Size() != 0 {
		t.Errorf("Size = %d, want 0", cache.Size())
	}

	// Items should not be present
	if _, ok := cache.Get("key1"); ok {
		t.Error("key1 should not be present after clear")
	}
}

// TestBlockCache_Size tests size tracking
func TestBlockCache_Size(t *testing.T) {
	cache := NewBlockCache(1024)

	data := []byte("12345678") // 8 bytes
	cache.Put("key1", data)

	if cache.Size() != 8 {
		t.Errorf("Size = %d, want 8", cache.Size())
	}

	cache.Put("key2", data)
	if cache.Size() != 16 {
		t.Errorf("Size = %d, want 16", cache.Size())
	}

	// Update should adjust size
	cache.Put("key1", []byte("123456")) // 6 bytes
	if cache.Size() != 14 {             // 6 + 8
		t.Errorf("Size = %d, want 14", cache.Size())
	}
}

// TestBlockCache_Stats tests statistics
func TestBlockCache_Stats(t *testing.T) {
	cache := NewBlockCache(1024)

	cache.Put("key1", []byte("value1"))
	cache.Put("key2", []byte("value2"))

	// Access key1 twice and key2 once
	cache.Get("key1")
	cache.Get("key1")
	cache.Get("key2")

	// Miss
	cache.Get("key3")

	stats := cache.Stats()

	if stats.ItemCount != 2 {
		t.Errorf("ItemCount = %d, want 2", stats.ItemCount)
	}
	if stats.Capacity != 1024 {
		t.Errorf("Capacity = %d, want 1024", stats.Capacity)
	}
	if stats.Size != 12 { // "value1" + "value2" = 12 bytes
		t.Errorf("Size = %d, want 12", stats.Size)
	}
	if stats.Hits != 3 {
		t.Errorf("Hits = %d, want 3", stats.Hits)
	}
	if stats.Misses != 1 {
		t.Errorf("Misses = %d, want 1", stats.Misses)
	}
	expectedHitRate := 3.0 / 4.0
	if stats.HitRate != expectedHitRate {
		t.Errorf("HitRate = %f, want %f", stats.HitRate, expectedHitRate)
	}
}

// TestBlockCache_ResetStats tests resetting statistics
func TestBlockCache_ResetStats(t *testing.T) {
	cache := NewBlockCache(1024)

	cache.Put("key1", []byte("value1"))
	cache.Get("key1")
	cache.Get("missing")

	stats := cache.Stats()
	if stats.Hits != 1 || stats.Misses != 1 {
		t.Error("Expected hits and misses to be recorded")
	}

	cache.ResetStats()

	stats = cache.Stats()
	if stats.Hits != 0 || stats.Misses != 0 || stats.HitRate != 0 {
		t.Error("Expected stats to be reset")
	}
}

// TestBlockCache_Concurrent tests concurrent access
func TestBlockCache_Concurrent(t *testing.T) {
	cache := NewBlockCache(10240, 16) // 16 shards

	// Run multiple goroutines
	var wg sync.WaitGroup
	numGoroutines := 10
	opsPerGoroutine := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				key := string(rune('a' + id%26))
				cache.Put(key, []byte{byte(j)})
				cache.Get(key)
			}
		}(i)
	}

	wg.Wait()

	// Verify stats are consistent
	stats := cache.Stats()
	expectedOps := int64(numGoroutines * opsPerGoroutine)
	if stats.Hits+stats.Misses != expectedOps {
		t.Errorf("Total operations = %d, want %d", stats.Hits+stats.Misses, expectedOps)
	}
}

// TestBlockCache_ConcurrentDifferentKeys tests concurrent access with different keys
func TestBlockCache_ConcurrentDifferentKeys(t *testing.T) {
	cache := NewBlockCache(1024*1024, 64) // 1MB cache with 64 shards

	var wg sync.WaitGroup
	numGoroutines := 100
	keysPerGoroutine := 50

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < keysPerGoroutine; j++ {
				key := string(rune('a'+id%26)) + string(rune('0'+j%10))
				data := []byte{byte(id), byte(j)}
				cache.Put(key, data)
			}
			for j := 0; j < keysPerGoroutine; j++ {
				key := string(rune('a'+id%26)) + string(rune('0'+j%10))
				cache.Get(key)
			}
		}(i)
	}

	wg.Wait()

	stats := cache.Stats()
	t.Logf("Concurrent test stats: %+v", stats)
	if stats.ItemCount == 0 {
		t.Error("Expected some items in cache")
	}
}

// TestBlockCache_LargeItem tests that items larger than capacity are handled
func TestBlockCache_LargeItem(t *testing.T) {
	// Small cache
	cache := NewBlockCache(10)

	// Try to put large item
	largeData := make([]byte, 100)
	cache.Put("key1", largeData)

	// Item should not be cached (larger than capacity)
	if cache.Size() > cache.Capacity() {
		t.Errorf("Size %d should not exceed capacity %d", cache.Size(), cache.Capacity())
	}

	// Should be empty or 0
	if cache.Len() != 0 {
		t.Errorf("Cache should be empty for oversized item, got %d items", cache.Len())
	}
}

// TestBlockCache_DataIsolation tests that returned data cannot modify cache
func TestBlockCache_DataIsolation(t *testing.T) {
	cache := NewBlockCache(1024)

	original := []byte("original")
	cache.Put("key1", original)

	// Get the data
	got, ok := cache.Get("key1")
	if !ok {
		t.Fatal("Should get the item")
	}

	// Modify the returned data
	got[0] = 'X'

	// Get again, should still be original
	got2, ok := cache.Get("key1")
	if !ok {
		t.Fatal("Should get the item again")
	}

	if string(got2) != string(original) {
		t.Errorf("Cache data was modified! Got %q, want %q", got2, original)
	}
}

// TestBlockCache_ShardDistribution tests that keys are distributed across shards
func TestBlockCache_ShardDistribution(t *testing.T) {
	numShards := 8
	cache := NewBlockCache(1024*1024, numShards)

	// Put many keys
	for i := 0; i < 1000; i++ {
		key := string(rune('a' + i%26)) + string(rune('0'+i/26))
		cache.Put(key, []byte{byte(i)})
	}

	// Check that shards have data distributed
	nonEmptyShards := 0
	for i := 0; i < numShards; i++ {
		if cache.shards[i].lru.Len() > 0 {
			nonEmptyShards++
		}
	}

	// Most shards should have some data
	if nonEmptyShards < numShards/2 {
		t.Errorf("Keys not well distributed: only %d/%d shards have data", nonEmptyShards, numShards)
	}

	if cache.ShardCount() != numShards {
		t.Errorf("ShardCount = %d, want %d", cache.ShardCount(), numShards)
	}
}

// TestBlockCache_CapacityBoundary tests cache behavior at capacity boundaries
func TestBlockCache_CapacityBoundary(t *testing.T) {
	// Use single shard for deterministic testing
	cache := NewBlockCache(100, 1)

	// Add data to fill exactly 100 bytes
	cache.Put("key1", make([]byte, 50))
	cache.Put("key2", make([]byte, 50))

	if cache.Size() != 100 {
		t.Errorf("Size = %d, want 100", cache.Size())
	}

	// Both should be retrievable
	if _, ok := cache.Get("key1"); !ok {
		t.Error("key1 should be in cache")
	}
	if _, ok := cache.Get("key2"); !ok {
		t.Error("key2 should be in cache")
	}

	// Add a new item that requires eviction
	cache.Put("key3", make([]byte, 10))

	// key1 should be evicted (LRU), key2 and key3 should remain
	if _, ok := cache.Get("key1"); ok {
		t.Error("key1 should be evicted (LRU)")
	}
	if _, ok := cache.Get("key2"); !ok {
		t.Error("key2 should still be in cache")
	}
	if _, ok := cache.Get("key3"); !ok {
		t.Error("key3 should be in cache")
	}

	// Size should not exceed capacity
	if cache.Size() > cache.Capacity() {
		t.Errorf("Size %d should not exceed capacity %d", cache.Size(), cache.Capacity())
	}
}

// TestBlockCache_PerShardCapacityLimit tests that items larger than per-shard capacity are rejected
func TestBlockCache_PerShardCapacityLimit(t *testing.T) {
	// 1 MB total with 64 shards = ~16KB per shard
	cache := NewBlockCache(1024*1024, 64)
	perShardCapacity := cache.Capacity() / int64(cache.ShardCount())

	// Create item that exceeds per-shard capacity but fits in total capacity
	largeItem := make([]byte, perShardCapacity+1000)

	// Try to cache the large item
	cache.Put("large_key", largeItem)

	// Should not be cached (exceeds per-shard capacity)
	if cache.Len() != 0 {
		t.Errorf("Large item should not be cached, got %d items", cache.Len())
	}

	// Smaller item should be cached fine
	smallItem := make([]byte, perShardCapacity/2)
	cache.Put("small_key", smallItem)

	if cache.Len() != 1 {
		t.Errorf("Small item should be cached, got %d items", cache.Len())
	}
}

// BenchmarkBlockCache_Get benchmarks cache get operations
func BenchmarkBlockCache_Get(b *testing.B) {
	cache := NewBlockCache(1024*1024) // 1 MB
	cache.Put("key", []byte("value"))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get("key")
	}
}

// BenchmarkBlockCache_Put benchmarks cache put operations
func BenchmarkBlockCache_Put(b *testing.B) {
	cache := NewBlockCache(1024 * 1024) // 1 MB
	data := []byte("benchmark data for cache put operation")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Put(string(rune(i%100)), data)
	}
}

// BenchmarkBlockCache_ConcurrentGet benchmarks concurrent get operations
func BenchmarkBlockCache_ConcurrentGet(b *testing.B) {
	cache := NewBlockCache(1024*1024, 64)

	// Populate cache
	for i := 0; i < 1000; i++ {
		key := string(rune('a' + i%26))
		cache.Put(key, []byte{byte(i)})
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := string(rune('a' + i%26))
			cache.Get(key)
			i++
		}
	})
}

// BenchmarkBlockCache_ConcurrentPut benchmarks concurrent put operations
func BenchmarkBlockCache_ConcurrentPut(b *testing.B) {
	cache := NewBlockCache(1024*1024, 64)
	data := []byte("concurrent benchmark data")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := string(rune('a'+i%26)) + string(rune('0'+i/26%10))
			cache.Put(key, data)
			i++
		}
	})
}

// BenchmarkBlockCache_ConcurrentMixed benchmarks mixed concurrent operations
func BenchmarkBlockCache_ConcurrentMixed(b *testing.B) {
	cache := NewBlockCache(1024*1024, 64)
	data := []byte("mixed benchmark data")

	// Pre-populate
	for i := 0; i < 100; i++ {
		cache.Put(string(rune(i)), data)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%3 == 0 {
				// 33% writes
				cache.Put(string(rune(i%100)), data)
			} else {
				// 67% reads
				cache.Get(string(rune(i % 100)))
			}
			i++
		}
	})
}

// BenchmarkBlockCache_ShardedVsNonSharded compares performance with different shard counts
func BenchmarkBlockCache_ShardedVsNonSharded(b *testing.B) {
	shardCounts := []int{1, 4, 16, 64}
	data := []byte("test data for sharding comparison")

	for _, shards := range shardCounts {
		b.Run(fmt.Sprintf("shards_%d", shards), func(b *testing.B) {
			cache := NewBlockCache(1024*1024, shards)

			// Pre-populate
			for i := 0; i < 100; i++ {
				cache.Put(string(rune(i)), data)
			}

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					if i%2 == 0 {
						cache.Put(string(rune(i%100)), data)
					} else {
						cache.Get(string(rune(i % 100)))
					}
					i++
				}
			})
		})
	}
}
