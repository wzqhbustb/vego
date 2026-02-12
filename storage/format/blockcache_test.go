// Copyright 2024 Vego Authors
// Licensed under the Apache License, Version 2.0

package format

import (
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
	// Create cache that can hold exactly 2 items of 100 bytes
	cache := NewBlockCache(250)

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
	cache := NewBlockCache(250)

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
	if cache.Size() != 14 { // 6 + 8
		t.Errorf("Size = %d, want 14", cache.Size())
	}
}

// TestBlockCache_Stats tests statistics
func TestBlockCache_Stats(t *testing.T) {
	cache := NewBlockCache(1024)

	cache.Put("key1", []byte("value1"))
	cache.Put("key2", []byte("value2"))

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
}

// TestBlockCache_Concurrent tests concurrent access
func TestBlockCache_Concurrent(t *testing.T) {
	cache := NewBlockCache(10240)

	// Run multiple goroutines
	done := make(chan bool)

	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				key := string(rune('a' + id))
				cache.Put(key, []byte{byte(j)})
				cache.Get(key)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
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
}

// BenchmarkBlockCache_Get benchmarks cache get operations
func BenchmarkBlockCache_Get(b *testing.B) {
	cache := NewBlockCache(1024 * 1024) // 1 MB
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
