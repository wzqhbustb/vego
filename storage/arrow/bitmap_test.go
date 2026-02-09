package arrow

import "testing"

func TestNewBitmap(t *testing.T) {
	bm := NewBitmap(100)
	if bm.Len() != 100 {
		t.Errorf("expected length 100, got %d", bm.Len())
	}

	// All bits should be 0 initially
	if bm.CountSet() != 0 {
		t.Errorf("expected 0 set bits, got %d", bm.CountSet())
	}
}

func TestBitmapSetClear(t *testing.T) {
	bm := NewBitmap(10)

	// Set bit 5
	bm.Set(5)
	if !bm.IsSet(5) {
		t.Error("bit 5 should be set")
	}
	if bm.CountSet() != 1 {
		t.Errorf("expected 1 set bit, got %d", bm.CountSet())
	}

	// Clear bit 5
	bm.Clear(5)
	if bm.IsSet(5) {
		t.Error("bit 5 should be clear")
	}
	if bm.CountSet() != 0 {
		t.Errorf("expected 0 set bits, got %d", bm.CountSet())
	}
}

func TestBitmapMultipleBits(t *testing.T) {
	bm := NewBitmap(16)

	// Set multiple bits
	bm.Set(0)
	bm.Set(3)
	bm.Set(7)
	bm.Set(15)

	if bm.CountSet() != 4 {
		t.Errorf("expected 4 set bits, got %d", bm.CountSet())
	}

	// Verify specific bits
	if !bm.IsSet(0) || !bm.IsSet(3) || !bm.IsSet(7) || !bm.IsSet(15) {
		t.Error("expected bits not set")
	}

	// Verify unset bits
	if bm.IsSet(1) || bm.IsSet(2) || bm.IsSet(8) {
		t.Error("unexpected bits set")
	}
}

func TestBitmapSetAll(t *testing.T) {
	bm := NewBitmap(10)
	bm.SetAll()

	if bm.CountSet() != 10 {
		t.Errorf("expected 10 set bits, got %d", bm.CountSet())
	}

	for i := 0; i < 10; i++ {
		if !bm.IsSet(i) {
			t.Errorf("bit %d should be set", i)
		}
	}
}

func TestBitmapClearAll(t *testing.T) {
	bm := NewBitmap(10)
	bm.SetAll()
	bm.ClearAll()

	if bm.CountSet() != 0 {
		t.Errorf("expected 0 set bits, got %d", bm.CountSet())
	}
}

func TestBitmapResize(t *testing.T) {
	bm := NewBitmap(5)
	bm.Set(0)
	bm.Set(4)

	// Resize to larger
	bm.Resize(10)
	if bm.Len() != 10 {
		t.Errorf("expected length 10, got %d", bm.Len())
	}

	// Original bits should be preserved
	if !bm.IsSet(0) || !bm.IsSet(4) {
		t.Error("original bits should be preserved")
	}

	// New bits should be unset
	if bm.IsSet(9) {
		t.Error("new bit should be unset")
	}

	// Resize to smaller
	bm.Resize(3)
	if bm.Len() != 3 {
		t.Errorf("expected length 3, got %d", bm.Len())
	}
}

func TestBitmapResizeNoOp(t *testing.T) {
	bm := NewBitmap(10)
	bm.Set(5)

	bm.Resize(10) // Same size

	if bm.Len() != 10 {
		t.Errorf("expected length 10, got %d", bm.Len())
	}
	if !bm.IsSet(5) {
		t.Error("bit should be preserved")
	}
}

func TestBitmapBoundaryPanic(t *testing.T) {
	bm := NewBitmap(10)

	// Test Set panic
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for out of bounds Set")
		}
	}()
	bm.Set(10) // Out of bounds
}

func TestBitmapNegativeIndexPanic(t *testing.T) {
	bm := NewBitmap(10)

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for negative index")
		}
	}()
	bm.Set(-1)
}

func TestNewBitmapAllSet(t *testing.T) {
	bm := NewBitmapAllSet(8)
	if bm.CountSet() != 8 {
		t.Errorf("expected 8 set bits, got %d", bm.CountSet())
	}
}

func TestBitmapAcrossByteBoundary(t *testing.T) {
	bm := NewBitmap(20)

	// Set bits across byte boundaries
	bm.Set(7)  // Last bit of first byte
	bm.Set(8)  // First bit of second byte
	bm.Set(15) // Last bit of second byte
	bm.Set(16) // First bit of third byte

	if bm.CountSet() != 4 {
		t.Errorf("expected 4 set bits, got %d", bm.CountSet())
	}
}

// Benchmark CountSet performance
func BenchmarkBitmapCountSet(b *testing.B) {
	bm := NewBitmap(10000)
	// Set every other bit
	for i := 0; i < 10000; i += 2 {
		bm.Set(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = bm.CountSet()
	}
}
