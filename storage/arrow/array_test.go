package arrow

import "testing"

func TestInt32Array(t *testing.T) {
	data := []int32{1, 2, 3, 4, 5}
	arr := NewInt32Array(data, nil)

	if arr.Len() != 5 {
		t.Errorf("expected length 5, got %d", arr.Len())
	}

	if arr.DataType().ID() != INT32 {
		t.Errorf("expected INT32 type, got %v", arr.DataType().ID())
	}

	if arr.NullN() != 0 {
		t.Errorf("expected 0 nulls, got %d", arr.NullN())
	}

	for i, expected := range data {
		if arr.Value(i) != expected {
			t.Errorf("element %d: expected %d, got %d", i, expected, arr.Value(i))
		}
		if arr.IsNull(i) {
			t.Errorf("element %d should not be null", i)
		}
	}

	values := arr.Values()
	if len(values) != 5 {
		t.Errorf("expected 5 values, got %d", len(values))
	}
}

func TestInt32ArrayWithNulls(t *testing.T) {
	data := []int32{1, 0, 3, 0, 5}
	nullBitmap := NewBitmap(5)
	nullBitmap.Set(0) // 1 is valid
	nullBitmap.Set(2) // 3 is valid
	nullBitmap.Set(4) // 5 is valid
	// indices 1 and 3 are null

	arr := NewInt32Array(data, nullBitmap)

	if arr.NullN() != 2 {
		t.Errorf("expected 2 nulls, got %d", arr.NullN())
	}

	if !arr.IsNull(1) || !arr.IsNull(3) {
		t.Error("indices 1 and 3 should be null")
	}

	if arr.IsNull(0) || arr.IsNull(2) || arr.IsNull(4) {
		t.Error("indices 0, 2, 4 should not be null")
	}

	if !arr.IsValid(0) || !arr.IsValid(2) {
		t.Error("valid check failed")
	}
}

func TestFloat32Array(t *testing.T) {
	data := []float32{1.1, 2.2, 3.3}
	arr := NewFloat32Array(data, nil)

	if arr.Len() != 3 {
		t.Errorf("expected length 3, got %d", arr.Len())
	}

	for i, expected := range data {
		if arr.Value(i) != expected {
			t.Errorf("element %d: expected %f, got %f", i, expected, arr.Value(i))
		}
	}
}

func TestInt64Array(t *testing.T) {
	data := []int64{100, 200, 300, 400}
	arr := NewInt64Array(data, nil)

	if arr.DataType().ID() != INT64 {
		t.Errorf("expected INT64 type, got %v", arr.DataType().ID())
	}

	if arr.Len() != 4 {
		t.Errorf("expected length 4, got %d", arr.Len())
	}

	for i, expected := range data {
		if arr.Value(i) != expected {
			t.Errorf("element %d: expected %d, got %d", i, expected, arr.Value(i))
		}
	}
}

func TestFloat64Array(t *testing.T) {
	data := []float64{1.111, 2.222}
	arr := NewFloat64Array(data, nil)

	if arr.DataType().ID() != FLOAT64 {
		t.Errorf("expected FLOAT64 type, got %v", arr.DataType().ID())
	}

	values := arr.Values()
	if len(values) != 2 {
		t.Errorf("expected 2 values, got %d", len(values))
	}
}

func TestFixedSizeListArray(t *testing.T) {
	// Create 3 vectors of dimension 4
	floatData := []float32{
		1.0, 2.0, 3.0, 4.0, // Vector 0
		5.0, 6.0, 7.0, 8.0, // Vector 1
		9.0, 10.0, 11.0, 12.0, // Vector 2
	}
	floatArr := NewFloat32Array(floatData, nil)

	listType := FixedSizeListOf(PrimFloat32(), 4).(*FixedSizeListType)
	listArr := NewFixedSizeListArray(listType, floatArr, nil)

	if listArr.Len() != 3 {
		t.Errorf("expected 3 lists, got %d", listArr.Len())
	}

	if listArr.ListSize() != 4 {
		t.Errorf("expected list size 4, got %d", listArr.ListSize())
	}

	// Test ValueSlice
	slice0 := listArr.ValueSlice(0).([]float32)
	if len(slice0) != 4 {
		t.Errorf("expected slice length 4, got %d", len(slice0))
	}
	expectedSlice0 := []float32{1.0, 2.0, 3.0, 4.0}
	for i, v := range expectedSlice0 {
		if slice0[i] != v {
			t.Errorf("vector 0 element %d: expected %f, got %f", i, v, slice0[i])
		}
	}

	// Test second vector
	slice1 := listArr.ValueSlice(1).([]float32)
	if slice1[0] != 5.0 || slice1[3] != 8.0 {
		t.Error("vector 1 values incorrect")
	}
}

func TestFixedSizeListArrayWith768DimVector(t *testing.T) {
	// Simulate real HNSW scenario
	vectorData := make([]float32, 768*2) // 2 vectors
	for i := range vectorData {
		vectorData[i] = float32(i)
	}

	floatArr := NewFloat32Array(vectorData, nil)
	vecType := VectorType(768).(*FixedSizeListType)
	vecArr := NewFixedSizeListArray(vecType, floatArr, nil)

	if vecArr.Len() != 2 {
		t.Errorf("expected 2 vectors, got %d", vecArr.Len())
	}

	vec0 := vecArr.ValueSlice(0).([]float32)
	if len(vec0) != 768 {
		t.Errorf("expected 768 dimensions, got %d", len(vec0))
	}

	if vec0[0] != 0.0 || vec0[767] != 767.0 {
		t.Error("vector 0 boundary values incorrect")
	}

	vec1 := vecArr.ValueSlice(1).([]float32)
	if vec1[0] != 768.0 || vec1[767] != 1535.0 {
		t.Error("vector 1 boundary values incorrect")
	}
}

func TestListArray(t *testing.T) {
	// Create a list of int32: [[1,2], [3,4,5], [6]]
	valueData := []int32{1, 2, 3, 4, 5, 6}
	valueArr := NewInt32Array(valueData, nil)

	offsets := []int32{0, 2, 5, 6}

	listType := ListOf(PrimInt32()).(*ListType)
	listArr := NewListArray(listType, offsets, valueArr, nil)

	if listArr.Len() != 3 {
		t.Errorf("expected 3 lists, got %d", listArr.Len())
	}

	// Check offsets
	start0, end0 := listArr.ValueOffsets(0)
	if start0 != 0 || end0 != 2 {
		t.Errorf("list 0 offsets: expected (0,2), got (%d,%d)", start0, end0)
	}

	start1, end1 := listArr.ValueOffsets(1)
	if start1 != 2 || end1 != 5 {
		t.Errorf("list 1 offsets: expected (2,5), got (%d,%d)", start1, end1)
	}

	start2, end2 := listArr.ValueOffsets(2)
	if start2 != 5 || end2 != 6 {
		t.Errorf("list 2 offsets: expected (5,6), got (%d,%d)", start2, end2)
	}
}

func TestArrayValueOutOfBounds(t *testing.T) {
	data := []int32{1, 2, 3}
	arr := NewInt32Array(data, nil)

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for out of bounds access")
		}
	}()

	arr.Value(10) // Out of bounds
}

func TestArrayRelease(t *testing.T) {
	data := []int32{1, 2, 3}
	arr := NewInt32Array(data, nil)

	// Release should not panic
	arr.Release()
}

// Benchmark array access
func BenchmarkInt32ArrayValue(b *testing.B) {
	data := make([]int32, 10000)
	for i := range data {
		data[i] = int32(i)
	}
	arr := NewInt32Array(data, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = arr.Value(i % 10000)
	}
}

func BenchmarkFloat32ArrayAccess(b *testing.B) {
	data := make([]float32, 768)
	for i := range data {
		data[i] = float32(i)
	}
	arr := NewFloat32Array(data, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = arr.Values()
	}
}
