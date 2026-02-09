package arrow

import "testing"

func TestInt32Builder(t *testing.T) {
	builder := NewInt32Builder()

	builder.Append(1)
	builder.Append(2)
	builder.Append(3)

	if builder.Len() != 3 {
		t.Errorf("expected length 3, got %d", builder.Len())
	}

	arr := builder.NewArray()
	int32Arr := arr.(*Int32Array)

	if int32Arr.Len() != 3 {
		t.Errorf("expected array length 3, got %d", int32Arr.Len())
	}

	if int32Arr.Value(0) != 1 || int32Arr.Value(1) != 2 || int32Arr.Value(2) != 3 {
		t.Error("array values incorrect")
	}

	// Builder should be reset after NewArray
	if builder.Len() != 0 {
		t.Errorf("expected builder to be reset, got length %d", builder.Len())
	}
}

func TestInt32BuilderWithNulls(t *testing.T) {
	builder := NewInt32Builder()

	builder.Append(1)
	builder.AppendNull()
	builder.Append(3)
	builder.AppendNull()
	builder.Append(5)

	arr := builder.NewArray()
	int32Arr := arr.(*Int32Array)

	if int32Arr.Len() != 5 {
		t.Errorf("expected length 5, got %d", int32Arr.Len())
	}

	if int32Arr.NullN() != 2 {
		t.Errorf("expected 2 nulls, got %d", int32Arr.NullN())
	}

	if !int32Arr.IsNull(1) || !int32Arr.IsNull(3) {
		t.Error("expected nulls at indices 1 and 3")
	}

	if int32Arr.IsNull(0) || int32Arr.IsNull(2) || int32Arr.IsNull(4) {
		t.Error("unexpected nulls")
	}

	// Check non-null values
	if int32Arr.Value(0) != 1 || int32Arr.Value(2) != 3 || int32Arr.Value(4) != 5 {
		t.Error("non-null values incorrect")
	}
}

func TestInt32BuilderReserve(t *testing.T) {
	builder := NewInt32Builder()
	builder.Reserve(1000)

	for i := 0; i < 1000; i++ {
		builder.Append(int32(i))
	}

	arr := builder.NewArray()
	if arr.Len() != 1000 {
		t.Errorf("expected 1000 elements, got %d", arr.Len())
	}
}

func TestFloat32Builder(t *testing.T) {
	builder := NewFloat32Builder()

	builder.Append(1.1)
	builder.Append(2.2)
	builder.AppendNull()
	builder.Append(4.4)

	arr := builder.NewArray()
	float32Arr := arr.(*Float32Array)

	if float32Arr.Len() != 4 {
		t.Errorf("expected length 4, got %d", float32Arr.Len())
	}

	if float32Arr.Value(0) != 1.1 || float32Arr.Value(1) != 2.2 {
		t.Error("values incorrect")
	}

	if !float32Arr.IsNull(2) {
		t.Error("expected null at index 2")
	}
}

func TestFixedSizeListBuilder(t *testing.T) {
	listType := FixedSizeListOf(PrimFloat32(), 3).(*FixedSizeListType)
	builder := NewFixedSizeListBuilder(listType)

	builder.AppendValues([]float32{1.0, 2.0, 3.0})
	builder.AppendValues([]float32{4.0, 5.0, 6.0})
	builder.AppendValues([]float32{7.0, 8.0, 9.0})

	if builder.Len() != 3 {
		t.Errorf("expected 3 lists, got %d", builder.Len())
	}

	arr := builder.NewArray()
	listArr := arr.(*FixedSizeListArray)

	if listArr.Len() != 3 {
		t.Errorf("expected 3 lists in array, got %d", listArr.Len())
	}

	// Verify first list
	slice0 := listArr.ValueSlice(0).([]float32)
	if len(slice0) != 3 || slice0[0] != 1.0 || slice0[2] != 3.0 {
		t.Error("list 0 incorrect")
	}

	// Builder should be reset
	if builder.Len() != 0 {
		t.Error("builder not reset")
	}
}

func TestFixedSizeListBuilderWithNulls(t *testing.T) {
	listType := FixedSizeListOf(PrimFloat32(), 2).(*FixedSizeListType)
	builder := NewFixedSizeListBuilder(listType)

	builder.AppendValues([]float32{1.0, 2.0})
	builder.AppendNull()
	builder.AppendValues([]float32{5.0, 6.0})

	arr := builder.NewArray()
	listArr := arr.(*FixedSizeListArray)

	if listArr.NullN() != 1 {
		t.Errorf("expected 1 null, got %d", listArr.NullN())
	}

	if !listArr.IsNull(1) {
		t.Error("expected null at index 1")
	}
}

func TestFixedSizeListBuilderSizeMismatch(t *testing.T) {
	listType := FixedSizeListOf(PrimFloat32(), 3).(*FixedSizeListType)
	builder := NewFixedSizeListBuilder(listType)

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for size mismatch")
		}
	}()

	builder.AppendValues([]float32{1.0, 2.0}) // Wrong size
}

func TestFixedSizeListBuilder768DimVector(t *testing.T) {
	vecType := VectorType(768).(*FixedSizeListType)
	builder := NewFixedSizeListBuilder(vecType)

	// Add 10 vectors
	for i := 0; i < 10; i++ {
		vec := make([]float32, 768)
		for j := range vec {
			vec[j] = float32(i*768 + j)
		}
		builder.AppendValues(vec)
	}

	arr := builder.NewArray()
	vecArr := arr.(*FixedSizeListArray)

	if vecArr.Len() != 10 {
		t.Errorf("expected 10 vectors, got %d", vecArr.Len())
	}

	// Verify first vector
	vec0 := vecArr.ValueSlice(0).([]float32)
	if len(vec0) != 768 {
		t.Errorf("expected 768 dimensions, got %d", len(vec0))
	}
	if vec0[0] != 0.0 || vec0[767] != 767.0 {
		t.Error("vector 0 values incorrect")
	}
}

func TestListBuilder(t *testing.T) {
	listType := ListOf(PrimInt32()).(*ListType)
	valueBuilder := NewInt32Builder()
	builder := NewListBuilder(listType, valueBuilder)

	// First list: [1, 2]
	builder.Append(true)
	valueBuilder.Append(1)
	valueBuilder.Append(2)
	builder.UpdateOffset()

	// Second list: [3, 4, 5]
	builder.Append(true)
	valueBuilder.Append(3)
	valueBuilder.Append(4)
	valueBuilder.Append(5)
	builder.UpdateOffset()

	// Third list: null
	builder.AppendNull()

	if builder.Len() != 3 {
		t.Errorf("expected 3 lists, got %d", builder.Len())
	}

	arr := builder.NewArray()
	listArr := arr.(*ListArray)

	if listArr.Len() != 3 {
		t.Errorf("expected 3 lists in array, got %d", listArr.Len())
	}

	if listArr.NullN() != 1 {
		t.Errorf("expected 1 null, got %d", listArr.NullN())
	}

	// Check offsets
	start0, end0 := listArr.ValueOffsets(0)
	if end0-start0 != 2 {
		t.Errorf("first list should have 2 elements, got %d", end0-start0)
	}

	start1, end1 := listArr.ValueOffsets(1)
	if end1-start1 != 3 {
		t.Errorf("second list should have 3 elements, got %d", end1-start1)
	}
}

// Benchmark builder performance
func BenchmarkInt32BuilderAppend(b *testing.B) {
	builder := NewInt32Builder()
	builder.Reserve(b.N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		builder.Append(int32(i))
	}
}

func BenchmarkFloat32BuilderAppend(b *testing.B) {
	builder := NewFloat32Builder()
	builder.Reserve(b.N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		builder.Append(float32(i))
	}
}
