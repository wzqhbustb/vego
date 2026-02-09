package arrow

import "testing"

func TestNewRecordBatch(t *testing.T) {
	schema := NewSchema([]Field{
		NewField("id", PrimInt32(), false),
		NewField("value", PrimFloat32(), false),
	}, nil)

	idArr := NewInt32Array([]int32{1, 2, 3}, nil)
	valueArr := NewFloat32Array([]float32{1.1, 2.2, 3.3}, nil)

	batch, err := NewRecordBatch(schema, 3, []Array{idArr, valueArr})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if batch.NumRows() != 3 {
		t.Errorf("expected 3 rows, got %d", batch.NumRows())
	}

	if batch.NumCols() != 2 {
		t.Errorf("expected 2 columns, got %d", batch.NumCols())
	}

	if !batch.Schema().Equal(schema) {
		t.Error("schema mismatch")
	}
}

func TestRecordBatchColumnAccess(t *testing.T) {
	schema := NewSchema([]Field{
		NewField("id", PrimInt32(), false),
		NewField("value", PrimFloat32(), false),
	}, nil)

	idArr := NewInt32Array([]int32{1, 2, 3}, nil)
	valueArr := NewFloat32Array([]float32{1.1, 2.2, 3.3}, nil)

	batch, _ := NewRecordBatch(schema, 3, []Array{idArr, valueArr})

	// Access by index
	col0 := batch.Column(0).(*Int32Array)
	if col0.Value(0) != 1 {
		t.Error("column 0 value incorrect")
	}

	col1 := batch.Column(1).(*Float32Array)
	if col1.Value(1) != 2.2 {
		t.Error("column 1 value incorrect")
	}

	// Access by name
	idCol, found := batch.ColumnByName("id")
	if !found {
		t.Error("column 'id' not found")
	}
	if idCol.Len() != 3 {
		t.Error("id column length incorrect")
	}

	_, found = batch.ColumnByName("nonexistent")
	if found {
		t.Error("should not find nonexistent column")
	}
}

func TestRecordBatchValidation(t *testing.T) {
	schema := NewSchema([]Field{
		NewField("id", PrimInt32(), false),
		NewField("value", PrimFloat32(), false),
	}, nil)

	// Test: wrong number of columns
	idArr := NewInt32Array([]int32{1, 2, 3}, nil)
	_, err := NewRecordBatch(schema, 3, []Array{idArr})
	if err == nil {
		t.Error("expected error for wrong column count")
	}

	// Test: wrong row count
	valueArr := NewFloat32Array([]float32{1.1, 2.2}, nil) // Only 2 rows
	_, err = NewRecordBatch(schema, 3, []Array{idArr, valueArr})
	if err == nil {
		t.Error("expected error for row count mismatch")
	}

	// Test: wrong type
	wrongArr := NewFloat32Array([]float32{1.1, 2.2, 3.3}, nil)
	_, err = NewRecordBatch(schema, 3, []Array{wrongArr, valueArr})
	if err == nil {
		t.Error("expected error for type mismatch")
	}
}

func TestRecordBatchBuilder(t *testing.T) {
	schema := SchemaForVectors(768)
	builder := NewRecordBatchBuilder(schema)

	// Add 3 rows
	for i := 0; i < 3; i++ {
		builder.Field(0).(*Int32Builder).Append(int32(i))

		vec := make([]float32, 768)
		for j := range vec {
			vec[j] = float32(i*768 + j)
		}
		builder.Field(1).(*FixedSizeListBuilder).AppendValues(vec)

		builder.Field(2).(*Int32Builder).Append(int32(i % 3))
	}

	batch, err := builder.NewBatch()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if batch.NumRows() != 3 {
		t.Errorf("expected 3 rows, got %d", batch.NumRows())
	}

	if batch.NumCols() != 3 {
		t.Errorf("expected 3 columns, got %d", batch.NumCols())
	}

	// Verify data
	idCol := batch.Column(0).(*Int32Array)
	if idCol.Value(0) != 0 || idCol.Value(2) != 2 {
		t.Error("id column values incorrect")
	}

	vecCol := batch.Column(1).(*FixedSizeListArray)
	if vecCol.Len() != 3 {
		t.Error("vector column length incorrect")
	}

	vec0 := vecCol.ValueSlice(0).([]float32)
	if len(vec0) != 768 {
		t.Errorf("expected 768 dimensions, got %d", len(vec0))
	}
}

func TestRecordBatchBuilderValidation(t *testing.T) {
	schema := NewSchema([]Field{
		NewField("id", PrimInt32(), false),
		NewField("value", PrimFloat32(), false),
	}, nil)

	builder := NewRecordBatchBuilder(schema)

	// Add unequal number of elements
	builder.Field(0).(*Int32Builder).Append(1)
	builder.Field(0).(*Int32Builder).Append(2)
	builder.Field(1).(*Float32Builder).Append(1.1) // Only one element

	_, err := builder.NewBatch()
	if err == nil {
		t.Error("expected error for unequal row counts")
	}
}

func TestNewBuilderForType(t *testing.T) {
	tests := []struct {
		dtype        DataType
		expectedType string
	}{
		{PrimInt32(), "*arrow.Int32Builder"},
		{PrimInt64(), "*arrow.Int64Builder"},
		{PrimFloat32(), "*arrow.Float32Builder"},
		{PrimFloat64(), "*arrow.Float64Builder"},
		{FixedSizeListOf(PrimFloat32(), 3), "*arrow.FixedSizeListBuilder"},
		{ListOf(PrimInt32()), "*arrow.ListBuilder"},
	}

	for _, tt := range tests {
		builder := NewBuilderForType(tt.dtype)
		if builder == nil {
			t.Errorf("NewBuilderForType(%v) returned nil", tt.dtype.Name())
		}
	}
}

func TestRecordBatchRelease(t *testing.T) {
	schema := NewSchema([]Field{
		NewField("id", PrimInt32(), false),
	}, nil)

	idArr := NewInt32Array([]int32{1, 2, 3}, nil)
	batch, _ := NewRecordBatch(schema, 3, []Array{idArr})

	// Release should not panic
	batch.Release()
}

// Benchmark RecordBatch creation
func BenchmarkRecordBatchCreation(b *testing.B) {
	schema := SchemaForVectors(768)

	// Prepare data
	ids := make([]int32, 100)
	vectors := make([]float32, 100*768)
	levels := make([]int32, 100)
	for i := range ids {
		ids[i] = int32(i)
		levels[i] = int32(i % 5)
		for j := 0; j < 768; j++ {
			vectors[i*768+j] = float32(i*768 + j)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idArr := NewInt32Array(ids, nil)
		floatArr := NewFloat32Array(vectors, nil)
		vecType := VectorType(768).(*FixedSizeListType)
		vecArr := NewFixedSizeListArray(vecType, floatArr, nil)
		levelArr := NewInt32Array(levels, nil)

		_, _ = NewRecordBatch(schema, 100, []Array{idArr, vecArr, levelArr})
	}
}
