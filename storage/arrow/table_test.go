package arrow

import "testing"

func TestNewTable(t *testing.T) {
	schema := NewSchema([]Field{
		NewField("id", PrimInt32(), false),
		NewField("value", PrimFloat32(), false),
	}, nil)

	// Create two batches
	batch1, _ := NewRecordBatch(schema, 2, []Array{
		NewInt32Array([]int32{1, 2}, nil),
		NewFloat32Array([]float32{1.1, 2.2}, nil),
	})

	batch2, _ := NewRecordBatch(schema, 3, []Array{
		NewInt32Array([]int32{3, 4, 5}, nil),
		NewFloat32Array([]float32{3.3, 4.4, 5.5}, nil),
	})

	table, err := NewTable(schema, []*RecordBatch{batch1, batch2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if table.NumRows() != 5 {
		t.Errorf("expected 5 rows, got %d", table.NumRows())
	}

	if table.NumCols() != 2 {
		t.Errorf("expected 2 columns, got %d", table.NumCols())
	}

	if table.NumChunks() != 2 {
		t.Errorf("expected 2 chunks, got %d", table.NumChunks())
	}
}

func TestTableChunkAccess(t *testing.T) {
	schema := NewSchema([]Field{
		NewField("id", PrimInt32(), false),
	}, nil)

	batch1, _ := NewRecordBatch(schema, 2, []Array{
		NewInt32Array([]int32{1, 2}, nil),
	})

	batch2, _ := NewRecordBatch(schema, 3, []Array{
		NewInt32Array([]int32{3, 4, 5}, nil),
	})

	table, _ := NewTable(schema, []*RecordBatch{batch1, batch2})

	chunk0 := table.Chunk(0)
	if chunk0.NumRows() != 2 {
		t.Errorf("chunk 0: expected 2 rows, got %d", chunk0.NumRows())
	}

	chunk1 := table.Chunk(1)
	if chunk1.NumRows() != 3 {
		t.Errorf("chunk 1: expected 3 rows, got %d", chunk1.NumRows())
	}

	chunks := table.Chunks()
	if len(chunks) != 2 {
		t.Errorf("expected 2 chunks, got %d", len(chunks))
	}
}

func TestTableSchemaMismatch(t *testing.T) {
	schema1 := NewSchema([]Field{
		NewField("id", PrimInt32(), false),
	}, nil)

	schema2 := NewSchema([]Field{
		NewField("different", PrimInt32(), false),
	}, nil)

	batch1, _ := NewRecordBatch(schema1, 2, []Array{
		NewInt32Array([]int32{1, 2}, nil),
	})

	batch2, _ := NewRecordBatch(schema2, 2, []Array{
		NewInt32Array([]int32{3, 4}, nil),
	})

	_, err := NewTable(schema1, []*RecordBatch{batch1, batch2})
	if err == nil {
		t.Error("expected error for schema mismatch")
	}
}

func TestTableBuilder(t *testing.T) {
	schema := NewSchema([]Field{
		NewField("id", PrimInt32(), false),
		NewField("value", PrimFloat32(), false),
	}, nil)

	builder := NewTableBuilder(schema)

	// Add first batch
	batch1, _ := NewRecordBatch(schema, 2, []Array{
		NewInt32Array([]int32{1, 2}, nil),
		NewFloat32Array([]float32{1.1, 2.2}, nil),
	})
	err := builder.AppendBatch(batch1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Add second batch
	batch2, _ := NewRecordBatch(schema, 3, []Array{
		NewInt32Array([]int32{3, 4, 5}, nil),
		NewFloat32Array([]float32{3.3, 4.4, 5.5}, nil),
	})
	err = builder.AppendBatch(batch2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	table, err := builder.NewTable()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if table.NumRows() != 5 {
		t.Errorf("expected 5 rows, got %d", table.NumRows())
	}

	if table.NumChunks() != 2 {
		t.Errorf("expected 2 chunks, got %d", table.NumChunks())
	}
}

func TestTableBuilderSchemaMismatch(t *testing.T) {
	schema1 := NewSchema([]Field{
		NewField("id", PrimInt32(), false),
	}, nil)

	schema2 := NewSchema([]Field{
		NewField("different", PrimInt32(), false),
	}, nil)

	builder := NewTableBuilder(schema1)

	batch, _ := NewRecordBatch(schema2, 2, []Array{
		NewInt32Array([]int32{1, 2}, nil),
	})

	err := builder.AppendBatch(batch)
	if err == nil {
		t.Error("expected error for schema mismatch")
	}
}

func TestTableRelease(t *testing.T) {
	schema := NewSchema([]Field{
		NewField("id", PrimInt32(), false),
	}, nil)

	batch, _ := NewRecordBatch(schema, 2, []Array{
		NewInt32Array([]int32{1, 2}, nil),
	})

	table, _ := NewTable(schema, []*RecordBatch{batch})

	// Release should not panic
	table.Release()
}

func TestTableString(t *testing.T) {
	schema := NewSchema([]Field{
		NewField("id", PrimInt32(), false),
	}, nil)

	batch, _ := NewRecordBatch(schema, 2, []Array{
		NewInt32Array([]int32{1, 2}, nil),
	})

	table, _ := NewTable(schema, []*RecordBatch{batch})

	str := table.String()
	if str == "" {
		t.Error("table string representation is empty")
	}
}

func TestTableWithHNSWSchema(t *testing.T) {
	schema := SchemaForVectors(768)

	// Create multiple batches
	var batches []*RecordBatch
	for b := 0; b < 3; b++ {
		builder := NewRecordBatchBuilder(schema)

		for i := 0; i < 10; i++ {
			id := b*10 + i
			builder.Field(0).(*Int32Builder).Append(int32(id))

			vec := make([]float32, 768)
			for j := range vec {
				vec[j] = float32(id*768 + j)
			}
			builder.Field(1).(*FixedSizeListBuilder).AppendValues(vec)

			builder.Field(2).(*Int32Builder).Append(int32(id % 5))
		}

		batch, _ := builder.NewBatch()
		batches = append(batches, batch)
	}

	table, err := NewTable(schema, batches)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if table.NumRows() != 30 {
		t.Errorf("expected 30 rows, got %d", table.NumRows())
	}

	if table.NumChunks() != 3 {
		t.Errorf("expected 3 chunks, got %d", table.NumChunks())
	}

	// Verify first chunk
	chunk0 := table.Chunk(0)
	idCol := chunk0.Column(0).(*Int32Array)
	if idCol.Value(0) != 0 || idCol.Value(9) != 9 {
		t.Error("chunk 0 id values incorrect")
	}
}

// Benchmark table creation
func BenchmarkTableCreation(b *testing.B) {
	schema := SchemaForVectors(768)

	// Prepare batches
	var batches []*RecordBatch
	for i := 0; i < 10; i++ {
		builder := NewRecordBatchBuilder(schema)
		for j := 0; j < 100; j++ {
			builder.Field(0).(*Int32Builder).Append(int32(j))
			vec := make([]float32, 768)
			builder.Field(1).(*FixedSizeListBuilder).AppendValues(vec)
			builder.Field(2).(*Int32Builder).Append(int32(j % 5))
		}
		batch, _ := builder.NewBatch()
		batches = append(batches, batch)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = NewTable(schema, batches)
	}
}
