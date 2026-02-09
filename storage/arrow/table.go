package arrow

import (
	"fmt"

	lerrors "github.com/wzqhbkjdx/vego/storage/errors"
)

// Table represents a collection of RecordBatches with the same schema
// This is Arrow's "dataset" abstraction
type Table struct {
	schema  *Schema
	chunks  []*RecordBatch
	numRows int64
}

// NewTable creates a new table from record batches
func NewTable(schema *Schema, chunks []*RecordBatch) (*Table, error) {
	// Validate all chunks have the same schema
	for i, chunk := range chunks {
		if !chunk.Schema().Equal(schema) {
			return nil, lerrors.New(lerrors.ErrSchemaMismatch).
				Op("new_table").
				Context("chunk_index", i).
				Context("message", "chunk schema mismatch with table schema").
				Build()
		}
	}

	// Calculate total rows
	var numRows int64
	for _, chunk := range chunks {
		numRows += int64(chunk.NumRows())
	}

	return &Table{
		schema:  schema,
		chunks:  chunks,
		numRows: numRows,
	}, nil
}

// Schema returns the table schema
func (t *Table) Schema() *Schema {
	return t.schema
}

// NumRows returns the total number of rows
func (t *Table) NumRows() int64 {
	return t.numRows
}

// NumCols returns the number of columns
func (t *Table) NumCols() int {
	return t.schema.NumFields()
}

// NumChunks returns the number of record batches
func (t *Table) NumChunks() int {
	return len(t.chunks)
}

// Chunk returns the record batch at index i
func (t *Table) Chunk(i int) *RecordBatch {
	return t.chunks[i]
}

// Chunks returns all record batches
func (t *Table) Chunks() []*RecordBatch {
	return t.chunks
}

// Release releases all record batches
func (t *Table) Release() {
	for _, chunk := range t.chunks {
		chunk.Release()
	}
}

// String returns a human-readable representation
func (t *Table) String() string {
	return fmt.Sprintf("Table{schema: %s, rows: %d, chunks: %d}",
		t.schema.String(), t.numRows, len(t.chunks))
}

// --- TableBuilder ---

// TableBuilder helps build tables incrementally
type TableBuilder struct {
	schema *Schema
	chunks []*RecordBatch
}

// NewTableBuilder creates a new table builder
func NewTableBuilder(schema *Schema) *TableBuilder {
	return &TableBuilder{
		schema: schema,
		chunks: make([]*RecordBatch, 0),
	}
}

// AppendBatch appends a record batch to the table
func (b *TableBuilder) AppendBatch(batch *RecordBatch) error {
	if !batch.Schema().Equal(b.schema) {
		return lerrors.New(lerrors.ErrSchemaMismatch).
			Op("table_builder_append_batch").
			Context("message", "batch schema mismatch with table schema").
			Build()
	}
	b.chunks = append(b.chunks, batch)
	return nil
}

// NewTable creates a new table from the builder
func (b *TableBuilder) NewTable() (*Table, error) {
	return NewTable(b.schema, b.chunks)
}

// Release releases all record batches
func (b *TableBuilder) Release() {
	for _, chunk := range b.chunks {
		chunk.Release()
	}
}
