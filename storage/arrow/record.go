package arrow

import (
	"fmt"

	lerrors "github.com/wzqhbkjdx/vego/storage/errors"
)

// RecordBatch represents a collection of equal-length arrays (a "table slice")
// This is Arrow's fundamental unit for columnar data
type RecordBatch struct {
	schema  *Schema
	numRows int
	columns []Array
}

// NewRecordBatch creates a new record batch
func NewRecordBatch(schema *Schema, numRows int, columns []Array) (*RecordBatch, error) {
	if schema.NumFields() != len(columns) {
		return nil, lerrors.New(lerrors.ErrInvalidArgument).
			Op("new_record_batch").
			Context("schema_fields", schema.NumFields()).
			Context("column_count", len(columns)).
			Context("message", "schema/column count mismatch").
			Build()
	}

	for i, col := range columns {
		if col.Len() != numRows {
			return nil, lerrors.New(lerrors.ErrInvalidArgument).
				Op("new_record_batch").
				Context("column_index", i).
				Context("column_rows", col.Len()).
				Context("expected_rows", numRows).
				Context("message", "column row count mismatch").
				Build()
		}

		field := schema.Field(i)
		if col.DataType().ID() != field.Type.ID() {
			return nil, lerrors.TypeMismatch("new_record_batch", field.Name,
				field.Type.Name(), col.DataType().Name())
		}
	}

	return &RecordBatch{
		schema:  schema,
		numRows: numRows,
		columns: columns,
	}, nil
}

// Schema returns the schema
func (r *RecordBatch) Schema() *Schema {
	return r.schema
}

// NumRows returns the number of rows
func (r *RecordBatch) NumRows() int {
	return r.numRows
}

// NumCols returns the number of columns
func (r *RecordBatch) NumCols() int {
	return len(r.columns)
}

// Column returns the column at index i
func (r *RecordBatch) Column(i int) Array {
	return r.columns[i]
}

// ColumnByName returns the column with the given field name
func (r *RecordBatch) ColumnByName(name string) (Array, bool) {
	_, idx, ok := r.schema.FieldByName(name)
	if !ok {
		return nil, false
	}
	return r.columns[idx], true
}

// Columns returns all columns
func (r *RecordBatch) Columns() []Array {
	return r.columns
}

// Release releases all column arrays
func (r *RecordBatch) Release() {
	for _, col := range r.columns {
		col.Release()
	}
}

// String returns a human-readable representation
func (r *RecordBatch) String() string {
	return fmt.Sprintf("RecordBatch{schema: %s, rows: %d}", r.schema.String(), r.numRows)
}

// --- Typed column accessors ---

func (r *RecordBatch) Int32Column(i int) *Int32Array {
	return r.columns[i].(*Int32Array)
}

func (r *RecordBatch) Float32Column(i int) *Float32Array {
	return r.columns[i].(*Float32Array)
}

func (r *RecordBatch) VectorColumn(i int) *FixedSizeListArray {
	return r.columns[i].(*FixedSizeListArray)
}

// --- RecordBatchBuilder ---

// RecordBatchBuilder helps build record batches incrementally
type RecordBatchBuilder struct {
	schema   *Schema
	builders []Builder
}

// NewRecordBatchBuilder creates a new record batch builder
func NewRecordBatchBuilder(schema *Schema) *RecordBatchBuilder {
	builders := make([]Builder, schema.NumFields())

	for i := 0; i < schema.NumFields(); i++ {
		field := schema.Field(i)
		builders[i] = NewBuilderForType(field.Type)
	}

	return &RecordBatchBuilder{
		schema:   schema,
		builders: builders,
	}
}

// Field returns the builder for field at index i
func (b *RecordBatchBuilder) Field(i int) Builder {
	return b.builders[i]
}

// NewBatch creates a new record batch from the builders (resets builders)
func (b *RecordBatchBuilder) NewBatch() (*RecordBatch, error) {
	numRows := b.builders[0].Len()

	// Verify all builders have the same length
	for i, builder := range b.builders {
		if builder.Len() != numRows {
			return nil, lerrors.New(lerrors.ErrInvalidArgument).
				Op("record_batch_builder_new_batch").
				Context("builder_index", i).
				Context("builder_rows", builder.Len()).
				Context("expected_rows", numRows).
				Context("message", "builder row count mismatch").
				Build()
		}
	}

	// Build arrays
	columns := make([]Array, len(b.builders))
	for i, builder := range b.builders {
		columns[i] = builder.NewArray()
	}

	return NewRecordBatch(b.schema, numRows, columns)
}

// Release releases all builders
func (b *RecordBatchBuilder) Release() {
	for _, builder := range b.builders {
		builder.Release()
	}
}

// --- Helper: Create builder for a type ---

func NewBuilderForType(dtype DataType) Builder {
	switch dtype.ID() {
	case INT32:
		return NewInt32Builder()
	case INT64:
		return NewInt64Builder()
	case FLOAT32:
		return NewFloat32Builder()
	case FLOAT64:
		return NewFloat64Builder()
	case FIXED_SIZE_LIST:
		listType := dtype.(*FixedSizeListType)
		return NewFixedSizeListBuilder(listType)
	case LIST:
		listType := dtype.(*ListType)
		valueBuilder := NewBuilderForType(listType.Elem())
		return NewListBuilder(listType, valueBuilder)
	default:
		panic(fmt.Sprintf("unsupported type: %s", dtype.Name()))
	}
}
