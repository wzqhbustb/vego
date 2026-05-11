package column

import (
	"github.com/wzqhbustb/vego/core"
	"github.com/wzqhbustb/vego/storage/format"
)

// BatchWriter writes RecordBatch data to a columnar file.
type BatchWriter interface {
	WriteRecordBatch(batch *core.RecordBatch) error
	Close() error
}

// BatchReader reads RecordBatch data from a columnar file.
type BatchReader interface {
	ReadRecordBatch() (*core.RecordBatch, error)
	Schema() *core.Schema
	NumRows() int64
	Close() error
}

// IndexedBatchWriter extends BatchWriter with RowIndex support for V1.1+ files.
type IndexedBatchWriter interface {
	BatchWriter
	AddRowID(docID string, rowIndex int64) error
	SetBlockSize(blockSize int32)
}

// IndexedBatchReader extends BatchReader with RowIndex support for V1.1+ files.
type IndexedBatchReader interface {
	BatchReader
	HasRowIndex() bool
	GetVersion() format.VersionPolicy
	LoadRowIndex() error
	GetRowIndex() *format.RowIndex
	LookupRowID(docID string) (int64, error)
	ReadRowAt(rowIdx int64) ([]interface{}, error)
}

// Compile-time assertions: ensure concrete types implement interfaces.
var (
	_ BatchWriter        = (*Writer)(nil)
	_ BatchReader        = (*Reader)(nil)
	_ IndexedBatchWriter = (*RowIndexWriter)(nil)
	_ IndexedBatchReader = (*RowIndexReader)(nil)
)
