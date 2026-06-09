package column

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"time"

	"github.com/wzqhbustb/vego/core"
	"github.com/wzqhbustb/vego/storage/encoding"
	"github.com/wzqhbustb/vego/storage/format"
	"github.com/wzqhbustb/vego/vfs"
)

const (
	// HeaderReservedSize is the fixed size reserved for file header
	// This ensures header can be rewritten without affecting page offsets
	HeaderReservedSize = 8192 // 8KB should be enough for any reasonable schema

	// tempFileInfix is the infix used for temporary files during atomic writes.
	// Using a project-specific infix avoids collision with user files.
	tempFileInfix = ".vego-tmp."
)

// tempFileName generates a temporary file name for atomic writes.
func tempFileName(finalPath string) string {
	return finalPath + tempFileInfix + fmt.Sprintf("%d-%x", time.Now().UnixNano(), rand.Int63())
}

// Writer writes RecordBatch data to a Lance file.
//
// Thread Safety: Writer is NOT safe for concurrent use. WriteRecordBatch and Close
// must be called from a single goroutine. If you need concurrent writes, external
// synchronization is required.
type Writer struct {
	file       vfs.File
	fs         vfs.VFS  // filesystem reference for Rename/Remove
	finalPath  string   // target file path
	tmpPath    string   // temporary file path during writing
	header     *format.Header
	footer     *format.Footer
	pageWriter *PageWriter
	headerSize int64 // Always equals HeaderReservedSize
	currentPos int64 // Current write position
	factory    *encoding.EncoderFactory
	closed     bool
	columnStats *format.StatisticsList // Accumulated column statistics across all batches
	asyncWrite bool                    // Enable parallel column encoding
}

// WriterOption configures a Writer.
type WriterOption func(*Writer)

// WithAsyncWrite enables parallel column encoding (goroutine-per-column).
// Default is false (synchronous, zero goroutine overhead).
func WithAsyncWrite(enabled bool) WriterOption {
	return func(w *Writer) {
		w.asyncWrite = enabled
	}
}

// NewWriter creates a new column writer using the default local VFS.
func NewWriter(filename string, schema *core.Schema, factory *encoding.EncoderFactory, opts ...WriterOption) (*Writer, error) {
	return NewWriterWithVFS(filename, vfs.Local, schema, factory, opts...)
}

// NewWriterWithVFS creates a new column writer with a custom VFS.
func NewWriterWithVFS(filename string, fs vfs.VFS, schema *core.Schema, factory *encoding.EncoderFactory, opts ...WriterOption) (*Writer, error) {
	tmpPath := tempFileName(filename)
	file, err := fs.Create(tmpPath)
	if err != nil {
		return nil, core.IO("new_writer", filename, err)
	}

	if factory == nil {
		factory = encoding.NewEncoderFactory(3)
	}

	writer := &Writer{
		file:        file,
		fs:          fs,
		finalPath:   filename,
		tmpPath:     tmpPath,
		header:      format.NewHeader(schema, 0),
		footer:      format.NewFooter(),
		pageWriter:  NewPageWriter(factory), // 传递 factory
		factory:     factory,
		closed:      false,
		headerSize:  HeaderReservedSize,
		columnStats: format.NewStatisticsList(schema.NumFields()),
	}

	for _, opt := range opts {
		opt(writer)
	}

	if err := writer.writeHeaderWithPadding(); err != nil {
		file.Close()
		fs.Remove(tmpPath)
		return nil, core.New(core.ErrIO).
			Op("write_initial_header").
			Wrap(err).
			Build()
	}

	writer.currentPos = HeaderReservedSize

	return writer, nil
}

// writeHeaderWithPadding writes header and pads to HeaderReservedSize
func (w *Writer) writeHeaderWithPadding() error {
	// Serialize header to buffer first
	headerBuf := new(bytes.Buffer)
	_, err := w.header.WriteTo(headerBuf)
	if err != nil {
		return core.New(core.ErrIO).
			Op("serialize_header").
			Wrap(err).
			Build()
	}

	headerData := headerBuf.Bytes()
	headerLen := len(headerData)

	// Check if header fits in reserved space
	if headerLen > HeaderReservedSize {
		return core.New(core.ErrMetadataError).
			Op("write_header_with_padding").
			Context("header_size", headerLen).
			Context("reserved_size", HeaderReservedSize).
			Context("message", "header size exceeds reserved size").
			Build()
	}

	// Write header data
	if _, err := w.file.Write(headerData); err != nil {
		return core.IO("write_header_data", "", err)
	}

	// Write padding to fill reserved space
	paddingSize := HeaderReservedSize - headerLen
	if paddingSize > 0 {
		padding := make([]byte, paddingSize)
		if _, err := w.file.Write(padding); err != nil {
			return core.IO("write_header_padding", "", err)
		}
	}

	return nil
}

// WriteRecordBatch writes a RecordBatch to the file.
// When asyncWrite is enabled and the batch has more than one column,
// columns are encoded in parallel (goroutine-per-column), then written
// sequentially to preserve deterministic file layout.
func (w *Writer) WriteRecordBatch(batch *core.RecordBatch) error {
	if w.closed {
		return core.New(core.ErrInvalidArgument).
			Op("write_record_batch").
			Context("message", "writer is closed").
			Build()
	}

	if batch == nil {
		return core.New(core.ErrInvalidArgument).
			Op("write_record_batch").
			Context("message", "batch is nil").
			Build()
	}

	// Validate schema matches
	if !w.header.Schema.Equal(batch.Schema()) {
		return core.New(core.ErrSchemaMismatch).
			Op("write_record_batch").
			Context("message", "schema mismatch").
			Build()
	}

	// Update header row count
	w.header.NumRows += int64(batch.NumRows())

	if w.asyncWrite && batch.NumCols() > 1 {
		return w.writeRecordBatchAsync(batch)
	}

	// Synchronous path: one column at a time
	for colIdx := 0; colIdx < batch.NumCols(); colIdx++ {
		if err := w.writeColumnSync(colIdx, batch); err != nil {
			return err
		}
	}

	return nil
}

// writeRecordBatchAsync encodes all columns in parallel, then writes pages
// sequentially to preserve deterministic file layout.
func (w *Writer) writeRecordBatchAsync(batch *core.RecordBatch) error {
	type colResult struct {
		pages []*format.Page
		stats *format.ColumnStatistics
		err   error
	}

	numCols := batch.NumCols()
	results := make([]colResult, numCols)
	var wg sync.WaitGroup
	wg.Add(numCols)

	for colIdx := 0; colIdx < numCols; colIdx++ {
		go func(idx int) {
			defer wg.Done()
			column := batch.Column(idx)
			field := batch.Schema().Field(idx)
			results[idx].pages, results[idx].stats, results[idx].err = w.encodeColumn(int32(idx), column, field)
		}(colIdx)
	}
	wg.Wait()

	// Serial write: merge stats and write pages in column order
	for colIdx := 0; colIdx < numCols; colIdx++ {
		r := results[colIdx]
		field := batch.Schema().Field(colIdx)

		if r.err != nil {
			return r.err
		}

		// Accumulate column statistics
		if w.columnStats != nil && r.stats != nil {
			if existingStats := w.columnStats.GetColumnStats(int32(colIdx)); existingStats != nil {
				if err := existingStats.Merge(r.stats); err != nil {
					return core.New(core.ErrInvalidArgument).
						Op("accumulate_stats").
						Context("column_index", colIdx).
						Context("column_name", field.Name).
						Wrap(err).
						Build()
				}
			}
		}

		if err := w.writeColumnPages(int32(colIdx), r.pages); err != nil {
			return err
		}
	}

	return nil
}

// encodeColumn validates, computes statistics, and encodes a single column.
// Safe for concurrent use (only touches PageWriter, which is stateless).
func (w *Writer) encodeColumn(colIdx int32, column core.Array, field core.Field) (
	pages []*format.Page, stats *format.ColumnStatistics, err error,
) {
	if err = validateArray(column, field); err != nil {
		return nil, nil, core.New(core.ErrInvalidArgument).
			Op("write_record_batch").
			Context("column_index", colIdx).
			Context("column_name", field.Name).
			Context("message", "column validation failed").
			Wrap(err).
			Build()
	}

	if w.columnStats != nil {
		stats = format.ComputeColumnStatistics(column, colIdx)
	}

	pages, err = w.pageWriter.WritePages(column, colIdx)
	if err != nil {
		return nil, nil, core.New(core.ErrEncodeFailed).
			Op("write_column").
			Context("column_index", colIdx).
			Context("column_name", field.Name).
			Context("message", "create pages failed").
			Wrap(err).
			Build()
	}

	return pages, stats, nil
}

// writeColumnSync handles the full synchronous write for one column.
func (w *Writer) writeColumnSync(colIdx int, batch *core.RecordBatch) error {
	column := batch.Column(colIdx)
	field := batch.Schema().Field(colIdx)
	idx32 := int32(colIdx)

	pages, batchStats, err := w.encodeColumn(idx32, column, field)
	if err != nil {
		return err
	}

	if w.columnStats != nil && batchStats != nil {
		if existingStats := w.columnStats.GetColumnStats(idx32); existingStats != nil {
			if err := existingStats.Merge(batchStats); err != nil {
				return core.New(core.ErrInvalidArgument).
					Op("accumulate_stats").
					Context("column_index", colIdx).
					Context("column_name", field.Name).
					Wrap(err).
					Build()
			}
		}
	}

	if err := w.writeColumnPages(idx32, pages); err != nil {
		return err
	}

	return nil
}

// writeColumnPages writes pre-encoded pages to the file and updates footer metadata.
// Must be called sequentially (not concurrently) to preserve currentPos determinism.
func (w *Writer) writeColumnPages(columnIndex int32, pages []*format.Page) error {
	for pageNum, page := range pages {
		// Record current position (relative to file start)
		pageOffset := w.currentPos

		// Write page to file
		n, err := page.WriteTo(w.file)
		if err != nil {
			return core.IO("write_page", "", err)
		}

		// Update position
		w.currentPos += n

		// Add page index to footer
		w.footer.PageIndexList.Add(
			columnIndex,
			int32(pageNum),
			pageOffset,
			int32(n),
			page.NumValues,
			page.Encoding,
		)
	}

	return nil
}

// cleanupTemp removes the temporary file if it exists.
func (w *Writer) cleanupTemp() {
	if w.tmpPath != "" {
		_ = w.fs.Remove(w.tmpPath)
	}
}

// Close finalizes the file by writing header and footer, then performs
// atomic rename from temp file to final path.
func (w *Writer) Close() error {
	if w.closed {
		return core.New(core.ErrInvalidArgument).
			Op("close_writer").
			Context("message", "writer already closed").
			Build()
	}

	w.closed = true

	// Update footer
	w.footer.NumPages = int32(len(w.footer.PageIndexList.Indices))

	// Write column statistics (before footer)
	if w.columnStats != nil && w.columnStats.NumColumns > 0 {
		// Set the current position as stats offset
		w.footer.StatsOffset = w.currentPos
		w.footer.StatsCount = int32(w.columnStats.NumColumns)
		
		// Seek to stats position and write
		if _, err := w.file.Seek(w.currentPos, io.SeekStart); err != nil {
			w.cleanupTemp()
			return core.IO("seek_stats", "", err)
		}
		
		n, err := w.columnStats.WriteTo(w.file)
		if err != nil {
			w.cleanupTemp()
			return core.IO("write_stats", "", err)
		}
		w.currentPos += n
	}

	// Write footer at current position (after stats)
	if _, err := w.file.Seek(w.currentPos, io.SeekStart); err != nil {
		w.cleanupTemp()
		return core.IO("seek_footer", "", err)
	}

	if _, err := w.footer.WriteTo(w.file); err != nil {
		w.cleanupTemp()
		return core.IO("write_footer", "", err)
	}

	// Update header with final NumRows
	// Serialize to buffer first to check size
	headerBuf := new(bytes.Buffer)
	if _, err := w.header.WriteTo(headerBuf); err != nil {
		w.cleanupTemp()
		return core.New(core.ErrIO).
			Op("serialize_final_header").
			Wrap(err).
			Build()
	}

	headerData := headerBuf.Bytes()
	headerLen := len(headerData)

	// Verify header still fits in reserved space
	if headerLen > HeaderReservedSize {
		w.cleanupTemp()
		return core.New(core.ErrMetadataError).
			Op("close_writer").
			Context("header_size", headerLen).
			Context("reserved_size", HeaderReservedSize).
			Context("message", "final header size exceeds reserved size").
			Build()
	}

	// Seek back to beginning and rewrite header
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		w.cleanupTemp()
		return core.IO("seek_header", "", err)
	}

	// Write updated header (no need to write padding again, it's already there)
	if _, err := w.file.Write(headerData); err != nil {
		w.cleanupTemp()
		return core.IO("rewrite_header", "", err)
	}

	// Sync file to ensure data is written to disk
	if err := w.file.Sync(); err != nil {
		w.cleanupTemp()
		return core.IO("sync_file", "", err)
	}

	// Close file
	if err := w.file.Close(); err != nil {
		w.cleanupTemp()
		return core.IO("close_file", "", err)
	}

	// Atomic rename: temp -> final
	if w.tmpPath != "" && w.finalPath != "" {
		if err := w.fs.Rename(w.tmpPath, w.finalPath); err != nil {
			w.cleanupTemp()
			return core.IO("rename_file", w.finalPath, err)
		}
	}

	return nil
}
