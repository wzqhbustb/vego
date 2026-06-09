package column

import (
	"bytes"
	"io"
	"github.com/wzqhbustb/vego/core"
	"github.com/wzqhbustb/vego/storage/encoding"
	"github.com/wzqhbustb/vego/storage/format"
	"github.com/wzqhbustb/vego/vfs"
)

const (
	// HeaderReservedSize is the fixed size reserved for file header
	// This ensures header can be rewritten without affecting page offsets
	HeaderReservedSize = 8192 // 8KB should be enough for any reasonable schema
)

// Writer writes RecordBatch data to a Lance file.
//
// Thread Safety: Writer is NOT safe for concurrent use. WriteRecordBatch and Close
// must be called from a single goroutine. If you need concurrent writes, external
// synchronization is required.
type Writer struct {
	file       vfs.File
	header     *format.Header
	footer     *format.Footer
	pageWriter *PageWriter
	headerSize int64 // Always equals HeaderReservedSize
	currentPos int64 // Current write position
	factory    *encoding.EncoderFactory
	closed     bool
	columnStats *format.StatisticsList // Accumulated column statistics across all batches
}

// NewWriter creates a new column writer using the default local VFS.
func NewWriter(filename string, schema *core.Schema, factory *encoding.EncoderFactory) (*Writer, error) {
	return NewWriterWithVFS(filename, vfs.Local, schema, factory)
}

// NewWriterWithVFS creates a new column writer with a custom VFS.
func NewWriterWithVFS(filename string, fs vfs.VFS, schema *core.Schema, factory *encoding.EncoderFactory) (*Writer, error) {
	file, err := fs.Create(filename)
	if err != nil {
		return nil, core.IO("new_writer", filename, err)
	}

	if factory == nil {
		factory = encoding.NewEncoderFactory(3)
	}

	writer := &Writer{
		file:        file,
		header:      format.NewHeader(schema, 0),
		footer:      format.NewFooter(),
		pageWriter:  NewPageWriter(factory), // 传递 factory
		factory:     factory,
		closed:      false,
		headerSize:  HeaderReservedSize,
		columnStats: format.NewStatisticsList(schema.NumFields()),
	}

	if err := writer.writeHeaderWithPadding(); err != nil {
		file.Close()
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

// WriteRecordBatch writes a RecordBatch to the file
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

	// Write each column and accumulate statistics
	for colIdx := 0; colIdx < batch.NumCols(); colIdx++ {
		column := batch.Column(colIdx)
		field := batch.Schema().Field(colIdx)

		if err := validateArray(column, field); err != nil {
			return core.New(core.ErrInvalidArgument).
				Op("write_record_batch").
				Context("column_index", colIdx).
				Context("column_name", field.Name).
				Context("message", "column validation failed").
				Wrap(err).
				Build()
		}

		// Accumulate column statistics for this batch
		if w.columnStats != nil {
			batchStats := format.ComputeColumnStatistics(column, int32(colIdx))
			if existingStats := w.columnStats.GetColumnStats(int32(colIdx)); existingStats != nil {
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

		if err := w.writeColumn(int32(colIdx), column); err != nil {
			return core.New(core.ErrIO).
				Op("write_record_batch").
				Context("column_index", colIdx).
				Context("column_name", field.Name).
				Context("message", "write column failed").
				Wrap(err).
				Build()
		}
	}

	return nil
}

// writeColumn writes a single column (Array) to the file
func (w *Writer) writeColumn(columnIndex int32, array core.Array) error {
	// Convert array to pages
	pages, err := w.pageWriter.WritePages(array, columnIndex)
	if err != nil {
		return core.New(core.ErrEncodeFailed).
			Op("write_column").
			Context("message", "create pages failed").
			Wrap(err).
			Build()
	}

	// Write each page and record metadata
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
			page.Encoding, // 添加 encoding 参数
		)

	}

	return nil
}

// Close finalizes the file by writing header and footer
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
			return core.IO("seek_stats", "", err)
		}
		
		n, err := w.columnStats.WriteTo(w.file)
		if err != nil {
			return core.IO("write_stats", "", err)
		}
		w.currentPos += n
	}

	// Write footer at current position (after stats)
	if _, err := w.file.Seek(w.currentPos, io.SeekStart); err != nil {
		return core.IO("seek_footer", "", err)
	}

	if _, err := w.footer.WriteTo(w.file); err != nil {
		return core.IO("write_footer", "", err)
	}

	// Update header with final NumRows
	// Serialize to buffer first to check size
	headerBuf := new(bytes.Buffer)
	if _, err := w.header.WriteTo(headerBuf); err != nil {
		return core.New(core.ErrIO).
			Op("serialize_final_header").
			Wrap(err).
			Build()
	}

	headerData := headerBuf.Bytes()
	headerLen := len(headerData)

	// Verify header still fits in reserved space
	if headerLen > HeaderReservedSize {
		return core.New(core.ErrMetadataError).
			Op("close_writer").
			Context("header_size", headerLen).
			Context("reserved_size", HeaderReservedSize).
			Context("message", "final header size exceeds reserved size").
			Build()
	}

	// Seek back to beginning and rewrite header
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return core.IO("seek_header", "", err)
	}

	// Write updated header (no need to write padding again, it's already there)
	if _, err := w.file.Write(headerData); err != nil {
		return core.IO("rewrite_header", "", err)
	}

	// Sync file to ensure data is written to disk
	if err := w.file.Sync(); err != nil {
		return core.IO("sync_file", "", err)
	}

	// Close file
	if err := w.file.Close(); err != nil {
		return core.IO("close_file", "", err)
	}

	return nil
}
