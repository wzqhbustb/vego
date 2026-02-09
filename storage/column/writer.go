package column

import (
	"bytes"
	"io"
	"github.com/wzqhbkjdx/vego/storage/arrow"
	"github.com/wzqhbkjdx/vego/storage/encoding"
	lerrors "github.com/wzqhbkjdx/vego/storage/errors"
	"github.com/wzqhbkjdx/vego/storage/format"
	"os"
)

const (
	// HeaderReservedSize is the fixed size reserved for file header
	// This ensures header can be rewritten without affecting page offsets
	HeaderReservedSize = 8192 // 8KB should be enough for any reasonable schema
)

// Writer writes RecordBatch data to a Lance file
type Writer struct {
	file       *os.File
	header     *format.Header
	footer     *format.Footer
	pageWriter *PageWriter
	headerSize int64 // Always equals HeaderReservedSize
	currentPos int64 // Current write position
	factory    *encoding.EncoderFactory
	closed     bool
}

// NewWriter creates a new column writer
func NewWriter(filename string, schema *arrow.Schema, factory *encoding.EncoderFactory) (*Writer, error) {
	file, err := os.Create(filename)
	if err != nil {
		return nil, lerrors.IO("new_writer", filename, err)
	}

	if factory == nil {
		factory = encoding.NewEncoderFactory(3)
	}

	writer := &Writer{
		file:       file,
		header:     format.NewHeader(schema, 0),
		footer:     format.NewFooter(),
		pageWriter: NewPageWriter(factory), // 传递 factory
		factory:    factory,
		closed:     false,
		headerSize: HeaderReservedSize,
	}

	if err := writer.writeHeaderWithPadding(); err != nil {
		file.Close()
		return nil, lerrors.New(lerrors.ErrIO).
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
		return lerrors.New(lerrors.ErrIO).
			Op("serialize_header").
			Wrap(err).
			Build()
	}

	headerData := headerBuf.Bytes()
	headerLen := len(headerData)

	// Check if header fits in reserved space
	if headerLen > HeaderReservedSize {
		return lerrors.New(lerrors.ErrMetadataError).
			Op("write_header_with_padding").
			Context("header_size", headerLen).
			Context("reserved_size", HeaderReservedSize).
			Context("message", "header size exceeds reserved size").
			Build()
	}

	// Write header data
	if _, err := w.file.Write(headerData); err != nil {
		return lerrors.IO("write_header_data", "", err)
	}

	// Write padding to fill reserved space
	paddingSize := HeaderReservedSize - headerLen
	if paddingSize > 0 {
		padding := make([]byte, paddingSize)
		if _, err := w.file.Write(padding); err != nil {
			return lerrors.IO("write_header_padding", "", err)
		}
	}

	return nil
}

// WriteRecordBatch writes a RecordBatch to the file
func (w *Writer) WriteRecordBatch(batch *arrow.RecordBatch) error {
	if w.closed {
		return lerrors.New(lerrors.ErrInvalidArgument).
			Op("write_record_batch").
			Context("message", "writer is closed").
			Build()
	}

	if batch == nil {
		return lerrors.New(lerrors.ErrInvalidArgument).
			Op("write_record_batch").
			Context("message", "batch is nil").
			Build()
	}

	// Validate schema matches
	if !w.header.Schema.Equal(batch.Schema()) {
		return lerrors.New(lerrors.ErrSchemaMismatch).
			Op("write_record_batch").
			Context("message", "schema mismatch").
			Build()
	}

	// Update header row count
	w.header.NumRows += int64(batch.NumRows())

	// Write each column
	for colIdx := 0; colIdx < batch.NumCols(); colIdx++ {
		column := batch.Column(colIdx)
		field := batch.Schema().Field(colIdx)

		if err := validateArray(column, field); err != nil {
			return lerrors.New(lerrors.ErrInvalidArgument).
				Op("write_record_batch").
				Context("column_index", colIdx).
				Context("column_name", field.Name).
				Context("message", "column validation failed").
				Wrap(err).
				Build()
		}

		if err := w.writeColumn(int32(colIdx), column); err != nil {
			return lerrors.New(lerrors.ErrIO).
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
func (w *Writer) writeColumn(columnIndex int32, array arrow.Array) error {
	// Convert array to pages
	pages, err := w.pageWriter.WritePages(array, columnIndex)
	if err != nil {
		return lerrors.New(lerrors.ErrEncodeFailed).
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
			return lerrors.IO("write_page", "", err)
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
		return lerrors.New(lerrors.ErrInvalidArgument).
			Op("close_writer").
			Context("message", "writer already closed").
			Build()
	}

	w.closed = true

	// Update footer
	w.footer.NumPages = int32(len(w.footer.PageIndexList.Indices))

	// Write footer at current position (after all pages)
	if _, err := w.file.Seek(w.currentPos, io.SeekStart); err != nil {
		return lerrors.IO("seek_footer", "", err)
	}

	if _, err := w.footer.WriteTo(w.file); err != nil {
		return lerrors.IO("write_footer", "", err)
	}

	// Update header with final NumRows
	// Serialize to buffer first to check size
	headerBuf := new(bytes.Buffer)
	if _, err := w.header.WriteTo(headerBuf); err != nil {
		return lerrors.New(lerrors.ErrIO).
			Op("serialize_final_header").
			Wrap(err).
			Build()
	}

	headerData := headerBuf.Bytes()
	headerLen := len(headerData)

	// Verify header still fits in reserved space
	if headerLen > HeaderReservedSize {
		return lerrors.New(lerrors.ErrMetadataError).
			Op("close_writer").
			Context("header_size", headerLen).
			Context("reserved_size", HeaderReservedSize).
			Context("message", "final header size exceeds reserved size").
			Build()
	}

	// Seek back to beginning and rewrite header
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return lerrors.IO("seek_header", "", err)
	}

	// Write updated header (no need to write padding again, it's already there)
	if _, err := w.file.Write(headerData); err != nil {
		return lerrors.IO("rewrite_header", "", err)
	}

	// Close file
	if err := w.file.Close(); err != nil {
		return lerrors.IO("close_file", "", err)
	}

	return nil
}
