// Copyright 2024 Vego Authors
// Licensed under the Apache License, Version 2.0

package column

import (
	"io"

	"github.com/wzqhbustb/vego/storage/format"
	lerrors "github.com/wzqhbustb/vego/storage/errors"
)

// RowIndexReader extends Reader with RowIndex support for V1.1+ files
type RowIndexReader struct {
	*Reader
	rowIndex     *format.RowIndex
	version      format.VersionPolicy
	hasRowIndex  bool
	rowIndexLoaded bool
}

// NewRowIndexReader creates a reader with RowIndex support
func NewRowIndexReader(filename string) (*RowIndexReader, error) {
	reader, err := NewReader(filename)
	if err != nil {
		return nil, err
	}

	// Get file version from footer
	version := reader.footer.GetFormatVersion()

	// Check if file has RowIndex
	hasRowIndex := reader.footer.HasRowIndex()

	return &RowIndexReader{
		Reader:     reader,
		version:    version,
		hasRowIndex: hasRowIndex,
	}, nil
}

// LoadRowIndex loads the RowIndex from the file
// This is a lazy operation - RowIndex is only loaded when needed
func (r *RowIndexReader) LoadRowIndex() error {
	if r.rowIndexLoaded {
		return nil
	}

	// Check if file has RowIndex
	if !r.hasRowIndex {
		return lerrors.New(lerrors.ErrInvalidArgument).
			Op("load_rowindex").
			Context("message", "file does not contain RowIndex").
			Build()
	}

	// Get RowIndex info from footer
	offset, size, checksum, ok := r.footer.GetRowIndexInfo()
	if !ok {
		return lerrors.New(lerrors.ErrCorruptedFile).
			Op("load_rowindex").
			Context("message", "RowIndex info not found in footer metadata").
			Build()
	}

	// Seek to RowIndex Page position
	if _, err := r.file.Seek(offset, io.SeekStart); err != nil {
		return lerrors.New(lerrors.ErrIO).
			Op("seek_rowindex").
			Wrap(err).
			Build()
	}

	// Read the page
	page := &format.Page{}
	if _, err := page.ReadFrom(r.file); err != nil {
		return lerrors.New(lerrors.ErrIO).
			Op("read_rowindex_page").
			Wrap(err).
			Build()
	}

	// Verify page type
	if page.Type != format.PageTypeIndex {
		return lerrors.New(lerrors.ErrCorruptedFile).
			Op("load_rowindex").
			Context("page_type", page.Type).
			Context("expected", format.PageTypeIndex).
			Context("message", "invalid page type for RowIndex").
			Build()
	}

	// Verify size (declared_size includes page header, actual = header + CompressedSize)
	expectedSize := format.PageHeaderSize + page.CompressedSize
	if int32(size) != int32(expectedSize) {
		return lerrors.New(lerrors.ErrCorruptedFile).
			Op("load_rowindex").
			Context("declared_size", size).
			Context("actual_size", expectedSize).
			Context("compressed_size", page.CompressedSize).
			Context("header_size", format.PageHeaderSize).
			Context("message", "RowIndex page size mismatch").
			Build()
	}

	// Verify checksum
	if checksum != 0 && page.Checksum != checksum {
		return lerrors.New(lerrors.ErrCorruptedFile).
			Op("load_rowindex").
			Context("declared_checksum", checksum).
			Context("actual_checksum", page.Checksum).
			Context("message", "RowIndex page checksum mismatch").
			Build()
	}

	// Parse RowIndex from page data
	ri, err := format.RowIndexFromPage(page)
	if err != nil {
		return lerrors.New(lerrors.ErrDecodeFailed).
			Op("parse_rowindex").
			Wrap(err).
			Build()
	}

	r.rowIndex = ri
	r.rowIndexLoaded = true

	return nil
}

// LookupRowID returns the row index for the given document ID
// If RowIndex is not loaded, it will be loaded automatically
func (r *RowIndexReader) LookupRowID(docID string) (int64, error) {
	// Check if file has RowIndex capability
	if !r.hasRowIndex {
		// For V1.0 files without RowIndex, return error
		return -1, lerrors.New(lerrors.ErrInvalidArgument).
			Op("lookup_rowid").
			Context("version", r.version.String()).
			Context("message", "file does not support RowIndex (V1.0 format)").
			Build()
	}

	// Lazy load RowIndex
	if !r.rowIndexLoaded {
		if err := r.LoadRowIndex(); err != nil {
			return -1, err
		}
	}

	// Lookup
	rowIdx := r.rowIndex.Lookup(docID)
	if rowIdx == -1 {
		return -1, lerrors.New(lerrors.ErrInvalidArgument).
			Op("lookup_rowid").
			Context("doc_id", docID).
			Context("message", "document ID not found in RowIndex").
			Build()
	}

	return rowIdx, nil
}

// HasRowIndex returns true if the file contains a RowIndex
func (r *RowIndexReader) HasRowIndex() bool {
	return r.hasRowIndex
}

// GetVersion returns the file format version
func (r *RowIndexReader) GetVersion() format.VersionPolicy {
	return r.version
}

// GetRowIndex returns the loaded RowIndex (nil if not loaded)
func (r *RowIndexReader) GetRowIndex() *format.RowIndex {
	return r.rowIndex
}

// RowIndexStats returns statistics about the RowIndex
func (r *RowIndexReader) RowIndexStats() (format.RowIndexStats, error) {
	if !r.hasRowIndex {
		return format.RowIndexStats{}, lerrors.New(lerrors.ErrInvalidArgument).
			Op("rowindex_stats").
			Context("message", "file does not contain RowIndex").
			Build()
	}

	if !r.rowIndexLoaded {
		if err := r.LoadRowIndex(); err != nil {
			return format.RowIndexStats{}, err
		}
	}

	return r.rowIndex.Stats(), nil
}
