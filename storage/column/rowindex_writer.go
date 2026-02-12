// Copyright 2024 Vego Authors
// Licensed under the Apache License, Version 2.0

package column

import (
	"github.com/wzqhbustb/vego/storage/arrow"
	"github.com/wzqhbustb/vego/storage/encoding"
	"github.com/wzqhbustb/vego/storage/format"
	lerrors "github.com/wzqhbustb/vego/storage/errors"
)

// RowIndexWriter extends Writer with RowIndex and BlockCache support for V1.1+ files
type RowIndexWriter struct {
	*Writer
	rowIndex      *format.RowIndex
	version       format.VersionPolicy
	writeRowIndex bool
	blockSize     int32
	schema        *arrow.Schema
}

// NewRowIndexWriter creates a writer with RowIndex support
// If version is V1.0, RowIndex will not be written
func NewRowIndexWriter(filename string, schema *arrow.Schema, version format.VersionPolicy, factory *encoding.EncoderFactory) (*RowIndexWriter, error) {
	if factory == nil {
		factory = encoding.NewEncoderFactory(3)
	}

	writer, err := NewWriter(filename, schema, factory)
	if err != nil {
		return nil, err
	}

	return &RowIndexWriter{
		Writer:     writer,
		version:    version,
		rowIndex:   format.NewRowIndex(1000), // Default capacity
		writeRowIndex: version.HasFeature(format.FeatureRowIndex),
		blockSize:  format.DefaultBlockSize,
		schema:     schema,
	}, nil
}

// SetBlockSize sets the block size hint for BlockCache
// Only meaningful for V1.2+ files
func (w *RowIndexWriter) SetBlockSize(blockSize int32) {
	w.blockSize = blockSize
}

// AddRowID adds a document ID -> row index mapping
// This should be called after WriteRecordBatch for each document
func (w *RowIndexWriter) AddRowID(docID string, rowIndex int64) error {
	if !w.writeRowIndex {
		return nil // No-op for V1.0 files
	}
	return w.rowIndex.Insert(docID, rowIndex)
}

// Close finalizes the file, including RowIndex and BlockCache info if applicable
func (w *RowIndexWriter) Close() error {
	if w.closed {
		return lerrors.New(lerrors.ErrInvalidArgument).
			Op("close_rowindex_writer").
			Context("message", "writer already closed").
			Build()
	}

	// Write RowIndex Page if enabled and has entries
	if w.writeRowIndex && w.rowIndex.NumEntries > 0 {
		if err := w.writeRowIndexPage(); err != nil {
			return err
		}
	}

	// Update footer with version info
	w.footer.SetFormatVersion(w.version)

	// Set BlockCache info for V1.2+ files
	if w.version.HasFeature(format.FeatureBlockCache) {
		w.footer.SetBlockCacheInfo(w.blockSize)
	}

	// Call parent Close
	return w.Writer.Close()
}

// writeRowIndexPage writes the RowIndex as an independent Page
func (w *RowIndexWriter) writeRowIndexPage() error {
	// Convert RowIndex to Page
	page, err := w.rowIndex.ToPage()
	if err != nil {
		return lerrors.New(lerrors.ErrEncodeFailed).
			Op("write_rowindex_page").
			Wrap(err).
			Build()
	}

	// Record position before writing
	rowIndexOffset := w.currentPos

	// Write the page
	n, err := page.WriteTo(w.file)
	if err != nil {
		return lerrors.IO("write_rowindex_page", "", err)
	}

	// Update position
	w.currentPos += n

	// Store RowIndex info in Footer.Metadata
	w.footer.SetRowIndexInfo(rowIndexOffset, int32(n), page.Checksum)

	return nil
}

// GetRowIndex returns the current RowIndex (for testing/debugging)
func (w *RowIndexWriter) GetRowIndex() *format.RowIndex {
	return w.rowIndex
}
