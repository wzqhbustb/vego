package format

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"time"

	lerrors "github.com/wzqhbustb/vego/storage/errors"
)

// Footer represents the Lance file footer
// The footer is always at the end of the file and has a fixed maximum size
type Footer struct {
	Version       uint16            // File format version (redundant with header, for validation)
	NumPages      int32             // Total number of pages
	PageIndexList *PageIndexList    // Index of all pages
	CreatedAt     int64             // Unix timestamp
	ModifiedAt    int64             // Unix timestamp
	Metadata      map[string]string // Additional metadata
	Checksum      uint32            // Footer checksum
}

// NewFooter creates a new footer
func NewFooter() *Footer {
	now := time.Now().Unix()
	return &Footer{
		Version:       CurrentVersion,
		PageIndexList: NewPageIndexList(),
		CreatedAt:     now,
		ModifiedAt:    now,
		Metadata:      make(map[string]string),
	}
}

// AddMetadata adds a metadata entry
func (f *Footer) AddMetadata(key, value string) {
	f.Metadata[key] = value
	f.ModifiedAt = time.Now().Unix()
}

// Validate validates the footer
func (f *Footer) Validate() error {
	if err := ValidateVersion(f.Version); err != nil {
		return err
	}
	if f.NumPages < 0 {
		return lerrors.ValidationFailed("validate_footer", "",
			fmt.Sprintf("invalid page count: %d", f.NumPages))
	}
	if f.NumPages != int32(len(f.PageIndexList.Indices)) {
		return lerrors.New(lerrors.ErrMetadataError).
			Op("validate_footer").
			Context("field", "page_count").
			Context("declared", f.NumPages).
			Context("actual", len(f.PageIndexList.Indices)).
			Context("message", "page count mismatch").
			Build()
	}
	if f.CreatedAt <= 0 {
		return lerrors.ValidationFailed("validate_footer", "",
			fmt.Sprintf("invalid created timestamp: %d", f.CreatedAt))
	}
	return nil
}

// EncodedSize returns the encoded size of the footer
func (f *Footer) EncodedSize() int {
	// version(2) + numPages(4) + createdAt(8) + modifiedAt(8) + checksum(4)
	baseSize := 2 + 4 + 8 + 8 + 4

	// Add page index list size
	baseSize += f.PageIndexList.EncodedSize()

	// Add metadata size: count(4) + entries
	baseSize += 4
	for k, v := range f.Metadata {
		// key_len(4) + key + value_len(4) + value
		baseSize += 4 + len(k) + 4 + len(v)
	}

	return baseSize
}

// WriteTo writes the footer to a writer
func (f *Footer) WriteTo(w io.Writer) (int64, error) {
	if err := f.Validate(); err != nil {
		return 0, NewFileError("write footer", err)
	}

	buf := new(bytes.Buffer)

	// Write fixed fields
	binary.Write(buf, ByteOrder, f.Version)
	binary.Write(buf, ByteOrder, f.NumPages)
	binary.Write(buf, ByteOrder, f.CreatedAt)
	binary.Write(buf, ByteOrder, f.ModifiedAt)

	// Write page index list
	f.PageIndexList.WriteTo(buf)

	// Write metadata
	metaCount := int32(len(f.Metadata))
	binary.Write(buf, ByteOrder, metaCount)
	for k, v := range f.Metadata {
		// Write key
		keyLen := int32(len(k))
		binary.Write(buf, ByteOrder, keyLen)
		buf.WriteString(k)

		// Write value
		valueLen := int32(len(v))
		binary.Write(buf, ByteOrder, valueLen)
		buf.WriteString(v)
	}

	// Calculate checksum (excluding the checksum field itself)
	data := buf.Bytes()
	f.Checksum = crc32.ChecksumIEEE(data)

	// Write checksum
	binary.Write(buf, ByteOrder, f.Checksum)

	// Pad to FooterSize if needed
	footerBytes := buf.Bytes()
	if len(footerBytes) > FooterSize {
		// return 0, NewFileError("write footer", fmt.Errorf("footer too large: %d bytes (max %d)", len(footerBytes), FooterSize))
		return 0, lerrors.New(lerrors.ErrMetadataError).
			Op("write_footer").
			Context("field", "footer").
			Context("size", len(footerBytes)).
			Context("max_size", FooterSize).
			Context("message", "footer too large").
			Severity(lerrors.SeverityFatal).
			Build()
	}

	// Pad with zeros
	padding := make([]byte, FooterSize-len(footerBytes))
	buf.Write(padding)

	n, err := w.Write(buf.Bytes())
	return int64(n), err
}

// ReadFrom reads the footer from a reader
func (f *Footer) ReadFrom(r io.Reader) (int64, error) {
	// Read entire footer
	footerBuf := make([]byte, FooterSize)
	n, err := io.ReadFull(r, footerBuf)
	if err != nil {
		return int64(n), NewFileError("read footer", err)
	}

	reader := bytes.NewReader(footerBuf)

	// Read fixed fields
	binary.Read(reader, ByteOrder, &f.Version)
	binary.Read(reader, ByteOrder, &f.NumPages)
	binary.Read(reader, ByteOrder, &f.CreatedAt)
	binary.Read(reader, ByteOrder, &f.ModifiedAt)

	// Read page index list
	f.PageIndexList = NewPageIndexList()
	if _, err := f.PageIndexList.ReadFrom(reader); err != nil {
		return int64(n), err
	}

	// Read metadata
	var metaCount int32
	binary.Read(reader, ByteOrder, &metaCount)

	f.Metadata = make(map[string]string)
	for i := int32(0); i < metaCount; i++ {
		// Read key
		var keyLen int32
		binary.Read(reader, ByteOrder, &keyLen)
		keyBytes := make([]byte, keyLen)
		reader.Read(keyBytes)
		key := string(keyBytes)

		// Read value
		var valueLen int32
		binary.Read(reader, ByteOrder, &valueLen)
		valueBytes := make([]byte, valueLen)
		reader.Read(valueBytes)
		value := string(valueBytes)

		f.Metadata[key] = value
	}

	// Read checksum
	var storedChecksum uint32
	binary.Read(reader, ByteOrder, &storedChecksum)

	// Verify checksum
	// The checksum was calculated on the data BEFORE the checksum field
	// So we need to recalculate from the beginning up to where we just read the checksum
	currentPos := int(reader.Size()) - reader.Len() - 4 // Position before reading checksum
	dataForChecksum := footerBuf[:currentPos]

	computed := crc32.ChecksumIEEE(dataForChecksum)
	if computed != storedChecksum {
		return int64(n), lerrors.FormatCorrupted("", 0,
			fmt.Sprintf("footer checksum mismatch: computed 0x%08X vs stored 0x%08X", computed, storedChecksum))
	}

	f.Checksum = storedChecksum

	// Validate
	if err := f.Validate(); err != nil {
		return int64(n), err
	}

	return int64(n), nil
}

// GetPageOffset returns the file offset for a given page
func (f *Footer) GetPageOffset(columnIndex, pageNum int32) (int64, bool) {
	for _, idx := range f.PageIndexList.Indices {
		if idx.ColumnIndex == columnIndex && idx.PageNum == pageNum {
			return idx.Offset, true
		}
	}
	return 0, false
}

// GetColumnPages returns all pages for a given column
func (f *Footer) GetColumnPages(columnIndex int32) []PageIndex {
	return f.PageIndexList.FindByColumn(columnIndex)
}
