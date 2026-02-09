package format

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	lerrors "github.com/wzqhbkjdx/vego/storage/errors"
)

// Page represents a single data page in a Lance file
type Page struct {
	Type             PageType     // Page type
	Encoding         EncodingType // Encoding type
	ColumnIndex      int32        // Column index this page belongs to
	NumValues        int32        // Number of values in this page
	UncompressedSize int32        // Uncompressed data size
	CompressedSize   int32        // Compressed data size (or same as uncompressed if not compressed)
	Checksum         uint32       // CRC32 checksum
	Data             []byte       // Page data
	Offset           int64        // Offset in file (for reading)
}

// PageHeader is the fixed-size header for each page
type PageHeader struct {
	Type             PageType     // 1 byte
	Encoding         EncodingType // 1 byte
	ColumnIndex      int32        // 4 bytes
	NumValues        int32        // 4 bytes
	UncompressedSize int32        // 4 bytes
	CompressedSize   int32        // 4 bytes
	Checksum         uint32       // 4 bytes
	Reserved         [8]byte      // 8 bytes reserved
}

const PageHeaderSize = 1 + 1 + 4 + 4 + 4 + 4 + 4 + 8 // 30 bytes

// NewPage creates a new page
func NewPage(columnIndex int32, pageType PageType, encoding EncodingType) *Page {
	return &Page{
		Type:        pageType,
		Encoding:    encoding,
		ColumnIndex: columnIndex,
	}
}

// SetData sets the page data and updates sizes
func (p *Page) SetData(data []byte, uncompressedSize int32) {
	p.Data = data
	p.UncompressedSize = uncompressedSize
	p.CompressedSize = int32(len(data))
	p.Checksum = crc32.ChecksumIEEE(data)
}

// Validate validates the page
func (p *Page) Validate() error {
	if p.NumValues < 0 {
		return lerrors.ValidationFailed("validate_page", "",
			fmt.Sprintf("invalid num values: %d", p.NumValues))
	}
	if p.UncompressedSize < 0 {
		return lerrors.ValidationFailed("validate_page", "",
			fmt.Sprintf("invalid uncompressed size: %d", p.UncompressedSize))
	}
	if p.CompressedSize < 0 || p.CompressedSize > MaxPageSize {
		return lerrors.ValidationFailed("validate_page", "",
			fmt.Sprintf("invalid compressed size: %d", p.CompressedSize))
	}
	if int32(len(p.Data)) != p.CompressedSize {
		return lerrors.New(lerrors.ErrInvalidArgument).
			Op("validate_page").
			Context("field", "data").
			Context("actual_size", len(p.Data)).
			Context("declared_size", p.CompressedSize).
			Context("message", "data size mismatch").
			Build()
	}

	// Verify checksum
	computed := crc32.ChecksumIEEE(p.Data)
	if computed != p.Checksum {
		return lerrors.FormatCorrupted("", p.Offset,
			fmt.Sprintf("page checksum mismatch: computed 0x%08X vs stored 0x%08X", computed, p.Checksum))
	}

	return nil
}

// EncodedSize returns the total encoded size (header + data)
func (p *Page) EncodedSize() int {
	return PageHeaderSize + int(p.CompressedSize)
}

// WriteTo writes the page to a writer
func (p *Page) WriteTo(w io.Writer) (int64, error) {
	if err := p.Validate(); err != nil {
		return 0, NewFileError("write page", err)
	}

	buf := new(bytes.Buffer)

	// Write header
	header := PageHeader{
		Type:             p.Type,
		Encoding:         p.Encoding,
		ColumnIndex:      p.ColumnIndex,
		NumValues:        p.NumValues,
		UncompressedSize: p.UncompressedSize,
		CompressedSize:   p.CompressedSize,
		Checksum:         p.Checksum,
	}

	buf.WriteByte(byte(header.Type))
	buf.WriteByte(byte(header.Encoding))
	binary.Write(buf, ByteOrder, header.ColumnIndex)
	binary.Write(buf, ByteOrder, header.NumValues)
	binary.Write(buf, ByteOrder, header.UncompressedSize)
	binary.Write(buf, ByteOrder, header.CompressedSize)
	binary.Write(buf, ByteOrder, header.Checksum)
	binary.Write(buf, ByteOrder, header.Reserved)

	// Write data
	buf.Write(p.Data)

	n, err := w.Write(buf.Bytes())
	return int64(n), err
}

// ReadFrom reads the page from a reader
func (p *Page) ReadFrom(r io.Reader) (int64, error) {
	// Read header
	headerBuf := make([]byte, PageHeaderSize)
	n, err := io.ReadFull(r, headerBuf)
	if err != nil {
		return int64(n), NewFileError("read page header", err)
	}

	// Parse header
	p.Type = PageType(headerBuf[0])
	p.Encoding = EncodingType(headerBuf[1])

	reader := bytes.NewReader(headerBuf[2:])
	binary.Read(reader, ByteOrder, &p.ColumnIndex)
	binary.Read(reader, ByteOrder, &p.NumValues)
	binary.Read(reader, ByteOrder, &p.UncompressedSize)
	binary.Read(reader, ByteOrder, &p.CompressedSize)
	binary.Read(reader, ByteOrder, &p.Checksum)

	var reserved [8]byte
	binary.Read(reader, ByteOrder, &reserved)

	// Read data
	p.Data = make([]byte, p.CompressedSize)
	dataRead, err := io.ReadFull(r, p.Data)
	if err != nil {
		return int64(n + dataRead), NewFileError("read page data", err)
	}

	// Validate
	if err := p.Validate(); err != nil {
		return int64(n + dataRead), err
	}

	return int64(n + dataRead), nil
}

// UnmarshalBinary 从二进制数据解码 Page
func (p *Page) UnmarshalBinary(data []byte) error {
	// 简化实现：实际应该根据 Page 的序列化格式实现
	// 这里假设 Page 实现了 ReadFrom 接口
	buf := bytes.NewReader(data)
	_, err := p.ReadFrom(buf)
	return err
}

// MarshalBinary 将 Page 编码为二进制数据
func (p *Page) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	_, err := p.WriteTo(&buf)
	return buf.Bytes(), err
}

// PageIndex represents an index entry for a page
type PageIndex struct {
	ColumnIndex int32        // Column index
	PageNum     int32        // Page number within column
	Offset      int64        // Byte offset in file
	Size        int32        // Size in bytes
	NumValues   int32        // Number of values
	Encoding    EncodingType // Encoding type for this page
}

// EncodedSize returns the size of the encoded PageIndex
func (pi PageIndex) EncodedSize() int {
	return 4 + 4 + 8 + 4 + 4 + 1 // 添加 1 字节用于 Encoding
}

// WriteTo writes the page index to a writer
func (pi PageIndex) WriteTo(w io.Writer) (int64, error) {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, pi.ColumnIndex)
	binary.Write(&buf, binary.LittleEndian, pi.PageNum)
	binary.Write(&buf, binary.LittleEndian, pi.Offset)
	binary.Write(&buf, binary.LittleEndian, pi.Size)
	binary.Write(&buf, binary.LittleEndian, pi.NumValues)
	binary.Write(&buf, binary.LittleEndian, pi.Encoding) // 添加这行

	n, err := w.Write(buf.Bytes())
	return int64(n), err
}

// ReadFrom reads the page index from a reader
func (pi *PageIndex) ReadFrom(r io.Reader) (int64, error) {
	var total int64

	if err := binary.Read(r, binary.LittleEndian, &pi.ColumnIndex); err != nil {
		return total, err
	}
	total += 4

	if err := binary.Read(r, binary.LittleEndian, &pi.PageNum); err != nil {
		return total, err
	}
	total += 4

	if err := binary.Read(r, binary.LittleEndian, &pi.Offset); err != nil {
		return total, err
	}
	total += 8

	if err := binary.Read(r, binary.LittleEndian, &pi.Size); err != nil {
		return total, err
	}
	total += 4

	if err := binary.Read(r, binary.LittleEndian, &pi.NumValues); err != nil {
		return total, err
	}
	total += 4

	// 添加 Encoding 的读取
	if err := binary.Read(r, binary.LittleEndian, &pi.Encoding); err != nil {
		return total, err
	}
	total += 1

	return total, nil
}

// PageIndexList is a collection of page indices
type PageIndexList struct {
	Indices []PageIndex
}

// NewPageIndexList creates a new page index list
func NewPageIndexList() *PageIndexList {
	return &PageIndexList{
		Indices: make([]PageIndex, 0),
	}
}

func (l *PageIndexList) Add(columnIndex, pageNum int32, offset int64, size, numValues int32, encoding EncodingType) {
	l.Indices = append(l.Indices, PageIndex{
		ColumnIndex: columnIndex,
		PageNum:     pageNum,
		Offset:      offset,
		Size:        size,
		NumValues:   numValues,
		Encoding:    encoding, // 添加 Encoding 字段
	})
}

// FindByColumn returns all page indices for a given column
func (l *PageIndexList) FindByColumn(columnIndex int32) []PageIndex {
	var result []PageIndex
	for _, idx := range l.Indices {
		if idx.ColumnIndex == columnIndex {
			result = append(result, idx)
		}
	}
	return result
}

// EncodedSize returns the encoded size of the page index list
func (l *PageIndexList) EncodedSize() int {
	return 4 + len(l.Indices)*25 // 4(count) + 25*(ColumnIndex+PageNum+Offset+Size+NumValues+Encoding)
}

// WriteTo writes the page index list to a writer
func (l *PageIndexList) WriteTo(w io.Writer) (int64, error) {
	buf := new(bytes.Buffer)

	// Write count
	count := int32(len(l.Indices))
	binary.Write(buf, ByteOrder, count)

	// Write each index
	for _, idx := range l.Indices {
		binary.Write(buf, ByteOrder, idx.ColumnIndex)
		binary.Write(buf, ByteOrder, idx.PageNum)
		binary.Write(buf, ByteOrder, idx.Offset)
		binary.Write(buf, ByteOrder, idx.Size)
		binary.Write(buf, ByteOrder, idx.NumValues)
		binary.Write(buf, ByteOrder, idx.Encoding) // 添加这行
	}

	n, err := w.Write(buf.Bytes())
	return int64(n), err
}

// ReadFrom reads the page index list from a reader
func (l *PageIndexList) ReadFrom(r io.Reader) (int64, error) {
	// Read count
	var count int32
	if err := binary.Read(r, ByteOrder, &count); err != nil {
		return 4, NewFileError("read page index count", err)
	}

	bytesRead := int64(4)
	l.Indices = make([]PageIndex, count)

	// Read each index
	for i := int32(0); i < count; i++ {
		var idx PageIndex
		if err := binary.Read(r, ByteOrder, &idx.ColumnIndex); err != nil {
			return bytesRead, err
		}
		if err := binary.Read(r, ByteOrder, &idx.PageNum); err != nil {
			return bytesRead, err
		}
		if err := binary.Read(r, ByteOrder, &idx.Offset); err != nil {
			return bytesRead, err
		}
		if err := binary.Read(r, ByteOrder, &idx.Size); err != nil {
			return bytesRead, err
		}
		if err := binary.Read(r, ByteOrder, &idx.NumValues); err != nil {
			return bytesRead, err
		}
		if err := binary.Read(r, ByteOrder, &idx.Encoding); err != nil { // 添加这行
			return bytesRead, err
		}

		l.Indices[i] = idx
		bytesRead += 25 // 改为 25
	}

	return bytesRead, nil
}
