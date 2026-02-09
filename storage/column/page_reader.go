package column

import (
	"github.com/wzqhbkjdx/vego/storage/arrow"
	"github.com/wzqhbkjdx/vego/storage/encoding"
	lerrors "github.com/wzqhbkjdx/vego/storage/errors"
	"github.com/wzqhbkjdx/vego/storage/format"
)

// PageReader handles deserialization of Page data to Arrays
type PageReader struct{}

// NewPageReader creates a new page reader
func NewPageReader() *PageReader {
	return &PageReader{}
}

// ReadPage converts a Page back into an Array.
func (r *PageReader) ReadPage(page *format.Page, dataType arrow.DataType) (arrow.Array, error) {
	if page == nil {
		return nil, lerrors.New(lerrors.ErrInvalidArgument).
			Op("read_page").
			Context("message", "page is nil").
			Build()
	}

	if len(page.Data) == 0 {
		return nil, lerrors.New(lerrors.ErrInvalidArgument).
			Op("read_page").
			Context("message", "page data is empty").
			Build()
	}

	// Get decoder based on encoding type
	decoder, err := encoding.GetDecoder(page.Encoding)
	if err != nil {
		return nil, lerrors.New(lerrors.ErrUnsupportedType).
			Op("get_decoder").
			Context("encoding", page.Encoding).
			Wrap(err).
			Build()
	}

	if decoder == nil {
		return nil, lerrors.New(lerrors.ErrUnsupportedType).
			Op("read_page").
			Context("encoding", "plain").
			Context("message", "plain encoding is not supported: all pages must be encoded").
			Build()
	}

	// Decode the data
	array, err := decoder.Decode(page.Data, dataType)
	if err != nil {
		return nil, lerrors.New(lerrors.ErrDecodeFailed).
			Op("decode_page").
			Context("encoding", page.Encoding).
			Wrap(err).
			Build()
	}

	// Verify the decoded array has correct length
	if array.Len() != int(page.NumValues) {
		return nil, lerrors.New(lerrors.ErrInvalidArgument).
			Op("verify_decoded_array").
			Context("expected_values", page.NumValues).
			Context("actual_values", array.Len()).
			Context("message", "decoded array length mismatch").
			Build()
	}

	return array, nil
}

// page_reader.go - 修正后的 ReadPageFromData

// ReadPageFromData 直接从编码后的数据解码 Array（用于 AsyncIO 返回的数据）
// 注意：data 是完整的 Page 字节流（包含 30 字节 header），需要跳过 header
func (r *PageReader) ReadPageFromData(data []byte, encodingType format.EncodingType, numValues int32, dataType arrow.DataType) (arrow.Array, error) {
	// PageHeaderSize = 30 字节
	const PageHeaderSize = 30

	if len(data) < PageHeaderSize {
		return nil, lerrors.New(lerrors.ErrInvalidArgument).
			Op("read_page_from_data").
			Context("expected_bytes", PageHeaderSize).
			Context("actual_bytes", len(data)).
			Context("message", "page data too short").
			Build()
	}

	// 跳过 30 字节的 header，获取实际的编码数据
	encodedData := data[PageHeaderSize:]

	if len(encodedData) == 0 {
		return nil, lerrors.New(lerrors.ErrInvalidArgument).
			Op("read_page_from_data").
			Context("message", "page data is empty after header").
			Build()
	}

	// Get decoder
	decoder, err := encoding.GetDecoder(encodingType)
	if err != nil {
		return nil, lerrors.New(lerrors.ErrUnsupportedType).
			Op("get_decoder").
			Context("encoding", encodingType).
			Wrap(err).
			Build()
	}

	if decoder == nil {
		return nil, lerrors.New(lerrors.ErrUnsupportedType).
			Op("read_page_from_data").
			Context("encoding", "plain").
			Context("message", "plain encoding is not supported: all pages must be encoded").
			Build()
	}

	// Decode
	array, err := decoder.Decode(encodedData, dataType)
	if err != nil {
		return nil, lerrors.New(lerrors.ErrDecodeFailed).
			Op("decode_page_from_data").
			Context("encoding", encodingType).
			Wrap(err).
			Build()
	}

	if array.Len() != int(numValues) {
		return nil, lerrors.New(lerrors.ErrInvalidArgument).
			Op("verify_decoded_array_from_data").
			Context("expected_values", numValues).
			Context("actual_values", array.Len()).
			Context("message", "decoded array length mismatch").
			Build()
	}

	return array, nil
}
