package column

import (
	"bytes"
	"encoding/binary"

	"github.com/wzqhbustb/vego/core"
	"github.com/wzqhbustb/vego/storage/encoding"
	"github.com/wzqhbustb/vego/storage/format"
)

// PageReader handles deserialization of Page data to Arrays
type PageReader struct{}

// NewPageReader creates a new page reader
func NewPageReader() *PageReader {
	return &PageReader{}
}

// ReadPage converts a Page back into an Array.
func (r *PageReader) ReadPage(page *format.Page, dataType core.DataType) (core.Array, error) {
	if page == nil {
		return nil, core.New(core.ErrInvalidArgument).
			Op("read_page").
			Context("message", "page is nil").
			Build()
	}

	// Handle all-null case: empty data but valid null bitmap
	if len(page.Data) == 0 {
		// Check if this is an all-null page
		if page.NullBitmap != nil && int(page.NumValues) > 0 {
			return r.createAllNullArray(dataType, int(page.NumValues), page.NullBitmap)
		}
		return nil, core.New(core.ErrInvalidArgument).
			Op("read_page").
			Context("message", "page data is empty").
			Build()
	}

	// Get decoder based on encoding type
	decoder, err := encoding.GetDecoder(page.Encoding)
	if err != nil {
		return nil, core.New(core.ErrUnsupportedType).
			Op("get_decoder").
			Context("encoding", page.Encoding).
			Wrap(err).
			Build()
	}

	if decoder == nil {
		return nil, core.New(core.ErrUnsupportedType).
			Op("read_page").
			Context("encoding", "plain").
			Context("message", "plain encoding is not supported: all pages must be encoded").
			Build()
	}

	// Decode the data, passing null bitmap from page if available
	array, err := decoder.Decode(page.Data, dataType, page.NullBitmap, int(page.NumValues))
	if err != nil {
		return nil, core.New(core.ErrDecodeFailed).
			Op("decode_page").
			Context("encoding", page.Encoding).
			Wrap(err).
			Build()
	}

	// Verify the decoded array has correct length
	if array.Len() != int(page.NumValues) {
		return nil, core.New(core.ErrInvalidArgument).
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
// 注意：data 是完整的 Page 字节流（包含 30 字节 header + encoded data + optional null bitmap)
func (r *PageReader) ReadPageFromData(data []byte, encodingType format.EncodingType, numValues int32, dataType core.DataType) (core.Array, error) {
	const PageHeaderSize = 30

	if len(data) < PageHeaderSize {
		return nil, core.New(core.ErrInvalidArgument).
			Op("read_page_from_data").
			Context("expected_bytes", PageHeaderSize).
			Context("actual_bytes", len(data)).
			Context("message", "page data too short").
			Build()
	}

	// 解析 header 获取 compressed size 和 null bitmap size
	var compressedSize int32
	binary.Read(bytes.NewReader(data[14:18]), binary.LittleEndian, &compressedSize)

	// 从 reserved 字段获取 null bitmap size
	// Header 布局: [Type:1][Encoding:1][ColumnIndex:4][NumValues:4][UncompressedSize:4][CompressedSize:4][Checksum:4][Reserved:8]
	// Checksum 在偏移 18-21，Reserved 在偏移 22-29，取 Reserved 前 4 字节
	nullBitmapSize := binary.LittleEndian.Uint32(data[22:26])

	// 提取 encoded data (header 之后，null bitmap 之前)
	dataStart := PageHeaderSize
	dataEnd := PageHeaderSize + int(compressedSize)

	if dataEnd > len(data) {
		return nil, core.New(core.ErrInvalidArgument).
			Op("read_page_from_data").
			Context("expected_data_end", dataEnd).
			Context("actual_data_len", len(data)).
			Context("message", "page data truncated").
			Build()
	}

	encodedData := data[dataStart:dataEnd]

	// 提取 null bitmap
	var nullBitmap []byte
	if nullBitmapSize > 0 {
		bitmapStart := dataEnd
		bitmapEnd := dataEnd + int(nullBitmapSize)
		if bitmapEnd > len(data) {
			return nil, core.New(core.ErrInvalidArgument).
				Op("read_page_from_data").
				Context("expected_bitmap_end", bitmapEnd).
				Context("actual_data_len", len(data)).
				Context("message", "null bitmap truncated").
				Build()
		}
		nullBitmap = data[bitmapStart:bitmapEnd]
	}

	// 处理全 null 情况：encoded data 为空但有 null bitmap
	if len(encodedData) == 0 {
		if nullBitmap != nil && int(numValues) > 0 {
			return r.createAllNullArray(dataType, int(numValues), nullBitmap)
		}
		return nil, core.New(core.ErrInvalidArgument).
			Op("read_page_from_data").
			Context("message", "page data is empty and no null bitmap").
			Build()
	}

	// Get decoder
	decoder, err := encoding.GetDecoder(encodingType)
	if err != nil {
		return nil, core.New(core.ErrUnsupportedType).
			Op("get_decoder").
			Context("encoding", encodingType).
			Wrap(err).
			Build()
	}

	if decoder == nil {
		return nil, core.New(core.ErrUnsupportedType).
			Op("read_page_from_data").
			Context("encoding", "plain").
			Context("message", "plain encoding is not supported: all pages must be encoded").
			Build()
	}

	// Decode with null bitmap
	array, err := decoder.Decode(encodedData, dataType, nullBitmap, int(numValues))
	if err != nil {
		return nil, core.New(core.ErrDecodeFailed).
			Op("decode_page_from_data").
			Context("encoding", encodingType).
			Wrap(err).
			Build()
	}

	if array.Len() != int(numValues) {
		return nil, core.New(core.ErrInvalidArgument).
			Op("verify_decoded_array_from_data").
			Context("expected_values", numValues).
			Context("actual_values", array.Len()).
			Context("message", "decoded array length mismatch").
			Build()
	}

	return array, nil
}

// createAllNullArray creates an array with all null values
func (r *PageReader) createAllNullArray(dataType core.DataType, numValues int, nullBitmap []byte) (core.Array, error) {
	bitmap := core.NewBitmapFromBytes(nullBitmap, numValues)
	
	switch dataType.ID() {
	case core.INT32:
		values := make([]int32, numValues)
		return core.NewInt32Array(values, bitmap), nil
	case core.INT64:
		values := make([]int64, numValues)
		return core.NewInt64Array(values, bitmap), nil
	case core.FLOAT32:
		values := make([]float32, numValues)
		return core.NewFloat32Array(values, bitmap), nil
	case core.FLOAT64:
		values := make([]float64, numValues)
		return core.NewFloat64Array(values, bitmap), nil
	case core.FIXED_SIZE_LIST:
		// 全 null FixedSizeList 数组：需要创建嵌套的全 null 子数组
		listType := dataType.(*core.FixedSizeListType)
		return r.createAllNullFixedSizeListArray(listType, numValues, bitmap)
	default:
		return nil, core.New(core.ErrUnsupportedType).
			Op("create_all_null_array").
			Context("data_type", dataType.Name()).
			Build()
	}
}

// createAllNullFixedSizeListArray 创建全 null 的 FixedSizeList 数组
func (r *PageReader) createAllNullFixedSizeListArray(listType *core.FixedSizeListType, numValues int, bitmap *core.Bitmap) (core.Array, error) {
	elemType := listType.Elem()
	elemSize := listType.Size()
	
	// 创建全 null 的子数组
	totalElemCount := numValues * elemSize
	var elemArray core.Array
	
	switch elemType.ID() {
	case core.FLOAT32:
		elemValues := make([]float32, totalElemCount)
		elemArray = core.NewFloat32Array(elemValues, nil)
	case core.FLOAT64:
		elemValues := make([]float64, totalElemCount)
		elemArray = core.NewFloat64Array(elemValues, nil)
	case core.INT32:
		elemValues := make([]int32, totalElemCount)
		elemArray = core.NewInt32Array(elemValues, nil)
	case core.INT64:
		elemValues := make([]int64, totalElemCount)
		elemArray = core.NewInt64Array(elemValues, nil)
	default:
		return nil, core.New(core.ErrUnsupportedType).
			Op("create_all_null_fixed_size_list").
			Context("elem_type", elemType.Name()).
			Build()
	}
	
	return core.NewFixedSizeListArray(listType, elemArray, bitmap), nil
}
