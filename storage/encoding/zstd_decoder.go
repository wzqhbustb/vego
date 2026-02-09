package encoding

import (
	"encoding/binary"
	"sync"
	"unsafe"

	lerrors "github.com/wzqhbkjdx/vego/storage/errors"
	"github.com/wzqhbkjdx/vego/storage/arrow"

	"github.com/klauspost/compress/zstd"
)

type ZstdDecoder struct {
	decoderPool *sync.Pool
}

func NewZstdDecoder() (*ZstdDecoder, error) {
	pool := &sync.Pool{
		New: func() interface{} {
			dec, err := zstd.NewReader(nil)
			if err != nil {
				return err
			}
			return dec
		},
	}

	return &ZstdDecoder{decoderPool: pool}, nil
}

func (d *ZstdDecoder) Decode(data []byte, dtype arrow.DataType) (arrow.Array, error) {
	if len(data) < 6 {
		return nil, lerrors.New(lerrors.ErrCorruptedFile).
			Op("zstd_decode").
			Context("reason", "data too short").
			Context("min_required", 6).
			Context("actual", len(data)).
			Build()
	}

	// Get decoder from pool
	decoderRaw := d.decoderPool.Get()
	if err, ok := decoderRaw.(error); ok {
		return nil, lerrors.New(lerrors.ErrDecodeFailed).
			Op("zstd_decode").
			Context("reason", "decoder pool error").
			Wrap(err).
			Build()
	}
	decoder := decoderRaw.(*zstd.Decoder)
	defer d.decoderPool.Put(decoder)

	// Decompress
	decompressed, err := decoder.DecodeAll(data, nil)
	if err != nil {
		return nil, lerrors.New(lerrors.ErrDecodeFailed).
			Op("zstd_decode").
			Context("stage", "decompress").
			Wrap(err).
			Build()
	}

	// Reconstruct array based on type
	return bytesToArray(decompressed, dtype)
}

// bytesToArray converts bytes back to Arrow array
// Format: [numValues:4][values...][bitmapLen:2][bitmap...]
func bytesToArray(data []byte, dtype arrow.DataType) (arrow.Array, error) {
	if len(data) < 6 {
		return nil, lerrors.New(lerrors.ErrCorruptedFile).
			Op("zstd_bytes_to_array").
			Context("reason", "data too short for header").
			Context("min_required", 6).
			Context("actual", len(data)).
			Build()
	}

	numValues := int(binary.LittleEndian.Uint32(data[0:4]))

	switch dtype.ID() {
	case arrow.INT32:
		return bytesToInt32Array(data, numValues)
	case arrow.INT64:
		return bytesToInt64Array(data, numValues)
	case arrow.FLOAT32:
		return bytesToFloat32Array(data, numValues)
	case arrow.FLOAT64:
		return bytesToFloat64Array(data, numValues)
	case arrow.FIXED_SIZE_LIST:
		listType := dtype.(*arrow.FixedSizeListType)
		return bytesToFixedSizeListArray(data, listType, numValues)
	default:
		return nil, lerrors.New(lerrors.ErrUnsupportedType).
			Op("zstd_bytes_to_array").
			Build()
	}
}

func bytesToInt32Array(data []byte, numValues int) (arrow.Array, error) {
	valueSize := 4 * numValues
	if len(data) < 4+valueSize+2 {
		return nil, lerrors.New(lerrors.ErrCorruptedFile).
			Op("zstd_bytes_to_int32").
			Context("reason", "insufficient data").
			Context("expected", 4+valueSize+2).
			Context("actual", len(data)).
			Build()
	}

	// Extract values
	valuesBuf := data[4 : 4+valueSize]
	values := make([]int32, numValues)
	for i := 0; i < numValues; i++ {
		values[i] = int32(binary.LittleEndian.Uint32(valuesBuf[i*4:]))
	}

	// Extract bitmap
	bitmapLen := int(binary.LittleEndian.Uint16(data[4+valueSize:]))
	var nullBitmap *arrow.Bitmap
	if bitmapLen > 0 {
		bitmapStart := 4 + valueSize + 2
		if len(data) < bitmapStart+bitmapLen {
			return nil, lerrors.New(lerrors.ErrCorruptedFile).
				Op("zstd_bytes_to_int32").
				Context("reason", "insufficient data for bitmap").
				Context("expected", bitmapStart+bitmapLen).
				Context("actual", len(data)).
				Build()
		}
		bitmapData := data[bitmapStart : bitmapStart+bitmapLen]
		nullBitmap = arrow.NewBitmapFromBytes(bitmapData, numValues)
	}

	return arrow.NewInt32Array(values, nullBitmap), nil
}

func bytesToInt64Array(data []byte, numValues int) (arrow.Array, error) {
	valueSize := 8 * numValues
	if len(data) < 4+valueSize+2 {
		return nil, lerrors.New(lerrors.ErrCorruptedFile).
			Op("zstd_bytes_to_int64").
			Context("reason", "insufficient data").
			Context("expected", 4+valueSize+2).
			Context("actual", len(data)).
			Build()
	}

	valuesBuf := data[4 : 4+valueSize]
	values := make([]int64, numValues)
	for i := 0; i < numValues; i++ {
		values[i] = int64(binary.LittleEndian.Uint64(valuesBuf[i*8:]))
	}

	bitmapLen := int(binary.LittleEndian.Uint16(data[4+valueSize:]))
	var nullBitmap *arrow.Bitmap
	if bitmapLen > 0 {
		bitmapStart := 4 + valueSize + 2
		if len(data) < bitmapStart+bitmapLen {
			return nil, lerrors.New(lerrors.ErrCorruptedFile).
				Op("zstd_bytes_to_int64").
				Context("reason", "insufficient data for bitmap").
				Context("expected", bitmapStart+bitmapLen).
				Context("actual", len(data)).
				Build()
		}
		bitmapData := data[bitmapStart : bitmapStart+bitmapLen]
		nullBitmap = arrow.NewBitmapFromBytes(bitmapData, numValues)
	}

	return arrow.NewInt64Array(values, nullBitmap), nil
}

func bytesToFloat32Array(data []byte, numValues int) (arrow.Array, error) {
	// Reuse int32 deserialization then convert bits
	arr, err := bytesToInt32Array(data, numValues)
	if err != nil {
		return nil, err
	}
	int32Arr := arr.(*arrow.Int32Array)
	values := int32Arr.Values()

	// Convert int32 bits to float32
	floatValues := make([]float32, numValues)
	for i, v := range values {
		floatValues[i] = float32FromBits(uint32(v))
	}

	// Get null bitmap if exists
	var nullBitmap *arrow.Bitmap
	if int32Arr.NullN() > 0 {
		nullBitmap = int32Arr.Data().NullBitmap()
	}

	return arrow.NewFloat32Array(floatValues, nullBitmap), nil
}

func bytesToFloat64Array(data []byte, numValues int) (arrow.Array, error) {
	arr, err := bytesToInt64Array(data, numValues)
	if err != nil {
		return nil, err
	}
	int64Arr := arr.(*arrow.Int64Array)
	values := int64Arr.Values()

	floatValues := make([]float64, numValues)
	for i, v := range values {
		floatValues[i] = float64FromBits(uint64(v))
	}

	var nullBitmap *arrow.Bitmap
	if int64Arr.NullN() > 0 {
		nullBitmap = int64Arr.Data().NullBitmap()
	}

	return arrow.NewFloat64Array(floatValues, nullBitmap), nil
}

// bytesToFixedSizeListArray 解码 FixedSizeListArray
// 格式: [numLists:4][childValues...][bitmapLen:2][listNullBitmap...]
func bytesToFixedSizeListArray(data []byte, listType *arrow.FixedSizeListType, numLists int) (arrow.Array, error) {
	elemType := listType.Elem()
	listSize := listType.Size()

	// Total child elements = numLists * listSize
	totalChildValues := numLists * listSize

	// 计算 header 大小
	// [numLists:4] 已经读取，现在需要跳过它来获取 child 数据
	childDataStart := 4

	// 为 child array 构造一个模拟的数据包
	// 格式: [numChildValues:4][childValues...][bitmapLen:2][bitmap...]
	// 注意：对于 FixedSizeListArray，我们不存储 child-level 的 bitmap，只存储 list-level 的
	// 所以 child 的 bitmapLen 是 0

	// 计算 child values 的大小
	childValueSize := 0
	switch elemType.ID() {
	case arrow.FLOAT32:
		childValueSize = 4 * totalChildValues
	case arrow.INT32:
		childValueSize = 4 * totalChildValues
	case arrow.FLOAT64:
		childValueSize = 8 * totalChildValues
	case arrow.INT64:
		childValueSize = 8 * totalChildValues
	default:
		return nil, lerrors.New(lerrors.ErrUnsupportedType).
			Op("zstd_bytes_to_fsl").
			Context("element_type_id", elemType.ID()).
			Build()
	}

	// 检查数据是否足够
	minSize := 4 + childValueSize + 2
	if len(data) < minSize {
		return nil, lerrors.New(lerrors.ErrCorruptedFile).
			Op("zstd_bytes_to_fsl").
			Context("reason", "insufficient data").
			Context("expected", minSize).
			Context("actual", len(data)).
			Build()
	}

	// 提取 child values
	childValuesEnd := childDataStart + childValueSize
	childValuesData := data[childDataStart:childValuesEnd]

	// 创建 child array 的数据包（不包含 bitmap）
	childPacket := make([]byte, 4+childValueSize+2)
	binary.LittleEndian.PutUint32(childPacket[0:4], uint32(totalChildValues))
	copy(childPacket[4:4+childValueSize], childValuesData)
	binary.LittleEndian.PutUint16(childPacket[4+childValueSize:4+childValueSize+2], 0) // no child bitmap

	// 解码 child array
	var childArray arrow.Array
	var err error

	switch elemType.ID() {
	case arrow.FLOAT32:
		childArray, err = bytesToFloat32Array(childPacket, totalChildValues)
	case arrow.INT32:
		childArray, err = bytesToInt32Array(childPacket, totalChildValues)
	case arrow.FLOAT64:
		childArray, err = bytesToFloat64Array(childPacket, totalChildValues)
	case arrow.INT64:
		childArray, err = bytesToInt64Array(childPacket, totalChildValues)
	}

	if err != nil {
		return nil, lerrors.New(lerrors.ErrDecodeFailed).
			Op("zstd_bytes_to_fsl").
			Context("stage", "decode_child").
			Wrap(err).
			Build()
	}

	// 提取 list-level null bitmap
	bitmapLenOffset := childValuesEnd
	bitmapLen := int(binary.LittleEndian.Uint16(data[bitmapLenOffset : bitmapLenOffset+2]))

	var listNullBitmap *arrow.Bitmap
	if bitmapLen > 0 {
		bitmapStart := bitmapLenOffset + 2
		if len(data) < bitmapStart+bitmapLen {
			return nil, lerrors.New(lerrors.ErrCorruptedFile).
				Op("zstd_bytes_to_fsl").
				Context("reason", "insufficient data for list null bitmap").
				Context("expected", bitmapStart+bitmapLen).
				Context("actual", len(data)).
				Build()
		}
		bitmapData := data[bitmapStart : bitmapStart+bitmapLen]
		listNullBitmap = arrow.NewBitmapFromBytes(bitmapData, numLists)
	}

	return arrow.NewFixedSizeListArray(listType, childArray, listNullBitmap), nil
}

func float32FromBits(bits uint32) float32 {
	return *(*float32)(unsafe.Pointer(&bits))
}

func float64FromBits(bits uint64) float64 {
	return *(*float64)(unsafe.Pointer(&bits))
}
