package encoding

import (
	"encoding/binary"
	"sync"
	"unsafe"

	lerrors "github.com/wzqhbustb/vego/storage/errors"
	"github.com/wzqhbustb/vego/core"

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

// Decode decompresses zstd data and reconstructs the Arrow array.
// Note: Zstd encoding embeds null information within the compressed data itself
// (format: [numValues:4][values...][bitmapLen:4][bitmap...]), so the nullBitmap
// and numValues parameters are ignored. This is different from other encoders
// (RLE/BitPacking/BSS/Dictionary) which store null bitmap separately.
func (d *ZstdDecoder) Decode(data []byte, dtype core.DataType, nullBitmap []byte, numValues int) (core.Array, error) {
	if len(data) < 8 {
		return nil, lerrors.New(lerrors.ErrCorruptedFile).
			Op("zstd_decode").
			Context("reason", "data too short").
			Context("min_required", 8).
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
// Format: [numValues:4][values...][bitmapLen:4][bitmap...]
// Note: bitmapLen uses uint32 (4 bytes) to support large datasets (>520k rows)
func bytesToArray(data []byte, dtype core.DataType) (core.Array, error) {
	if len(data) < 8 {
		return nil, lerrors.New(lerrors.ErrCorruptedFile).
			Op("zstd_bytes_to_array").
			Context("reason", "data too short for header").
			Context("min_required", 8).
			Context("actual", len(data)).
			Build()
	}

	numValues := int(binary.LittleEndian.Uint32(data[0:4]))

	switch dtype.ID() {
	case core.INT32:
		return bytesToInt32Array(data, numValues)
	case core.INT64:
		return bytesToInt64Array(data, numValues)
	case core.FLOAT32:
		return bytesToFloat32Array(data, numValues)
	case core.FLOAT64:
		return bytesToFloat64Array(data, numValues)
	case core.FIXED_SIZE_LIST:
		listType := dtype.(*core.FixedSizeListType)
		return bytesToFixedSizeListArray(data, listType, numValues)
	default:
		return nil, lerrors.New(lerrors.ErrUnsupportedType).
			Op("zstd_bytes_to_array").
			Build()
	}
}

func bytesToInt32Array(data []byte, numValues int) (core.Array, error) {
	valueSize := 4 * numValues
	if len(data) < 4+valueSize+4 {
		return nil, lerrors.New(lerrors.ErrCorruptedFile).
			Op("zstd_bytes_to_int32").
			Context("reason", "insufficient data").
			Context("expected", 4+valueSize+4).
			Context("actual", len(data)).
			Build()
	}

	// Extract values
	valuesBuf := data[4 : 4+valueSize]
	values := make([]int32, numValues)
	for i := 0; i < numValues; i++ {
		values[i] = int32(binary.LittleEndian.Uint32(valuesBuf[i*4:]))
	}

	// Extract bitmap (bitmapLen is uint32 = 4 bytes)
	bitmapLen := int(binary.LittleEndian.Uint32(data[4+valueSize:]))
	var nullBitmap *core.Bitmap
	if bitmapLen > 0 {
		bitmapStart := 4 + valueSize + 4
		if len(data) < bitmapStart+bitmapLen {
			return nil, lerrors.New(lerrors.ErrCorruptedFile).
				Op("zstd_bytes_to_int32").
				Context("reason", "insufficient data for bitmap").
				Context("expected", bitmapStart+bitmapLen).
				Context("actual", len(data)).
				Build()
		}
		bitmapData := data[bitmapStart : bitmapStart+bitmapLen]
		nullBitmap = core.NewBitmapFromBytes(bitmapData, numValues)
	}

	return core.NewInt32Array(values, nullBitmap), nil
}

func bytesToInt64Array(data []byte, numValues int) (core.Array, error) {
	valueSize := 8 * numValues
	if len(data) < 4+valueSize+4 {
		return nil, lerrors.New(lerrors.ErrCorruptedFile).
			Op("zstd_bytes_to_int64").
			Context("reason", "insufficient data").
			Context("expected", 4+valueSize+4).
			Context("actual", len(data)).
			Build()
	}

	valuesBuf := data[4 : 4+valueSize]
	values := make([]int64, numValues)
	for i := 0; i < numValues; i++ {
		values[i] = int64(binary.LittleEndian.Uint64(valuesBuf[i*8:]))
	}

	bitmapLen := int(binary.LittleEndian.Uint32(data[4+valueSize:]))
	var nullBitmap *core.Bitmap
	if bitmapLen > 0 {
		bitmapStart := 4 + valueSize + 4
		if len(data) < bitmapStart+bitmapLen {
			return nil, lerrors.New(lerrors.ErrCorruptedFile).
				Op("zstd_bytes_to_int64").
				Context("reason", "insufficient data for bitmap").
				Context("expected", bitmapStart+bitmapLen).
				Context("actual", len(data)).
				Build()
		}
		bitmapData := data[bitmapStart : bitmapStart+bitmapLen]
		nullBitmap = core.NewBitmapFromBytes(bitmapData, numValues)
	}

	return core.NewInt64Array(values, nullBitmap), nil
}

func bytesToFloat32Array(data []byte, numValues int) (core.Array, error) {
	// Reuse int32 deserialization then convert bits
	arr, err := bytesToInt32Array(data, numValues)
	if err != nil {
		return nil, err
	}
	int32Arr := arr.(*core.Int32Array)
	values := int32Arr.Values()

	// Convert int32 bits to float32
	floatValues := make([]float32, numValues)
	for i, v := range values {
		floatValues[i] = float32FromBits(uint32(v))
	}

	// Get null bitmap if exists
	var nullBitmap *core.Bitmap
	if int32Arr.NullN() > 0 {
		nullBitmap = int32Arr.Data().NullBitmap()
	}

	return core.NewFloat32Array(floatValues, nullBitmap), nil
}

func bytesToFloat64Array(data []byte, numValues int) (core.Array, error) {
	arr, err := bytesToInt64Array(data, numValues)
	if err != nil {
		return nil, err
	}
	int64Arr := arr.(*core.Int64Array)
	values := int64Arr.Values()

	floatValues := make([]float64, numValues)
	for i, v := range values {
		floatValues[i] = float64FromBits(uint64(v))
	}

	var nullBitmap *core.Bitmap
	if int64Arr.NullN() > 0 {
		nullBitmap = int64Arr.Data().NullBitmap()
	}

	return core.NewFloat64Array(floatValues, nullBitmap), nil
}

// bytesToFixedSizeListArray 解码 FixedSizeListArray
// 格式: [numLists:4][childValues...][bitmapLen:4][listNullBitmap...]
func bytesToFixedSizeListArray(data []byte, listType *core.FixedSizeListType, numLists int) (core.Array, error) {
	elemType := listType.Elem()
	listSize := listType.Size()

	// Total child elements = numLists * listSize
	totalChildValues := numLists * listSize

	// 计算 header 大小
	// [numLists:4] 已经读取，现在需要跳过它来获取 child 数据
	childDataStart := 4

	// 为 child array 构造一个模拟的数据包
	// 格式: [numChildValues:4][childValues...][bitmapLen:4][bitmap...]
	// 注意：对于 FixedSizeListArray，我们不存储 child-level 的 bitmap，只存储 list-level 的
	// 所以 child 的 bitmapLen 是 0

	// 计算 child values 的大小
	childValueSize := 0
	switch elemType.ID() {
	case core.FLOAT32:
		childValueSize = 4 * totalChildValues
	case core.INT32:
		childValueSize = 4 * totalChildValues
	case core.FLOAT64:
		childValueSize = 8 * totalChildValues
	case core.INT64:
		childValueSize = 8 * totalChildValues
	default:
		return nil, lerrors.New(lerrors.ErrUnsupportedType).
			Op("zstd_bytes_to_fsl").
			Context("element_type_id", elemType.ID()).
			Build()
	}

	// 检查数据是否足够
	minSize := 4 + childValueSize + 4
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
	childPacket := make([]byte, 4+childValueSize+4)
	binary.LittleEndian.PutUint32(childPacket[0:4], uint32(totalChildValues))
	copy(childPacket[4:4+childValueSize], childValuesData)
	binary.LittleEndian.PutUint32(childPacket[4+childValueSize:4+childValueSize+4], 0) // no child bitmap

	// 解码 child array
	var childArray core.Array
	var err error

	switch elemType.ID() {
	case core.FLOAT32:
		childArray, err = bytesToFloat32Array(childPacket, totalChildValues)
	case core.INT32:
		childArray, err = bytesToInt32Array(childPacket, totalChildValues)
	case core.FLOAT64:
		childArray, err = bytesToFloat64Array(childPacket, totalChildValues)
	case core.INT64:
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
	bitmapLen := int(binary.LittleEndian.Uint32(data[bitmapLenOffset : bitmapLenOffset+4]))

	var listNullBitmap *core.Bitmap
	if bitmapLen > 0 {
		bitmapStart := bitmapLenOffset + 4
		if len(data) < bitmapStart+bitmapLen {
			return nil, lerrors.New(lerrors.ErrCorruptedFile).
				Op("zstd_bytes_to_fsl").
				Context("reason", "insufficient data for list null bitmap").
				Context("expected", bitmapStart+bitmapLen).
				Context("actual", len(data)).
				Build()
		}
		bitmapData := data[bitmapStart : bitmapStart+bitmapLen]
		listNullBitmap = core.NewBitmapFromBytes(bitmapData, numLists)
	}

	return core.NewFixedSizeListArray(listType, childArray, listNullBitmap), nil
}

func float32FromBits(bits uint32) float32 {
	return *(*float32)(unsafe.Pointer(&bits))
}

func float64FromBits(bits uint64) float64 {
	return *(*float64)(unsafe.Pointer(&bits))
}
