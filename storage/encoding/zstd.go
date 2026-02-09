package encoding

import (
	"encoding/binary"
	"sync"

	lerrors "github.com/wzqhbkjdx/vego/storage/errors"
	"github.com/wzqhbkjdx/vego/storage/arrow"
	"github.com/wzqhbkjdx/vego/storage/format"

	"github.com/klauspost/compress/zstd"
)

type ZstdEncoder struct {
	level       int
	encoderPool *sync.Pool
}

func NewZstdEncoder(level int) *ZstdEncoder {
	if level < 1 {
		level = 1
	}
	if level > 9 {
		level = 9
	}

	pool := &sync.Pool{
		New: func() interface{} {
			var encoderLevel zstd.EncoderLevel
			switch {
			case level <= 3:
				encoderLevel = zstd.SpeedFastest
			case level <= 6:
				encoderLevel = zstd.SpeedDefault
			case level <= 8:
				encoderLevel = zstd.SpeedBetterCompression
			default:
				encoderLevel = zstd.SpeedBestCompression
			}
			enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(encoderLevel))
			return enc
		},
	}

	return &ZstdEncoder{level: level, encoderPool: pool}
}

func (e *ZstdEncoder) Type() format.EncodingType {
	return format.EncodingZstd
}

func (e *ZstdEncoder) Encode(array arrow.Array) (*EncodedData, error) {
	if array.Len() == 0 {
		return nil, ErrEmptyArray
	}

	// 将 Array 转换为字节（包含 values 和 null bitmap）
	data, err := arrayToBytesWithNull(array)
	if err != nil {
		return nil, lerrors.New(lerrors.ErrEncodeFailed).
			Op("zstd_encode").
			Context("stage", "array_to_bytes").
			Wrap(err).
			Build()
	}

	encoder := e.encoderPool.Get().(*zstd.Encoder)
	defer e.encoderPool.Put(encoder)

	encoder.Reset(nil)
	compressed := encoder.EncodeAll(data, make([]byte, 0, len(data)/2))

	return &EncodedData{
		Data:     compressed,
		Type:     format.EncodingZstd,
		Metadata: nil,
	}, nil
}

func (e *ZstdEncoder) EstimateSize(array arrow.Array) int {
	// 保守估计：原始大小的 50%
	return array.Len() * GetValueSize(array.DataType().ID()) / 2
}

func (e *ZstdEncoder) SupportsType(dtype arrow.DataType) bool {
	return true // Zstd 支持所有类型
}

// arrayToBytesWithNull 将 Array 转换为字节（包含 values 和 null bitmap）
// 格式: [numValues:4][values...][bitmapLen:2][bitmap...]
// 对于 FixedSizeListArray，bitmap 是 list-level 的
func arrayToBytesWithNull(array arrow.Array) ([]byte, error) {
	numValues := array.Len()

	// 获取 values 的字节
	valuesBytes, err := ArrayToBytes(array)
	if err != nil {
		return nil, err
	}

	// 计算 bitmap 大小
	var bitmapBytes []byte
	if array.NullN() > 0 {
		nullBitmap := array.Data().NullBitmap()
		if nullBitmap != nil {
			// 只取需要的字节数
			bitmapSize := (numValues + 7) / 8
			bitmapBytes = nullBitmap.Bytes()
			if len(bitmapBytes) > bitmapSize {
				bitmapBytes = bitmapBytes[:bitmapSize]
			}
		}
	}

	// 构建输出: [numValues:4][values...][bitmapLen:2][bitmap...]
	totalSize := 4 + len(valuesBytes) + 2 + len(bitmapBytes)
	buf := make([]byte, 0, totalSize)

	// Write numValues (4 bytes)
	numValuesBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(numValuesBuf, uint32(numValues))
	buf = append(buf, numValuesBuf...)

	// Write values
	buf = append(buf, valuesBytes...)

	// Write bitmapLen (2 bytes) and bitmap
	bitmapLenBuf := make([]byte, 2)
	binary.LittleEndian.PutUint16(bitmapLenBuf, uint16(len(bitmapBytes)))
	buf = append(buf, bitmapLenBuf...)
	buf = append(buf, bitmapBytes...)

	return buf, nil
}
