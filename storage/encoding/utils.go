package encoding

import "github.com/wzqhbkjdx/vego/storage/arrow"

func GetValueSize(typeID arrow.TypeID) int {
	switch typeID {
	// case arrow.INT8, arrow.UINT8:
	// return 1
	// case arrow.INT16, arrow.UINT16:
	// return 2
	case arrow.INT32 /** arrow.UINT32, **/, arrow.FLOAT32:
		return 4
	case arrow.INT64 /** arrow.UINT64, **/, arrow.FLOAT64:
		return 8
	default:
		return 8
	}
}
