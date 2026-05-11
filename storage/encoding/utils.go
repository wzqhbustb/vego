package encoding

import "github.com/wzqhbustb/vego/core"

func GetValueSize(typeID core.TypeID) int {
	switch typeID {
	// case core.INT8, core.UINT8:
	// return 1
	// case core.INT16, core.UINT16:
	// return 2
	case core.INT32 /** core.UINT32, **/, core.FLOAT32:
		return 4
	case core.INT64 /** core.UINT64, **/, core.FLOAT64:
		return 8
	default:
		return 8
	}
}
