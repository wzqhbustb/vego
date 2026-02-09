package arrow

// Array is the interface for all Arrow arrays
type Array interface {
	// DataType returns the data type of this array
	DataType() DataType

	// Len returns the number of elements
	Len() int

	// NullN returns the number of null elements
	NullN() int

	// IsNull returns true if element i is null
	IsNull(i int) bool

	// IsValid returns true if element i is not null
	IsValid(i int) bool

	// Data returns the underlying array data
	Data() *ArrayData

	// Release releases the array resources
	Release()
}

// ArrayData holds the memory buffers for an array
type ArrayData struct {
	dtype      DataType
	length     int
	nulls      int          // count of null values
	nullBitmap *Bitmap      // null bitmap (nil means no nulls)
	buffers    []*Buffer    // data buffers
	children   []*ArrayData // for nested types
}

// NewArrayData creates a new ArrayData
func NewArrayData(dtype DataType, length int, buffers []*Buffer, nullBitmap *Bitmap, children []*ArrayData) *ArrayData {
	nulls := 0
	if nullBitmap != nil {
		nulls = nullBitmap.Len() - nullBitmap.CountSet()
	}

	return &ArrayData{
		dtype:      dtype,
		length:     length,
		nulls:      nulls,
		nullBitmap: nullBitmap,
		buffers:    buffers,
		children:   children,
	}
}

// DataType returns the data type
func (d *ArrayData) DataType() DataType { return d.dtype }

// Len returns the number of elements
func (d *ArrayData) Len() int { return d.length }

// NullN returns the number of nulls
func (d *ArrayData) NullN() int { return d.nulls }

// Buffers returns the data buffers
func (d *ArrayData) Buffers() []*Buffer { return d.buffers }

// Children returns child array data (for nested types)
func (d *ArrayData) Children() []*ArrayData { return d.children }

// NullBitmap returns the null bitmap
func (d *ArrayData) NullBitmap() *Bitmap { return d.nullBitmap }

// --- Int32Array ---
type Int32Array struct {
	data *ArrayData
}

// NewInt32Array creates a new int32 array
func NewInt32Array(data []int32, nullBitmap *Bitmap) *Int32Array {
	buf := NewInt32Buffer(data)
	arrayData := NewArrayData(PrimInt32(), len(data), []*Buffer{buf}, nullBitmap, nil)
	return &Int32Array{data: arrayData}
}

func (a *Int32Array) DataType() DataType { return a.data.dtype }
func (a *Int32Array) Len() int           { return a.data.length }
func (a *Int32Array) NullN() int         { return a.data.nulls }
func (a *Int32Array) Data() *ArrayData   { return a.data }
func (a *Int32Array) Release()           {}

func (a *Int32Array) IsNull(i int) bool {
	if a.data.nullBitmap == nil {
		return false
	}
	return !a.data.nullBitmap.IsSet(i)
}

func (a *Int32Array) IsValid(i int) bool {
	return !a.IsNull(i)
}

// Value returns the value at index i
func (a *Int32Array) Value(i int) int32 {
	if i < 0 || i >= a.Len() {
		panic("index out of range")
	}
	return a.data.buffers[0].Int32()[i]
}

// Values returns all values as a slice
func (a *Int32Array) Values() []int32 {
	return a.data.buffers[0].Int32()
}

// --- Int64Array ---
type Int64Array struct {
	data *ArrayData
}

func NewInt64Array(data []int64, nullBitmap *Bitmap) *Int64Array {
	buf := NewInt64Buffer(data)
	arrayData := NewArrayData(PrimInt64(), len(data), []*Buffer{buf}, nullBitmap, nil)
	return &Int64Array{data: arrayData}
}

func (a *Int64Array) DataType() DataType { return a.data.dtype }
func (a *Int64Array) Len() int           { return a.data.length }
func (a *Int64Array) NullN() int         { return a.data.nulls }
func (a *Int64Array) Data() *ArrayData   { return a.data }
func (a *Int64Array) Release()           {}
func (a *Int64Array) IsNull(i int) bool {
	if a.data.nullBitmap == nil {
		return false
	}
	return !a.data.nullBitmap.IsSet(i)
}
func (a *Int64Array) IsValid(i int) bool { return !a.IsNull(i) }

func (a *Int64Array) Value(i int) int64 {
	return a.data.buffers[0].Int64()[i]
}

func (a *Int64Array) Values() []int64 {
	return a.data.buffers[0].Int64()
}

// --- Float32Array ---
type Float32Array struct {
	data *ArrayData
}

func NewFloat32Array(data []float32, nullBitmap *Bitmap) *Float32Array {
	buf := NewFloat32Buffer(data)
	arrayData := NewArrayData(PrimFloat32(), len(data), []*Buffer{buf}, nullBitmap, nil)
	return &Float32Array{data: arrayData}
}

func (a *Float32Array) DataType() DataType { return a.data.dtype }
func (a *Float32Array) Len() int           { return a.data.length }
func (a *Float32Array) NullN() int         { return a.data.nulls }
func (a *Float32Array) Data() *ArrayData   { return a.data }
func (a *Float32Array) Release()           {}
func (a *Float32Array) IsNull(i int) bool {
	if a.data.nullBitmap == nil {
		return false
	}
	return !a.data.nullBitmap.IsSet(i)
}
func (a *Float32Array) IsValid(i int) bool { return !a.IsNull(i) }

func (a *Float32Array) Value(i int) float32 {
	return a.data.buffers[0].Float32()[i]
}

func (a *Float32Array) Values() []float32 {
	return a.data.buffers[0].Float32()
}

// --- Float64Array ---
type Float64Array struct {
	data *ArrayData
}

func NewFloat64Array(data []float64, nullBitmap *Bitmap) *Float64Array {
	buf := NewFloat64Buffer(data)
	arrayData := NewArrayData(PrimFloat64(), len(data), []*Buffer{buf}, nullBitmap, nil)
	return &Float64Array{data: arrayData}
}

func (a *Float64Array) DataType() DataType { return a.data.dtype }
func (a *Float64Array) Len() int           { return a.data.length }
func (a *Float64Array) NullN() int         { return a.data.nulls }
func (a *Float64Array) Data() *ArrayData   { return a.data }
func (a *Float64Array) Release()           {}
func (a *Float64Array) IsNull(i int) bool {
	if a.data.nullBitmap == nil {
		return false
	}
	return !a.data.nullBitmap.IsSet(i)
}
func (a *Float64Array) IsValid(i int) bool { return !a.IsNull(i) }

func (a *Float64Array) Value(i int) float64 {
	return a.data.buffers[0].Float64()[i]
}

func (a *Float64Array) Values() []float64 {
	return a.data.buffers[0].Float64()
}

// --- FixedSizeListArray (for vectors) ---

type FixedSizeListArray struct {
	data   *ArrayData
	values Array // The underlying value array
}

// NewFixedSizeListArray creates a fixed-size list array (for vectors)
func NewFixedSizeListArray(listType *FixedSizeListType, values Array, nullBitmap *Bitmap) *FixedSizeListArray {
	length := values.Len() / listType.Size()
	arrayData := NewArrayData(listType, length, nil, nullBitmap, []*ArrayData{values.Data()})
	return &FixedSizeListArray{
		data:   arrayData,
		values: values,
	}
}

func (a *FixedSizeListArray) DataType() DataType { return a.data.dtype }
func (a *FixedSizeListArray) Len() int           { return a.data.length }
func (a *FixedSizeListArray) NullN() int         { return a.data.nulls }
func (a *FixedSizeListArray) Data() *ArrayData   { return a.data }
func (a *FixedSizeListArray) Release()           { a.values.Release() }
func (a *FixedSizeListArray) IsNull(i int) bool {
	if a.data.nullBitmap == nil {
		return false
	}
	return !a.data.nullBitmap.IsSet(i)
}
func (a *FixedSizeListArray) IsValid(i int) bool { return !a.IsNull(i) }

// ValueArray returns the underlying values array
func (a *FixedSizeListArray) Values() Array {
	return a.values
}

// ListSize returns the fixed size of each list
func (a *FixedSizeListArray) ListSize() int {
	return a.data.dtype.(*FixedSizeListType).Size()
}

// ValueSlice returns the slice for list at index i (zero-copy)
func (a *FixedSizeListArray) ValueSlice(i int) interface{} {
	size := a.ListSize()
	start := i * size
	end := start + size

	switch arr := a.values.(type) {
	case *Float32Array:
		return arr.Values()[start:end]
	case *Int32Array:
		return arr.Values()[start:end]
	default:
		panic("unsupported element type")
	}
}

// --- ListArray (variable-length) ---

type ListArray struct {
	data    *ArrayData
	offsets *Buffer // int32 offsets
	values  Array   // values array
}

// NewListArray creates a variable-length list array
func NewListArray(listType *ListType, offsets []int32, values Array, nullBitmap *Bitmap) *ListArray {
	offsetBuf := NewInt32Buffer(offsets)
	length := len(offsets) - 1 // number of lists
	arrayData := NewArrayData(listType, length, []*Buffer{offsetBuf}, nullBitmap, []*ArrayData{values.Data()})

	return &ListArray{
		data:    arrayData,
		offsets: offsetBuf,
		values:  values,
	}
}

func (a *ListArray) DataType() DataType { return a.data.dtype }
func (a *ListArray) Len() int           { return a.data.length }
func (a *ListArray) NullN() int         { return a.data.nulls }
func (a *ListArray) Data() *ArrayData   { return a.data }
func (a *ListArray) Release()           { a.values.Release() }
func (a *ListArray) IsNull(i int) bool {
	if a.data.nullBitmap == nil {
		return false
	}
	return !a.data.nullBitmap.IsSet(i)
}
func (a *ListArray) IsValid(i int) bool { return !a.IsNull(i) }

// Offsets returns the offset buffer
func (a *ListArray) Offsets() []int32 {
	return a.offsets.Int32()
}

// Values returns the underlying values array
func (a *ListArray) Values() Array {
	return a.values
}

// ValueOffset returns the start and end offset for list at index i
func (a *ListArray) ValueOffsets(i int) (start, end int32) {
	offsets := a.Offsets()
	return offsets[i], offsets[i+1]
}
