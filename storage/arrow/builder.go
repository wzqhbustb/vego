package arrow

// Builder is the interface for building arrays incrementally
type Builder interface {
	// Reserve reserves space for n additional elements
	Reserve(n int)

	// AppendNull appends a null value
	AppendNull()

	// Len returns the number of elements appended
	Len() int

	// NewArray builds the array (resets builder)
	NewArray() Array

	// Release releases builder resources
	Release()
}

// --- Int32Builder ---

type Int32Builder struct {
	data     []int32
	nulls    *Bitmap
	hasNulls bool
}

func NewInt32Builder() *Int32Builder {
	return &Int32Builder{
		data:  make([]int32, 0, 16),
		nulls: NewBitmap(0),
	}
}

func (b *Int32Builder) Reserve(n int) {
	if cap(b.data)-len(b.data) < n {
		newCap := len(b.data) + n
		newData := make([]int32, len(b.data), newCap)
		copy(newData, b.data)
		b.data = newData
	}
}

func (b *Int32Builder) Append(v int32) {
	b.data = append(b.data, v)
	if b.hasNulls {
		b.nulls.Resize(len(b.data))
		b.nulls.Set(len(b.data) - 1)
	}
}

func (b *Int32Builder) AppendNull() {
	if !b.hasNulls {
		b.hasNulls = true
		b.nulls = NewBitmap(len(b.data))
		b.nulls.SetAll()
	}
	b.data = append(b.data, 0) // placeholder
	b.nulls.Resize(len(b.data))
	b.nulls.Clear(len(b.data) - 1)
}

func (b *Int32Builder) Len() int {
	return len(b.data)
}

func (b *Int32Builder) NewArray() Array {
	var nullBitmap *Bitmap
	if b.hasNulls {
		nullBitmap = b.nulls
	}

	arr := NewInt32Array(b.data, nullBitmap)

	// Reset
	b.data = make([]int32, 0, 16)
	b.nulls = NewBitmap(0)
	b.hasNulls = false

	return arr
}

func (b *Int32Builder) Release() {}

// --- Int64Builder ---

type Int64Builder struct {
	data     []int64
	nulls    *Bitmap
	hasNulls bool
}

func NewInt64Builder() *Int64Builder {
	return &Int64Builder{
		data:  make([]int64, 0, 16),
		nulls: NewBitmap(0),
	}
}

func (b *Int64Builder) Reserve(n int) {
	if cap(b.data)-len(b.data) < n {
		newCap := len(b.data) + n
		newData := make([]int64, len(b.data), newCap)
		copy(newData, b.data)
		b.data = newData
	}
}

func (b *Int64Builder) Append(v int64) {
	b.data = append(b.data, v)
	if b.hasNulls {
		b.nulls.Resize(len(b.data))
		b.nulls.Set(len(b.data) - 1)
	}
}

func (b *Int64Builder) AppendNull() {
	if !b.hasNulls {
		b.hasNulls = true
		b.nulls = NewBitmap(len(b.data))
		b.nulls.SetAll()
	}
	b.data = append(b.data, 0) // placeholder
	b.nulls.Resize(len(b.data))
	b.nulls.Clear(len(b.data) - 1)
}

func (b *Int64Builder) Len() int {
	return len(b.data)
}

func (b *Int64Builder) NewArray() Array {
	var nullBitmap *Bitmap
	if b.hasNulls {
		nullBitmap = b.nulls
	}

	arr := NewInt64Array(b.data, nullBitmap)

	// Reset
	b.data = make([]int64, 0, 16)
	b.nulls = NewBitmap(0)
	b.hasNulls = false

	return arr
}

func (b *Int64Builder) Release() {}

// --- Float32Builder ---

type Float32Builder struct {
	data     []float32
	nulls    *Bitmap
	hasNulls bool
}

func NewFloat32Builder() *Float32Builder {
	return &Float32Builder{
		data:  make([]float32, 0, 16),
		nulls: NewBitmap(0),
	}
}

func (b *Float32Builder) Reserve(n int) {
	if cap(b.data)-len(b.data) < n {
		newCap := len(b.data) + n
		newData := make([]float32, len(b.data), newCap)
		copy(newData, b.data)
		b.data = newData
	}
}

func (b *Float32Builder) Append(v float32) {
	b.data = append(b.data, v)
	if b.hasNulls {
		b.nulls.Resize(len(b.data))
		b.nulls.Set(len(b.data) - 1)
	}
}

func (b *Float32Builder) AppendNull() {
	if !b.hasNulls {
		b.hasNulls = true
		b.nulls = NewBitmap(len(b.data))
		b.nulls.SetAll()
	}
	b.data = append(b.data, 0)
	b.nulls.Resize(len(b.data))
	b.nulls.Clear(len(b.data) - 1)
}

func (b *Float32Builder) Len() int {
	return len(b.data)
}

func (b *Float32Builder) NewArray() Array {
	var nullBitmap *Bitmap
	if b.hasNulls {
		nullBitmap = b.nulls
	}

	arr := NewFloat32Array(b.data, nullBitmap)

	b.data = make([]float32, 0, 16)
	b.nulls = NewBitmap(0)
	b.hasNulls = false

	return arr
}

func (b *Float32Builder) Release() {}

// --- Float64Builder ---

type Float64Builder struct {
	data     []float64
	nulls    *Bitmap
	hasNulls bool
}

func NewFloat64Builder() *Float64Builder {
	return &Float64Builder{
		data:  make([]float64, 0, 16),
		nulls: NewBitmap(0),
	}
}

func (b *Float64Builder) Reserve(n int) {
	if cap(b.data)-len(b.data) < n {
		newCap := len(b.data) + n
		newData := make([]float64, len(b.data), newCap)
		copy(newData, b.data)
		b.data = newData
	}
}

func (b *Float64Builder) Append(v float64) {
	b.data = append(b.data, v)
	if b.hasNulls {
		b.nulls.Resize(len(b.data))
		b.nulls.Set(len(b.data) - 1)
	}
}

func (b *Float64Builder) AppendNull() {
	if !b.hasNulls {
		b.hasNulls = true
		b.nulls = NewBitmap(len(b.data))
		b.nulls.SetAll()
	}
	b.data = append(b.data, 0)
	b.nulls.Resize(len(b.data))
	b.nulls.Clear(len(b.data) - 1)
}

func (b *Float64Builder) Len() int {
	return len(b.data)
}

func (b *Float64Builder) NewArray() Array {
	var nullBitmap *Bitmap
	if b.hasNulls {
		nullBitmap = b.nulls
	}

	arr := NewFloat64Array(b.data, nullBitmap)

	b.data = make([]float64, 0, 16)
	b.nulls = NewBitmap(0)
	b.hasNulls = false

	return arr
}

func (b *Float64Builder) Release() {}

// --- FixedSizeListBuilder (for vectors) ---

type FixedSizeListBuilder struct {
	listType *FixedSizeListType
	values   *Float32Builder
	nulls    *Bitmap
	hasNulls bool
	length   int // number of lists
}

func NewFixedSizeListBuilder(listType *FixedSizeListType) *FixedSizeListBuilder {
	return &FixedSizeListBuilder{
		listType: listType,
		values:   NewFloat32Builder(),
		nulls:    NewBitmap(0),
	}
}

func (b *FixedSizeListBuilder) Reserve(n int) {
	b.values.Reserve(n * b.listType.Size())
}

// AppendValues appends a complete list
func (b *FixedSizeListBuilder) AppendValues(values []float32) {
	if len(values) != b.listType.Size() {
		panic("fixed-size list size mismatch")
	}
	for _, v := range values {
		b.values.Append(v)
	}
	if b.hasNulls {
		b.nulls.Resize(b.length + 1)
		b.nulls.Set(b.length)
	}
	b.length++
}

func (b *FixedSizeListBuilder) AppendNull() {
	if !b.hasNulls {
		b.hasNulls = true
		b.nulls = NewBitmap(b.length)
		b.nulls.SetAll()
	}
	// Append placeholder values
	for i := 0; i < b.listType.Size(); i++ {
		b.values.Append(0)
	}
	b.nulls.Resize(b.length + 1)
	b.nulls.Clear(b.length)
	b.length++
}

func (b *FixedSizeListBuilder) Len() int {
	return b.length
}

func (b *FixedSizeListBuilder) NewArray() Array {
	valuesArr := b.values.NewArray()

	var nullBitmap *Bitmap
	if b.hasNulls {
		nullBitmap = b.nulls
	}

	arr := NewFixedSizeListArray(b.listType, valuesArr, nullBitmap)

	b.length = 0
	b.nulls = NewBitmap(0)
	b.hasNulls = false

	return arr
}

func (b *FixedSizeListBuilder) Release() {
	b.values.Release()
}

// --- ListBuilder (variable-length) ---

type ListBuilder struct {
	listType *ListType
	offsets  []int32
	values   Builder
	nulls    *Bitmap
	hasNulls bool
}

func NewListBuilder(listType *ListType, valueBuilder Builder) *ListBuilder {
	return &ListBuilder{
		listType: listType,
		offsets:  []int32{0}, // Start with offset 0
		values:   valueBuilder,
		nulls:    NewBitmap(0),
	}
}

func (b *ListBuilder) Reserve(n int) {
	// Reserve for offsets
	if cap(b.offsets)-len(b.offsets) < n {
		newOffsets := make([]int32, len(b.offsets), len(b.offsets)+n)
		copy(newOffsets, b.offsets)
		b.offsets = newOffsets
	}
}

// Append marks the start of a new list
func (b *ListBuilder) Append(valid bool) {
	currentOffset := b.offsets[len(b.offsets)-1]
	b.offsets = append(b.offsets, currentOffset) // Will be updated by ValueBuilder

	if !valid {
		if !b.hasNulls {
			b.hasNulls = true
			b.nulls = NewBitmap(len(b.offsets) - 2)
			b.nulls.SetAll()
		}
		b.nulls.Resize(len(b.offsets) - 1)
		b.nulls.Clear(len(b.offsets) - 2)
	} else if b.hasNulls {
		b.nulls.Resize(len(b.offsets) - 1)
		b.nulls.Set(len(b.offsets) - 2)
	}
}

// ValueBuilder returns the value builder
func (b *ListBuilder) ValueBuilder() Builder {
	return b.values
}

// UpdateOffset updates the last offset to current value builder length
func (b *ListBuilder) UpdateOffset() {
	b.offsets[len(b.offsets)-1] = int32(b.values.Len())
}

func (b *ListBuilder) AppendNull() {
	b.Append(false)
	b.UpdateOffset()
}

func (b *ListBuilder) Len() int {
	return len(b.offsets) - 1
}

func (b *ListBuilder) NewArray() Array {
	valuesArr := b.values.NewArray()

	var nullBitmap *Bitmap
	if b.hasNulls {
		nullBitmap = b.nulls
	}

	arr := NewListArray(b.listType, b.offsets, valuesArr, nullBitmap)

	b.offsets = []int32{0}
	b.nulls = NewBitmap(0)
	b.hasNulls = false

	return arr
}

func (b *ListBuilder) Release() {
	b.values.Release()
}
