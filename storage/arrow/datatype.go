package arrow

import "fmt"

// TypeID is an enum of supported data types
type TypeID int

const (
	INT32 TypeID = iota
	INT64
	FLOAT32
	FLOAT64
	BINARY
	STRING
	FIXED_SIZE_LIST
	LIST
	STRUCT
)

// DataType represents the type of data stored in a column
type DataType interface {
	ID() TypeID
	Name() string
	ByteWidth() int // -1 for variable length
}

// --- Primitive Types ---

type Int32Type struct{}

func (t *Int32Type) ID() TypeID     { return INT32 }
func (t *Int32Type) Name() string   { return "int32" }
func (t *Int32Type) ByteWidth() int { return 4 }

type Int64Type struct{}

func (t *Int64Type) ID() TypeID     { return INT64 }
func (t *Int64Type) Name() string   { return "int64" }
func (t *Int64Type) ByteWidth() int { return 8 }

type Float32Type struct{}

func (t *Float32Type) ID() TypeID     { return FLOAT32 }
func (t *Float32Type) Name() string   { return "float32" }
func (t *Float32Type) ByteWidth() int { return 4 }

type Float64Type struct{}

func (t *Float64Type) ID() TypeID     { return FLOAT64 }
func (t *Float64Type) Name() string   { return "float64" }
func (t *Float64Type) ByteWidth() int { return 8 }

// --- Variable-Length Types ---

type BinaryType struct{}

func (t *BinaryType) ID() TypeID     { return BINARY }
func (t *BinaryType) Name() string   { return "binary" }
func (t *BinaryType) ByteWidth() int { return -1 }

type StringType struct{}

func (t *StringType) ID() TypeID     { return STRING }
func (t *StringType) Name() string   { return "utf8" }
func (t *StringType) ByteWidth() int { return -1 }

// --- Nested Types ---

// FixedSizeListType represents a fixed-size list (used for vectors)
type FixedSizeListType struct {
	elem DataType
	size int
}

func (t *FixedSizeListType) ID() TypeID { return FIXED_SIZE_LIST }
func (t *FixedSizeListType) Name() string {
	return fmt.Sprintf("fixed_size_list<%s>[%d]", t.elem.Name(), t.size)
}
func (t *FixedSizeListType) ByteWidth() int {
	if t.elem.ByteWidth() < 0 {
		return -1
	}
	return t.elem.ByteWidth() * t.size
}
func (t *FixedSizeListType) Elem() DataType { return t.elem }
func (t *FixedSizeListType) Size() int      { return t.size }

// ListType represents a variable-length list
type ListType struct {
	elem DataType
}

func (t *ListType) ID() TypeID     { return LIST }
func (t *ListType) Name() string   { return fmt.Sprintf("list<%s>", t.elem.Name()) }
func (t *ListType) ByteWidth() int { return -1 }
func (t *ListType) Elem() DataType { return t.elem }

// StructType represents a struct with named fields
type StructType struct {
	fields []Field
}

func (t *StructType) ID() TypeID      { return STRUCT }
func (t *StructType) Name() string    { return "struct" }
func (t *StructType) ByteWidth() int  { return -1 }
func (t *StructType) Fields() []Field { return t.fields }

// --- Type Constructors ---

func PrimInt32() DataType   { return &Int32Type{} }
func PrimInt64() DataType   { return &Int64Type{} }
func PrimFloat32() DataType { return &Float32Type{} }
func PrimFloat64() DataType { return &Float64Type{} }
func PrimBinary() DataType  { return &BinaryType{} }
func PrimString() DataType  { return &StringType{} }

func FixedSizeListOf(elem DataType, size int) DataType {
	return &FixedSizeListType{elem: elem, size: size}
}

func ListOf(elem DataType) DataType {
	return &ListType{elem: elem}
}

func StructOf(fields []Field) DataType {
	return &StructType{fields: fields}
}

// VectorType creates a fixed-size float32 vector type (for embeddings)
func VectorType(dim int) DataType {
	return FixedSizeListOf(PrimFloat32(), dim)
}
