package format

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"github.com/wzqhbkjdx/vego/storage/arrow"
	"strconv"
	"strings"

	lerrors "github.com/wzqhbkjdx/vego/storage/errors"
)

// Header represents the Lance file header
type Header struct {
	Magic      uint32        // Magic number (0x4C414E43)
	Version    uint16        // File format version
	Flags      uint16        // Feature flags
	Schema     *arrow.Schema // Arrow schema
	NumRows    int64         // Total number of rows
	NumColumns int32         // Number of columns
	PageSize   int32         // Default page size
	Reserved   [32]byte      // Reserved for future use
}

// HeaderFlags defines feature flags
type HeaderFlags uint16

const (
	FlagCompressed HeaderFlags = 1 << iota // Data is compressed
	FlagEncrypted                          // Data is encrypted
	FlagIndexed                            // File has indices
	FlagVersioned                          // File has version metadata
)

// NewHeader creates a new header
func NewHeader(schema *arrow.Schema, numRows int64) *Header {
	return &Header{
		Magic:      MagicNumber,
		Version:    CurrentVersion,
		Flags:      0,
		Schema:     schema,
		NumRows:    numRows,
		NumColumns: int32(schema.NumFields()),
		PageSize:   DefaultPageSize,
	}
}

// SetFlag sets a feature flag
func (h *Header) SetFlag(flag HeaderFlags) {
	h.Flags |= uint16(flag)
}

// HasFlag checks if a flag is set
func (h *Header) HasFlag(flag HeaderFlags) bool {
	return (h.Flags & uint16(flag)) != 0
}

// Validate validates the header
func (h *Header) Validate() error {
	if err := ValidateMagicNumber(h.Magic); err != nil {
		return err
	}
	if err := ValidateVersion(h.Version); err != nil {
		return err
	}
	if h.Schema == nil {
		return lerrors.New(lerrors.ErrInvalidArgument).
			Op("validate_header").
			Context("field", "schema").
			Context("message", "schema is nil").
			Build()
	}
	if h.NumRows < 0 {
		return lerrors.ValidationFailed("validate_header", "",
			fmt.Sprintf("invalid row count: %d", h.NumRows))
	}
	if h.NumColumns != int32(h.Schema.NumFields()) {
		return lerrors.SchemaMismatch("", "column_count",
			fmt.Sprintf("%d", h.Schema.NumFields()),
			fmt.Sprintf("%d", h.NumColumns))
	}
	if h.PageSize <= 0 || h.PageSize > MaxPageSize {
		return lerrors.ValidationFailed("validate_header", "",
			fmt.Sprintf("invalid page size: %d (must be <= %d)", h.PageSize, MaxPageSize))
	}
	return nil
}

// EncodedSize returns the encoded size of the header (without schema)
func (h *Header) EncodedSize() int {
	// Fixed fields: magic(4) + version(2) + flags(2) + numRows(8) + numColumns(4) + pageSize(4) + reserved(32)
	return 4 + 2 + 2 + 8 + 4 + 4 + 32
}

// WriteTo writes the header to a writer
func (h *Header) WriteTo(w io.Writer) (int64, error) {
	if err := h.Validate(); err != nil {
		return 0, NewFileError("write header", err)
	}

	buf := new(bytes.Buffer)

	// Write fixed fields
	binary.Write(buf, ByteOrder, h.Magic)
	binary.Write(buf, ByteOrder, h.Version)
	binary.Write(buf, ByteOrder, h.Flags)
	binary.Write(buf, ByteOrder, h.NumRows)
	binary.Write(buf, ByteOrder, h.NumColumns)
	binary.Write(buf, ByteOrder, h.PageSize)
	binary.Write(buf, ByteOrder, h.Reserved)

	// Serialize schema to JSON
	schemaJSON := serializeSchemaToJSON(h.Schema)

	// Validate schema size before writing
	if len(schemaJSON) > MaxSchemaSize {
		return 0, lerrors.New(lerrors.ErrMetadataError).
			Op("write_header").
			Context("field", "schema").
			Context("size", len(schemaJSON)).
			Context("max_size", MaxSchemaSize).
			Context("message", "schema too large").
			Build()
	}

	schemaLen := int32(len(schemaJSON))
	binary.Write(buf, ByteOrder, schemaLen)
	buf.Write(schemaJSON)

	// Write to output
	n, err := w.Write(buf.Bytes())
	return int64(n), err
}

// ReadFrom reads the header from a reader
func (h *Header) ReadFrom(r io.Reader) (int64, error) {
	buf := make([]byte, h.EncodedSize())
	n, err := io.ReadFull(r, buf)
	if err != nil {
		return int64(n), NewFileError("read header", err)
	}

	reader := bytes.NewReader(buf)

	// Read fixed fields
	binary.Read(reader, ByteOrder, &h.Magic)
	binary.Read(reader, ByteOrder, &h.Version)
	binary.Read(reader, ByteOrder, &h.Flags)
	binary.Read(reader, ByteOrder, &h.NumRows)
	binary.Read(reader, ByteOrder, &h.NumColumns)
	binary.Read(reader, ByteOrder, &h.PageSize)
	binary.Read(reader, ByteOrder, &h.Reserved)

	// Validate before reading schema
	if err := ValidateMagicNumber(h.Magic); err != nil {
		return int64(n), err
	}
	if err := ValidateVersion(h.Version); err != nil {
		return int64(n), err
	}

	// Read schema length with validation
	var schemaLen int32
	if err := binary.Read(r, ByteOrder, &schemaLen); err != nil {
		return int64(n) + 4, NewFileError("read schema length", err)
	}

	// Validate schema length using constant
	if schemaLen < 0 || schemaLen > MaxSchemaSize {
		return int64(n) + 4, lerrors.New(lerrors.ErrMetadataError).
			Op("read_header").
			Context("field", "schema").
			Context("schema_length", schemaLen).
			Context("max_size", MaxSchemaSize).
			Build()
	}

	// Read schema JSON
	schemaJSON := make([]byte, schemaLen)
	if _, err := io.ReadFull(r, schemaJSON); err != nil {
		return int64(n) + 4 + int64(schemaLen), NewFileError("read schema", err)
	}

	// Deserialize schema
	schema, err := deserializeSchemaFromJSON(schemaJSON)
	if err != nil {
		return int64(n) + 4 + int64(schemaLen), NewFileError("deserialize schema", err)
	}
	h.Schema = schema

	return int64(n) + 4 + int64(schemaLen), nil
}

// serializeSchemaToJSON with proper escaping
func serializeSchemaToJSON(schema *arrow.Schema) []byte {
	// Use standard json.Marshal for safety
	type fieldJSON struct {
		Name     string `json:"name"`
		Type     string `json:"type"`
		Nullable bool   `json:"nullable"`
	}

	type schemaJSON struct {
		Fields   []fieldJSON       `json:"fields"`
		Metadata map[string]string `json:"metadata"`
	}

	fields := make([]fieldJSON, schema.NumFields())
	for i := 0; i < schema.NumFields(); i++ {
		field := schema.Field(i)
		fields[i] = fieldJSON{
			Name:     field.Name,
			Type:     serializeTypeName(field.Type),
			Nullable: field.Nullable,
		}
	}

	data := schemaJSON{
		Fields:   fields,
		Metadata: schema.Metadata(),
	}

	result, _ := json.Marshal(data) // Error impossible with these types
	return result
}

// serializeTypeName converts DataType to string representation
func serializeTypeName(dt arrow.DataType) string {
	switch t := dt.(type) {
	case *arrow.Int32Type:
		return "int32"
	case *arrow.Int64Type:
		return "int64"
	case *arrow.Float32Type:
		return "float32"
	case *arrow.Float64Type:
		return "float64"
	case *arrow.BinaryType:
		return "binary"
	case *arrow.StringType:
		return "string"
	case *arrow.FixedSizeListType:
		elemType := serializeTypeName(t.Elem())
		return fmt.Sprintf("fixed_size_list[%d]<%s>", t.Size(), elemType)
	default:
		return dt.Name()
	}
}

func deserializeSchemaFromJSON(data []byte) (*arrow.Schema, error) {
	// Parse JSON structure
	var schemaJSON struct {
		Fields []struct {
			Name     string `json:"name"`
			Type     string `json:"type"`
			Nullable bool   `json:"nullable"`
		} `json:"fields"`
		Metadata map[string]string `json:"metadata"`
	}

	if err := json.Unmarshal(data, &schemaJSON); err != nil {
		return nil, lerrors.MetadataError("deserialize_schema", "", "json_parse", err)
	}

	if len(schemaJSON.Fields) == 0 {
		return nil, lerrors.New(lerrors.ErrMetadataError).
			Op("deserialize_schema").
			Context("field", "fields").
			Context("message", "schema has no fields").
			Build()
	}

	// Convert JSON fields to arrow.Field
	fields := make([]arrow.Field, len(schemaJSON.Fields)) // 注意：不是指针切片
	for i, f := range schemaJSON.Fields {
		dataType, err := parseDataType(f.Type)
		if err != nil {
			return nil, lerrors.New(lerrors.ErrMetadataError).
				Op("deserialize_schema").
				Context("field", f.Name).
				Context("error_phase", "type_parsing").
				Wrap(err).
				Build()
		}

		fields[i] = arrow.Field{
			Name:     f.Name,
			Type:     dataType,
			Nullable: f.Nullable,
			Metadata: make(map[string]string),
		}
	}

	// Create schema with metadata using constructor
	schema := arrow.NewSchema(fields, schemaJSON.Metadata)

	return schema, nil
}

// parseDataType parses a type string to arrow.DataType
func parseDataType(typeStr string) (arrow.DataType, error) {
	// Handle basic types
	switch typeStr {
	case "int32":
		return arrow.PrimInt32(), nil
	case "int64":
		return arrow.PrimInt64(), nil
	case "float32":
		return arrow.PrimFloat32(), nil
	case "float64":
		return arrow.PrimFloat64(), nil
	case "binary":
		return arrow.PrimBinary(), nil
	case "string", "utf8":
		return arrow.PrimString(), nil
	}

	// Handle FixedSizeList (e.g., "fixed_size_list[768]<float32>")
	if strings.HasPrefix(typeStr, "fixed_size_list") {
		return parseFixedSizeListType(typeStr)
	}

	return nil, lerrors.UnsupportedType("parse_data_type", typeStr, "")
}

// parseFixedSizeListType parses "fixed_size_list[768]<float32>" format
func parseFixedSizeListType(typeStr string) (arrow.DataType, error) {
	// Extract size: "fixed_size_list[768]<float32>" -> 768
	sizeStart := strings.Index(typeStr, "[")
	sizeEnd := strings.Index(typeStr, "]")
	if sizeStart == -1 || sizeEnd == -1 {
		return nil, lerrors.ValidationFailed("parse_fixed_size_list", "",
			fmt.Sprintf("invalid format: %s", typeStr))
	}

	sizeStr := typeStr[sizeStart+1 : sizeEnd]
	size, err := strconv.Atoi(sizeStr)
	if err != nil {
		return nil, lerrors.ValidationFailed("parse_fixed_size_list", "",
			fmt.Sprintf("invalid list size format: %s", sizeStr))
	}

	// Validate size range
	if size <= 0 || size > MaxVectorDimension {
		return nil, lerrors.ValidationFailed("parse_fixed_size_list", "",
			fmt.Sprintf("size %d out of range [1, %d]", size, MaxVectorDimension))
	}

	// Extract element type: "fixed_size_list[768]<float32>" -> float32
	elemStart := strings.Index(typeStr, "<")
	elemEnd := strings.Index(typeStr, ">")
	if elemStart == -1 || elemEnd == -1 {
		return nil, lerrors.ValidationFailed("parse_fixed_size_list", "",
			fmt.Sprintf("invalid format: %s", typeStr))
	}

	elemTypeStr := typeStr[elemStart+1 : elemEnd]
	elemType, err := parseDataType(elemTypeStr)
	if err != nil {
		return nil, lerrors.New(lerrors.ErrUnsupportedType).
			Op("parse_fixed_size_list").
			Context("phase", "element_type").
			Wrap(err).
			Build()
	}

	// 使用构造函数创建
	return arrow.FixedSizeListOf(elemType, size), nil
}
