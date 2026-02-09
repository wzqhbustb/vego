package format

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/wzqhbkjdx/vego/storage/arrow"
	lerrors "github.com/wzqhbkjdx/vego/storage/errors"
	"strings"
	"testing"
)

// assertErrorCode 检查错误是否为指定的错误码
func assertErrorCode(t *testing.T, err error, code lerrors.ErrorCode, msg string) {
	t.Helper()
	if err == nil {
		t.Errorf("%s: expected error, got nil", msg)
		return
	}
	if !lerrors.Is(err, code) {
		t.Errorf("%s: expected error code %v, got %v (err=%v)",
			msg, code, lerrors.GetCode(err), err)
	}
}

// assertErrorContains 检查错误消息包含子串（向后兼容）
func assertErrorContains(t *testing.T, err error, substr string, msg string) {
	t.Helper()
	if err == nil {
		t.Errorf("%s: expected error, got nil", msg)
		return
	}
	if !strings.Contains(err.Error(), substr) {
		t.Errorf("%s: expected error containing %q, got %q", msg, substr, err.Error())
	}
}

// TestHeaderSerializationRoundtrip tests basic header write/read roundtrip
func TestHeaderSerializationRoundtrip(t *testing.T) {
	// Create a schema with various field types
	schema := arrow.SchemaForVectors(768)

	// Create header
	original := NewHeader(schema, 1000)
	original.SetFlag(FlagCompressed)
	original.SetFlag(FlagIndexed)

	// Serialize
	buf := new(bytes.Buffer)
	n, err := original.WriteTo(buf)
	if err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	if n == 0 {
		t.Fatal("WriteTo returned 0 bytes")
	}

	// Deserialize
	deserialized := &Header{}
	readBytes, err := deserialized.ReadFrom(buf)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}

	if readBytes != n {
		t.Errorf("ReadFrom bytes mismatch: wrote %d, read %d", n, readBytes)
	}

	// Verify fields
	if deserialized.Magic != original.Magic {
		t.Errorf("Magic mismatch: got 0x%X, want 0x%X", deserialized.Magic, original.Magic)
	}

	if deserialized.Version != original.Version {
		t.Errorf("Version mismatch: got %d, want %d", deserialized.Version, original.Version)
	}

	if deserialized.Flags != original.Flags {
		t.Errorf("Flags mismatch: got %d, want %d", deserialized.Flags, original.Flags)
	}

	if deserialized.NumRows != original.NumRows {
		t.Errorf("NumRows mismatch: got %d, want %d", deserialized.NumRows, original.NumRows)
	}

	if deserialized.NumColumns != original.NumColumns {
		t.Errorf("NumColumns mismatch: got %d, want %d", deserialized.NumColumns, original.NumColumns)
	}

	if deserialized.PageSize != original.PageSize {
		t.Errorf("PageSize mismatch: got %d, want %d", deserialized.PageSize, original.PageSize)
	}

	// Verify schema
	if deserialized.Schema == nil {
		t.Fatal("Deserialized schema is nil")
	}

	if deserialized.Schema.NumFields() != original.Schema.NumFields() {
		t.Errorf("Schema field count mismatch: got %d, want %d",
			deserialized.Schema.NumFields(), original.Schema.NumFields())
	}

	// Verify each field
	for i := 0; i < original.Schema.NumFields(); i++ {
		origField := original.Schema.Field(i)
		deserField := deserialized.Schema.Field(i)

		if origField.Name != deserField.Name {
			t.Errorf("Field %d name mismatch: got %q, want %q", i, deserField.Name, origField.Name)
		}

		if origField.Type.ID() != deserField.Type.ID() {
			t.Errorf("Field %d type mismatch: got %v, want %v", i, deserField.Type.ID(), origField.Type.ID())
		}

		if origField.Nullable != deserField.Nullable {
			t.Errorf("Field %d nullable mismatch: got %v, want %v", i, deserField.Nullable, origField.Nullable)
		}
	}

	// Verify metadata
	origMeta := original.Schema.Metadata()
	deserMeta := deserialized.Schema.Metadata()

	if len(origMeta) != len(deserMeta) {
		t.Errorf("Metadata count mismatch: got %d, want %d", len(deserMeta), len(origMeta))
	}

	for k, v := range origMeta {
		if deserMeta[k] != v {
			t.Errorf("Metadata[%q] mismatch: got %q, want %q", k, deserMeta[k], v)
		}
	}
}

// TestHeaderWithDifferentVectorDimensions tests various vector dimensions
func TestHeaderWithDifferentVectorDimensions(t *testing.T) {
	dimensions := []int{128, 384, 768, 1536, 3072}

	for _, dim := range dimensions {
		t.Run(fmt.Sprintf("dim_%d", dim), func(t *testing.T) {
			schema := arrow.SchemaForVectors(dim)
			header := NewHeader(schema, 500)

			buf := new(bytes.Buffer)
			_, err := header.WriteTo(buf)
			if err != nil {
				t.Fatalf("WriteTo failed for dim %d: %v", dim, err)
			}

			deserialized := &Header{}
			_, err = deserialized.ReadFrom(buf)
			if err != nil {
				t.Fatalf("ReadFrom failed for dim %d: %v", dim, err)
			}

			// Verify vector field type
			vectorField := deserialized.Schema.Field(1) // "vector" is second field
			if vectorField.Type.ID() != arrow.FIXED_SIZE_LIST {
				t.Errorf("Vector field type mismatch: got %v, want FIXED_SIZE_LIST", vectorField.Type.ID())
			}

			listType, ok := vectorField.Type.(*arrow.FixedSizeListType)
			if !ok {
				t.Fatal("Failed to cast vector type to FixedSizeListType")
			}

			if listType.Size() != dim {
				t.Errorf("Vector dimension mismatch: got %d, want %d", listType.Size(), dim)
			}
		})
	}
}

// TestHeaderWithCustomSchema tests serialization with custom schema
func TestHeaderWithCustomSchema(t *testing.T) {
	// Create custom schema with various types
	fields := []arrow.Field{
		arrow.NewField("id", arrow.PrimInt32(), false),
		arrow.NewField("score", arrow.PrimFloat64(), true), // nullable
		arrow.NewField("name", arrow.PrimString(), false),
		arrow.NewField("embedding", arrow.VectorType(512), false),
	}

	metadata := map[string]string{
		"version":     "1.0",
		"description": "Custom test schema",
		"author":      "test",
	}

	schema := arrow.NewSchema(fields, metadata)
	header := NewHeader(schema, 2000)

	buf := new(bytes.Buffer)
	_, err := header.WriteTo(buf)
	if err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	deserialized := &Header{}
	_, err = deserialized.ReadFrom(buf)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}

	// Verify all fields
	if deserialized.Schema.NumFields() != 4 {
		t.Fatalf("Expected 4 fields, got %d", deserialized.Schema.NumFields())
	}

	// Verify nullable field
	scoreField := deserialized.Schema.Field(1)
	if !scoreField.Nullable {
		t.Error("score field should be nullable")
	}

	// Verify metadata
	if deserialized.Schema.Metadata()["version"] != "1.0" {
		t.Error("Metadata version mismatch")
	}
}

// TestHeaderValidation tests header validation using error codes
func TestHeaderValidation(t *testing.T) {
	tests := []struct {
		name        string
		modify      func(*Header)
		wantErr     string            // 向后兼容：检查错误消息子串
		wantErrCode lerrors.ErrorCode // 新的：检查错误码
	}{
		{
			name: "invalid magic",
			modify: func(h *Header) {
				h.Magic = 0xDEADBEEF
			},
			wantErr:     "invalid magic number",
			wantErrCode: lerrors.ErrInvalidMagic,
		},
		{
			name: "nil schema",
			modify: func(h *Header) {
				h.Schema = nil
			},
			wantErr:     "schema is nil",
			wantErrCode: lerrors.ErrInvalidArgument,
		},
		{
			name: "negative row count",
			modify: func(h *Header) {
				h.NumRows = -100
			},
			wantErr:     "invalid row count",
			wantErrCode: lerrors.ErrInvalidArgument,
		},
		{
			name: "column count mismatch",
			modify: func(h *Header) {
				h.NumColumns = 999
			},
			wantErr:     "column count mismatch",
			wantErrCode: lerrors.ErrSchemaMismatch,
		},
		{
			name: "invalid page size",
			modify: func(h *Header) {
				h.PageSize = -1
			},
			wantErr:     "invalid page size",
			wantErrCode: lerrors.ErrInvalidArgument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := arrow.SchemaForVectors(768)
			header := NewHeader(schema, 1000)
			tt.modify(header)

			err := header.Validate()
			if err == nil {
				t.Fatal("Expected validation error, got nil")
			}

			// 优先检查错误码（新的方式）
			if tt.wantErrCode != 0 {
				assertErrorCode(t, err, tt.wantErrCode, tt.name)
			} else if tt.wantErr != "" {
				// 向后兼容
				assertErrorContains(t, err, tt.wantErr, tt.name)
			}
		})
	}
}

// TestHeaderFlags tests flag operations
func TestHeaderFlags(t *testing.T) {
	schema := arrow.SchemaForVectors(768)
	header := NewHeader(schema, 1000)

	// Initially no flags
	if header.HasFlag(FlagCompressed) {
		t.Error("Should not have FlagCompressed initially")
	}

	// Set flag
	header.SetFlag(FlagCompressed)
	if !header.HasFlag(FlagCompressed) {
		t.Error("Should have FlagCompressed after setting")
	}

	// Set multiple flags
	header.SetFlag(FlagIndexed)
	header.SetFlag(FlagVersioned)

	if !header.HasFlag(FlagCompressed) {
		t.Error("Should still have FlagCompressed")
	}
	if !header.HasFlag(FlagIndexed) {
		t.Error("Should have FlagIndexed")
	}
	if !header.HasFlag(FlagVersioned) {
		t.Error("Should have FlagVersioned")
	}

	// Check flag not set
	if header.HasFlag(FlagEncrypted) {
		t.Error("Should not have FlagEncrypted")
	}

	// Verify flags persist through serialization
	buf := new(bytes.Buffer)
	header.WriteTo(buf)

	deserialized := &Header{}
	deserialized.ReadFrom(buf)

	if !deserialized.HasFlag(FlagCompressed) {
		t.Error("Deserialized header should have FlagCompressed")
	}
	if !deserialized.HasFlag(FlagIndexed) {
		t.Error("Deserialized header should have FlagIndexed")
	}
}

// TestSchemaWithSpecialCharacters tests JSON escaping
func TestSchemaWithSpecialCharacters(t *testing.T) {
	fields := []arrow.Field{
		arrow.NewField(`field"with"quotes`, arrow.PrimInt32(), false),
		arrow.NewField(`field\with\backslash`, arrow.PrimFloat32(), false),
		arrow.NewField("field\nwith\nnewline", arrow.PrimString(), false),
	}

	metadata := map[string]string{
		`key"quoted`:    `value"quoted`,
		`key\backslash`: `value\backslash`,
	}

	schema := arrow.NewSchema(fields, metadata)
	header := NewHeader(schema, 100)

	buf := new(bytes.Buffer)
	_, err := header.WriteTo(buf)
	if err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	deserialized := &Header{}
	_, err = deserialized.ReadFrom(buf)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}

	// Verify field names are preserved
	for i := 0; i < len(fields); i++ {
		origName := fields[i].Name
		deserName := deserialized.Schema.Field(i).Name
		if origName != deserName {
			t.Errorf("Field %d name mismatch: got %q, want %q", i, deserName, origName)
		}
	}

	// Verify metadata is preserved
	for k, v := range metadata {
		if deserialized.Schema.Metadata()[k] != v {
			t.Errorf("Metadata[%q] mismatch: got %q, want %q",
				k, deserialized.Schema.Metadata()[k], v)
		}
	}
}

// TestMaxSchemaSize tests schema size limit
func TestMaxSchemaSize(t *testing.T) {
	// Create schema with many fields to exceed 1MB limit
	// Each field JSON is ~90 bytes, need >11500 fields to exceed 1MB
	fields := make([]arrow.Field, 15000)
	for i := 0; i < 15000; i++ {
		// Each field name is long to increase JSON size
		name := fmt.Sprintf("very_long_field_name_to_increase_json_size_with_extra_padding_%d", i)
		fields[i] = arrow.NewField(name, arrow.PrimInt32(), false)
	}

	schema := arrow.NewSchema(fields, nil)
	header := NewHeader(schema, 1000)

	buf := new(bytes.Buffer)
	_, err := header.WriteTo(buf)

	if err == nil {
		t.Fatal("Expected error for oversized schema, got nil")
	}

	// 检查错误码而不是字符串
	assertErrorCode(t, err, lerrors.ErrMetadataError, "TestMaxSchemaSize")
	// 也可以检查包含特定上下文信息
	if !strings.Contains(err.Error(), "schema too large") &&
		!strings.Contains(err.Error(), "schema") {
		t.Logf("Warning: Error message changed: %v", err)
	}
}

// TestInvalidSchemaLength tests malicious schema length
func TestInvalidSchemaLength(t *testing.T) {
	schema := arrow.SchemaForVectors(768)
	header := NewHeader(schema, 1000)

	buf := new(bytes.Buffer)
	header.WriteTo(buf)

	// Modify the schema length to be invalid
	data := buf.Bytes()

	// Schema length is at offset 56 (after fixed fields)
	// Set it to 10MB (exceeds MaxSchemaSize)
	binary.LittleEndian.PutUint32(data[56:60], 10*1024*1024)

	// Try to read
	deserialized := &Header{}
	_, err := deserialized.ReadFrom(bytes.NewReader(data))

	if err == nil {
		t.Fatal("Expected error for invalid schema length, got nil")
	}

	// 检查错误码
	assertErrorCode(t, err, lerrors.ErrMetadataError, "TestInvalidSchemaLength")
}

// TestVectorDimensionLimit tests max vector dimension
func TestVectorDimensionLimit(t *testing.T) {
	tests := []struct {
		name      string
		dimension int
		wantError bool
		errCode   lerrors.ErrorCode // 新的：期望的错误码
	}{
		{"valid small", 128, false, 0},
		{"valid large", 10000, false, 0},
		{"valid max", 100000, false, 0},
		{"invalid too large", 100001, true, lerrors.ErrInvalidArgument},
		{"invalid negative", -1, true, lerrors.ErrInvalidArgument},
		{"invalid zero", 0, true, lerrors.ErrInvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Manually construct JSON to test parsing
			json := fmt.Sprintf(`{
                "fields": [
                    {"name":"vec","type":"fixed_size_list[%d]<float32>","nullable":false}
                ],
                "metadata": {}
            }`, tt.dimension)

			_, err := deserializeSchemaFromJSON([]byte(json))

			if tt.wantError && err == nil {
				t.Error("Expected error, got nil")
			}
			if !tt.wantError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			// 对于期望的错误，检查错误码
			if tt.wantError && err != nil && tt.errCode != 0 {
				assertErrorCode(t, err, tt.errCode, tt.name)
			}
		})
	}
}

// TestAllDataTypes tests serialization of all supported data types
func TestAllDataTypes(t *testing.T) {
	fields := []arrow.Field{
		arrow.NewField("int32_field", arrow.PrimInt32(), false),
		arrow.NewField("int64_field", arrow.PrimInt64(), false),
		arrow.NewField("float32_field", arrow.PrimFloat32(), false),
		arrow.NewField("float64_field", arrow.PrimFloat64(), false),
		arrow.NewField("binary_field", arrow.PrimBinary(), false),
		arrow.NewField("string_field", arrow.PrimString(), false),
		arrow.NewField("vector_field", arrow.FixedSizeListOf(arrow.PrimFloat32(), 768), false),
	}

	schema := arrow.NewSchema(fields, nil)
	header := NewHeader(schema, 1000)

	buf := new(bytes.Buffer)
	_, err := header.WriteTo(buf)
	if err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	deserialized := &Header{}
	_, err = deserialized.ReadFrom(buf)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}

	// Verify all types
	expectedTypes := []arrow.TypeID{
		arrow.INT32,
		arrow.INT64,
		arrow.FLOAT32,
		arrow.FLOAT64,
		arrow.BINARY,
		arrow.STRING,
		arrow.FIXED_SIZE_LIST,
	}

	for i, expected := range expectedTypes {
		actual := deserialized.Schema.Field(i).Type.ID()
		if actual != expected {
			t.Errorf("Field %d type mismatch: got %v, want %v", i, actual, expected)
		}
	}
}
