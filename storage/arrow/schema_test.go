package arrow

import "testing"

func TestPrimitiveTypes(t *testing.T) {
	tests := []struct {
		dtype     DataType
		id        TypeID
		name      string
		byteWidth int
	}{
		{PrimInt32(), INT32, "int32", 4},
		{PrimInt64(), INT64, "int64", 8},
		{PrimFloat32(), FLOAT32, "float32", 4},
		{PrimFloat64(), FLOAT64, "float64", 8},
		{PrimBinary(), BINARY, "binary", -1},
		{PrimString(), STRING, "utf8", -1},
	}

	for _, tt := range tests {
		if tt.dtype.ID() != tt.id {
			t.Errorf("%s: expected ID %v, got %v", tt.name, tt.id, tt.dtype.ID())
		}
		if tt.dtype.Name() != tt.name {
			t.Errorf("expected name %s, got %s", tt.name, tt.dtype.Name())
		}
		if tt.dtype.ByteWidth() != tt.byteWidth {
			t.Errorf("%s: expected width %d, got %d", tt.name, tt.byteWidth, tt.dtype.ByteWidth())
		}
	}
}

func TestFixedSizeListType(t *testing.T) {
	listType := FixedSizeListOf(PrimFloat32(), 768).(*FixedSizeListType)

	if listType.ID() != FIXED_SIZE_LIST {
		t.Errorf("expected FIXED_SIZE_LIST, got %v", listType.ID())
	}
	if listType.Size() != 768 {
		t.Errorf("expected size 768, got %d", listType.Size())
	}
	if listType.Elem().ID() != FLOAT32 {
		t.Errorf("expected Float32 element type, got %v", listType.Elem().ID())
	}
	if listType.ByteWidth() != 768*4 {
		t.Errorf("expected byte width %d, got %d", 768*4, listType.ByteWidth())
	}

	expectedName := "fixed_size_list<float32>[768]"
	if listType.Name() != expectedName {
		t.Errorf("expected name %s, got %s", expectedName, listType.Name())
	}
}

func TestListType(t *testing.T) {
	listType := ListOf(PrimInt32()).(*ListType)

	if listType.ID() != LIST {
		t.Errorf("expected LIST, got %v", listType.ID())
	}
	if listType.Elem().ID() != INT32 {
		t.Errorf("expected Int32 element type, got %v", listType.Elem().ID())
	}
	if listType.ByteWidth() != -1 {
		t.Errorf("expected variable width (-1), got %d", listType.ByteWidth())
	}
}

func TestVectorType(t *testing.T) {
	vecType := VectorType(768).(*FixedSizeListType)

	if vecType.Size() != 768 {
		t.Errorf("expected size 768, got %d", vecType.Size())
	}
	if vecType.Elem().ID() != FLOAT32 {
		t.Errorf("expected Float32 element type, got %v", vecType.Elem().ID())
	}
}

func TestNewField(t *testing.T) {
	field := NewField("test_field", PrimInt32(), true)

	if field.Name != "test_field" {
		t.Errorf("expected name 'test_field', got %s", field.Name)
	}
	if field.Type.ID() != INT32 {
		t.Errorf("expected Int32 type, got %v", field.Type.ID())
	}
	if !field.Nullable {
		t.Error("expected nullable to be true")
	}
	if field.Metadata == nil {
		t.Error("expected metadata map to be initialized")
	}
}

func TestFieldWithMetadata(t *testing.T) {
	field := NewField("id", PrimInt32(), false)
	field.WithMetadata("key1", "value1").WithMetadata("key2", "value2")

	if field.Metadata["key1"] != "value1" {
		t.Errorf("expected metadata key1=value1, got %s", field.Metadata["key1"])
	}
	if field.Metadata["key2"] != "value2" {
		t.Errorf("expected metadata key2=value2, got %s", field.Metadata["key2"])
	}
}

func TestNewSchema(t *testing.T) {
	fields := []Field{
		NewField("id", PrimInt32(), false),
		NewField("value", PrimFloat32(), true),
	}
	schema := NewSchema(fields, nil)

	if schema.NumFields() != 2 {
		t.Errorf("expected 2 fields, got %d", schema.NumFields())
	}

	field0 := schema.Field(0)
	if field0.Name != "id" {
		t.Errorf("expected field name 'id', got %s", field0.Name)
	}

	field1 := schema.Field(1)
	if field1.Name != "value" {
		t.Errorf("expected field name 'value', got %s", field1.Name)
	}
}

func TestSchemaFieldByName(t *testing.T) {
	fields := []Field{
		NewField("id", PrimInt32(), false),
		NewField("vector", VectorType(768), false),
		NewField("level", PrimInt32(), false),
	}
	schema := NewSchema(fields, nil)

	field, idx, found := schema.FieldByName("vector")
	if !found {
		t.Error("expected to find 'vector' field")
	}
	if idx != 1 {
		t.Errorf("expected index 1, got %d", idx)
	}
	if field.Name != "vector" {
		t.Errorf("expected name 'vector', got %s", field.Name)
	}

	_, _, found = schema.FieldByName("nonexistent")
	if found {
		t.Error("expected not to find 'nonexistent' field")
	}
}

func TestSchemaEqual(t *testing.T) {
	fields1 := []Field{
		NewField("id", PrimInt32(), false),
		NewField("value", PrimFloat32(), true),
	}
	schema1 := NewSchema(fields1, nil)

	fields2 := []Field{
		NewField("id", PrimInt32(), false),
		NewField("value", PrimFloat32(), true),
	}
	schema2 := NewSchema(fields2, nil)

	if !schema1.Equal(schema2) {
		t.Error("expected schemas to be equal")
	}

	// Different field count
	fields3 := []Field{
		NewField("id", PrimInt32(), false),
	}
	schema3 := NewSchema(fields3, nil)

	if schema1.Equal(schema3) {
		t.Error("expected schemas to be different (field count)")
	}

	// Different field name
	fields4 := []Field{
		NewField("id", PrimInt32(), false),
		NewField("different", PrimFloat32(), true),
	}
	schema4 := NewSchema(fields4, nil)

	if schema1.Equal(schema4) {
		t.Error("expected schemas to be different (field name)")
	}
}

func TestSchemaForVectors(t *testing.T) {
	schema := SchemaForVectors(768)

	if schema.NumFields() != 3 {
		t.Errorf("expected 3 fields, got %d", schema.NumFields())
	}

	// Check id field
	idField := schema.Field(0)
	if idField.Name != "id" || idField.Type.ID() != INT32 {
		t.Error("id field incorrect")
	}

	// Check vector field
	vecField := schema.Field(1)
	if vecField.Name != "vector" {
		t.Error("vector field name incorrect")
	}
	vecType, ok := vecField.Type.(*FixedSizeListType)
	if !ok || vecType.Size() != 768 {
		t.Error("vector field type incorrect")
	}

	// Check level field
	levelField := schema.Field(2)
	if levelField.Name != "level" || levelField.Type.ID() != INT32 {
		t.Error("level field incorrect")
	}

	// Check metadata
	if schema.Metadata()["purpose"] != "hnsw_vectors" {
		t.Error("purpose metadata incorrect")
	}
	if schema.Metadata()["dimension"] != "768" {
		t.Error("dimension metadata incorrect")
	}
}

func TestSchemaForHNSWGraph(t *testing.T) {
	schema := SchemaForHNSWGraph()

	if schema.NumFields() != 3 {
		t.Errorf("expected 3 fields, got %d", schema.NumFields())
	}

	// Check node_id field
	nodeIDField := schema.Field(0)
	if nodeIDField.Name != "node_id" {
		t.Error("node_id field name incorrect")
	}

	// Check layer field
	layerField := schema.Field(1)
	if layerField.Name != "layer" {
		t.Error("layer field name incorrect")
	}

	// Check neighbors field (list of int32)
	neighborsField := schema.Field(2)
	if neighborsField.Name != "neighbors" {
		t.Error("neighbors field name incorrect")
	}
	listType, ok := neighborsField.Type.(*ListType)
	if !ok || listType.Elem().ID() != INT32 {
		t.Error("neighbors field should be list<int32>")
	}

	// Check metadata
	if schema.Metadata()["purpose"] != "hnsw_graph" {
		t.Error("purpose metadata incorrect")
	}
}
