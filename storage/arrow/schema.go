package arrow

import (
	"fmt"
	"strings"
)

// Field represents a named typed column
type Field struct {
	Name     string
	Type     DataType
	Nullable bool
	Metadata map[string]string
}

// NewField creates a new field
func NewField(name string, dtype DataType, nullable bool) Field {
	return Field{
		Name:     name,
		Type:     dtype,
		Nullable: nullable,
		Metadata: make(map[string]string),
	}
}

// WithMetadata adds metadata to the field
func (f *Field) WithMetadata(key, value string) *Field {
	f.Metadata[key] = value
	return f
}

// Schema represents a collection of fields (table schema)
type Schema struct {
	fields   []Field
	metadata map[string]string
}

// NewSchema creates a new schema
func NewSchema(fields []Field, metadata map[string]string) *Schema {
	if metadata == nil {
		metadata = make(map[string]string)
	}
	return &Schema{
		fields:   fields,
		metadata: metadata,
	}
}

// NumFields returns the number of fields
func (s *Schema) NumFields() int {
	return len(s.fields)
}

// Field returns the field at index i
func (s *Schema) Field(i int) Field {
	return s.fields[i]
}

// Fields returns all fields
func (s *Schema) Fields() []Field {
	return s.fields
}

// FieldByName returns the field with the given name
func (s *Schema) FieldByName(name string) (Field, int, bool) {
	for i, field := range s.fields {
		if field.Name == name {
			return field, i, true
		}
	}
	return Field{}, -1, false
}

// Metadata returns the schema metadata
func (s *Schema) Metadata() map[string]string {
	return s.metadata
}

// String returns a human-readable representation
func (s *Schema) String() string {
	var sb strings.Builder
	sb.WriteString("Schema{\n")
	for i, field := range s.fields {
		nullable := ""
		if field.Nullable {
			nullable = ", nullable"
		}
		sb.WriteString(fmt.Sprintf("  %d: %s: %s%s\n", i, field.Name, field.Type.Name(), nullable))
	}
	if len(s.metadata) > 0 {
		sb.WriteString("  metadata: ")
		sb.WriteString(fmt.Sprintf("%v\n", s.metadata))
	}
	sb.WriteString("}")
	return sb.String()
}

// Equal checks if two schemas are equal
func (s *Schema) Equal(other *Schema) bool {
	if s.NumFields() != other.NumFields() {
		return false
	}
	for i := 0; i < s.NumFields(); i++ {
		f1, f2 := s.fields[i], other.fields[i]
		if f1.Name != f2.Name || f1.Type.ID() != f2.Type.ID() || f1.Nullable != f2.Nullable {
			return false
		}
	}
	return true
}

// --- Pre-defined Schemas for HNSW ---

// SchemaForVectors creates a schema for vector storage
// Columns: id, vector, level
func SchemaForVectors(dimension int) *Schema {
	return NewSchema([]Field{
		NewField("id", PrimInt32(), false),
		NewField("vector", VectorType(dimension), false),
		NewField("level", PrimInt32(), false),
	}, map[string]string{
		"purpose":   "hnsw_vectors",
		"dimension": fmt.Sprintf("%d", dimension),
	})
}

// SchemaForHNSWGraph creates a schema for HNSW graph edges
// Columns: node_id, layer, neighbors
func SchemaForHNSWGraph() *Schema {
	return NewSchema([]Field{
		NewField("node_id", PrimInt32(), false),
		NewField("layer", PrimInt32(), false),
		NewField("neighbors", ListOf(PrimInt32()), false),
	}, map[string]string{
		"purpose": "hnsw_graph",
	})
}
