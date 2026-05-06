package memory

import (
	"testing"
	"time"

	vego "github.com/wzqhbustb/vego/vego"
)

func TestMemoryToDoc(t *testing.T) {
	now := time.Date(2026, 4, 17, 12, 0, 0, 0, time.UTC)
	m := &Memory{
		ID:           "mem-1",
		Content:      "hello world",
		MemoryType:   TypeInsight,
		State:        StateActive,
		Tags:         []string{"go", "test"},
		Metadata:     map[string]interface{}{"key": "value"},
		Source:       "agent-1",
		AgentID:      "agent-1",
		SessionID:    "sess-1",
		Seq:          42,
		ContentHash:  "abc123",
		Version:      3,
		SupersededBy: "mem-2",
		Score:        0.95,
		RelativeAge:  "just now",
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	vec := []float32{0.1, 0.2, 0.3}

	doc, err := memoryToDoc(m, vec)
	if err != nil {
		t.Fatalf("memoryToDoc failed: %v", err)
	}

	if doc.ID != "mem-1" {
		t.Errorf("ID: want mem-1, got %s", doc.ID)
	}
	if len(doc.Vector) != len(vec) {
		t.Errorf("Vector length: want %d, got %d", len(vec), len(doc.Vector))
	}
	if !doc.Timestamp.Equal(now) {
		t.Errorf("Timestamp: want %v, got %v", now, doc.Timestamp)
	}

	// Check metadata keys
	if doc.Metadata[metaKeyState] != string(StateActive) {
		t.Errorf("_state: want %s, got %v", StateActive, doc.Metadata[metaKeyState])
	}
	if doc.Metadata[metaKeyType] != string(TypeInsight) {
		t.Errorf("_type: want %s, got %v", TypeInsight, doc.Metadata[metaKeyType])
	}
	dataStr, ok := doc.Metadata[metaKeyData].(string)
	if !ok || dataStr == "" {
		t.Fatalf("_data: expected non-empty string, got %v", doc.Metadata[metaKeyData])
	}

	// Verify _state / _type are consistent with _data
	restored, err := docToMemory(doc)
	if err != nil {
		t.Fatalf("docToMemory failed: %v", err)
	}
	if string(restored.State) != doc.Metadata[metaKeyState] {
		t.Errorf("_state inconsistent: _state=%v, _data.State=%v",
			doc.Metadata[metaKeyState], restored.State)
	}
	if string(restored.MemoryType) != doc.Metadata[metaKeyType] {
		t.Errorf("_type inconsistent: _type=%v, _data.MemoryType=%v",
			doc.Metadata[metaKeyType], restored.MemoryType)
	}

	// Verify query-time fields are NOT persisted in _data
	if restored.Score != 0 {
		t.Errorf("Score should be cleared before storage: want 0, got %f", restored.Score)
	}
	if restored.RelativeAge != "" {
		t.Errorf("RelativeAge should be cleared before storage: want empty, got %q", restored.RelativeAge)
	}
	// Original memory should remain unchanged
	if m.Score != 0.95 {
		t.Errorf("Original memory Score should not be modified: want 0.95, got %f", m.Score)
	}
	if m.RelativeAge != "just now" {
		t.Errorf("Original memory RelativeAge should not be modified: want 'just now', got %q", m.RelativeAge)
	}

	// Verify vector is correctly copied (not shared reference)
	for i, v := range vec {
		if doc.Vector[i] != v {
			t.Errorf("Vector[%d]: want %v, got %v", i, v, doc.Vector[i])
		}
	}
	// Mutating doc.Vector should not affect original vec
	doc.Vector[0] = 99.0
	if vec[0] != 0.1 {
		t.Errorf("Original vec was mutated by doc.Vector modification")
	}
}

func TestMemoryToDocNil(t *testing.T) {
	_, err := memoryToDoc(nil, nil)
	if err == nil {
		t.Error("Expected error for nil memory")
	}
}

func TestDocToMemoryNil(t *testing.T) {
	_, err := docToMemory(nil)
	if err == nil {
		t.Error("Expected error for nil document")
	}
}

func TestDocToMemory(t *testing.T) {
	now := time.Date(2026, 4, 17, 12, 0, 0, 0, time.UTC)
	m := &Memory{
		ID:          "mem-2",
		Content:     "test content",
		MemoryType:  TypePinned,
		State:       StateArchived,
		Tags:        []string{"tag1"},
		Metadata:    map[string]interface{}{"foo": "bar"},
		AgentID:     "agent-2",
		SessionID:   "sess-2",
		Seq:         7,
		ContentHash: "hash789",
		Version:     1,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	vec := []float32{0.4, 0.5}

	doc, err := memoryToDoc(m, vec)
	if err != nil {
		t.Fatalf("memoryToDoc failed: %v", err)
	}

	// Restore from document
	got, err := docToMemory(doc)
	if err != nil {
		t.Fatalf("docToMemory failed: %v", err)
	}

	if got.ID != "mem-2" {
		t.Errorf("ID: want mem-2, got %s", got.ID)
	}
	if got.Content != "test content" {
		t.Errorf("Content: want 'test content', got %s", got.Content)
	}
	if got.MemoryType != TypePinned {
		t.Errorf("MemoryType: want %s, got %s", TypePinned, got.MemoryType)
	}
	if got.State != StateArchived {
		t.Errorf("State: want %s, got %s", StateArchived, got.State)
	}
	if len(got.Tags) != 1 || got.Tags[0] != "tag1" {
		t.Errorf("Tags: want [tag1], got %v", got.Tags)
	}
	if got.Metadata["foo"] != "bar" {
		t.Errorf("Metadata: want bar, got %v", got.Metadata["foo"])
	}
	if got.AgentID != "agent-2" {
		t.Errorf("AgentID: want agent-2, got %s", got.AgentID)
	}
	if got.SessionID != "sess-2" {
		t.Errorf("SessionID: want sess-2, got %s", got.SessionID)
	}
	if got.Seq != 7 {
		t.Errorf("Seq: want 7, got %d", got.Seq)
	}
	if got.ContentHash != "hash789" {
		t.Errorf("ContentHash: want hash789, got %s", got.ContentHash)
	}
	if got.Version != 1 {
		t.Errorf("Version: want 1, got %d", got.Version)
	}
	if !got.CreatedAt.Equal(now) {
		t.Errorf("CreatedAt: want %v, got %v", now, got.CreatedAt)
	}
	if !got.UpdatedAt.Equal(now) {
		t.Errorf("UpdatedAt: want %v, got %v", now, got.UpdatedAt)
	}
	// Score and RelativeAge are populated at query time, not stored
	if got.Score != 0 {
		t.Errorf("Score: want 0 (not stored), got %f", got.Score)
	}
}

func TestDocToMemoryIDOverride(t *testing.T) {
	// docToMemory should use Document.ID as the source of truth
	m := &Memory{ID: "old-id", Content: "c", State: StateActive}
	doc, _ := memoryToDoc(m, []float32{0.0})
	doc.ID = "new-id"

	got, err := docToMemory(doc)
	if err != nil {
		t.Fatalf("docToMemory failed: %v", err)
	}
	if got.ID != "new-id" {
		t.Errorf("ID should be overridden by Document.ID: want new-id, got %s", got.ID)
	}
}

func TestDocToMemoryMissingData(t *testing.T) {
	doc := &vego.Document{
		ID:       "bad",
		Metadata: map[string]interface{}{},
	}
	_, err := docToMemory(doc)
	if err == nil {
		t.Error("Expected error for missing _data field")
	}
}

func TestDocToMemoryInvalidData(t *testing.T) {
	doc := &vego.Document{
		ID:       "bad",
		Metadata: map[string]interface{}{metaKeyData: "not-json"},
	}
	_, err := docToMemory(doc)
	if err == nil {
		t.Error("Expected error for invalid JSON in _data field")
	}
}

func TestRoundTripNilMetadata(t *testing.T) {
	m := &Memory{
		ID:         "nil-meta",
		Content:    "content",
		MemoryType: TypeSession,
		State:      StateActive,
		CreatedAt:  time.Now().UTC().Truncate(time.Millisecond),
		UpdatedAt:  time.Now().UTC().Truncate(time.Millisecond),
	}
	doc, err := memoryToDoc(m, []float32{0.1})
	if err != nil {
		t.Fatalf("memoryToDoc failed: %v", err)
	}
	got, err := docToMemory(doc)
	if err != nil {
		t.Fatalf("docToMemory failed: %v", err)
	}
	if got.ID != m.ID || got.Content != m.Content {
		t.Errorf("Round-trip mismatch: got %+v", got)
	}
	if got.Metadata != nil {
		// json.Marshal of nil map produces null, json.Unmarshal into map produces nil
		t.Errorf("Metadata should remain nil after round-trip, got %v (type %T)", got.Metadata, got.Metadata)
	}
}

func TestRoundTripEmptyTags(t *testing.T) {
	m := &Memory{
		ID:         "empty-tags",
		Content:    "content",
		MemoryType: TypeInsight,
		State:      StateActive,
		Tags:       []string{},
		CreatedAt:  time.Now().UTC().Truncate(time.Millisecond),
		UpdatedAt:  time.Now().UTC().Truncate(time.Millisecond),
	}
	doc, err := memoryToDoc(m, []float32{0.0})
	if err != nil {
		t.Fatalf("memoryToDoc failed: %v", err)
	}
	got, err := docToMemory(doc)
	if err != nil {
		t.Fatalf("docToMemory failed: %v", err)
	}
	if len(got.Tags) != 0 {
		t.Errorf("Tags: want empty, got %v", got.Tags)
	}
}

func TestRoundTripComplexMetadata(t *testing.T) {
	now := time.Date(2026, 4, 17, 12, 0, 0, 0, time.UTC)
	m := &Memory{
		ID:      "complex-meta",
		Content: "content",
		MemoryType: TypeInsight,
		State:   StateActive,
		Metadata: map[string]interface{}{
			"nested": map[string]interface{}{
				"key": "value",
				"num": 42.0,
			},
			"timestamp":    now,
			"tags":         []string{"a", "b"},
			"bool_field":   true,
			"null_field":   nil,
		},
		CreatedAt: now,
		UpdatedAt: now,
	}
	doc, err := memoryToDoc(m, []float32{0.0})
	if err != nil {
		t.Fatalf("memoryToDoc failed: %v", err)
	}
	got, err := docToMemory(doc)
	if err != nil {
		t.Fatalf("docToMemory failed: %v", err)
	}

	nested, ok := got.Metadata["nested"].(map[string]interface{})
	if !ok {
		t.Fatalf("nested map type lost: got %T", got.Metadata["nested"])
	}
	if nested["key"] != "value" {
		t.Errorf("nested.key: want value, got %v", nested["key"])
	}
	if nested["num"] != 42.0 {
		t.Errorf("nested.num: want 42.0, got %v", nested["num"])
	}

	ts, ok := got.Metadata["timestamp"].(string)
	if !ok {
		t.Fatalf("timestamp type lost: got %T", got.Metadata["timestamp"])
	}
	parsed, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		t.Fatalf("timestamp parse failed: %v", err)
	}
	if !parsed.Equal(now) {
		t.Errorf("timestamp: want %v, got %v", now, parsed)
	}

	tags, ok := got.Metadata["tags"].([]interface{})
	if !ok {
		t.Fatalf("tags type lost: got %T", got.Metadata["tags"])
	}
	if len(tags) != 2 || tags[0] != "a" || tags[1] != "b" {
		t.Errorf("tags: want [a b], got %v", tags)
	}

	if got.Metadata["bool_field"] != true {
		t.Errorf("bool_field: want true, got %v", got.Metadata["bool_field"])
	}
	if got.Metadata["null_field"] != nil {
		t.Errorf("null_field: want nil, got %v", got.Metadata["null_field"])
	}
}

func TestRoundTripUnicodeContent(t *testing.T) {
	m := &Memory{
		ID:         "unicode",
		Content:    "Hello 世界 🌍 \"quoted\" \n newline",
		MemoryType: TypeInsight,
		State:      StateActive,
		CreatedAt:  time.Now().UTC().Truncate(time.Millisecond),
		UpdatedAt:  time.Now().UTC().Truncate(time.Millisecond),
	}
	doc, err := memoryToDoc(m, []float32{0.0})
	if err != nil {
		t.Fatalf("memoryToDoc failed: %v", err)
	}
	got, err := docToMemory(doc)
	if err != nil {
		t.Fatalf("docToMemory failed: %v", err)
	}
	if got.Content != m.Content {
		t.Errorf("Content round-trip failed: want %q, got %q", m.Content, got.Content)
	}
}
