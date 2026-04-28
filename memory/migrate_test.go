package memory

import (
	"context"
	"errors"
	"testing"

	vego "github.com/wzqhbustb/vego/vego"
)

var errTestMigrationFailed = errors.New("migration failed")

// ----------------------------------------------------------------------
// migrateMemory unit tests
// ----------------------------------------------------------------------

func TestMigrateMemory_NoOp(t *testing.T) {
	doc := &vego.Document{
		ID: "test-1",
		Metadata: map[string]interface{}{
			metaKeyData:      `{"id":"test-1","content":"hello"}`,
			metaKeySchemaVer: float64(CurrentSchemaVersion),
		},
	}
	m, err := docToMemory(doc)
	if err != nil {
		t.Fatalf("docToMemory: %v", err)
	}
	migrated, err := migrateMemory(doc, m)
	if err != nil {
		t.Fatalf("migrateMemory: %v", err)
	}
	if migrated {
		t.Error("expected no migration for current version")
	}
}

func TestMigrateMemory_MissingVersion(t *testing.T) {
	// Register a migration that adds a tag.
	RegisterMigration(0, func(m *Memory) error {
		m.Tags = append(m.Tags, "migrated-v1")
		return nil
	})
	t.Cleanup(func() { delete(migrations, 0) })

	doc := &vego.Document{
		ID: "test-2",
		Metadata: map[string]interface{}{
			metaKeyData: `{"id":"test-2","content":"hello","state":"active"}`,
			// No _schema_ver — simulates pre-versioning data.
		},
	}
	m, err := docToMemory(doc)
	if err != nil {
		t.Fatalf("docToMemory: %v", err)
	}
	migrated, err := migrateMemory(doc, m)
	if err != nil {
		t.Fatalf("migrateMemory: %v", err)
	}
	if !migrated {
		t.Error("expected migration for missing version")
	}
	if len(m.Tags) != 1 || m.Tags[0] != "migrated-v1" {
		t.Errorf("expected [migrated-v1], got %v", m.Tags)
	}
	// Verify metadata was updated.
	if v, ok := doc.Metadata[metaKeySchemaVer]; !ok || v.(float64) != float64(CurrentSchemaVersion) {
		t.Errorf("metadata _schema_ver not updated: %v", doc.Metadata[metaKeySchemaVer])
	}
	// Verify _data was updated (tags are now in the re-serialized JSON).
	dataStr := doc.Metadata[metaKeyData].(string)
	if dataStr == `{"id":"test-2","content":"hello","state":"active"}` {
		t.Error("_data should have been re-serialized with migrated tags")
	}
}

func TestMigrateMemory_NoRegisteredMigration_BackwardCompat(t *testing.T) {
	// Ensure no migration registered for version 0.
	delete(migrations, 0)

	doc := &vego.Document{
		ID: "test-3",
		Metadata: map[string]interface{}{
			metaKeyData: `{"id":"test-3","content":"hello"}`,
			// No _schema_ver — pre-versioning data.
		},
	}
	m, err := docToMemory(doc)
	if err != nil {
		t.Fatalf("docToMemory: %v", err)
	}
	migrated, err := migrateMemory(doc, m)
	if err != nil {
		t.Fatalf("migrateMemory should succeed for v0 without registered migration (backward compat): %v", err)
	}
	if !migrated {
		t.Error("expected migration (version stamp) for missing version")
	}
	// Verify the doc was stamped with CurrentSchemaVersion.
	if v := getSchemaVersion(doc); v != CurrentSchemaVersion {
		t.Errorf("expected _schema_ver=%d after stamp, got %d", CurrentSchemaVersion, v)
	}
}

func TestMigrateMemory_MigrationError(t *testing.T) {
	RegisterMigration(0, func(m *Memory) error {
		return errTestMigrationFailed
	})
	t.Cleanup(func() { delete(migrations, 0) })

	doc := &vego.Document{
		ID: "test-4",
		Metadata: map[string]interface{}{
			metaKeyData: `{"id":"test-4","content":"hello"}`,
		},
	}
	m, err := docToMemory(doc)
	if err != nil {
		t.Fatalf("docToMemory: %v", err)
	}
	_, err = migrateMemory(doc, m)
	if err == nil {
		t.Fatal("expected migration error to propagate")
	}
}

// ----------------------------------------------------------------------
// getSchemaVersion unit tests
// ----------------------------------------------------------------------

func TestGetSchemaVersion(t *testing.T) {
	tests := []struct {
		name     string
		meta     map[string]interface{}
		expected int
	}{
		{"missing", map[string]interface{}{}, 0},
		{"float64", map[string]interface{}{metaKeySchemaVer: float64(3)}, 3},
		{"int", map[string]interface{}{metaKeySchemaVer: 2}, 2},
		{"string ignored", map[string]interface{}{metaKeySchemaVer: "1"}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := &vego.Document{ID: "test", Metadata: tt.meta}
			if got := getSchemaVersion(doc); got != tt.expected {
				t.Errorf("getSchemaVersion = %d, want %d", got, tt.expected)
			}
		})
	}
}

// ----------------------------------------------------------------------
// End-to-end: rebuild with migration
// ----------------------------------------------------------------------

func TestRebuildWithMigration(t *testing.T) {
	RegisterMigration(0, func(m *Memory) error {
		m.Tags = append(m.Tags, "migrated-v1")
		return nil
	})
	t.Cleanup(func() { delete(migrations, 0) })

	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()

	// Store a document normally (gets CurrentSchemaVersion).
	mem, err := s.Store(ctx, "normal doc", nil)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}

	// Manually strip _schema_ver from the Vego document to simulate
	// pre-versioning data that needs migration.
	doc, err := s.coll.GetContext(ctx, mem.ID)
	if err != nil {
		t.Fatalf("GetContext: %v", err)
	}
	delete(doc.Metadata, metaKeySchemaVer)
	// Update to persist the stripped metadata.
	if err := s.coll.UpdateContext(ctx, doc); err != nil {
		t.Fatalf("UpdateContext (strip version): %v", err)
	}

	// Clear in-memory indexes and rebuild — migration should run.
	s.inverted.Clear()
	s.contentHashIndex.Clear()
	if err := s.rebuildIndexes(); err != nil {
		t.Fatalf("rebuildIndexes: %v", err)
	}

	// Verify the migrated doc now has the tag and schema version in storage.
	doc2, err := s.coll.GetContext(ctx, mem.ID)
	if err != nil {
		t.Fatalf("GetContext after rebuild: %v", err)
	}
	m2, err := docToMemory(doc2)
	if err != nil {
		t.Fatalf("docToMemory after rebuild: %v", err)
	}
	if len(m2.Tags) != 1 || m2.Tags[0] != "migrated-v1" {
		t.Errorf("expected [migrated-v1], got %v", m2.Tags)
	}
	ver := getSchemaVersion(doc2)
	if ver != CurrentSchemaVersion {
		t.Errorf("expected _schema_ver=%d, got %d", CurrentSchemaVersion, ver)
	}
}
