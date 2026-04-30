package memory

import (
	"encoding/json"
	"fmt"
	"log/slog"

	vego "github.com/wzqhbustb/vego/vego"
)

// Migration transforms a Memory from one schema version to the next.
// It receives the Memory to mutate in-place and returns an error if migration fails.
type Migration func(m *Memory) error

// migrations maps from-version to the migration that upgrades to from-version+1.
// Registered via RegisterMigration, called during rebuildIndexes.
var migrations = make(map[int]Migration)

// RegisterMigration registers a migration from a specific schema version.
// Not safe for concurrent use: call during init() or single-goroutine setup.
func RegisterMigration(fromVersion int, m Migration) {
	if m == nil {
		panic(fmt.Sprintf("RegisterMigration: migration function for version %d is nil", fromVersion))
	}
	migrations[fromVersion] = m
}

// getSchemaVersion reads the _schema_ver from document metadata.
// Returns 0 if the key is missing (pre-versioning data).
func getSchemaVersion(doc *vego.Document) int {
	v, ok := doc.Metadata[metaKeySchemaVer]
	if !ok {
		return 0
	}
	switch val := v.(type) {
	case float64:
		return int(val)
	case int:
		return val
	default:
		return 0
	}
}

// migrateMemory applies the migration chain to bring m up to CurrentSchemaVersion.
// doc.Metadata is updated in-place with the re-serialized Memory and new version.
// Returns true if the document was modified and needs to be persisted.
//
// Backward compatibility: if the document has no schema version (fromVer == 0)
// and no migration is registered for version 0, the document is assumed to be
// compatible with the current schema and is simply stamped with CurrentSchemaVersion.
func migrateMemory(doc *vego.Document, m *Memory) (bool, error) {
	fromVer := getSchemaVersion(doc)
	if fromVer >= CurrentSchemaVersion {
		return false, nil
	}

	for v := fromVer; v < CurrentSchemaVersion; v++ {
		fn, ok := migrations[v]
		if !ok {
			// Version 0 means "pre-versioning era". If no explicit migration is
			// registered, assume the data is already compatible and just stamp.
			if v == 0 {
				break
			}
			return false, fmt.Errorf("no migration registered for version %d -> %d", v, v+1)
		}
		if err := fn(m); err != nil {
			return false, fmt.Errorf("migration %d -> %d: %w", v, v+1, err)
		}
	}

	data, err := json.Marshal(m)
	if err != nil {
		return false, fmt.Errorf("marshal migrated memory %s: %w", doc.ID, err)
	}
	doc.Metadata[metaKeyData] = string(data)
	doc.Metadata[metaKeySchemaVer] = float64(CurrentSchemaVersion)
	doc.Metadata[metaKeyState] = string(m.State)
	doc.Metadata[metaKeyType] = string(m.MemoryType)

	slog.Info("migrated memory schema", "id", doc.ID, "from", fromVer, "to", CurrentSchemaVersion)
	return true, nil
}
