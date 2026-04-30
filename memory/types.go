package memory

import (
	"encoding/json"
	"fmt"
	"time"

	vego "github.com/wzqhbustb/vego/vego"
)

// MemoryType categorizes the nature of a memory.
type MemoryType string

const (
	TypePinned  MemoryType = "pinned"
	TypeInsight MemoryType = "insight"
	TypeSession MemoryType = "session"
)

// MemoryState represents the lifecycle state of a memory.
// Deleted state is implemented via metadata update (not Vego's DeleteContext)
// so that Get() can still retrieve the record.
type MemoryState string

const (
	StateActive   MemoryState = "active"
	StatePaused   MemoryState = "paused"
	StateArchived MemoryState = "archived"
	StateDeleted  MemoryState = "deleted"
)

// CurrentSchemaVersion is the current Memory schema version.
// It is written to every document at creation time and checked during
// rebuildIndexes so that stale documents can be migrated.
const CurrentSchemaVersion = 1

// Metadata keys used for Vego Document serialization.
const (
	metaKeyData      = "_data"      // Full Memory JSON string
	metaKeyState     = "_state"     // Redundant index field for SearchWithFilter
	metaKeyType      = "_type"      // Redundant index field for SearchWithFilter
	metaKeySchemaVer = "_schema_ver" // Schema version (int)
)

// Memory is the core domain type for the Agent memory service.
type Memory struct {
	ID           string                 `json:"id"`
	Content      string                 `json:"content"`
	MemoryType   MemoryType             `json:"memory_type"`
	State        MemoryState            `json:"state"`
	Tags         []string               `json:"tags"`
	Metadata     map[string]interface{} `json:"metadata"` // Includes temporal sub-fields
	Source       string                 `json:"source"`   // Creator Agent ID
	AgentID      string                 `json:"agent_id"`
	SessionID    string                 `json:"session_id"` // Associated session ID (ModeRaw source)
	Seq          int                    `json:"seq"`        // Message sequence number (ModeRaw ordering)
	ContentHash  string                 `json:"content_hash"` // SHA256(content), for ModeRaw deduplication
	Version      int                    `json:"version"`
	SupersededBy string                 `json:"superseded_by"` // ID of the memory that supersedes this one
	PreviousID   string                 `json:"previous_id"`   // ID of the memory this one replaces (Update)
	Score        float64                `json:"score"`       // Search score (0-1, populated at query time)
	RelativeAge  string                 `json:"relative_age"` // Human-readable age (populated at query time)
	CreatedAt    time.Time              `json:"created_at"`
	UpdatedAt    time.Time              `json:"updated_at"`
	// Vector is a transient field for Bootstrap/Import only.
	// It is NOT persisted in JSON (json:"-") to avoid duplicating the binary vector.
	Vector []float32 `json:"-"`
}

// MemoryFilter provides filtering criteria for memory search queries.
type MemoryFilter struct {
	Query      string
	Tags       []string
	State      string
	MemoryType string
	AgentID    string
	SessionID  string
	Limit      int
	Offset     int
	MinScore   float64
	// MinScoreSet distinguishes between MinScore=0 (accept all)
	// and the zero-value sentinel (not set, use config default).
	MinScoreSet bool
	// LimitSet distinguishes between Limit=0 (return zero results)
	// and the zero-value sentinel (not set, use config default).
	LimitSet bool
}

// ----------------------------------------------------------------------
// Ingestion types
// ----------------------------------------------------------------------

// IngestMode controls how raw input is processed.
type IngestMode int

const (
	ModeNormal IngestMode = iota // LLM-based fact extraction
	ModeRaw                      // Direct session storage, skip LLM
)

// Message is a raw input unit for ingestion.
type Message struct {
	Role      string
	Content   string
	Tags      []string
	AgentID   string
	SessionID string
	Timestamp time.Time
}

// ExtractedFact is a structured fact extracted from messages.
type ExtractedFact struct {
	Content     string                 `json:"content"`
	Tags        []string               `json:"tags"`
	QueryIntent bool                   `json:"query_intent"` // true means search intent, should be dropped
	SourceMsg   int                    `json:"source_msg"`   // index of source message in the input slice
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// IngestRequest is the unified entry point for message ingestion.
// It supports two modes:
//   - ModeNormal: LLM fact extraction → Reconcile (AgentID required)
//   - ModeRaw:    Direct session storage with dedup (SessionID required)
type IngestRequest struct {
	Messages  []Message
	Mode      IngestMode
	SessionID string // Required for ModeRaw
	AgentID   string // Required for ModeNormal
}

// IngestResult summarizes the outcome of an Ingest or Reconcile operation.
type IngestResult struct {
	Added   int
	Updated int
	Deleted int
	Skipped int
}

// memoryToDoc converts a Memory to a Vego Document for storage.
// vec is the embedding vector, generated by the caller.
func memoryToDoc(m *Memory, vec []float32) (*vego.Document, error) {
	if m == nil {
		return nil, fmt.Errorf("memory is nil")
	}
	if len(vec) == 0 {
		return nil, fmt.Errorf("vector is empty")
	}

	// Defensive copy: clear query-time fields before serialization
	// so that search scores / relative ages are never persisted.
	toStore := *m
	toStore.Score = 0
	toStore.RelativeAge = ""
	if toStore.UpdatedAt.IsZero() {
		toStore.UpdatedAt = time.Now()
	}

	data, err := json.Marshal(&toStore)
	if err != nil {
		return nil, fmt.Errorf("marshal memory: %w", err)
	}
	meta := map[string]interface{}{
		metaKeyData:      string(data),
		metaKeyState:     string(toStore.State),
		metaKeyType:      string(toStore.MemoryType),
		metaKeySchemaVer: CurrentSchemaVersion,
	}
	// Deep-copy vector so caller's slice is not aliased
	vecCopy := make([]float32, len(vec))
	copy(vecCopy, vec)

	ts := toStore.UpdatedAt
	if ts.IsZero() {
		ts = time.Now()
	}
	return &vego.Document{
		ID:        m.ID,
		Vector:    vecCopy,
		Metadata:  meta,
		Timestamp: ts,
	}, nil
}

// docToMemory deserializes a Vego Document into a Memory.
// The full Memory is restored from the _data JSON field.
func docToMemory(doc *vego.Document) (*Memory, error) {
	if doc == nil {
		return nil, fmt.Errorf("document is nil")
	}
	dataStr, ok := doc.Metadata[metaKeyData].(string)
	if !ok || dataStr == "" {
		return nil, fmt.Errorf("document %s: missing or empty %s field", doc.ID, metaKeyData)
	}
	var m Memory
	if err := json.Unmarshal([]byte(dataStr), &m); err != nil {
		return nil, fmt.Errorf("unmarshal memory %s: %w", doc.ID, err)
	}
	m.ID = doc.ID // Document ID is the source of truth
	return &m, nil
}

// MemoryStats holds aggregate statistics about the memory store.
type MemoryStats struct {
	Total    int            // Total documents in the collection
	Active   int            // StateActive count
	Paused   int            // StatePaused count
	Archived int            // StateArchived count
	Deleted  int            // StateDeleted count
	ByType   map[string]int // Distribution by MemoryType
	Vego     vego.CollectionStats // Underlying Vego collection stats
}

// copyMap returns a copy of a metadata map, or nil if src is nil.
// Known pointer types (e.g. *TemporalMetadata) are deep-copied so that
// the returned map does not share mutable references with the source.
func copyMap(src map[string]interface{}) map[string]interface{} {
	if src == nil {
		return nil
	}
	out := make(map[string]interface{}, len(src))
	for k, v := range src {
		switch val := v.(type) {
		case *TemporalMetadata:
			if val != nil {
				cp := *val
				out[k] = &cp
			} else {
				out[k] = nil
			}
		default:
			out[k] = v
		}
	}
	return out
}
