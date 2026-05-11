package catalog

import "sync"

// IDMapping maintains bidirectional mappings between document IDs and HNSW node IDs.
//
// INVARIANT: Delete only removes the doc→node mapping; the node→doc mapping is
// preserved for concurrent search safety (delayed cleanup). Call Replace() after
// compaction to rebuild a clean mapping.
type IDMapping struct {
	docToNode map[string]int
	nodeToDoc map[int]string
	mu        sync.RWMutex
}

// NewIDMapping creates a new empty IDMapping.
func NewIDMapping() *IDMapping {
	return &IDMapping{
		docToNode: make(map[string]int),
		nodeToDoc: make(map[int]string),
	}
}

// Map returns the node ID for a document ID.
func (m *IDMapping) Map(docID string) (nodeID int, ok bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	nodeID, ok = m.docToNode[docID]
	return
}

// Reverse returns the document ID for a node ID.
func (m *IDMapping) Reverse(nodeID int) (docID string, ok bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	docID, ok = m.nodeToDoc[nodeID]
	return
}

// Put stores or updates a bidirectional mapping.
func (m *IDMapping) Put(docID string, nodeID int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Remove old reverse mapping if document already exists
	if oldNode, ok := m.docToNode[docID]; ok {
		delete(m.nodeToDoc, oldNode)
	}
	m.docToNode[docID] = nodeID
	m.nodeToDoc[nodeID] = docID
}

// Delete removes the doc→node mapping. The node→doc mapping is intentionally
// preserved for concurrent search safety (delayed cleanup).
func (m *IDMapping) Delete(docID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.docToNode, docID)
}

// Count returns the number of doc→node mappings.
func (m *IDMapping) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.docToNode)
}

// All returns a shallow copy of all doc→node mappings.
func (m *IDMapping) All() map[string]int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cpy := make(map[string]int, len(m.docToNode))
	for k, v := range m.docToNode {
		cpy[k] = v
	}
	return cpy
}

// AllReverse returns a shallow copy of all node→doc mappings.
func (m *IDMapping) AllReverse() map[int]string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cpy := make(map[int]string, len(m.nodeToDoc))
	for k, v := range m.nodeToDoc {
		cpy[k] = v
	}
	return cpy
}

// Replace atomically replaces both mappings. Used after compaction.
func (m *IDMapping) Replace(docToNode map[string]int, nodeToDoc map[int]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.docToNode = docToNode
	m.nodeToDoc = nodeToDoc
}
