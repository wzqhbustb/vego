package hnsw

import "sync"

// Node represents a single node in the HNSW graph.
type Node struct {
	id     int       // Unique identifier for the node.
	vector []float32 // The vector associated with the node.
	level  int       // The level of the node in the HNSW hierarchy.

	connections [][]int // Connections to other nodes at different levels.

	mu sync.RWMutex // Mutex for concurrent access to the node's connections.
}

func NewNode(id int, vector []float32, level int) *Node {
	connections := make([][]int, level+1)
	for i := range connections {
		connections[i] = make([]int, 0)
	}
	return &Node{
		id:          id,
		vector:      vector,
		level:       level,
		connections: connections,
	}
}

func (n *Node) ID() int {
	return n.id
}

func (n *Node) Vector() []float32 {
	result := make([]float32, len(n.vector))
	copy(result, n.vector)
	return result
}

func (n *Node) Level() int {
	return n.level
}

// GetConnections returns the connections of the node at the specified level.
func (n *Node) GetConnections(level int) []int {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if level < 0 || level >= len(n.connections) {
		return nil
	}

	result := make([]int, len(n.connections[level]))
	copy(result, n.connections[level])
	return result
}

// AddConnection adds a connection to another node at the specified level.
func (n *Node) AddConnection(level int, neighborID int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if level < 0 || level >= len(n.connections) {
		return
	}
	n.connections[level] = append(n.connections[level], neighborID)
}

// SetConnections sets the connections of the node at the specified level.
func (n *Node) SetConnections(level int, neighbors []int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if level < 0 || level >= len(n.connections) {
		return
	}

	n.connections[level] = make([]int, len(neighbors))
	copy(n.connections[level], neighbors)
}

// ConnectionCount returns the number of connections at the specified level.
func (n *Node) ConnectionCount(level int) int {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if level < 0 || level >= len(n.connections) {
		return 0
	}
	return len(n.connections[level])
}
