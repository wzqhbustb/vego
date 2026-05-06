package hnsw

// insert handles the insertion of a new node into the HNSW index.
func (h *HNSWIndex) insert(newNode *Node) {
	h.globalLock.RLock()
	ep := int(h.entryPoint)
	maxLvl := int(h.maxLevel)
	nodes := h.nodes // snapshot for search phase
	h.globalLock.RUnlock()

	newNodeLevel := newNode.Level()
	newNodeID := newNode.ID()

	// Phase 1: From top layer to newNodeLevel+1, use greedy search to find entry point
	currentNearest := ep
	for lc := maxLvl; lc > newNodeLevel; lc-- {
		nearest := h.searchLayer(newNode.Vector(), currentNearest, 1, lc, nodes)
		if len(nearest) == 0 {
			// Theoretically won't happen, but add protection
			break
		}
		currentNearest = nearest[0].ID
	}

	// Phase 2: From newNodeLevel to layer 0, establish connections
	for lc := min(newNodeLevel, maxLvl); lc >= 0; lc-- {
		// Re-snapshot nodes to see any nodes added by concurrent inserters.
		// This ensures neighbor IDs from searchLayer are valid for vector access.
		h.globalLock.RLock()
		nodes = h.nodes
		h.globalLock.RUnlock()

		// Search for nearest neighbors at current layer
		candidates := h.searchLayer(newNode.Vector(), currentNearest, h.efConstruction, lc, nodes)

		// Select M neighbors (heuristic pruning)
		m := h.Mmax
		if lc == 0 {
			m = h.Mmax0
		}

		neighbors := h.selectNeighborsHeuristic(newNode.Vector(), candidates, m, nodes)

		// Add bidirectional connections
		for _, neighbor := range neighbors {
			// New node -> neighbor
			newNode.AddConnection(lc, neighbor.ID)

			// Neighbor -> new node
			// Bounds check against snapshot (should always be valid)
			if neighbor.ID < 0 || neighbor.ID >= len(nodes) {
				continue
			}
			neighborNode := nodes[neighbor.ID]
			neighborNode.AddConnection(lc, newNodeID)

			// If neighbor's connection count exceeds limit, pruning is needed
			maxConn := h.Mmax
			if lc == 0 {
				maxConn = h.Mmax0
			}

			if neighborNode.ConnectionCount(lc) > maxConn {
				// Reselect neighbors
				neighborConnections := neighborNode.GetConnections(lc)
				candidatesForPrune := make([]SearchResult, 0, len(neighborConnections))

				// Read neighbor vector directly (immutable, no lock needed)
				neighborVec := neighborNode.vector

				// Re-snapshot for pruning to see latest nodes
				h.globalLock.RLock()
				pruneNodes := h.nodes
				h.globalLock.RUnlock()

				for _, connID := range neighborConnections {
					if connID < 0 || connID >= len(pruneNodes) {
						continue // Skip out-of-snapshot nodes
					}
					// Read vector directly (immutable, no lock needed)
					connVec := pruneNodes[connID].vector
					dist := h.distFunc(neighborVec, connVec)
					candidatesForPrune = append(candidatesForPrune, SearchResult{ID: connID, Distance: dist})
				}

				prunedNeighbors := h.selectNeighborsHeuristic(neighborVec, candidatesForPrune, maxConn, pruneNodes)
				prunedIDs := make([]int, len(prunedNeighbors))
				for i, n := range prunedNeighbors {
					prunedIDs[i] = n.ID
				}
				neighborNode.SetConnections(lc, prunedIDs)
			}
		}

		// Update entry point for next layer
		if len(neighbors) > 0 {
			currentNearest = neighbors[0].ID
		}
	}

	// If new node's level is higher, update global entry point and max level
	// Note: Must re-read h.maxLevel inside the lock to avoid TOCTOU race
	h.globalLock.Lock()
	if int32(newNodeLevel) > h.maxLevel {
		h.entryPoint = int32(newNodeID)
		h.maxLevel = int32(newNodeLevel)
	}
	h.globalLock.Unlock()
}
