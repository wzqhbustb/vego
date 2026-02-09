package hnsw

// insert handles the insertion of a new node into the HNSW index.
func (h *HNSWIndex) insert(newNode *Node) {
	h.globalLock.RLock()
	ep := int(h.entryPoint)
	maxLvl := int(h.maxLevel)
	h.globalLock.RUnlock()

	newNodeLevel := newNode.Level()
	newNodeID := newNode.ID()

	// Phase 1: From top layer to newNodeLevel+1, use greedy search to find entry point
	currentNearest := ep
	for lc := maxLvl; lc > newNodeLevel; lc-- {
		nearest := h.searchLayer(newNode.Vector(), currentNearest, 1, lc)
		if len(nearest) == 0 {
			// Theoretically won't happen, but add protection
			break
		}
		currentNearest = nearest[0].ID
	}

	// Phase 2: From newNodeLevel to layer 0, establish connections
	for lc := min(newNodeLevel, maxLvl); lc >= 0; lc-- {
		// Search for nearest neighbors at current layer
		candidates := h.searchLayer(newNode.Vector(), currentNearest, h.efConstruction, lc)

		// Select M neighbors (heuristic pruning)
		m := h.Mmax
		if lc == 0 {
			m = h.Mmax0
		}

		neighbors := h.selectNeighborsHeuristic(newNode.Vector(), candidates, m)

		// Add bidirectional connections
		for _, neighbor := range neighbors {
			// New node -> neighbor
			newNode.AddConnection(lc, neighbor.ID)

			// Neighbor -> new node
			neighborNode := h.nodes[neighbor.ID]
			neighborNode.AddConnection(lc, newNodeID)

			// If neighbor's connection count exceeds limit, pruning is needed
			maxConn := h.Mmax
			if lc == 0 {
				maxConn = h.Mmax0
			}

			if neighborNode.ConnectionCount(lc) > maxConn {
				// Reselect neighbors
				neighborConnections := neighborNode.GetConnections(lc)
				candidatesForPrune := make([]SearchResult, len(neighborConnections))

				for i, connID := range neighborConnections {
					dist := h.distFunc(neighborNode.Vector(), h.nodes[connID].Vector())
					candidatesForPrune[i] = SearchResult{ID: connID, Distance: dist}
				}

				prunedNeighbors := h.selectNeighborsHeuristic(neighborNode.Vector(), candidatesForPrune, maxConn)
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
	if newNodeLevel > maxLvl {
		h.globalLock.Lock()
		h.entryPoint = int32(newNodeID)
		h.maxLevel = int32(newNodeLevel)
		h.globalLock.Unlock()
	}
}
