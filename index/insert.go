package hnsw

// insert handles the insertion of a new node into the HNSW index.
func (h *HNSWIndex) insert(newNode *Node) {
	h.globalLock.RLock()
	ep := int(h.entryPoint)
	maxLvl := int(h.maxLevel)
	h.globalLock.RUnlock()

	newNodeLevel := newNode.Level()
	newNodeID := newNode.ID()

	// 阶段1：从顶层到 newNodeLevel+1，使用贪心搜索找到入口点
	currentNearest := ep
	for lc := maxLvl; lc > newNodeLevel; lc-- {
		nearest := h.searchLayer(newNode.Vector(), currentNearest, 1, lc)
		if len(nearest) == 0 {
			// 理论上不会发生，但添加保护
			break
		}
		currentNearest = nearest[0].ID
	}

	// 阶段2：从 newNodeLevel 到第 0 层，建立连接
	for lc := min(newNodeLevel, maxLvl); lc >= 0; lc-- {
		// 在当前层搜索最近邻
		candidates := h.searchLayer(newNode.Vector(), currentNearest, h.efConstruction, lc)

		// 选择 M 个邻居（启发式剪枝）
		m := h.Mmax
		if lc == 0 {
			m = h.Mmax0
		}

		neighbors := h.selectNeighborsHeuristic(newNode.Vector(), candidates, m)

		// 添加双向连接
		for _, neighbor := range neighbors {
			// 新节点 -> 邻居
			newNode.AddConnection(lc, neighbor.ID)

			// 邻居 -> 新节点
			neighborNode := h.nodes[neighbor.ID]
			neighborNode.AddConnection(lc, newNodeID)

			// 如果邻居的连接数超过限制，需要剪枝
			maxConn := h.Mmax
			if lc == 0 {
				maxConn = h.Mmax0
			}

			if neighborNode.ConnectionCount(lc) > maxConn {
				// 重新选择邻居
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

		// 更新下一层的入口点
		if len(neighbors) > 0 {
			currentNearest = neighbors[0].ID
		}
	}

	// 如果新节点的层级更高，更新全局入口点和最大层级
	if newNodeLevel > maxLvl {
		h.globalLock.Lock()
		h.entryPoint = int32(newNodeID)
		h.maxLevel = int32(newNodeLevel)
		h.globalLock.Unlock()
	}
}
