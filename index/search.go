package hnsw

import (
	"container/heap"
	"sort"
)

// PriorityQueue implements a min-heap
type PriorityQueue []*Item

type Item struct {
	value    int     // Node ID
	priority float32 // Distance (priority)
	index    int     // Index in heap
}

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].priority < pq[j].priority // Min-heap
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

// MaxHeap (used to maintain result set)
type MaxHeap []*Item

func (h MaxHeap) Len() int { return len(h) }

func (h MaxHeap) Less(i, j int) bool {
	return h[i].priority > h[j].priority // Max-heap
}

func (h MaxHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *MaxHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*Item)
	item.index = n
	*h = append(*h, item)
}

func (h *MaxHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[0 : n-1]
	return item
}

func (h *MaxHeap) Peek() interface{} {
	if len(*h) == 0 {
		return nil
	}
	return (*h)[0]
}

// search finds k nearest neighbors in the index
func (h *HNSWIndex) search(query []float32, k int, ef int, ep int, topLevel int) ([]SearchResult, error) {
	// Phase 1: From top layer to layer 1, use greedy search
	currentNearest := ep
	for lc := topLevel; lc > 0; lc-- {
		nearest := h.searchLayer(query, currentNearest, 1, lc)
		if len(nearest) > 0 {
			currentNearest = nearest[0].ID
		}
	}

	// Phase 2: Search at layer 0 using ef
	candidates := h.searchLayer(query, currentNearest, ef, 0)

	// Return top k results
	if len(candidates) > k {
		return candidates[:k], nil
	}

	return candidates, nil
}

func (h *HNSWIndex) searchLayerAggressive(query []float32, ep int, ef int, level int) []SearchResult {
	visited := make(map[int]bool)

	// Candidate set, min-heap, sorted by distance ascending
	candidates := &PriorityQueue{}
	heap.Init(candidates)

	// Result set, max-heap, sorted by distance descending
	results := &MaxHeap{}
	heap.Init(results)

	// Calculate entry point distance
	epDist := h.distFunc(query, h.nodes[ep].Vector())

	heap.Push(candidates, &Item{value: ep, priority: epDist})
	heap.Push(results, &Item{value: ep, priority: epDist})
	visited[ep] = true

	for candidates.Len() > 0 {
		// Get closest candidate
		current := heap.Pop(candidates).(*Item)

		// Optimization: only check when result set is full
		if results.Len() >= ef {
			furthest := results.Peek().(*Item)
			if current.priority > furthest.priority {
				break
			}
		}

		if current.value < 0 || current.value >= len(h.nodes) {
			continue // Skip invalid nodes
		}

		// Check all neighbors of current node
		neighbors := h.nodes[current.value].GetConnections(level)

		for _, neighborID := range neighbors {
			if visited[neighborID] {
				continue
			}

			if neighborID < 0 || neighborID >= len(h.nodes) {
				continue // Skip invalid neighbors
			}

			visited[neighborID] = true

			// Calculate distance
			dist := h.distFunc(query, h.nodes[neighborID].Vector())

			// If result set not full or current distance is closer, add to candidates
			if results.Len() < ef {
				heap.Push(candidates, &Item{value: neighborID, priority: dist})
				heap.Push(results, &Item{value: neighborID, priority: dist})
			} else {
				furthest := results.Peek().(*Item)
				if dist < furthest.priority {
					heap.Push(candidates, &Item{value: neighborID, priority: dist})
					heap.Push(results, &Item{value: neighborID, priority: dist})
					heap.Pop(results)
				}
			}
		}
	}

	// Convert to result array (sorted from nearest to farthest)
	resultArray := make([]SearchResult, results.Len())
	for i := results.Len() - 1; i >= 0; i-- {
		item := heap.Pop(results).(*Item)
		resultArray[i] = SearchResult{
			ID:       item.value,
			Distance: item.priority,
		}
	}

	return resultArray
}

// searchLayerConservative
func (h *HNSWIndex) searchLayer(query []float32, ep int, ef int, level int) []SearchResult {
	estimatedVisits := int(float64(ef) * 2.0 * float64(h.Mmax))
	visited := make(map[int]bool, estimatedVisits)

	candidates := &PriorityQueue{}
	results := &MaxHeap{}
	heap.Init(candidates)
	heap.Init(results)

	epDist := h.distFunc(query, h.nodes[ep].vector)
	heap.Push(candidates, &Item{value: ep, priority: epDist})
	heap.Push(results, &Item{value: ep, priority: epDist})
	visited[ep] = true

	for candidates.Len() > 0 {
		current := heap.Pop(candidates).(*Item)

		// 边界检查
		if current.value < 0 || current.value >= len(h.nodes) {
			continue
		}

		// 保守的提前终止
		if results.Len() >= ef {
			furthest := (*results)[0]
			if current.priority > furthest.priority {
				break
			}
		}

		// 遍历邻居
		for _, neighborID := range h.nodes[current.value].connections[level] {
			if visited[neighborID] {
				continue
			}

			if neighborID < 0 || neighborID >= len(h.nodes) {
				continue
			}

			visited[neighborID] = true
			dist := h.distFunc(query, h.nodes[neighborID].vector)

			// 更精确的浮点容差
			shouldAdd := false
			if results.Len() < ef {
				shouldAdd = true
			} else {
				furthest := (*results)[0]
				const (
					absoluteTolerance = 1e-5
					relativeTolerance = 0.01
				)
				tolerance := furthest.priority*relativeTolerance + absoluteTolerance
				if dist < furthest.priority+tolerance {
					shouldAdd = true
				}
			}

			if shouldAdd {
				// 移除 maxCandidates 限制
				heap.Push(candidates, &Item{value: neighborID, priority: dist})

				// results 保持原有逻辑
				heap.Push(results, &Item{value: neighborID, priority: dist})
				if results.Len() > ef {
					heap.Pop(results)
				}
			}
		}
	}

	// 转换为结果数组
	resultArray := make([]SearchResult, results.Len())
	for i := results.Len() - 1; i >= 0; i-- {
		item := heap.Pop(results).(*Item)
		resultArray[i] = SearchResult{ID: item.value, Distance: item.priority}
	}
	return resultArray
}

func (h *HNSWIndex) selectNeighborsHeuristic(query []float32, candidates []SearchResult, m int) []SearchResult {
	if len(candidates) <= m {
		return candidates
	}

	result := make([]SearchResult, 0, m)
	working := make([]SearchResult, len(candidates))
	copy(working, candidates)

	sort.Slice(working, func(i, j int) bool {
		return working[i].Distance < working[j].Distance
	})

	for _, candidate := range working {
		if len(result) >= m {
			break
		}

		good := true
		candidateVec := h.nodes[candidate.ID].Vector()

		// Explicitly document heuristic logic
		// Rejection condition: if candidate is closer to selected neighbor than to query
		// Purpose: ensure diversity and coverage of neighbors
		for _, selected := range result {
			selectedVec := h.nodes[selected.ID].Vector()
			distToSelected := h.distFunc(candidateVec, selectedVec)

			// candidate.Distance is the distance from candidate to query
			if distToSelected < candidate.Distance {
				good = false
				break
			}
		}

		if good {
			result = append(result, candidate)
		}
	}

	// Supplementary logic remains unchanged
	if len(result) < m {
		selected := make(map[int]bool, len(result))
		for _, r := range result {
			selected[r.ID] = true
		}

		for _, candidate := range working {
			if len(result) >= m {
				break
			}
			if !selected[candidate.ID] {
				result = append(result, candidate)
			}
		}
	}

	return result
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
