package hnsw

import (
	"container/heap"
	"sort"
)

// 优先队列实现（最小堆）
type PriorityQueue []*Item

type Item struct {
	value    int     // 节点ID
	priority float32 // 距离（优先级）
	index    int     // 在堆中的索引
}

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].priority < pq[j].priority // 最小堆
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

// 最大堆（用于维护结果集）
type MaxHeap []*Item

func (h MaxHeap) Len() int { return len(h) }

func (h MaxHeap) Less(i, j int) bool {
	return h[i].priority > h[j].priority // 最大堆
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

// search 在索引中搜索 k 个最近邻
func (h *HNSWIndex) search(query []float32, k int, ef int, ep int, topLevel int) ([]SearchResult, error) {
	// 阶段1：从顶层到第1层，使用贪心搜索
	currentNearest := ep
	for lc := topLevel; lc > 0; lc-- {
		nearest := h.searchLayer(query, currentNearest, 1, lc)
		if len(nearest) > 0 {
			currentNearest = nearest[0].ID
		}
	}

	// 阶段2：在第0层使用 ef 进行搜索
	candidates := h.searchLayer(query, currentNearest, ef, 0)

	// 返回前 k 个结果
	if len(candidates) > k {
		return candidates[:k], nil
	}

	return candidates, nil
}

func (h *HNSWIndex) searchLayer(query []float32, ep int, ef int, level int) []SearchResult {
	visited := make(map[int]bool)

	// 候选集，最小堆，按距离从小到大
	candidates := &PriorityQueue{}
	heap.Init(candidates)

	// 结果集，最大堆，按距离从大到小
	results := &MaxHeap{}
	heap.Init(results)

	// 计算入口点距离
	epDist := h.distFunc(query, h.nodes[ep].Vector())

	heap.Push(candidates, &Item{value: ep, priority: epDist})
	heap.Push(results, &Item{value: ep, priority: epDist})
	visited[ep] = true

	for candidates.Len() > 0 {
		// 取距离最近的候选点
		current := heap.Pop(candidates).(*Item)

		// 优化：只在结果集满时检查
		if results.Len() >= ef {
			furthest := results.Peek().(*Item)
			if current.priority > furthest.priority {
				break
			}
		}

		if current.value < 0 || current.value >= len(h.nodes) {
			continue // 跳过无效节点
		}

		// 检查当前节点的所有邻居
		neighbors := h.nodes[current.value].GetConnections(level)

		for _, neighborID := range neighbors {
			if visited[neighborID] {
				continue
			}

			if neighborID < 0 || neighborID >= len(h.nodes) {
				continue // 跳过无效邻居
			}

			visited[neighborID] = true

			// 计算距离
			dist := h.distFunc(query, h.nodes[neighborID].Vector())

			// 如果结果集未满，或者当前距离更近，添加到候选集
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

	// 转换为结果数组（从近到远排序）
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

		// 明确注释启发式逻辑
		// 拒绝条件：如果候选点更接近已选邻居，而非 query
		// 目的：保证邻居的多样性和覆盖范围
		for _, selected := range result {
			selectedVec := h.nodes[selected.ID].Vector()
			distToSelected := h.distFunc(candidateVec, selectedVec)

			// candidate.Distance 是候选点到 query 的距离
			if distToSelected < candidate.Distance {
				good = false
				break
			}
		}

		if good {
			result = append(result, candidate)
		}
	}

	// 补充逻辑保持不变
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
