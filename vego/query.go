package vego

// SearchResult represents a search result
type SearchResult struct {
	Document *Document
	Distance float32
}

// SearchOptions contains search options
type SearchOptions struct {
	EF     int    // Search scope (0 = use default)
	Filter Filter // Optional metadata filter
}

// SearchOption is a functional option for search
type SearchOption func(*SearchOptions)

// WithEF sets the search scope (ef parameter)
func WithEF(ef int) SearchOption {
	return func(o *SearchOptions) {
		o.EF = ef
	}
}

// Filter is an interface for document filtering
type Filter interface {
	Match(doc *Document) bool
}

// MetadataFilter filters by metadata field
type MetadataFilter struct {
	Field    string
	Operator string // eq, ne, gt, lt, contains, etc.
	Value    interface{}
}

func (f *MetadataFilter) Match(doc *Document) bool {
	val, exists := doc.Metadata[f.Field]
	if !exists {
		return false
	}

	switch f.Operator {
	case "eq":
		return val == f.Value
	case "ne":
		return val != f.Value
	case "gt":
		return compareGreater(val, f.Value)
	case "gte":
		return compareGreater(val, f.Value) || val == f.Value
	case "lt":
		return compareLess(val, f.Value)
	case "lte":
		return compareLess(val, f.Value) || val == f.Value
	case "in":
		if slice, ok := f.Value.([]interface{}); ok {
			for _, v := range slice {
				if val == v {
					return true
				}
			}
		}
		return false
	case "contains":
		if str, ok := val.(string); ok {
			if substr, ok := f.Value.(string); ok {
				return contains(str, substr)
			}
		}
		return false
	default:
		return false
	}
}

// Helper functions for type-safe comparison
func compareGreater(a, b interface{}) bool {
	switch v := a.(type) {
	case int:
		if bv, ok := b.(int); ok {
			return v > bv
		}
	case int64:
		if bv, ok := b.(int64); ok {
			return v > bv
		}
	case float64:
		if bv, ok := b.(float64); ok {
			return v > bv
		}
	case string:
		if bv, ok := b.(string); ok {
			return v > bv
		}
	}
	return false
}

func compareLess(a, b interface{}) bool {
	switch v := a.(type) {
	case int:
		if bv, ok := b.(int); ok {
			return v < bv
		}
	case int64:
		if bv, ok := b.(int64); ok {
			return v < bv
		}
	case float64:
		if bv, ok := b.(float64); ok {
			return v < bv
		}
	case string:
		if bv, ok := b.(string); ok {
			return v < bv
		}
	}
	return false
}

func contains(s, substr string) bool {
	// Simple contains check
	return len(s) >= len(substr) && indexOf(s, substr) >= 0
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// AndFilter combines multiple filters with AND
type AndFilter struct {
	Filters []Filter
}

func (f *AndFilter) Match(doc *Document) bool {
	for _, filter := range f.Filters {
		if !filter.Match(doc) {
			return false
		}
	}
	return true
}

// OrFilter combines multiple filters with OR
type OrFilter struct {
	Filters []Filter
}

func (f *OrFilter) Match(doc *Document) bool {
	for _, filter := range f.Filters {
		if filter.Match(doc) {
			return true
		}
	}
	return false
}
