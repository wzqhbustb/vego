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
	// Add more operators as needed
	default:
		return false
	}
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
